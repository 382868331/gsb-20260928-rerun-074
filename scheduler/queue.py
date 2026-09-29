"""队列策略：老化提权 + 前瞻目标规划 + 饥饿容量预留。

公平策略（Aging + Headroom Reservation）：

1. experiment_id 是稳定身份，队列只调整服务顺序，从不重建实验。
2. 基准顺序为提交时间 FIFO；等待超过 starvation_threshold 后，每多等一个
   时间单位优先级分数减 1（老化提权）。
3. plan_targets 用前瞻模拟为每个排队任务算出"最早可启动的目标节点"。
   饥饿任务的目标容量被硬预留，其他任务不得占用；因此大任务不会被持续
   到来的小任务无限饿死，等待时间有上界。
4. 需求超过任何单机空容量的实验在提交时即被拒绝，绝不排产不存在的容量。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class QueueEntry:
    experiment_id: str
    memory_mb: int
    submit_time: int
    duration: int = 1

    def priority_score(self, now: int, starvation_threshold: int) -> int:
        wait = max(0, now - self.submit_time)
        age_credit = max(0, wait - starvation_threshold)
        return self.submit_time - age_credit

    def is_starved(self, now: int, starvation_threshold: int) -> bool:
        return now - self.submit_time >= starvation_threshold


@dataclass
class Target:
    experiment_id: str
    node_id: str
    start_time: int
    starved: bool


class FairQueuePolicy:
    def __init__(self, starvation_threshold: int = 20) -> None:
        if starvation_threshold <= 0:
            raise ValueError("饥饿阈值必须为正数")
        self.starvation_threshold = starvation_threshold
        self.pending: list[QueueEntry] = []

    def add(self, entry: QueueEntry) -> None:
        if any(e.experiment_id == entry.experiment_id for e in self.pending):
            raise ValueError(f"实验 {entry.experiment_id} 已在队列中")
        self.pending.append(entry)

    def remove(self, experiment_id: str) -> Optional[QueueEntry]:
        for i, entry in enumerate(self.pending):
            if entry.experiment_id == experiment_id:
                return self.pending.pop(i)
        return None

    def __len__(self) -> int:
        return len(self.pending)

    def ordered(self, now: int) -> list[QueueEntry]:
        return sorted(
            self.pending,
            key=lambda e: (
                e.priority_score(now, self.starvation_threshold),
                e.submit_time,
                e.experiment_id,
            ),
        )

    def plan_targets(self, now: int, cluster, release_events: dict[str, list[tuple[int, int]]] | None = None) -> dict[str, Target]:
        """release_events[node_id] = [(结束时刻, 内存), ...]，来自模拟器在跑任务。"""
        release_events = release_events or {}
        """前瞻模拟：按服务顺序为每个排队任务确定最早启动节点。

        对每个节点维护假设的在跑/已规划任务结束事件；依次把排队任务放入
        最早可行的节点，容量不足时快进到任务释放时刻。饥饿任务目标确定后
        即占用规划容量，后续任务不能抢走。
        """
        states: dict[str, dict] = {}
        for node_id, runtime in cluster.runtime.items():
            if not runtime.available:
                continue
            # 每个任务仅需知道结束时刻；当前在跑任务结束时刻模拟器会推送，
            # 这里用保守假设：它们在未来各自释放，无法知道具体时刻时按
            # "立刻可被任何释放顺序满足"处理，由实际调度保证不超卖。
            states[node_id] = {
                "capacity": runtime.node.memory_mb,
                "slots": runtime.node.max_jobs,
                "used_mem": 0,
                "used_slots": 0,
                "events": [(t, m, 1) for t, m in release_events.get(node_id, [])],
            }

        targets: dict[str, Target] = {}
        for entry in self.ordered(now):
            choice: Optional[tuple[str, int]] = None
            for node_id, state in states.items():
                start = self._earliest_start(entry.memory_mb, now, state, commit=False)
                if start is not None and (choice is None or (start, node_id) < (choice[1], choice[0])):
                    choice = (node_id, start)
            if choice is None:
                continue
            node_id, start = choice
            starved = entry.is_starved(now, self.starvation_threshold)
            targets[entry.experiment_id] = Target(entry.experiment_id, node_id, start, starved)
            # 饥饿任务立即硬预留；非饥饿任务同样登记规划，避免互相抢占错配
            self._earliest_start(entry.memory_mb, now, states[node_id], commit=True,
                                 duration=entry.duration, forced_time=start)
        return targets

    @staticmethod
    def _earliest_start(memory_mb, now, state, commit, duration=None, forced_time=None):
        events = sorted(state["events"])
        total_mem = state["used_mem"] + sum(m for _, m, _ in events)
        total_slots = state["used_slots"] + len(events)
        free_mem = state["capacity"] - total_mem
        free_slots = state["slots"] - total_slots
        t = now
        idx = 0
        while free_mem < memory_mb or free_slots < 1:
            future = [e for e in events if e[0] > t]
            if not future:
                return None
            t = future[0][0]
            while idx < len(events) and events[idx][0] <= t:
                _, mem, slot = events[idx]
                free_mem += mem
                free_slots += slot
                idx += 1
        start = forced_time if forced_time is not None else t
        if commit:
            state["events"].append((start + duration, memory_mb, 1))
        return start
