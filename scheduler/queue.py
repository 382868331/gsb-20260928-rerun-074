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

HORIZON = 10 ** 9


@dataclass
class QueueEntry:
    experiment_id: str
    memory_mb: int
    submit_time: int
    duration: int = 1
    priority: int = 0
    stage: int = 0

    def priority_score(self, now: int, starvation_threshold: int) -> tuple[int, int]:
        wait = max(0, now - self.submit_time)
        age_credit = max(0, wait - starvation_threshold)
        return -self.priority, self.submit_time - age_credit

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
            key=lambda e: (*e.priority_score(now, self.starvation_threshold),
                           e.submit_time, e.experiment_id),
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
            # 当前真实占用计入账目；已登记结束事件的在跑任务用释放事件
            # 表示其占用会在对应时刻归还。未提供结束事件的占用保守地
            # 视为永不释放，绝不超卖规划。
            known = [m for t, m in release_events.get(node_id, []) if t > now]
            states[node_id] = {
                "capacity": runtime.node.memory_mb,
                "slots": runtime.node.max_jobs,
                "used_mem": runtime.used_memory - sum(known),
                "used_slots": len(runtime.running) - len(known),
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
            # 饥饿任务从 now 起硬预留目标容量（释放时刻设为无穷远），
            # 持续到来的小任务无法占用；非饥饿任务登记真实结束时刻。
            self._earliest_start(entry.memory_mb, now, states[node_id], commit=True,
                                 duration=HORIZON if starved else entry.duration,
                                 forced_time=now if starved else start)
        return targets

    @staticmethod
    def _earliest_start(memory_mb, now, state, commit, duration=None, forced_time=None):
        events = sorted(state["events"])
        t = now
        idx = 0
        def occupied(at):
            outstanding = [(m, s) for et, m, s in events if et > at]
            mem = state["used_mem"] + sum(m for m, _ in outstanding)
            slots = state["used_slots"] + len(outstanding)
            return mem, slots
        used_mem, used_slots = occupied(t)
        while used_mem + memory_mb > state["capacity"] or used_slots + 1 > state["slots"]:
            upcoming = events[idx] if idx < len(events) else None
            if upcoming is None:
                return None
            t = upcoming[0]
            idx += 1
            used_mem, used_slots = occupied(t)
        start = forced_time if forced_time is not None else t
        if commit:
            state["events"].append((start + duration, memory_mb, 1))
            state["events"].sort()
        return start
