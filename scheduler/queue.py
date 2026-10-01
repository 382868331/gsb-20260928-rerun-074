"""队列策略：老化提权 + 前瞻目标规划 + 饥饿容量预留。

公平策略（Aging + Headroom Reservation）：

1. experiment_id 是稳定身份，队列只调整服务顺序，从不重建实验。
2. 基准顺序为提交时间 FIFO；等待超过 starvation_threshold 后，每多等一个
   时间单位优先级分数减 1（老化提权）。
3. plan_targets 用前瞻模拟为每个排队任务算出"最早可启动的目标节点"，
   多类资源全有或全无：所有资源与并发槽同时满足才算可启动。
   饥饿任务的目标容量被硬预留，其他任务不得占用；因此大任务不会被持续
   到来的小任务无限饿死，等待时间有上界。
4. 节点维护窗口在前瞻中生效：窗口内不可启动；声明 migratable=False 的
   阶段还必须整体落在下一窗口开始之前，否则推迟到维护结束之后。
5. 需求超过任何单机空容量的实验在提交时即被拒绝，绝不排产不存在的容量。
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
    priority: int = 0
    stage_index: int = 0
    resources: Optional[dict] = None
    migratable: bool = True

    def requirement(self) -> dict:
        """本阶段完整资源需求（全有或全无）；缺省退化为单内存。"""
        if self.resources:
            req = {"memory_mb": self.memory_mb}
            req.update(self.resources)
            return req
        return {"memory_mb": self.memory_mb}

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
                -e.priority,
                e.priority_score(now, self.starvation_threshold),
                e.submit_time,
                e.experiment_id,
            ),
        )

    def plan_targets(
        self,
        now: int,
        cluster,
        release_events: dict[str, list[tuple]] | None = None,
        maintenance: dict[str, list[tuple]] | None = None,
    ) -> dict[str, Target]:
        """release_events[node_id] = [(释放时刻, 资源字典[, 实验ID]), ...]。
        maintenance[node_id] = [(start, end), ...]：维护区间内不可放置。

        同时包含正常阶段结束与在声明安全点让出的释放：若某个在跑任务同时
        给出普通结束与更早的让出释放，前瞻取更早者，保证高优让出容量能被
        等待任务看见；同一实验重复出现时合并，不重复计算释放资源。
        """
        raw = release_events or {}
        release_events = {}
        for node_id, evs in raw.items():
            by_eid: dict[str, tuple[int, dict]] = {}
            inline: list[tuple[int, dict]] = []
            for ev in evs:
                if len(ev) == 3:
                    t, req, eid = ev
                    if eid in by_eid:
                        old_t, old_req = by_eid[eid]
                        merged = {
                            k: max(old_req.get(k, 0), req.get(k, 0))
                            for k in set(old_req) | set(req)
                        }
                        by_eid[eid] = (min(old_t, t), merged)
                    else:
                        by_eid[eid] = (t, dict(req))
                else:
                    t, req = ev
                    inline.append((t, dict(req)))
            release_events[node_id] = inline + [(t, req) for t, req in by_eid.values()]
        maintenance = maintenance or {}
        states: dict[str, dict] = {}
        for node_id, runtime in cluster.runtime.items():
            if not runtime.available:
                continue
            current_used: dict[str, int] = {}
            for _, req in release_events.get(node_id, []):
                for res, amount in req.items():
                    current_used[res] = current_used.get(res, 0) + amount
            states[node_id] = {
                "capacity": runtime.node.capacity_map(),
                "slots": runtime.node.max_jobs,
                # 当前占用 = 所有未来释放事件的资源量；事件用带符号增量表示：
                # 释放为负增量，规划承诺为 [start, start+duration) 的正/负对。
                "used": current_used,
                "used_slots": len(release_events.get(node_id, [])),
                "events": [
                    (t, {res: -amount for res, amount in req.items()}, -1)
                    for t, req in release_events.get(node_id, [])
                ],
                "windows": sorted(maintenance.get(node_id, [])),
                "in_maintenance": runtime.in_maintenance,
            }

        targets: dict[str, Target] = {}
        for entry in self.ordered(now):
            req = entry.requirement()
            choice: Optional[tuple[str, int]] = None
            for node_id, state in states.items():
                start = self._earliest_start(req, entry.duration, entry.migratable,
                                             now, state, commit=False)
                if start is not None and (choice is None or (start, node_id) < (choice[1], choice[0])):
                    choice = (node_id, start)
            if choice is None:
                continue
            node_id, start = choice
            starved = entry.is_starved(now, self.starvation_threshold)
            targets[entry.experiment_id] = Target(entry.experiment_id, node_id, start, starved)
            # 饥饿任务立即硬预留；非饥饿任务同样登记规划，避免互相抢占错配
            self._earliest_start(req, entry.duration, entry.migratable,
                                 now, states[node_id], commit=True, forced_time=start)
        return targets

    @staticmethod
    def _earliest_start(req, duration, migratable, now, state, commit, forced_time=None):
        """多资源全有或全无前瞻：所有资源与并发槽同时满足才可启动。

        事件为带符号增量（时刻, 资源增量, 并发槽增量）：当前占用在未来
        释放时刻给出负增量；规划承诺只在 [start, start+duration) 占用。
        维护窗口处理：窗口内不可启动；非迁移阶段不能跨维护运行，若会撞上
        下一窗口开始时刻，则整体推迟到该窗口结束之后。
        """
        deltas = sorted(state["events"], key=lambda e: e[0])
        capacity = state["capacity"]
        windows = state.get("windows", [])

        def window_end_covering(t):
            for start, end in windows:
                if start <= t < end:
                    return end
            return None

        def next_window_start(t):
            for start, _ in windows:
                if start > t:
                    return start
            return None

        used = dict(state["used"])
        slots_used = state["used_slots"]

        def fits():
            if state["slots"] - slots_used < 1:
                return False
            return all(
                capacity.get(res, 0) - used.get(res, 0) >= amount
                for res, amount in req.items()
            )

        t = now
        if state.get("in_maintenance"):
            end = window_end_covering(t)
            if end is None and windows:
                end = windows[0][1]
            if end is not None:
                t = max(t, end)
        idx = 0

        def apply_up_to(time):
            nonlocal idx, slots_used
            while idx < len(deltas) and deltas[idx][0] <= time:
                _, dreq, dslots = deltas[idx]
                for res, amount in dreq.items():
                    used[res] = used.get(res, 0) + amount
                slots_used += dslots
                idx += 1

        apply_up_to(t)
        while True:
            while not fits():
                future = [d for d in deltas if d[0] > t]
                if not future:
                    return None
                t = future[0][0]
                apply_up_to(t)
            end = window_end_covering(t)
            if end is not None:
                t = end
                apply_up_to(t)
                continue
            if not migratable:
                nws = next_window_start(t)
                if nws is not None and t + duration > nws:
                    t = window_end_covering(nws) or nws
                    apply_up_to(t)
                    continue
            break
        start = forced_time if forced_time is not None else t
        if commit:
            state["events"].append((start, dict(req), 1))
            state["events"].append((start + duration,
                                    {res: -amount for res, amount in req.items()}, -1))
        return start
