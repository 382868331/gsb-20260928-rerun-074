"""队列策略：老化提权、原子多资源前瞻与阻塞原因分类。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


RESOURCE_SHORTAGE = "resource_shortage"
MAINTENANCE = "maintenance"
BUDGET_LIMIT = "budget_limit"


@dataclass
class QueueEntry:
    experiment_id: str
    memory_mb: int
    submit_time: int
    duration: int = 1
    priority: int = 0
    stage_index: int = 0
    resources: Optional[dict[str, int]] = None
    pinned_node_id: Optional[str] = None

    def priority_score(self, now: int, starvation_threshold: int) -> int:
        wait = max(0, now - self.submit_time)
        age_credit = max(0, wait - starvation_threshold)
        return self.submit_time - age_credit

    def is_starved(self, now: int, starvation_threshold: int) -> bool:
        return now - self.submit_time >= starvation_threshold

    @property
    def demand(self) -> dict[str, int]:
        return {"memory_mb": self.memory_mb, "slots": 1, **(self.resources or {})}


@dataclass
class Target:
    experiment_id: str
    node_id: str
    start_time: int
    starved: bool


@dataclass
class Blockage:
    experiment_id: str
    reason: str
    detail: str = ""


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
        release_events: Optional[dict[str, list[tuple]]] = None,
        outages: Optional[dict[str, list[tuple[int, int]]]] = None,
        schedule_budget: Optional[int] = None,
        reservation_events: Optional[dict[str, list[tuple]]] = None,
    ) -> tuple[dict[str, Target], list[Blockage]]:
        release_events = release_events or {}
        reservation_events = reservation_events or {}
        outages = outages or {}
        states: dict[str, dict] = {}
        for node_id, runtime in cluster.runtime.items():
            merged_events: dict[str, tuple[int, dict[str, int]]] = {}
            for item in release_events.get(node_id, []):
                t, demand, eid = item
                if eid not in merged_events or t < merged_events[eid][0]:
                    merged_events[eid] = (t, dict(demand))
            events = [(t, demand, eid) for eid, (t, demand) in merged_events.items()]
            states[node_id] = {
                "capacity": dict(runtime.node.resource_capacity),
                "events": events,
                "outages": sorted(outages.get(node_id, [])),
                "available": runtime.available,
            }

        targets: dict[str, Target] = {}
        blockages: list[Blockage] = []
        budget = schedule_budget
        for entry in self.ordered(now):
            if budget is not None and budget <= 0:
                blockages.append(Blockage(entry.experiment_id, BUDGET_LIMIT, "schedule budget exhausted"))
                continue

            node_ids = [entry.pinned_node_id] if entry.pinned_node_id else list(states)
            choices: list[tuple[int, str]] = []
            for node_id in node_ids:
                extra = [
                    (t, delta, None)
                    for t, delta, owner in reservation_events.get(node_id, [])
                    if owner != entry.experiment_id
                ]
                start = self._earliest_start(
                    entry.demand, now, states[node_id], duration=entry.duration, extra_events=extra
                )
                if start is not None:
                    choices.append((start, node_id))

            if not choices:
                if self._cluster_has_capacity(cluster, entry.demand):
                    reason = MAINTENANCE
                    detail = "only eligible node is under maintenance"
                else:
                    reason = RESOURCE_SHORTAGE
                    detail = "no node has all requested resources"
                blockages.append(Blockage(entry.experiment_id, reason, detail))
                continue

            start, node_id = min(choices)
            targets[entry.experiment_id] = Target(
                entry.experiment_id,
                node_id,
                start,
                entry.is_starved(now, self.starvation_threshold),
            )
            if start > now:
                chosen_state = states[node_id]
                maintenance_delayed = (
                    FairQueuePolicy._online_at(chosen_state, now) != now
                    and all(
                        chosen_state["capacity"].get(name, 0) >= amount
                        for name, amount in entry.demand.items()
                    )
                )
                blockages.append(Blockage(
                    entry.experiment_id,
                    MAINTENANCE if maintenance_delayed else RESOURCE_SHORTAGE,
                    f"earliest_start={start} node={node_id}",
                ))
            extra = [
                (t, delta, None)
                for t, delta, owner in reservation_events.get(node_id, [])
                if owner != entry.experiment_id
            ]
            self._earliest_start(
                entry.demand,
                now,
                states[node_id],
                commit=True,
                duration=entry.duration,
                forced_time=start,
                extra_events=extra,
            )
            if budget is not None:
                budget -= 1
        return targets, blockages

    @staticmethod
    def _cluster_has_capacity(cluster, demand: dict[str, int]) -> bool:
        return any(
            all(node.resource_capacity.get(name, 0) >= amount for name, amount in demand.items())
            for node in cluster.nodes.values()
        )

    @staticmethod
    def _earliest_start(demand, now, state, commit=False, duration=None, forced_time=None, extra_events=None):
        events = sorted(list(state["events"]) + list(extra_events or []), key=lambda item: item[0])
        free = {
            name: state["capacity"][name] - sum(item.get(name, 0) for _, item, _ in events)
            for name in state["capacity"]
        }
        t = now
        idx = 0
        while True:
            while idx < len(events) and events[idx][0] <= t:
                _, item, _ = events[idx]
                for name, amount in item.items():
                    free[name] = free.get(name, 0) + amount
                idx += 1
            online_at = FairQueuePolicy._online_at(state, t)
            resource_ok = all(free.get(name, 0) >= amount for name, amount in demand.items())
            if online_at is not None and resource_ok:
                start = forced_time if forced_time is not None else online_at
                violation = (
                    FairQueuePolicy._first_interval_violation(events, idx, free, demand, start, start + duration)
                    if duration is not None
                    else None
                )
                if violation is None:
                    if commit:
                        # 区间占用：开始负增量、结束正增量。只占 [start, start+duration)，
                        # 不把"未来启动"保守地算成从现在就占用，否则会挡住别人
                        # 已确认的更早预约窗口。
                        state["events"].append((start, {name: -amount for name, amount in demand.items()}, None))
                        state["events"].append((start + duration, dict(demand), None))
                    return start
                t = violation
                continue
            future_release = events[idx][0] if idx < len(events) else None
            future_online = FairQueuePolicy._future_online_after(state, t)
            if future_release is None and future_online is None:
                return None
            t = min(value for value in (future_release, future_online) if value is not None)

    @staticmethod
    def _first_interval_violation(events, idx, free, demand, start, end):
        """[start, end) 内首个令 demand 不再满足的事件时刻；全程可行返回 None。

        预约窗口开始是负增量事件（承诺增加、可用减少），只在起点校验会
        让后到任务静默穿过他人窗口，因此必须做区间级前瞻。
        """
        projected = dict(free)
        j = idx
        while j < len(events) and events[j][0] < end:
            t, item, _ = events[j]
            if t <= start:
                j += 1
                continue
            for name, amount in item.items():
                projected[name] = projected.get(name, 0) + amount
            if any(projected.get(name, 0) < amount for name, amount in demand.items()):
                return t
            j += 1
        return None

    @staticmethod
    def _online_at(state, time: int) -> Optional[int]:
        if state["available"] and not any(start <= time < end for start, end in state["outages"]):
            return time
        endings = [end for start, end in state["outages"] if start <= time < end]
        return min(endings) if endings else None

    @staticmethod
    def _future_online_after(state, time: int) -> Optional[int]:
        endings = [end for start, end in state["outages"] if start > time]
        return min(endings) if endings else None
