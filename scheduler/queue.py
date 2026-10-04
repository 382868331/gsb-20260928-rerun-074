"""队列策略：老化提权、原子多资源前瞻与阻塞原因分类。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


RESOURCE_SHORTAGE = "resource_shortage"
MAINTENANCE = "maintenance"
BUDGET_LIMIT = "budget_limit"
RESERVATION_CONFLICT = "reservation_conflict"
DEADLINE_MISSED = "deadline_missed"


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
    reservation_id: Optional[str] = None
    reserved_start: Optional[int] = None
    deadline: Optional[int] = None

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
        self._current_reservation_id: Optional[str] = None

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
        reservations: Optional[list] = None,
    ) -> tuple[dict[str, Target], list[Blockage]]:
        release_events = release_events or {}
        outages = outages or {}
        states: dict[str, dict] = {}
        reservations = reservations or []
        for node_id, runtime in cluster.runtime.items():
            events: list[tuple[int, dict[str, int], object]] = []
            merged_events: dict[object, tuple[int, dict[str, int]]] = {}
            for item in release_events.get(node_id, []):
                t, demand, eid = item
                if eid not in merged_events or t < merged_events[eid][0]:
                    merged_events[eid] = (t, dict(demand))
            events.extend((t, demand, eid) for eid, (t, demand) in merged_events.items())
            for reservation in reservations:
                if reservation.node_id != node_id:
                    continue
                demand = dict(reservation.demand)
                events.append((reservation.start, {k: -v for k, v in demand.items()}, reservation.reservation_id))
                events.append((reservation.end, demand, reservation.reservation_id))
            states[node_id] = {
                "capacity": dict(runtime.node.resource_capacity),
                "events": sorted(events, key=lambda item: item[0]),
                "outages": sorted(outages.get(node_id, [])),
                "available": runtime.available,
                "initial_used": dict(runtime.used),
            }

        targets: dict[str, Target] = {}
        blockages: list[Blockage] = []
        budget = schedule_budget
        for entry in self.ordered(now):
            self._current_reservation_id = entry.reservation_id
            if budget is not None and budget <= 0:
                blockages.append(Blockage(entry.experiment_id, BUDGET_LIMIT, "schedule budget exhausted"))
                continue

            node_ids = [entry.pinned_node_id] if entry.pinned_node_id else list(states)
            choices: list[tuple[int, str]] = []
            exact_time = entry.reserved_start
            for node_id in node_ids:
                start = self._earliest_start(
                    entry.demand,
                    now,
                    states[node_id],
                    duration=entry.duration,
                    exact_time=exact_time,
                    deadline=entry.deadline,
                    current_reservation_id=entry.reservation_id,
                )
                if start is not None:
                    choices.append((start, node_id))

            if not choices:
                if entry.reservation_id is not None:
                    reason = RESERVATION_CONFLICT
                    detail = f"reservation={entry.reservation_id} cannot be honored"
                elif self._cluster_has_capacity(cluster, entry.demand):
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
            if entry.deadline is not None and start + entry.duration > entry.deadline:
                blockages.append(Blockage(
                    entry.experiment_id,
                    DEADLINE_MISSED,
                    f"finish={start + entry.duration} deadline={entry.deadline} node={node_id}",
                ))
            self._earliest_start(
                entry.demand,
                now,
                states[node_id],
                commit=True,
                duration=entry.duration,
                forced_time=start,
                current_reservation_id=entry.reservation_id,
            )
            if budget is not None:
                budget -= 1
        self._current_reservation_id = None
        return targets, blockages

    @staticmethod
    def _cluster_has_capacity(cluster, demand: dict[str, int]) -> bool:
        return any(
            all(node.resource_capacity.get(name, 0) >= amount for name, amount in demand.items())
            for node in cluster.nodes.values()
        )

    @staticmethod
    def _earliest_start(demand, now, state, commit=False, duration=None, forced_time=None, exact_time=None, deadline=None, current_reservation_id=None):
        duration = duration or 1
        events = sorted(
            (
                item for item in state["events"]
                if item[2] != current_reservation_id or current_reservation_id is None
            ),
            key=lambda item: item[0],
        )
        candidates = {now}
        if exact_time is not None:
            candidates.add(exact_time)
        candidates.update(t for t, _, _ in events if t >= now)
        candidates.update(start for start, _ in state["outages"] if start >= now)
        candidates.update(end for _, end in state["outages"] if end >= now)
        for t in sorted(candidates):
            if deadline is not None and t + duration > deadline:
                continue
            if exact_time is not None and t != exact_time:
                continue
            filtered_state = dict(state)
            filtered_state["events"] = events
            if FairQueuePolicy._interval_fits(demand, t, t + duration, filtered_state):
                start = forced_time if forced_time is not None else t
                if commit:
                    state["events"] = [
                        item for item in state["events"]
                        if item[2] != current_reservation_id or current_reservation_id is None
                    ]
                    used_demand = {k: -v for k, v in demand.items()}
                    state["events"].append((start, used_demand, None))
                    state["events"].append((start + duration, dict(demand), None))
                    state["events"].sort(key=lambda item: item[0])
                return start
        return None

    @staticmethod
    def _interval_fits(demand, start, end, state) -> bool:
        events = sorted(state["events"], key=lambda item: item[0])
        free = {
            name: state["capacity"][name] - state["initial_used"].get(name, 0)
            for name in state["capacity"]
        }
        if any(start < outage_end and outage_start < end for outage_start, outage_end in state["outages"]):
            return False
        changes = sorted({start, end, *(t for t, _, _ in events if start < t < end)})
        for time in changes:
            if time >= end:
                continue
            current = dict(free)
            for event_time, item, _ in events:
                if event_time <= time:
                    for name, amount in item.items():
                        current[name] = current.get(name, 0) + amount
            if any(current.get(name, 0) < amount for name, amount in demand.items()):
                return False
        return True

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
