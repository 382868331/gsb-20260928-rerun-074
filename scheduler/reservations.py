"""资源预约：承诺（预约窗口）与实际占用分离，可审计、可撤回、可校验。

预约不等于占用：ReservedWindow 只在前瞻规划中阻塞其他实验，真正 start 时
仍走 NodeRuntime.allocate；窗口被消费（consumed）后即停止阻塞，绝不与
实际占用重复扣账。承诺（Reservation.events）、实际执行（RunRecord.events）
与违约原因（Reservation.breach_reasons）三条账目分别保存。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from .models import RunEvent, Stage


class ReservationStatus(str, Enum):
    CONFIRMED = "confirmed"
    FULFILLED = "fulfilled"
    WITHDRAWN = "withdrawn"
    BREACHED = "breached"


@dataclass
class ReservedWindow:
    """一个阶段的预约窗口：[start, end) 内在 node_id 上为其保留 demand。"""

    window_id: str
    node_id: str
    stage_index: int
    start: int
    end: int
    demand: dict[str, int]
    consumed: bool = False
    released: bool = False

    @property
    def active(self) -> bool:
        return not self.consumed and not self.released

    def covers(self, time: int) -> bool:
        return self.start <= time < self.end

    def overlaps(self, start: int, end: int) -> bool:
        return self.start < end and start < self.end

    def to_dict(self) -> dict:
        return {
            "window_id": self.window_id,
            "node_id": self.node_id,
            "stage_index": self.stage_index,
            "start": self.start,
            "end": self.end,
            "demand": dict(self.demand),
            "consumed": self.consumed,
            "released": self.released,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ReservedWindow":
        return cls(
            window_id=data["window_id"],
            node_id=data["node_id"],
            stage_index=data["stage_index"],
            start=data["start"],
            end=data["end"],
            demand=dict(data["demand"]),
            consumed=data.get("consumed", False),
            released=data.get("released", False),
        )


@dataclass
class Reservation:
    """一组有共同交付期限的阶段的资源承诺。"""

    reservation_id: str
    experiment_id: str
    deadline: int
    windows: list[ReservedWindow] = field(default_factory=list)
    status: ReservationStatus = ReservationStatus.CONFIRMED
    events: list[RunEvent] = field(default_factory=list)
    breach_reasons: list[str] = field(default_factory=list)

    def log(self, time: int, kind: str, detail: str = "") -> None:
        self.events.append(RunEvent(time, kind, detail))

    @property
    def active_windows(self) -> list[ReservedWindow]:
        return [w for w in self.windows if w.active]

    def to_dict(self) -> dict:
        return {
            "reservation_id": self.reservation_id,
            "experiment_id": self.experiment_id,
            "deadline": self.deadline,
            "status": self.status.value,
            "windows": [w.to_dict() for w in self.windows],
            "events": [e.to_dict() for e in self.events],
            "breach_reasons": list(self.breach_reasons),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Reservation":
        return cls(
            reservation_id=data["reservation_id"],
            experiment_id=data["experiment_id"],
            deadline=data["deadline"],
            windows=[ReservedWindow.from_dict(w) for w in data.get("windows", [])],
            status=ReservationStatus(data["status"]),
            events=[RunEvent.from_dict(e) for e in data.get("events", [])],
            breach_reasons=list(data.get("breach_reasons", [])),
        )


class ReservationConflictError(RuntimeError):
    """期限内排不下全部阶段：携带冲突范围，绝不伪造可行日程。"""

    def __init__(self, message: str, conflicts: list[dict]) -> None:
        super().__init__(message)
        self.conflicts = conflicts


def earliest_interval(
    capacity: dict[str, int],
    events: list[tuple[int, dict[str, int]]],
    outages: list[tuple[int, int]],
    demand: dict[str, int],
    cursor: int,
    duration: int,
) -> Optional[int]:
    """在 [cursor, +inf) 内找最早 t，使 [t, t+duration) 全程满足 demand。

    events 是 (时刻, 承诺增量) 列表：正数表示承诺增加（预约窗口开始），
    负数表示承诺减少（窗口结束或在跑任务释放）。维护区间内节点不可用。
    无任何可行时刻时返回 None。
    """
    ordered = sorted(events, key=lambda item: item[0])
    committed: dict[str, int] = {}
    idx = 0
    while idx < len(ordered) and ordered[idx][0] <= cursor:
        _apply(committed, ordered[idx][1])
        idx += 1
    t = cursor
    while True:
        if _fits(capacity, committed, demand):
            overlap_end = _outage_overlap_end(outages, t, t + duration)
            if overlap_end is None:
                projected = dict(committed)
                ok = True
                j = idx
                while j < len(ordered) and ordered[j][0] < t + duration:
                    _apply(projected, ordered[j][1])
                    if not _fits(capacity, projected, demand):
                        ok = False
                        break
                    j += 1
                if ok:
                    return t
            else:
                t = max(t, overlap_end)
                while idx < len(ordered) and ordered[idx][0] <= t:
                    _apply(committed, ordered[idx][1])
                    idx += 1
                continue
        if idx >= len(ordered):
            return None
        nxt = ordered[idx][0]
        while idx < len(ordered) and ordered[idx][0] == nxt:
            _apply(committed, ordered[idx][1])
            idx += 1
        t = max(nxt, cursor)


def plan_stage_windows(
    stages: list[Stage],
    node_states: dict[str, dict],
    cursor: int,
    deadline: int,
) -> Optional[list[tuple[str, int, int]]]:
    """依次为每个阶段选 (node_id, start, end)；任一阶段超期限返回 None。

    node_states[node_id] = {"capacity": ..., "events": [(t, delta)], "outages": [...]}
    规划成功后会把新窗口作为承诺事件写回各 node_state，保证阶段间不重复扣账。
    """
    planned: list[tuple[str, int, int]] = []
    for stage in stages:
        demand = stage.resource_request
        best: Optional[tuple[int, str]] = None
        for node_id, state in node_states.items():
            start = earliest_interval(
                state["capacity"], state["events"], state["outages"], demand, cursor, stage.duration
            )
            if start is None or start + stage.duration > deadline:
                continue
            if best is None or start < best[0]:
                best = (start, node_id)
        if best is None:
            return None
        start, node_id = best
        end = start + stage.duration
        planned.append((node_id, start, end))
        state = node_states[node_id]
        state["events"].append((start, dict(demand)))
        state["events"].append((end, {name: -amount for name, amount in demand.items()}))
        cursor = end
    return planned


def _apply(committed: dict[str, int], delta: dict[str, int]) -> None:
    for name, amount in delta.items():
        committed[name] = committed.get(name, 0) + amount


def _fits(capacity: dict[str, int], committed: dict[str, int], demand: dict[str, int]) -> bool:
    return all(
        capacity.get(name, 0) - committed.get(name, 0) >= amount
        for name, amount in demand.items()
    )


def _outage_overlap_end(outages: list[tuple[int, int]], start: int, end: int) -> Optional[int]:
    for o_start, o_end in outages:
        if o_start < end and start < o_end:
            return o_end
    return None
