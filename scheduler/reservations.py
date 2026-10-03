"""资源预约台账：预约承诺与真实占用分开记账。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ReservationStatus(str, Enum):
    CONFIRMED = "confirmed"
    CONSUMED = "consumed"
    CANCELLED = "cancelled"
    SUPERSEDED = "superseded"
    BREACHED = "breached"


@dataclass
class Reservation:
    reservation_id: str
    experiment_id: str
    stage_index: int
    node_id: str
    start: int
    end: int
    demand: dict[str, int]
    status: ReservationStatus = ReservationStatus.CONFIRMED
    breach_reason: str = ""
    replaces: Optional[str] = None

    def overlaps(self, start: int, end: int) -> bool:
        return self.start < end and start < self.end

    def to_dict(self) -> dict:
        return {
            "reservation_id": self.reservation_id,
            "experiment_id": self.experiment_id,
            "stage_index": self.stage_index,
            "node_id": self.node_id,
            "start": self.start,
            "end": self.end,
            "demand": dict(self.demand),
            "status": self.status.value,
            "breach_reason": self.breach_reason,
            "replaces": self.replaces,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Reservation":
        return cls(
            reservation_id=data["reservation_id"],
            experiment_id=data["experiment_id"],
            stage_index=data["stage_index"],
            node_id=data["node_id"],
            start=data["start"],
            end=data["end"],
            demand=dict(data["demand"]),
            status=ReservationStatus(data.get("status", ReservationStatus.CONFIRMED.value)),
            breach_reason=data.get("breach_reason", ""),
            replaces=data.get("replaces"),
        )


@dataclass(frozen=True)
class ConflictRange:
    experiment_id: str
    stage_index: int
    requested_start: int
    requested_end: int
    deadline: int
    node_id: str
    reason: str
    blocking_start: Optional[int] = None
    blocking_end: Optional[int] = None
    blocking_reservation_id: Optional[str] = None
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "experiment_id": self.experiment_id,
            "stage_index": self.stage_index,
            "requested_start": self.requested_start,
            "requested_end": self.requested_end,
            "deadline": self.deadline,
            "node_id": self.node_id,
            "reason": self.reason,
            "blocking_start": self.blocking_start,
            "blocking_end": self.blocking_end,
            "blocking_reservation_id": self.blocking_reservation_id,
            "message": self.message,
        }


@dataclass
class ReservationReport:
    ok: bool
    reservations: list[Reservation] = field(default_factory=list)
    conflicts: list[ConflictRange] = field(default_factory=list)
    released_capacity: list[Reservation] = field(default_factory=list)
    feasibility: dict[str, str] = field(default_factory=dict)

    @property
    def reservation(self) -> Optional[Reservation]:
        return self.reservations[0] if self.reservations else None


class ReservationLedger:
    def __init__(self) -> None:
        self._items: dict[str, Reservation] = {}

    def list(self, status: Optional[ReservationStatus] = None) -> list[Reservation]:
        values = list(self._items.values())
        if status is not None:
            values = [item for item in values if item.status == status]
        return sorted(values, key=lambda item: (item.start, item.end, item.reservation_id))

    def get(self, reservation_id: str) -> Reservation:
        return self._items[reservation_id]

    def confirmed_for(self, experiment_id: str, stage_index: int, at_time: Optional[int] = None) -> Optional[Reservation]:
        matches = [
            item for item in self._items.values()
            if item.experiment_id == experiment_id
            and item.stage_index == stage_index
            and item.status == ReservationStatus.CONFIRMED
            and (at_time is None or item.start >= at_time)
        ]
        return min(matches, key=lambda item: item.start, default=None)

    def active_promises(self, include_breached: bool = False) -> list[Reservation]:
        allowed = {ReservationStatus.CONFIRMED}
        if include_breached:
            allowed.add(ReservationStatus.BREACHED)
        return [item for item in self._items.values() if item.status in allowed]

    def add(self, reservation: Reservation) -> None:
        if reservation.reservation_id in self._items:
            raise ValueError(f"预约 {reservation.reservation_id} 已存在")
        self._items[reservation.reservation_id] = reservation

    def replace(self, reservation: Reservation) -> None:
        self._items[reservation.reservation_id] = reservation

    def consume(self, reservation_id: str) -> Optional[Reservation]:
        item = self._items.get(reservation_id)
        if item is None or item.status != ReservationStatus.CONFIRMED:
            return None
        item.status = ReservationStatus.CONSUMED
        item.breach_reason = ""
        return item

    def cancel(self, reservation_id: str, reason: str = "") -> Optional[Reservation]:
        item = self._items.get(reservation_id)
        if item is None or item.status not in (ReservationStatus.CONFIRMED, ReservationStatus.BREACHED):
            return None
        item.status = ReservationStatus.CANCELLED
        item.breach_reason = reason
        return item

    def supersede(self, reservation_id: str) -> Optional[Reservation]:
        item = self._items.get(reservation_id)
        if item is None or item.status not in (ReservationStatus.CONFIRMED, ReservationStatus.BREACHED):
            return None
        item.status = ReservationStatus.SUPERSEDED
        return item

    def mark_breached(self, reservation_id: str, reason: str) -> Optional[Reservation]:
        item = self._items.get(reservation_id)
        if item is None or item.status != ReservationStatus.CONFIRMED:
            return None
        item.status = ReservationStatus.BREACHED
        item.breach_reason = reason
        return item

    def overlapping(
        self,
        node_id: str,
        start: int,
        end: int,
        *,
        exclude_id: Optional[str] = None,
        include_breached: bool = False,
    ) -> list[Reservation]:
        return [
            item for item in self.active_promises(include_breached=include_breached)
            if item.node_id == node_id
            and item.overlaps(start, end)
            and item.reservation_id != exclude_id
        ]

    def to_list(self) -> list[dict]:
        return [item.to_dict() for item in self.list()]

    def load_list(self, data: list[dict]) -> None:
        self._items = {}
        for raw in data:
            item = Reservation.from_dict(raw)
            self._items[item.reservation_id] = item
