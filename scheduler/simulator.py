"""离散事件模拟器：提交、多资源放置、维护迁移、完成、失败与取消。"""

from __future__ import annotations

import heapq
import itertools
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .models import (
    Cluster,
    ExperimentRequest,
    Node,
    Outage,
    RunRecord,
    RunStatus,
    Stage,
)
from .queue import FairQueuePolicy, QueueEntry
from .queue import BUDGET_LIMIT, MAINTENANCE, RESOURCE_SHORTAGE
from .reservations import (
    ConflictRange,
    Reservation,
    ReservationLedger,
    ReservationReport,
    ReservationStatus,
)


class CapacityInfeasibleError(RuntimeError):
    """需求超过任何单个空节点的容量：排产即不可能，拒绝入队。"""


@dataclass
class SimulationResult:
    now: int
    started: list[str] = field(default_factory=list)
    waiting: list[str] = field(default_factory=list)
    yielded: list[str] = field(default_factory=list)
    completed: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    cancelled: list[str] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)


class Scheduler:
    def __init__(
        self,
        nodes: list[Node],
        starvation_threshold: int = 20,
        now: int = 0,
        schedule_budget: Optional[int] = None,
    ) -> None:
        self.cluster = Cluster(nodes)
        self.queue = FairQueuePolicy(starvation_threshold=starvation_threshold)
        self.now = now
        self.schedule_budget = schedule_budget
        self.records: dict[str, RunRecord] = {}
        self.requests: dict[str, ExperimentRequest] = {}
        self._heap: list[tuple[int, int, str]] = []
        self._counter = itertools.count()
        self._event_for: dict[str, tuple[int, int]] = {}
        self.pending_arrivals: set[str] = set()
        self._yield_requested: dict[str, str] = {}
        self.outages: dict[str, list[tuple[int, int]]] = {node.node_id: [] for node in nodes}
        self.last_blockages: dict[str, str] = {}
        self.reservations = ReservationLedger()

    # ---- 提交与容量校验 -------------------------------------------------

    def submit(self, request: ExperimentRequest) -> RunRecord:
        if request.experiment_id in self.records:
            raise ValueError(f"实验 {request.experiment_id} 已存在，身份不可复用")
        self._assert_capacity_feasible(request)
        self.requests[request.experiment_id] = request
        record = RunRecord(
            experiment_id=request.experiment_id,
            submit_time=request.submit_time,
        )
        stages = request.stage_list
        summary = ",".join(f"{s.memory_mb}MB/{s.duration}t" for s in stages)
        record.log(request.submit_time, "submitted", summary)
        self.records[request.experiment_id] = record
        if request.submit_time <= self.now:
            self._enqueue_request(request, stage_index=0)
        else:
            self._push(request.submit_time, ("arrive", request.experiment_id))
            self.pending_arrivals.add(request.experiment_id)
        return record

    def _refresh_runtime_availability(self) -> None:
        for node_id, runtime in self.cluster.runtime.items():
            runtime.available = not self._node_under_maintenance(node_id, self.now)

    def _enqueue_request(
        self,
        request: ExperimentRequest,
        stage_index: int,
        pinned_node_id: Optional[str] = None,
    ) -> QueueEntry:
        stage = request.stage_list[stage_index]
        reservation = self.reservations.confirmed_for(
            request.experiment_id,
            stage_index,
            at_time=self.now,
        )
        entry = QueueEntry(
                experiment_id=request.experiment_id,
                memory_mb=stage.memory_mb,
                submit_time=request.submit_time,
                duration=stage.duration,
                priority=request.priority,
                stage_index=stage_index,
                resources=dict(stage.resources),
                pinned_node_id=pinned_node_id or (reservation.node_id if reservation else None),
                reservation_id=reservation.reservation_id if reservation else None,
                reserved_start=reservation.start if reservation else None,
                deadline=reservation.end if reservation else None,
            )
        self.queue.add(entry)
        return entry

    def _assert_capacity_feasible(self, request: ExperimentRequest) -> None:
        if not any(
            n.max_jobs >= 1 and all(
                n.resource_capacity.get(name, 0) >= amount
                for s in request.stage_list
                for name, amount in s.resource_request.items()
            )
            for n in self.cluster.nodes.values()
        ):
            raise CapacityInfeasibleError(
                f"实验 {request.experiment_id} 存在超过任何节点单机容量的阶段，"
                "不会排产不存在的容量"
            )

    # ---- 主循环 ---------------------------------------------------------

    def run_until(
        self,
        end_time: Optional[int] = None,
        schedule_budget: Optional[int] = "__use_default__",
    ) -> SimulationResult:
        """推进模拟；end_time 为空时跑到队列清空、无在跑任务为止。"""
        if schedule_budget == "__use_default__":
            schedule_budget = self.schedule_budget
        result = SimulationResult(now=self.now)
        remaining_schedule_budget = schedule_budget
        def schedule_once() -> None:
            nonlocal remaining_schedule_budget
            started = self._schedule_pending(result, remaining_schedule_budget)
            if remaining_schedule_budget is not None:
                remaining_schedule_budget -= started
        def process_due_now() -> None:
            if self._heap and self._heap[0][0] <= self.now:
                self._process_events_at(self.now, result)
        while True:
            if end_time is not None and self.now >= end_time:
                process_due_now()
                schedule_once()
                break
            if not self._heap and not self.queue:
                schedule_once()
                break
            if not self._heap:
                schedule_once()
                # 队列非空但无事件：跳到下一个预约开启时刻，否则安全前进一步
                open_time = self._next_reservation_open_time()
                self.now = open_time if open_time is not None else self.now + 1
                continue
            fire_time = self._heap[0][0]
            open_time = self._next_reservation_open_time()
            if open_time is not None:
                fire_time = min(fire_time, open_time)
            if end_time is not None:
                fire_time = min(fire_time, end_time)
            if fire_time > self.now:
                schedule_once()
                self.now = fire_time
                continue
            self.now = fire_time
            self._process_events_at(fire_time, result)
            schedule_once()
        result.now = self.now
        return result

    def _next_reservation_open_time(self) -> Optional[int]:
        """最近的未来预约开启时刻；保证时间推进不会跳过已承诺窗口。"""
        starts = [
            item.start
            for item in self.reservations.active_promises()
            if item.start > self.now
        ]
        return min(starts) if starts else None

    def _process_events_at(self, time: int, result: SimulationResult) -> None:
        due: list[tuple] = []
        while self._heap and self._heap[0][0] <= time:
            _, _, payload = heapq.heappop(self._heap)
            kind, eid = payload
            due.append((kind, eid))
        for kind, eid in due:
            if kind == "arrive":
                req = self.requests[eid]
                self._enqueue_request(req, stage_index=0)
                self.pending_arrivals.discard(eid)
                continue
            if kind == "outage_on":
                self._apply_outage(eid, available=False, result=result)
            elif kind == "outage_off":
                self._apply_outage(eid, available=True, result=result)
            elif kind == "stage_end":
                self._handle_stage_end(eid, result)
            elif kind == "maintenance_resume":
                self._resume_after_maintenance(eid, result)
            elif kind == "fail":
                self._finish(eid, RunStatus.FAILED, result)
            elif kind == "early_finish":
                self._finish_early(eid, result)
            elif kind == "cancel":
                self._cancel(eid, result)

    # ---- 调度放置 -------------------------------------------------------

    def _schedule_pending(self, result: SimulationResult, schedule_budget: Optional[int] = None) -> int:
        self._prune_stale_yield_requests()
        self._reconcile_due_reservations()
        release_events = self._collect_release_events()
        budget = self.schedule_budget if schedule_budget is None else schedule_budget
        targets, blockages = self.queue.plan_targets(
            self.now,
            self.cluster,
            release_events,
            outages=self.outages,
            schedule_budget=budget,
            reservations=self.reservations.active_promises(include_breached=True),
        )
        self.last_blockages = {b.experiment_id: b.reason for b in blockages}
        result.blocked.extend(b.experiment_id for b in blockages)
        remaining_budget = budget
        started_count = 0
        blocked: list[QueueEntry] = []
        for entry in self.queue.ordered(self.now):
            eid = entry.experiment_id
            if remaining_budget is not None and remaining_budget <= 0:
                blocked.append(entry)
                continue
            target = targets.get(eid)
            if target is None or target.start_time > self.now:
                blocked.append(entry)
                continue
            runtime = self.cluster.runtime_for(target.node_id)
            reservation = self._reservation_for_entry(entry)
            if reservation is not None:
                if (
                    reservation.status is not ReservationStatus.CONFIRMED
                    or reservation.node_id != target.node_id
                    or not (reservation.start <= self.now <= reservation.end)
                ):
                    blocked.append(entry)
                    continue
                self.reservations.consume(reservation.reservation_id)
                self.records[eid].log(
                    self.now,
                    "reservation_consumed",
                    f"reservation={reservation.reservation_id} node={target.node_id}",
                )
            if not runtime.can_fit(resources=entry.demand):
                blocked.append(entry)
                continue
            self._start_stage(eid, target.node_id, entry.stage_index, result)
            started_count += 1
            if remaining_budget is not None:
                remaining_budget -= 1
        self._request_safe_point_yields(blocked, release_events, result)
        return started_count

    def _prune_stale_yield_requests(self) -> None:
        """让出请求必须仍有对应的高优排队任务才成立。

        请求者完成、取消或自身已启动后，被让出的低优任务不再需要在安全点
        额外脱离调度；若其安全点本可在同节点接续下一阶段，就直接接续。
        """
        if not self._yield_requested:
            return
        active_blocked_ids: set[str] = {
            e.experiment_id for e in self.queue.ordered(self.now) if e.priority > 0
        }
        for victim, requester in list(self._yield_requested.items()):
            if requester not in active_blocked_ids:
                del self._yield_requested[victim]

    def _reservation_for_entry(self, entry: QueueEntry) -> Optional[Reservation]:
        if entry.reservation_id is None:
            return None
        try:
            return self.reservations.get(entry.reservation_id)
        except KeyError:
            return None

    def _reconcile_due_reservations(self) -> None:
        for entry in self.queue.pending:
            reservation = self._reservation_for_entry(entry)
            if reservation is None:
                continue
            if reservation.status is ReservationStatus.BREACHED:
                entry.pinned_node_id = None
                entry.reservation_id = None
                entry.reserved_start = None
                entry.deadline = None
                continue
            if reservation.status != ReservationStatus.CONFIRMED:
                self._detach_entry_reservation(entry)
                continue
            if self.now < reservation.start:
                entry.pinned_node_id = reservation.node_id
                entry.reserved_start = reservation.start
                entry.deadline = reservation.end
                continue
            if self.now >= reservation.end:
                reason = f"reserved_window=[{reservation.start},{reservation.end}) missed at {self.now}"
                self.reservations.mark_breached(reservation.reservation_id, reason)
                record = self.records.get(reservation.experiment_id)
                if record is not None:
                    record.log(self.now, "reservation_breached", f"reservation={reservation.reservation_id} {reason}")
                self._detach_entry_reservation(entry)
                continue
            entry.pinned_node_id = reservation.node_id
            entry.reserved_start = self.now
            entry.deadline = reservation.end

    def _detach_entry_reservation(self, entry: QueueEntry) -> None:
        entry.pinned_node_id = None
        entry.reservation_id = None
        entry.reserved_start = None
        entry.deadline = None

    def _bind_queue_reservation(self, reservation: Reservation) -> None:
        for entry in self.queue.pending:
            if entry.experiment_id == reservation.experiment_id and entry.stage_index == reservation.stage_index:
                entry.reservation_id = reservation.reservation_id
                entry.pinned_node_id = reservation.node_id
                entry.reserved_start = reservation.start
                entry.deadline = reservation.end

    def _request_safe_point_yields(
        self,
        blocked: list[QueueEntry],
        release_events: dict[str, list[tuple]],
        result: SimulationResult,
    ) -> None:
        """容量不足的高优先级任务可请求在跑任务在声明安全点让出。

        只允许在安全点接续：这里只登记让出请求，真正的释放发生在被让出
        任务的某个 safe_point 阶段结束时刻；非安全边界绝不抢占。
        """
        for entry in blocked:
            if entry.priority <= 0 or entry.experiment_id in result.started:
                continue
            candidates = self._yield_candidates_for(entry)
            if not candidates:
                continue
            # 按节点贪心：只要某节点累计释放量能放下该阶段，就在该节点登记让出
            by_node: dict[str, list[tuple[int, int, str]]] = {}
            for node_id, demand, safepoint, victim in candidates:
                by_node.setdefault(node_id, []).append((demand, safepoint, victim))
            for node_id, offers in by_node.items():
                offers.sort(key=lambda o: (o[1], o[2]))
                runtime = self.cluster.runtime_for(node_id)
                totals: dict[str, int] = {}
                picks: list[str] = []
                for demand, sp, victim in offers:
                    for name, amount in demand.items():
                        totals[name] = totals.get(name, 0) + amount
                    picks.append(victim)
                    free = runtime.free_resources()
                    if all(free.get(name, 0) + totals.get(name, 0) >= amount for name, amount in entry.demand.items()):
                        break
                free = runtime.free_resources()
                if all(free.get(name, 0) + totals.get(name, 0) >= amount for name, amount in entry.demand.items()):
                    for victim in picks:
                        if victim not in self._yield_requested:
                            self._yield_requested[victim] = entry.experiment_id
                            result.yielded.append(victim)
                            rec = self.records[victim]
                            rec.log(self.now, "yield_requested", f"for={entry.experiment_id}")
                    return

    def _yield_candidates_for(self, entry: QueueEntry) -> list[tuple[str, dict[str, int], int, str]]:
        """返回 (节点, 该任务当前阶段内存, 下一安全点时刻, 实验ID)。

        只有优先级严格低于排队任务、且未来存在声明安全点的在跑任务才可让出。
        """
        candidates: list[tuple[str, int, int, str]] = []
        for eid, record in self.records.items():
            if record.status is not RunStatus.RUNNING or record.node_id is None:
                continue
            req = self.requests[eid]
            if req.priority >= entry.priority:
                continue
            sp = self._next_safe_point(eid)
            if sp is None:
                continue
            stage = req.stage_list[record.stage_index]
            candidates.append((record.node_id, stage.resource_request, sp, eid))
        return candidates

    def _next_safe_point(self, eid: str) -> Optional[int]:
        """在跑任务下一个 safe_point 阶段边界时刻；没有则 None（不可打断）。"""
        record = self.records[eid]
        req = self.requests[eid]
        assert record.stage_started_at is not None
        t = record.stage_started_at
        current_remaining = record.stage_remaining or req.stage_list[record.stage_index].duration
        t += current_remaining
        if req.stage_list[record.stage_index].safe_point:
            return t
        for idx in range(record.stage_index + 1, len(req.stage_list)):
            t += req.stage_list[idx].duration
            if req.stage_list[idx].safe_point:
                return t
        return None

    def _collect_release_events(self) -> dict[str, list[tuple]]:
        events: dict[str, list[tuple]] = {}
        for fire_time, _, (kind, eid) in self._heap:
            if kind not in ("stage_end", "fail"):
                continue
            record = self.records.get(eid)
            if record is None or record.status is not RunStatus.RUNNING or record.node_id is None:
                continue
            stage = self.requests[eid].stage_list[record.stage_index]
            events.setdefault(record.node_id, []).append((fire_time, stage.resource_request, eid))
        # 已被请求让出的任务，其下一安全点也是确定的未来释放事件
        for eid in list(self._yield_requested):
            record = self.records.get(eid)
            if record is None or record.status is not RunStatus.RUNNING or record.node_id is None:
                del self._yield_requested[eid]
                continue
            sp = self._next_safe_point(eid)
            if sp is None:
                continue
            stage = self.requests[eid].stage_list[record.stage_index]
            events.setdefault(record.node_id, []).append((sp, stage.resource_request, eid))
        return events

    def _start_stage(self, eid: str, node_id: str, stage_index: int, result: SimulationResult) -> None:
        req = self.requests[eid]
        record = self.records[eid]
        stage = req.stage_list[stage_index]
        resume_duration = record.stage_remaining
        duration = resume_duration if resume_duration is not None else stage.duration
        self.cluster.runtime_for(node_id).allocate(eid, stage.resource_request)
        record.status = RunStatus.RUNNING
        record.node_id = node_id
        record.stage_index = stage_index
        record.stage_started_at = self.now
        record.stage_remaining = duration
        self._remove_existing_stage_end(eid)
        if record.start_time is None:
            record.start_time = self.now
        started_before = any(
            event.kind == "stage_started" and f"stage={stage_index}" in event.detail
            for event in record.events
        )
        if not started_before:
            record.log(self.now, "stage_started", f"stage={stage_index} node={node_id} resources={stage.resource_request}")
        else:
            record.log(self.now, "stage_resumed", f"stage={stage_index} node={node_id} remaining={duration}")
        self.queue.remove(eid)
        self._yield_requested.pop(eid, None)
        record.pinned_node_id = None
        end_time = self.now + duration
        self._push(end_time, ("stage_end", eid), key=eid)
        result.started.append(eid)

    def _remove_existing_stage_end(self, eid: str) -> None:
        self._event_for.pop(eid, None)
        before = len(self._heap)
        self._heap = [item for item in self._heap if item[2] != ("stage_end", eid)]
        if len(self._heap) != before:
            heapq.heapify(self._heap)

    def _handle_stage_end(self, eid: str, result: SimulationResult) -> None:
        """阶段结束：释放本阶段不再需要的资源，再决定完成/接续/显式等待。

        - 已是最后一个阶段：实验完成。
        - 声明了安全点且被高优先级任务请求让出：释放后回到队列显式等待。
        - 非安全边界：下一阶段在同节点立刻放得下则直接接续（绝不被抢占），
          放不下才释放并显式等待（不会占着无用容量阻塞其他实验）。
        - 普通安全边界：释放后重新排队，由公平策略统一调度下一阶段。
        """
        record = self.records.get(eid)
        if record is None or record.status is not RunStatus.RUNNING:
            return
        req = self.requests[eid]
        finished_stage = record.stage_index
        runtime = self.cluster.runtime_for(record.node_id)
        released = runtime.release(eid)  # 幂等：只释放一次
        assert released
        self._event_for.pop(eid, None)
        record.log(self.now, "stage_completed", f"stage={finished_stage} node={record.node_id}")
        record.stage_remaining = None
        next_index = finished_stage + 1
        if next_index >= len(req.stage_list):
            self._cancel_future_reservations(eid, finished_stage, "experiment_completed")
            record.status = RunStatus.COMPLETED
            record.end_time = self.now
            record.stage_started_at = None
            self._yield_requested.pop(eid, None)
            record.log(self.now, "completed", f"node={record.node_id}")
            result.completed.append(eid)
            return
        stage = req.stage_list[finished_stage]
        must_yield = stage.safe_point and eid in self._yield_requested
        if must_yield:
            self._yield_requested.pop(eid, None)
            record.log(self.now, "yielded", f"at_stage={finished_stage}")
        if not stage.safe_point and not must_yield:
            next_stage = req.stage_list[next_index]
            if runtime.can_fit(resources=next_stage.resource_request) and not self._node_under_maintenance(record.node_id, self.now):
                self._start_stage(eid, record.node_id, next_index, result)
                return
        # 释放后进入明确等待：不保留任何节点容量
        record.status = RunStatus.WAITING
        record.node_id = None
        record.stage_index = next_index
        record.stage_started_at = None
        record.stage_remaining = None
        self._enqueue_request(req, stage_index=next_index)
        record.log(self.now, "waiting", f"stage={next_index}")
        result.waiting.append(eid)

    def _finish(self, eid: str, status: RunStatus, result: SimulationResult) -> None:
        record = self.records.get(eid)
        if record is None or record.status is not RunStatus.RUNNING:
            return
        runtime = self.cluster.runtime_for(record.node_id)
        released = runtime.release(eid)
        assert released
        self._cancel_future_reservations(eid, record.stage_index + 1, status.value)
        record.status = status
        record.end_time = self.now
        record.stage_started_at = None
        record.pinned_node_id = None
        record.log(self.now, status.value, f"node={record.node_id}")
        self._event_for.pop(eid, None)
        (result.completed if status is RunStatus.COMPLETED else result.failed).append(eid)

    def _finish_early(self, eid: str, result: SimulationResult) -> None:
        record = self.records.get(eid)
        if record is None or record.status is not RunStatus.RUNNING:
            return
        self._remove_existing_stage_end(eid)
        self._finish(eid, RunStatus.COMPLETED, result)
        record.log(self.now, "finished_early")

    def _cancel(self, eid: str, result: SimulationResult) -> None:
        record = self.records.get(eid)
        if record is None or record.status.terminal:
            return
        if record.status is RunStatus.RUNNING:
            released = self.cluster.runtime_for(record.node_id).release(eid)
            assert released
        else:
            self.queue.remove(eid)
        self._cancel_future_reservations(eid, record.stage_index, "experiment_cancelled")
        record.status = RunStatus.CANCELLED
        record.end_time = self.now
        record.stage_started_at = None
        record.pinned_node_id = None
        self._yield_requested.pop(eid, None)
        record.log(self.now, "cancelled")
        for key in list(self._event_for):
            if str(key).startswith(f"maintenance-resume-{eid}-"):
                self._event_for.pop(key, None)
        self._event_for.pop(eid, None)
        result.cancelled.append(eid)

    # ---- 节点维护 -------------------------------------------------------

    def add_outage(self, node_id: str, outage: Outage) -> None:
        if node_id not in self.cluster.nodes:
            raise ValueError(f"未知节点 {node_id}")
        self.outages.setdefault(node_id, []).append((outage.start, outage.end))
        self._push(outage.start, ("outage_on", node_id), key=f"outage-on-{node_id}-{outage.start}")
        self._push(outage.end, ("outage_off", node_id), key=f"outage-off-{node_id}-{outage.end}")
        self._mark_outage_reservation_breaches(node_id, outage)

    def _mark_outage_reservation_breaches(self, node_id: str, outage: Outage) -> None:
        for item in self.reservations.list(ReservationStatus.CONFIRMED):
            if item.node_id == node_id and item.overlaps(outage.start, outage.end):
                reason = f"maintenance=[{outage.start},{outage.end}) overlaps promise=[{item.start},{item.end})"
                self.reservations.mark_breached(item.reservation_id, reason)
                record = self.records.get(item.experiment_id)
                if record is not None:
                    record.log(outage.start, "reservation_breached", f"reservation={item.reservation_id} {reason}")

    def _node_under_maintenance(self, node_id: str, time: int) -> bool:
        return any(start <= time < end for start, end in self.outages.get(node_id, []))

    def _apply_outage(self, node_id: str, available: bool, result: SimulationResult) -> None:
        runtime = self.cluster.runtime_for(node_id)
        if available and self._node_under_maintenance(node_id, self.now):
            return
        runtime.available = available
        if available:
            return
        for eid in list(runtime.running.keys()):
            record = self.records[eid]
            stage = self.requests[eid].stage_list[record.stage_index]
            started_at = record.stage_started_at if record.stage_started_at is not None else self.now
            elapsed = self.now - started_at
            remaining_before = record.stage_remaining or stage.duration
            record.stage_remaining = max(1, remaining_before - elapsed)
            runtime.release(eid)
            event_handle = self._event_for.pop(eid, None)
            if event_handle is not None:
                self._heap = [
                    item for item in self._heap
                    if (item[0], item[1]) != tuple(event_handle)
                ]
                heapq.heapify(self._heap)
            if stage.migratable:
                record.status = RunStatus.WAITING
                record.node_id = None
                record.stage_started_at = None
                self._enqueue_request(self.requests[eid], record.stage_index)
                record.log(self.now, "maintenance_migratable_wait", f"stage={record.stage_index} remaining={record.stage_remaining}")
                result.waiting.append(eid)
            else:
                record.status = RunStatus.WAITING
                record.node_id = None
                record.pinned_node_id = node_id
                record.stage_started_at = None
                end = max(end for start, end in self.outages[node_id] if start <= self.now < end)
                self._push(end, ("maintenance_resume", eid), key=f"maintenance-resume-{eid}-{self.now}")
                record.log(self.now, "maintenance_pinned_wait", f"stage={record.stage_index} until={end}")
                result.waiting.append(eid)

    def _resume_after_maintenance(self, eid: str, result: SimulationResult) -> None:
        record = self.records.get(eid)
        if record is None or record.status is not RunStatus.WAITING or record.pinned_node_id is None:
            return
        node_id = record.pinned_node_id
        if self._node_under_maintenance(node_id, self.now):
            end = max(end for start, end in self.outages[node_id] if start <= self.now < end)
            self._push(end, ("maintenance_resume", eid), key=f"maintenance-resume-{eid}-{self.now}")
            return
        runtime = self.cluster.runtime_for(node_id)
        stage = self.requests[eid].stage_list[record.stage_index]
        runtime.available = True
        runtime.allocate(eid, stage.resource_request)
        record.status = RunStatus.RUNNING
        record.node_id = node_id
        record.pinned_node_id = None
        record.stage_started_at = self.now
        duration = record.stage_remaining or stage.duration
        record.log(self.now, "stage_resumed", f"stage={record.stage_index} node={node_id} remaining={duration}")
        self.queue.remove(eid)
        self._push(self.now + duration, ("stage_end", eid), key=eid)
        result.started.append(eid)

    # ---- 预约 -----------------------------------------------------------

    def reserve_stage(
        self,
        experiment_id: str,
        stage_index: int,
        deadline: int,
        *,
        earliest_start: Optional[int] = None,
        reservation_id: Optional[str] = None,
    ) -> ReservationReport:
        request = self.requests[experiment_id]
        stages = request.stage_list
        if not 0 <= stage_index < len(stages):
            raise IndexError("阶段序号超出范围")
        start = max(self.now if earliest_start is None else earliest_start, self.now)
        reservation_id = reservation_id or f"{experiment_id}-s{stage_index}-{len(self.reservations.to_list()) + 1}"
        stage = stages[stage_index]
        window = self._find_stage_window(experiment_id, stage_index, stage.resource_request, stage.duration, start, deadline)
        if window is None:
            return ReservationReport(
                ok=False,
                conflicts=[ConflictRange(
                    experiment_id=experiment_id,
                    stage_index=stage_index,
                    requested_start=start,
                    requested_end=deadline,
                    deadline=deadline,
                    node_id="",
                    reason=RESOURCE_SHORTAGE,
                    message=f"stage={stage_index} cannot finish by deadline={deadline}",
                )],
            )
        node_id, actual_start, actual_end = window
        reservation = Reservation(
            reservation_id=reservation_id,
            experiment_id=experiment_id,
            stage_index=stage_index,
            node_id=node_id,
            start=actual_start,
            end=actual_end,
            demand=dict(stage.resource_request),
        )
        self.reservations.add(reservation)
        self._bind_queue_reservation(reservation)
        record = self.records[experiment_id]
        record.log(actual_start, "reservation_confirmed", f"reservation={reservation_id} stage={stage_index} node={node_id}")
        return ReservationReport(ok=True, reservations=[reservation])

    def reserve_deadline(
        self,
        experiment_id: str,
        deadline: int,
        *,
        earliest_start: Optional[int] = None,
        reservation_ids: Optional[list[str]] = None,
    ) -> ReservationReport:
        request = self.requests[experiment_id]
        stages = request.stage_list
        if deadline < self.now:
            raise ValueError("交付期限不能早于当前时间")
        start = max(self.now if earliest_start is None else earliest_start, self.now)
        proposed: list[tuple[int, str, int, int]] = []
        conflicts: list[ConflictRange] = []
        for index, stage in enumerate(stages):
            ids = reservation_ids or []
            reservation_id = ids[index] if index < len(ids) else f"{experiment_id}-s{index}-{len(self.reservations.to_list()) + index + 1}"
            latest_finish = deadline - sum(s.duration for s in stages[index + 1:])
            window = self._find_stage_window(
                experiment_id,
                index,
                stage.resource_request,
                stage.duration,
                start,
                latest_finish,
                proposed=proposed,
            )
            if window is None:
                blocker = self._reservation_blocker(experiment_id, index, stage.resource_request, start, latest_finish, stage.duration)
                conflicts.append(ConflictRange(
                    experiment_id=experiment_id,
                    stage_index=index,
                    requested_start=start,
                    requested_end=latest_finish,
                    deadline=deadline,
                    node_id=blocker.node_id if blocker else "",
                    reason=blocker.reason if blocker else RESOURCE_SHORTAGE,
                    blocking_start=blocker.blocking_start if blocker else None,
                    blocking_end=blocker.blocking_end if blocker else None,
                    blocking_reservation_id=blocker.blocking_reservation_id if blocker else None,
                    message=blocker.message if blocker else f"stage={index} cannot finish by {latest_finish} while meeting deadline={deadline}",
                ))
                continue
            node_id, actual_start, actual_end = window
            proposed.append((index, node_id, actual_start, actual_end))
            start = actual_end
        if conflicts:
            return ReservationReport(ok=False, conflicts=conflicts)
        items: list[Reservation] = []
        for index, node_id, actual_start, actual_end in proposed:
            ids = reservation_ids or []
            reservation_id = ids[index] if index < len(ids) else f"{experiment_id}-s{index}-{len(self.reservations.to_list()) + index + 1}"
            stage = stages[index]
            item = Reservation(
                reservation_id=reservation_id,
                experiment_id=experiment_id,
                stage_index=index,
                node_id=node_id,
                start=actual_start,
                end=actual_end,
                demand=dict(stage.resource_request),
            )
            self.reservations.add(item)
            self._bind_queue_reservation(item)
            items.append(item)
        self.records[experiment_id].log(items[0].start, "reservation_deadline_confirmed", f"deadline={deadline} reservations={','.join(i.reservation_id for i in items)}")
        return ReservationReport(ok=True, reservations=items)

    def _reservation_blocker(
        self,
        experiment_id: str,
        stage_index: int,
        demand: dict[str, int],
        earliest: int,
        latest_finish: int,
        duration: int,
    ) -> Optional[ConflictRange]:
        latest_start = latest_finish - duration
        if latest_start < earliest:
            return ConflictRange(experiment_id, stage_index, earliest, latest_finish, latest_finish, "", RESOURCE_SHORTAGE, message="deadline is shorter than stage duration")
        for node_id, node in self.cluster.nodes.items():
            if not all(node.resource_capacity.get(name, 0) >= amount for name, amount in demand.items()):
                continue
            for start, end in self.outages.get(node_id, []):
                if start < latest_finish and earliest < end:
                    return ConflictRange(experiment_id, stage_index, earliest, latest_finish, latest_finish, node_id, MAINTENANCE, start, end, message=f"maintenance=[{start},{end}) leaves no stage window")
            for item in self.reservations.active_promises(include_breached=True):
                if item.node_id == node_id and item.start < latest_finish and earliest < item.end:
                    return ConflictRange(experiment_id, stage_index, earliest, latest_finish, latest_finish, node_id, "reservation_conflict", item.start, item.end, item.reservation_id, f"blocked by reservation={item.reservation_id}")
        return None

    def _find_stage_window(
        self,
        experiment_id: str,
        stage_index: int,
        demand: dict[str, int],
        duration: int,
        earliest: int,
        latest_finish: int,
        proposed: Optional[list[tuple[int, str, int, int]]] = None,
    ) -> Optional[tuple[str, int, int]]:
        if latest_finish - duration < earliest:
            return None
        proposed = proposed or []
        releases = self._collect_release_events()
        for node_id, node in self.cluster.nodes.items():
            if not all(node.resource_capacity.get(name, 0) >= amount for name, amount in demand.items()):
                continue
            boundaries = {earliest, latest_finish - duration}
            for start, end in self.outages.get(node_id, []):
                boundaries.update({start, end})
            for fire_time, _, _ in releases.get(node_id, []):
                boundaries.add(fire_time)
            for item in self.reservations.active_promises(include_breached=True):
                if item.node_id == node_id:
                    boundaries.update({item.start, item.end})
            for idx, proposed_node, proposed_start, proposed_end in proposed:
                if proposed_node == node_id:
                    boundaries.update({proposed_start, proposed_end})
            for candidate in sorted(boundaries):
                if not earliest <= candidate <= latest_finish - duration:
                    continue
                if self._window_available(node_id, demand, candidate, candidate + duration, releases, proposed, experiment_id):
                    return node_id, candidate, candidate + duration
        return None

    def _window_available(
        self,
        node_id: str,
        demand: dict[str, int],
        start: int,
        end: int,
        releases: dict[str, list[tuple]],
        proposed: list[tuple[int, str, int, int]],
        experiment_id: str,
    ) -> bool:
        if any(start < outage_end and outage_start < end for outage_start, outage_end in self.outages.get(node_id, [])):
            return False
        node = self.cluster.nodes[node_id]
        release_by_eid = {eid: fire_time for fire_time, _, eid in releases.get(node_id, [])}
        allocations = dict(self.cluster.runtime_for(node_id).allocations)
        for time in range(start, end):
            used = {name: 0 for name in node.resource_capacity}
            for eid, item_demand in allocations.items():
                if release_by_eid.get(eid, 10**18) > time:
                    for name, amount in item_demand.items():
                        used[name] = used.get(name, 0) + amount
            for item in self.reservations.active_promises(include_breached=True):
                if item.node_id == node_id and item.start <= time < item.end:
                    for name, amount in item.demand.items():
                        used[name] = used.get(name, 0) + amount
            for idx, proposed_node, proposed_start, proposed_end in proposed:
                if proposed_node == node_id and proposed_start <= time < proposed_end:
                    stage_demand = self.requests[experiment_id].stage_list[idx].resource_request
                    for name, amount in stage_demand.items():
                        used[name] = used.get(name, 0) + amount
            if any(used.get(name, 0) + amount > node.resource_capacity.get(name, 0) for name, amount in demand.items()):
                return False
        return True

    def cancel_reservation(self, reservation_id: str, reason: str = "user_cancelled") -> ReservationReport:
        item = self.reservations.cancel(reservation_id, reason)
        if item is None:
            return ReservationReport(ok=False)
        record = self.records.get(item.experiment_id)
        if record is not None:
            record.log(self.now, "reservation_cancelled", f"reservation={reservation_id} reason={reason}")
        for entry in self.queue.pending:
            if entry.reservation_id == reservation_id:
                self._detach_entry_reservation(entry)
        return ReservationReport(ok=True, released_capacity=[item])

    def adjust_reservation(self, reservation_id: str, new_start: int, new_end: int, node_id: Optional[str] = None) -> ReservationReport:
        old = self.reservations.get(reservation_id)
        if old.status not in (ReservationStatus.CONFIRMED, ReservationStatus.BREACHED):
            return ReservationReport(ok=False)
        demand = dict(old.demand)
        duration = new_end - new_start
        if duration <= 0:
            raise ValueError("预约结束时间必须晚于开始时间")
        candidate_nodes = [node_id or old.node_id]
        feasible_node = candidate_nodes[0]
        if not self._window_available_without(candidate_nodes[0], demand, new_start, new_end, reservation_id):
            alternatives = [nid for nid in self.cluster.nodes if nid != old.node_id and self._window_available_without(nid, demand, new_start, new_end, reservation_id)]
            if not alternatives:
                return ReservationReport(ok=False, conflicts=[ConflictRange(old.experiment_id, old.stage_index, new_start, new_end, new_end, candidate_nodes[0], RESOURCE_SHORTAGE, message="adjustment has no feasible node")])
            feasible_node = alternatives[0]
        released = self.reservations.cancel(reservation_id, "adjusted")
        new_id = f"{reservation_id}-r{len(self.reservations.to_list()) + 1}"
        item = Reservation(new_id, old.experiment_id, old.stage_index, feasible_node, new_start, new_end, demand, replaces=reservation_id)
        self.reservations.add(item)
        self._bind_queue_reservation(item)
        return ReservationReport(ok=True, reservations=[item], released_capacity=[released] if released else [])

    def _window_available_without(self, node_id: str, demand: dict[str, int], start: int, end: int, exclude_id: str) -> bool:
        node = self.cluster.nodes[node_id]
        if any(start < outage_end and outage_start < end for outage_start, outage_end in self.outages.get(node_id, [])):
            return False
        for time in range(start, end):
            used = {name: 0 for name in node.resource_capacity}
            for item in self.reservations.active_promises(include_breached=True):
                if item.node_id == node_id and item.start <= time < item.end and item.reservation_id != exclude_id:
                    for name, amount in item.demand.items():
                        used[name] = used.get(name, 0) + amount
            if any(used.get(name, 0) + amount > node.resource_capacity.get(name, 0) for name, amount in demand.items()):
                return False
        return True

    def reservation_capacity_view(self, time: Optional[int] = None) -> dict[str, dict[str, dict[str, int]]]:
        time = self.now if time is None else time
        view = {}
        for node_id, node in self.cluster.nodes.items():
            promised = {name: 0 for name in node.resource_capacity}
            actual = {name: 0 for name in node.resource_capacity}
            if self.now <= time:
                for eid, demand in self.cluster.runtime_for(node_id).allocations.items():
                    end_times = [t for t, _, (kind, other) in self._heap if kind in ("stage_end", "fail") and other == eid and t > time]
                    if end_times:
                        for name, amount in demand.items():
                            actual[name] += amount
            for item in self.reservations.active_promises(include_breached=True):
                if item.node_id == node_id and item.start <= time < item.end:
                    for name, amount in item.demand.items():
                        promised[name] += amount
            capacity = node.resource_capacity
            view[node_id] = {
                "capacity": dict(capacity),
                "actual": actual,
                "promised": promised,
                "available": {name: capacity[name] - actual[name] - promised[name] for name in capacity},
            }
        return view

    def evaluate_reservation_change(
        self,
        *,
        cancel_ids: Optional[list[str]] = None,
        adjustments: Optional[dict[str, tuple[int, int]]] = None,
        end_time: Optional[int] = None,
    ) -> dict:
        """在临时副本上评估取消/调整，返回终态、启动时间和等待原因对比。"""
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
            state_path = Path(handle.name)
        try:
            self.save_state(state_path)
            baseline = Scheduler.load_state(state_path)
            baseline.run_until(end_time=end_time)
            before = {
                "statuses": {eid: rec.status.value for eid, rec in baseline.records.items()},
                "start_times": {eid: rec.start_time for eid, rec in baseline.records.items()},
                "end_times": {eid: rec.end_time for eid, rec in baseline.records.items()},
                "waiting_reasons": dict(baseline.last_blockages),
            }
            scenario = Scheduler.load_state(state_path)
            released: list[str] = []
            for reservation_id in cancel_ids or []:
                report = scenario.cancel_reservation(reservation_id, "what_if_cancel")
                if report.ok:
                    released.append(reservation_id)
            for reservation_id, (new_start, new_end) in (adjustments or {}).items():
                report = scenario.adjust_reservation(reservation_id, new_start, new_end)
                if not report.ok:
                    return {
                        "ok": False,
                        "reservation_id": reservation_id,
                        "conflicts": [c.to_dict() for c in report.conflicts],
                    }
            scenario.run_until(end_time=end_time)
            return {
                "ok": True,
                "released": released,
                "before": before,
                "after": {
                    "statuses": {eid: rec.status.value for eid, rec in scenario.records.items()},
                    "start_times": {eid: rec.start_time for eid, rec in scenario.records.items()},
                    "end_times": {eid: rec.end_time for eid, rec in scenario.records.items()},
                    "waiting_reasons": dict(scenario.last_blockages),
                },
            }
        finally:
            state_path.unlink(missing_ok=True)

    def _cancel_future_reservations(self, experiment_id: str, completed_stage: int, reason: str) -> None:
        for item in self.reservations.list(ReservationStatus.CONFIRMED):
            if item.experiment_id == experiment_id and item.stage_index >= completed_stage:
                self.reservations.cancel(item.reservation_id, reason)

    # ---- 外部干预：取消 / 注入失败 --------------------------------------

    def request_cancel(self, eid: str, at_time: int) -> None:
        if at_time < self.now:
            raise ValueError("不能在过去的时间取消任务")
        self._push(max(at_time, self.now + 1) if at_time == self.now else at_time, ("cancel", eid), key=eid)

    def request_fail(self, eid: str, at_time: int) -> None:
        if at_time < self.now:
            raise ValueError("不能在过去的时间注入失败")
        fire = at_time if at_time > self.now else self.now + 1
        self._push(fire, ("fail", eid), key=eid)

    def request_early_finish(self, eid: str, at_time: int) -> None:
        if at_time < self.now:
            raise ValueError("不能在过去的时间标记提前结束")
        fire = at_time if at_time > self.now else self.now + 1
        record = self.records.get(eid)
        if record is not None and record.status is RunStatus.RUNNING:
            self._remove_existing_stage_end(eid)
        self._push(fire, ("early_finish", eid), key=eid)

    # ---- 事件堆 ---------------------------------------------------------

    def _push(self, time: int, payload: tuple, key: Optional[str] = None) -> None:
        seq = next(self._counter)
        heapq.heappush(self._heap, (time, seq, payload))
        if key is not None:
            self._event_for[key] = (time, seq)

    # ---- 持久化 ---------------------------------------------------------

    def save_state(self, path: str | Path) -> None:
        data = {
            "now": self.now,
            "starvation_threshold": self.queue.starvation_threshold,
            "nodes": [
                {
                    "node_id": n.node_id,
                    "memory_mb": n.memory_mb,
                    "max_jobs": n.max_jobs,
                    "capacities": dict(n.capacities),
                }
                for n in self.cluster.nodes.values()
            ],
            "runtimes": {
                node_id: runtime.to_dict() for node_id, runtime in self.cluster.runtime.items()
            },
            "outages": {
                node_id: [list(window) for window in windows]
                for node_id, windows in self.outages.items()
            },
            "requests": [
                {
                    "experiment_id": r.experiment_id,
                    "memory_mb": r.memory_mb,
                    "duration": r.duration,
                    "submit_time": r.submit_time,
                    "name": r.name,
                    "priority": r.priority,
                    "stages": (
                        None
                        if r.stages is None
                        else [
                            {
                                "memory_mb": s.memory_mb,
                                "duration": s.duration,
                                "safe_point": s.safe_point,
                                "resources": dict(s.resources),
                                "migratable": s.migratable,
                            }
                            for s in r.stages
                        ]
                    ),
                }
                for r in self.requests.values()
            ],
            "records": [rec.to_dict() for rec in self.records.values()],
            "heap": [
                {"time": t, "seq": s, "payload_kind": p[0], "experiment_id": p[1]}
                for t, s, p in sorted(self._heap)
            ],
            "counter": max((s for _, s, _ in self._heap), default=-1),
            "pending_arrivals": sorted(self.pending_arrivals),
            "schedule_budget": self.schedule_budget,
            "last_blockages": dict(self.last_blockages),
            "yield_requested": [
                {"victim": victim, "requester": requester}
                for victim, requester in sorted(self._yield_requested.items())
            ],
            "reservations": self.reservations.to_list(),
            "event_for": {k: list(v) for k, v in self._event_for.items()},
        }
        Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    @classmethod
    def load_state(cls, path: str | Path) -> "Scheduler":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        nodes = [Node(**n) for n in data["nodes"]]
        scheduler = cls(nodes, starvation_threshold=data["starvation_threshold"], now=data["now"])
        scheduler.outages = {
            node_id: [tuple(window) for window in windows]
            for node_id, windows in data.get("outages", {}).items()
        }
        for node_id in scheduler.cluster.runtime:
            scheduler.outages.setdefault(node_id, [])
        scheduler.schedule_budget = data.get("schedule_budget")
        for raw in data["requests"]:
            raw = dict(raw)
            stages_raw = raw.pop("stages", None)
            raw["stages"] = (
                None
                if stages_raw is None
                else tuple(
                    Stage(
                        memory_mb=s["memory_mb"],
                        duration=s["duration"],
                        safe_point=s.get("safe_point", True),
                        resources=dict(s.get("resources", {})),
                        migratable=s.get("migratable", False),
                    )
                    for s in stages_raw
                )
            )
            scheduler.requests[raw["experiment_id"]] = ExperimentRequest(**raw)
        for raw in data["records"]:
            record = RunRecord.from_dict(raw)
            scheduler.records[record.experiment_id] = record
            if (
                record.status in (RunStatus.PENDING, RunStatus.WAITING)
                and record.experiment_id not in data.get("pending_arrivals", [])
                and record.pinned_node_id is None
            ):
                req = scheduler.requests[record.experiment_id]
                scheduler._enqueue_request(
                    req,
                    stage_index=record.stage_index,
                    pinned_node_id=record.pinned_node_id,
                )
        runtime_data = data.get("runtimes", {})
        if runtime_data:
            for node_id, raw_runtime in runtime_data.items():
                scheduler.cluster.runtime_for(node_id).load_dict(raw_runtime)
        else:
            for eid, record in scheduler.records.items():
                if record.status is RunStatus.RUNNING and record.node_id:
                    req = scheduler.requests[eid]
                    scheduler.cluster.runtime_for(record.node_id).allocate(
                        eid,
                        req.stage_list[record.stage_index].resource_request,
                    )
        scheduler.pending_arrivals = set(data.get("pending_arrivals", []))
        scheduler.reservations.load_list(data.get("reservations", []))
        for entry in scheduler.queue.pending:
            reservation = scheduler.reservations.confirmed_for(
                entry.experiment_id,
                entry.stage_index,
                at_time=scheduler.now,
            )
            if reservation is not None:
                entry.reservation_id = reservation.reservation_id
                entry.pinned_node_id = reservation.node_id
                entry.reserved_start = reservation.start
                entry.deadline = reservation.end
        scheduler.last_blockages = dict(data.get("last_blockages", {}))
        raw_yield = data.get("yield_requested", [])
        scheduler._yield_requested = {
            item["victim"]: item["requester"] for item in raw_yield
        } if raw_yield and isinstance(raw_yield[0], dict) else {}
        for raw in data["heap"]:
            heapq.heappush(
                scheduler._heap,
                (raw["time"], raw["seq"], (raw["payload_kind"], raw["experiment_id"])),
            )
        scheduler._counter = itertools.count(data.get("counter", -1) + 1)
        scheduler._refresh_runtime_availability()
        return scheduler
