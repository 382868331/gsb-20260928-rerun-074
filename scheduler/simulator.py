"""离散事件模拟器：提交、放置、完成、失败、取消与节点临时不可用。"""

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
)
from .queue import FairQueuePolicy, QueueEntry


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


class Scheduler:
    def __init__(
        self,
        nodes: list[Node],
        starvation_threshold: int = 20,
        now: int = 0,
    ) -> None:
        self.cluster = Cluster(nodes)
        self.queue = FairQueuePolicy(starvation_threshold=starvation_threshold)
        self.now = now
        self.records: dict[str, RunRecord] = {}
        self.requests: dict[str, ExperimentRequest] = {}
        self._heap: list[tuple[int, int, str]] = []
        self._counter = itertools.count()
        self._event_for: dict[str, tuple[int, int]] = {}
        self.pending_arrivals: set[str] = set()
        # 阶段结束时恰好被高优先任务选中让出：不自动续下一阶段
        self.yield_pending: set[str] = set()
        self._active_event_seq: dict[str, int] = {}
        # 受害者在自身安全点停下后，等到全体让出时刻才重新参与放置
        self._yield_until: dict[str, int] = {}
        self._held: set[str] = set()

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
        record.log(request.submit_time, "submitted", f"{request.memory_mb}MB/{request.duration}t")
        self.records[request.experiment_id] = record
        if request.submit_time <= self.now:
            self.queue.add(self._entry_for(request.experiment_id))
        else:
            self._push(request.submit_time, ("arrive", request.experiment_id))
            self.pending_arrivals.add(request.experiment_id)
        return record

    def _assert_capacity_feasible(self, request: ExperimentRequest) -> None:
        for idx, stage in enumerate(request.stage_specs):
            if not any(
                n.memory_mb >= stage.memory_mb and n.max_jobs >= 1
                for n in self.cluster.nodes.values()
            ):
                raise CapacityInfeasibleError(
                    f"实验 {request.experiment_id} 阶段 {idx} 需要 {stage.memory_mb}MB，"
                    "超过任何节点的单机容量，不会排产不存在的容量"
                )

    def _entry_for(self, eid: str) -> QueueEntry:
        req = self.requests[eid]
        stage = self.records[eid].current_stage
        spec = req.stage_specs[stage]
        return QueueEntry(
            experiment_id=eid,
            memory_mb=spec.memory_mb,
            submit_time=req.submit_time,
            duration=spec.duration - self.records[eid].stage_elapsed,
            priority=req.priority,
            stage=stage,
        )

    # ---- 主循环 ---------------------------------------------------------

    def run_until(self, end_time: Optional[int] = None) -> SimulationResult:
        """推进模拟；end_time 为空时跑到队列清空、无在跑任务为止。"""
        result = SimulationResult(now=self.now)
        while True:
            if end_time is not None and self.now >= end_time:
                self._schedule_pending(result)
                break
            if not self._heap and not self.queue:
                self._schedule_pending(result)
                break
            if not self._heap:
                self._schedule_pending(result)
                # 队列非空但无事件：安全前进一步，避免死循环
                self.now += 1
                continue
            fire_time = self._heap[0][0]
            if end_time is not None:
                fire_time = min(fire_time, end_time)
            if fire_time > self.now:
                self._schedule_pending(result)
            self.now = fire_time
            self._process_events_at(fire_time, result)
            self._schedule_until_settled(result)
        result.now = self.now
        return result

    def _schedule_until_settled(self, result: SimulationResult) -> None:
        """同一时刻反复放置：阶段结束/让出释放的容量立刻可服务等待任务。"""
        while True:
            before = len(result.started)
            self._schedule_pending(result)
            if len(result.started) == before:
                return

    def _process_events_at(self, time: int, result: SimulationResult) -> None:
        due: list[tuple] = []
        while self._heap and self._heap[0][0] <= time:
            _, seq, payload = heapq.heappop(self._heap)
            kind, eid = payload
            due.append((kind, eid, seq))
        for kind, eid, seq in due:
            if kind == "arrive":
                self.queue.add(self._entry_for(eid))
                self.pending_arrivals.discard(eid)
                continue
            if kind == "requeue":
                if self.records[eid].status is RunStatus.WAITING:
                    self.queue.add(self._entry_for(eid))
                self._held.discard(eid)
                self.pending_arrivals.discard(eid)
                continue
            if kind == "outage_on":
                self._apply_outage(eid, available=False, result=result)
            elif kind == "outage_off":
                self._apply_outage(eid, available=True, result=result)
            elif kind == "complete":
                self._finish(eid, RunStatus.COMPLETED, result)
            elif kind == "stage":
                if self._is_active(eid, seq):
                    self._handle_stage_event(eid, result)
            elif kind == "yield":
                if self._is_active(eid, seq):
                    self._handle_yield(eid, result)
            elif kind == "fail":
                if self._is_active(eid, seq) or eid not in self._active_event_seq:
                    self._finish(eid, RunStatus.FAILED, result)
            elif kind == "cancel":
                self._cancel(eid, result)
            elif kind == "node_exit":
                self._apply_node_exit(eid, result)

    # ---- 调度放置 -------------------------------------------------------

    def _schedule_pending(self, result: SimulationResult) -> None:
        release_events = self._collect_release_events()
        targets = self.queue.plan_targets(self.now, self.cluster, release_events)
        for entry in self.queue.ordered(self.now):
            eid = entry.experiment_id
            if eid in self._held:
                continue
            target = targets.get(eid)
            if target is None or target.start_time > self.now:
                continue
            runtime = self.cluster.runtime_for(target.node_id)
            node_id = target.node_id if runtime.can_fit(entry.memory_mb) else None
            if node_id is None:
                continue
            self._start(eid, node_id, result)
        self._plan_preemptions(result)

    def _collect_release_events(self) -> dict[str, list[tuple[int, int]]]:
        events: dict[str, list[tuple[int, int]]] = {}
        for fire_time, _, (kind, eid) in self._heap:
            if kind == "node_exit":
                for victim_id, mem in self.cluster.runtime_for(eid).running.items():
                    rec = self.records.get(victim_id)
                    if rec is not None and rec.status is RunStatus.RUNNING and not self._has_event_before(victim_id, fire_time):
                        events.setdefault(eid, []).append((fire_time, mem))
                continue
            if kind not in ("stage", "fail"):
                continue
            record = self.records.get(eid)
            if record is None or record.status is not RunStatus.RUNNING or record.node_id is None:
                continue
            mem = self.requests[eid].stage_specs[record.current_stage].memory_mb
            events.setdefault(record.node_id, []).append((fire_time, mem))
        return events

    def _has_event_before(self, eid: str, t: int) -> bool:
        scheduled = self._event_for.get(eid)
        return scheduled is not None and scheduled[0] < t

    def _start(self, eid: str, node_id: str, result: SimulationResult) -> None:
        req = self.requests[eid]
        record = self.records[eid]
        stage_idx = record.current_stage
        spec = req.stage_specs[stage_idx]
        self.cluster.runtime_for(node_id).allocate(eid, spec.memory_mb)
        resumed = record.status is RunStatus.WAITING or record.stage_elapsed > 0
        record.status = RunStatus.RUNNING
        record.node_id = node_id
        if record.start_time is None:
            record.start_time = self.now
        record.stage_started_at = self.now - record.stage_elapsed
        record.log(
            self.now,
            "stage_started" if resumed or stage_idx > 0 else "started",
            f"node={node_id} stage={stage_idx} mem={spec.memory_mb} elapsed={record.stage_elapsed}",
        )
        self.queue.remove(eid)
        remaining = spec.duration - record.stage_elapsed
        self._push_active(self.now + remaining, ("stage", eid), key=eid)
        result.started.append(eid)

    def _push_active(self, time: int, payload: tuple, key: str) -> None:
        seq = next(self._counter)
        heapq.heappush(self._heap, (time, seq, payload))
        self._event_for[key] = (time, seq)
        self._active_event_seq[key] = seq

    def _is_active(self, eid: str, seq: int) -> bool:
        return self._active_event_seq.get(eid) == seq

    # ---- 多阶段：边界、显式等待与安全点让出 -----------------------------

    def _handle_stage_event(self, eid: str, result: SimulationResult) -> None:
        record = self.records.get(eid)
        if record is None or record.status is not RunStatus.RUNNING:
            return
        req = self.requests[eid]
        node_id = record.node_id
        finished_stage = record.current_stage
        released = self.cluster.runtime_for(node_id).release(eid)
        assert released
        self._event_for.pop(eid, None)
        self._active_event_seq.pop(eid, None)
        record.current_stage += 1
        record.stage_elapsed = 0
        record.stage_started_at = None
        record.log(self.now, "stage_completed", f"stage={finished_stage} node={node_id}")
        if record.current_stage >= len(req.stage_specs):
            record.status = RunStatus.COMPLETED
            record.end_time = self.now
            record.log(self.now, "completed", f"stages={len(req.stage_specs)}")
            result.completed.append(eid)
            return
        if eid in self.yield_pending:
            self.yield_pending.discard(eid)
            hold_until = self._yield_until.get(eid, self.now)
            self._enter_waiting(eid, result, detail="yield at stage boundary", hold_until=hold_until)
            return
        next_spec = req.stage_specs[record.current_stage]
        runtime = self.cluster.runtime_for(node_id)
        if runtime.can_fit(next_spec.memory_mb):
            runtime.allocate(eid, next_spec.memory_mb)
            record.stage_started_at = self.now
            self._push_active(self.now + next_spec.duration, ("stage", eid), key=eid)
            record.log(self.now, "stage_started",
                       f"node={node_id} stage={record.current_stage} mem={next_spec.memory_mb}")
        else:
            # 下一阶段放不下：进入明确等待，释放旧容量供其他实验使用
            self._enter_waiting(eid, result, detail="next stage lacks capacity")

    def _enter_waiting(self, eid: str, result: SimulationResult, detail: str,
                       hold_until: Optional[int] = None) -> None:
        record = self.records[eid]
        record.status = RunStatus.WAITING
        record.node_id = None
        record.log(self.now, "waiting", f"stage={record.current_stage} {detail}")
        self._yield_until.pop(eid, None)
        if hold_until is not None and hold_until > self.now:
            self.pending_arrivals.add(eid)
            self._held.add(eid)
            self._push(hold_until, ("requeue", eid))
        else:
            self.queue.add(self._entry_for(eid))
        result.waiting.append(eid)

    def _handle_yield(self, eid: str, result: SimulationResult) -> None:
        record = self.records.get(eid)
        if record is None or record.status is not RunStatus.RUNNING:
            return
        req = self.requests[eid]
        stage_idx = record.current_stage
        spec = req.stage_specs[stage_idx]
        node_id = record.node_id
        elapsed = self.now - (record.stage_started_at if record.stage_started_at is not None else self.now)
        released = self.cluster.runtime_for(node_id).release(eid)
        assert released
        self._event_for.pop(eid, None)
        self._active_event_seq.pop(eid, None)
        record.stage_elapsed = elapsed
        record.stage_started_at = None
        record.node_id = None
        record.log(self.now, "yielded", f"stage={stage_idx} elapsed={elapsed}")
        hold_until = self._yield_until.get(eid, self.now)
        self._enter_waiting(eid, result, detail="preempted at safe point",
                            hold_until=hold_until)
        result.yielded.append(eid)

    # ---- 高优先任务请求让出（只在受害者声明的安全点接续）-----------------

    def _plan_preemptions(self, result: SimulationResult) -> None:
        for entry in self.queue.ordered(self.now):
            if entry.priority <= 0:
                continue
            self._request_yield_for(entry)

    def _request_yield_for(self, entry: QueueEntry) -> None:
        eid = entry.experiment_id
        needed = entry.memory_mb
        # 选最近的、由低于该优先级的在跑任务释放安全点容量即可满足的节点
        best: Optional[tuple[int, str, list[str]]] = None
        for node_id, runtime in self.cluster.runtime.items():
            if not runtime.available:
                continue
            victims = self._victim_plan(node_id, needed, entry.priority, eid)
            if victims is None:
                continue
            yield_time = max(v[1] for v in victims)
            if best is None or (yield_time, node_id) < (best[0], best[1]):
                best = (yield_time, node_id, [(v[0], v[1]) for v in victims])
        if best is None:
            return
        yield_time, node_id, victim_ids = best
        for victim_id, victim_safe_time in victim_ids:
            self._arm_yield(victim_id, victim_safe_time, yield_time)

    def _victim_plan(self, node_id: str, needed: int, priority: int, requester: str):
        runtime = self.cluster.runtime_for(node_id)
        free_after_mem = runtime.free_memory()
        free_after_slots = runtime.free_slots()
        chosen: list[tuple[str, int, int]] = []
        candidates: list[tuple[int, int, str]] = []
        for victim_id, mem in runtime.running.items():
            req = self.requests[victim_id]
            rec = self.records[victim_id]
            if req.priority >= priority or rec.status is not RunStatus.RUNNING:
                continue
            spec = req.stage_specs[rec.current_stage]
            nxt = self._next_safe_point(rec, spec)
            if nxt is None:
                continue
            candidates.append((nxt, mem, victim_id))
        candidates.sort()
        for nxt, mem, victim_id in candidates:
            free_after_mem += mem
            free_after_slots += 1
            chosen.append((victim_id, nxt, mem))
            if free_after_mem >= needed and free_after_slots >= 1:
                break
        if free_after_mem < needed or free_after_slots < 1:
            return None
        return chosen

    def _next_safe_point(self, record: RunRecord, spec) -> Optional[int]:
        stage_start = record.stage_started_at if record.stage_started_at is not None else self.now
        for offset in spec.safe_points():
            t = stage_start + offset
            if t > self.now:
                return t
        return None

    def _arm_yield(self, victim_id: str, safe_time: int, release_at: int) -> None:
        record = self.records[victim_id]
        req = self.requests[victim_id]
        spec = req.stage_specs[record.current_stage]
        stage_start = record.stage_started_at if record.stage_started_at is not None else self.now
        offset = safe_time - stage_start
        if offset == spec.duration:
            self.yield_pending.add(victim_id)
            self._yield_until[victim_id] = release_at
            return
        if offset > spec.duration:
            return
        scheduled = self._event_for.get(victim_id)
        if scheduled is not None and scheduled[0] <= safe_time:
            return
        self._push_active(safe_time, ("yield", victim_id), key=victim_id)
        self._yield_until[victim_id] = release_at

    def _finish(self, eid: str, status: RunStatus, result: SimulationResult) -> None:
        record = self.records.get(eid)
        if record is None or record.status is not RunStatus.RUNNING:
            return
        runtime = self.cluster.runtime_for(record.node_id)
        released = runtime.release(eid)  # 幂等：只释放一次
        assert released
        record.status = status
        record.end_time = self.now
        record.log(self.now, status.value, f"node={record.node_id}")
        self._event_for.pop(eid, None)
        self._active_event_seq.pop(eid, None)
        (result.completed if status is RunStatus.COMPLETED else result.failed).append(eid)

    def _cancel(self, eid: str, result: SimulationResult) -> None:
        record = self.records.get(eid)
        if record is None or record.status.terminal:
            return
        if record.status is RunStatus.RUNNING:
            released = self.cluster.runtime_for(record.node_id).release(eid)
            assert released
        else:
            if not self.queue.remove(eid):
                return
        record.status = RunStatus.CANCELLED
        record.end_time = self.now
        record.log(self.now, "cancelled")
        self._event_for.pop(eid, None)
        self._active_event_seq.pop(eid, None)
        result.cancelled.append(eid)

    # ---- 节点临时不可用 -------------------------------------------------

    def add_outage(self, node_id: str, outage: Outage) -> None:
        if node_id not in self.cluster.nodes:
            raise ValueError(f"未知节点 {node_id}")
        self._push(outage.start, ("outage_on", node_id), key=f"outage-on-{node_id}-{outage.start}")
        self._push(outage.end, ("outage_off", node_id), key=f"outage-off-{node_id}-{outage.end}")

    def remove_node(self, node_id: str, at_time: int) -> None:
        """节点永久退出：该时刻后不再接受任务，在跑任务失败并释放账目。"""
        if node_id not in self.cluster.nodes:
            raise ValueError(f"未知节点 {node_id}")
        if at_time < self.now:
            raise ValueError("不能在过去安排节点退出")
        fire = at_time
        self._push(fire, ("node_exit", node_id), key=f"node-exit-{node_id}")

    def _apply_node_exit(self, node_id: str, result: SimulationResult) -> None:
        runtime = self.cluster.runtime_for(node_id)
        runtime.available = False
        for eid in list(runtime.running.keys()):
            record = self.records.get(eid)
            if record is None or record.status is not RunStatus.RUNNING:
                continue
            runtime.release(eid)
            self._event_for.pop(eid, None)
            self._active_event_seq.pop(eid, None)
            record.status = RunStatus.FAILED
            record.node_id = None
            record.end_time = self.now
            record.log(self.now, "failed", f"node {node_id} exited")
            result.failed.append(eid)

    def _apply_outage(self, node_id: str, available: bool, result: SimulationResult) -> None:
        runtime = self.cluster.runtime_for(node_id)
        runtime.available = available
        if available:
            return
        # 不可用时，正在该节点上运行的任务失败并释放资源（模拟节点丢失）
        for eid in list(runtime.running.keys()):
            self._event_for.pop(eid, None)
            self._active_event_seq.pop(eid, None)
            record = self.records[eid]
            runtime.release(eid)
            record.status = RunStatus.FAILED
            record.node_id = None
            record.end_time = self.now
            record.log(self.now, "failed", f"node {node_id} unavailable")
            result.failed.append(eid)

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
                {"node_id": n.node_id, "memory_mb": n.memory_mb, "max_jobs": n.max_jobs}
                for n in self.cluster.nodes.values()
            ],
            "requests": [r.to_dict() for r in self.requests.values()],
            "records": [rec.to_dict() for rec in self.records.values()],
            "heap": [
                {"time": t, "seq": s, "payload_kind": p[0], "experiment_id": p[1]}
                for t, s, p in sorted(self._heap)
            ],
            "counter": max((s for _, s, _ in self._heap), default=-1),
            "pending_arrivals": sorted(self.pending_arrivals),
            "held_yields": sorted(self._held),
            "event_for": {k: list(v) for k, v in self._event_for.items()},
            "yield_pending": sorted(self.yield_pending),
            "active_event_seq": dict(self._active_event_seq),
            "node_available": {nid: rt.available for nid, rt in self.cluster.runtime.items()},
        }
        Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    @classmethod
    def load_state(cls, path: str | Path) -> "Scheduler":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        nodes = [Node(**n) for n in data["nodes"]]
        scheduler = cls(nodes, starvation_threshold=data["starvation_threshold"], now=data["now"])
        pending_arrivals = set(data.get("pending_arrivals", []))
        for raw in data["requests"]:
            scheduler.requests[raw["experiment_id"]] = ExperimentRequest.from_dict(raw)
        for raw in data["records"]:
            record = RunRecord.from_dict(raw)
            scheduler.records[record.experiment_id] = record
            if (
                record.status in (RunStatus.PENDING, RunStatus.WAITING)
                and record.experiment_id not in pending_arrivals
            ):
                scheduler.queue.add(scheduler._entry_for(record.experiment_id))
            elif record.status is RunStatus.RUNNING:
                req = scheduler.requests[record.experiment_id]
                rt = scheduler.cluster.runtime_for(record.node_id)
                spec = req.stage_specs[record.current_stage]
                rt.allocate(record.experiment_id, spec.memory_mb)
        scheduler.pending_arrivals = pending_arrivals
        scheduler._held = set(data.get("held_yields", []))
        scheduler.yield_pending = set(data.get("yield_pending", []))
        for nid, available in data.get("node_available", {}).items():
            scheduler.cluster.runtime_for(nid).available = available
        for raw in data["heap"]:
            heapq.heappush(
                scheduler._heap,
                (raw["time"], raw["seq"], (raw["payload_kind"], raw["experiment_id"])),
            )
        scheduler._counter = itertools.count(data.get("counter", -1) + 1)
        scheduler._active_event_seq = {k: v for k, v in data.get("active_event_seq", {}).items()}
        return scheduler
