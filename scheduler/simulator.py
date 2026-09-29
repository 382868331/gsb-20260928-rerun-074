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
            self.queue.add(QueueEntry(
                experiment_id=request.experiment_id,
                memory_mb=request.memory_mb,
                submit_time=request.submit_time,
                duration=request.duration,
            ))
        else:
            self._push(request.submit_time, ("arrive", request.experiment_id))
            self.pending_arrivals.add(request.experiment_id)
        return record

    def _assert_capacity_feasible(self, request: ExperimentRequest) -> None:
        if not any(
            n.memory_mb >= request.memory_mb and n.max_jobs >= 1
            for n in self.cluster.nodes.values()
        ):
            raise CapacityInfeasibleError(
                f"实验 {request.experiment_id} 需要 {request.memory_mb}MB，"
                "超过任何节点的单机容量，不会排产不存在的容量"
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
            self._schedule_pending(result)
        result.now = self.now
        return result

    def _process_events_at(self, time: int, result: SimulationResult) -> None:
        due: list[tuple] = []
        while self._heap and self._heap[0][0] <= time:
            _, _, payload = heapq.heappop(self._heap)
            kind, eid = payload
            due.append((kind, eid))
        for kind, eid in due:
            if kind == "arrive":
                req = self.requests[eid]
                self.queue.add(QueueEntry(eid, req.memory_mb, req.submit_time, req.duration))
                self.pending_arrivals.discard(eid)
                continue
            if kind == "outage_on":
                self._apply_outage(eid, available=False, result=result)
            elif kind == "outage_off":
                self._apply_outage(eid, available=True, result=result)
            elif kind == "complete":
                self._finish(eid, RunStatus.COMPLETED, result)
            elif kind == "fail":
                self._finish(eid, RunStatus.FAILED, result)
            elif kind == "cancel":
                self._cancel(eid, result)

    # ---- 调度放置 -------------------------------------------------------

    def _schedule_pending(self, result: SimulationResult) -> None:
        release_events = self._collect_release_events()
        targets = self.queue.plan_targets(self.now, self.cluster, release_events)
        for entry in self.queue.ordered(self.now):
            eid = entry.experiment_id
            target = targets.get(eid)
            if target is None or target.start_time > self.now:
                continue
            runtime = self.cluster.runtime_for(target.node_id)
            node_id = target.node_id if runtime.can_fit(entry.memory_mb) else None
            if node_id is None:
                continue
            self._start(eid, node_id, result)

    def _collect_release_events(self) -> dict[str, list[tuple[int, int]]]:
        events: dict[str, list[tuple[int, int]]] = {}
        for fire_time, _, (kind, eid) in self._heap:
            if kind not in ("complete", "fail"):
                continue
            record = self.records.get(eid)
            if record is None or record.status is not RunStatus.RUNNING or record.node_id is None:
                continue
            mem = self.requests[eid].memory_mb
            events.setdefault(record.node_id, []).append((fire_time, mem))
        return events

    def _start(self, eid: str, node_id: str, result: SimulationResult) -> None:
        req = self.requests[eid]
        record = self.records[eid]
        self.cluster.runtime_for(node_id).allocate(eid, req.memory_mb)
        record.status = RunStatus.RUNNING
        record.node_id = node_id
        record.start_time = self.now
        record.log(self.now, "started", f"node={node_id}")
        self.queue.remove(eid)
        end_time = self.now + req.duration
        self._push(end_time, ("complete", eid), key=eid)
        result.started.append(eid)

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
        (result.completed if status is RunStatus.COMPLETED else result.failed).append(eid)

    def _cancel(self, eid: str, result: SimulationResult) -> None:
        record = self.records.get(eid)
        if record is None or record.status.terminal:
            return
        if record.status is RunStatus.RUNNING:
            released = self.cluster.runtime_for(record.node_id).release(eid)
            assert released
        else:
            self.queue.remove(eid)
        record.status = RunStatus.CANCELLED
        record.end_time = self.now
        record.log(self.now, "cancelled")
        self._event_for.pop(eid, None)
        result.cancelled.append(eid)

    # ---- 节点临时不可用 -------------------------------------------------

    def add_outage(self, node_id: str, outage: Outage) -> None:
        if node_id not in self.cluster.nodes:
            raise ValueError(f"未知节点 {node_id}")
        self._push(outage.start, ("outage_on", node_id), key=f"outage-on-{node_id}-{outage.start}")
        self._push(outage.end, ("outage_off", node_id), key=f"outage-off-{node_id}-{outage.end}")

    def _apply_outage(self, node_id: str, available: bool, result: SimulationResult) -> None:
        runtime = self.cluster.runtime_for(node_id)
        runtime.available = available
        if available:
            return
        # 不可用时，正在该节点上运行的任务失败并释放资源（模拟节点丢失）
        for eid in list(runtime.running.keys()):
            self._event_for.pop(eid, None)
            record = self.records[eid]
            runtime.release(eid)
            record.status = RunStatus.FAILED
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
            "requests": [
                {
                    "experiment_id": r.experiment_id,
                    "memory_mb": r.memory_mb,
                    "duration": r.duration,
                    "submit_time": r.submit_time,
                    "name": r.name,
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
            "event_for": {k: list(v) for k, v in self._event_for.items()},
        }
        Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    @classmethod
    def load_state(cls, path: str | Path) -> "Scheduler":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        nodes = [Node(**n) for n in data["nodes"]]
        scheduler = cls(nodes, starvation_threshold=data["starvation_threshold"], now=data["now"])
        for raw in data["requests"]:
            scheduler.requests[raw["experiment_id"]] = ExperimentRequest(**raw)
        for raw in data["records"]:
            record = RunRecord.from_dict(raw)
            scheduler.records[record.experiment_id] = record
            if (
                record.status is RunStatus.PENDING
                and record.experiment_id not in data.get("pending_arrivals", [])
            ):
                req = scheduler.requests[record.experiment_id]
                scheduler.queue.add(QueueEntry(
                    record.experiment_id, req.memory_mb, record.submit_time, req.duration
                ))
            elif record.status is RunStatus.RUNNING:
                req = scheduler.requests[record.experiment_id]
                rt = scheduler.cluster.runtime_for(record.node_id)
                rt.allocate(record.experiment_id, req.memory_mb)
        scheduler.pending_arrivals = set(data.get("pending_arrivals", []))
        for raw in data["heap"]:
            heapq.heappush(
                scheduler._heap,
                (raw["time"], raw["seq"], (raw["payload_kind"], raw["experiment_id"])),
            )
        scheduler._counter = itertools.count(data.get("counter", -1) + 1)
        return scheduler
