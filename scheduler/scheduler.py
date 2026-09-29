"""作业放置核心：内存与并发双重限制、公平预留、单次释放、状态持久化。"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from .models import Job, JobState, Node, RunRecord, TERMINAL_STATES
from .policy import FairAgingPolicy, FIFOQueuePolicy, QueuePolicy


class Scheduler:
    def __init__(self, policy: Optional[QueuePolicy] = None):
        self.policy: QueuePolicy = policy or FairAgingPolicy()
        self.nodes: Dict[str, Node] = {}
        self.jobs: Dict[str, Job] = {}
        # 全部运行记录；用 end_time is None 判断“当前占用资源”的记录
        self.runs: List[RunRecord] = []
        # 预留队列：job_id 的进入顺序；目标节点冻结，只留给预留任务
        self.reservation_order: List[str] = []
        self.reservation_target: Dict[str, str] = {}

    # ---- 集群管理 -------------------------------------------------------

    def add_node(self, node: Node) -> None:
        node.validate()
        if node.id in self.nodes:
            raise ValueError(f"节点 {node.id} 已存在")
        self.nodes[node.id] = node

    def set_node_available(
        self, node_id: str, available: bool, now: int, kill_running: bool = False
    ) -> List[str]:
        """节点临时不可用 / 恢复。

        kill_running=False（默认 cordon）：在跑任务继续跑完，但不再放新任务；
        kill_running=True：在跑任务立即失败并各释放一次资源。
        返回受影响（被杀掉）的 job_id。
        """
        node = self.nodes[node_id]
        node.available = available
        killed: List[str] = []
        if not available and kill_running:
            for job in list(self.jobs.values()):
                if job.state is JobState.RUNNING and job.node_id == node_id:
                    self._release(job, now, "node_down")
                    job.state = JobState.FAILED
                    killed.append(job.id)
        return killed

    # ---- 提交与取消 -----------------------------------------------------

    def submit(self, job: Job) -> JobState:
        job.validate()
        if job.id in self.jobs:
            raise ValueError(f"实验 {job.id} 已存在，实验身份不可重复")
        # 计划里不能安排不存在的容量：超过所有节点物理上限的需求直接拒绝
        if job.memory_mb > max(n.memory_mb for n in self.nodes.values()):
            job.state = JobState.REJECTED
            job.finish_time = job.submit_time
            self.jobs[job.id] = job
            return JobState.REJECTED
        job.state = JobState.QUEUED
        self.jobs[job.id] = job
        return JobState.QUEUED

    def cancel(self, job_id: str, now: int) -> JobState:
        job = self.jobs[job_id]
        if job.state in TERMINAL_STATES:
            return job.state
        if job.state in (JobState.QUEUED, JobState.RESERVED):
            # 任务尚未开始，不占资源，无资源可释放
            if job.state is JobState.RESERVED:
                self._drop_reservation(job_id)
            job.state = JobState.CANCELLED
            job.finish_time = now
        elif job.state is JobState.RUNNING:
            # 取消只释放一次：释放逻辑内部有幂等保护
            self._release(job, now, "cancelled")
            job.state = JobState.CANCELLED
        return job.state

    # ---- 占用视图 -------------------------------------------------------

    def _open_run(self, job_id: str) -> Optional[RunRecord]:
        for run in self.runs:
            if run.job_id == job_id and not run.is_closed:
                return run
        return None

    def used_memory(self, node_id: str) -> int:
        return sum(
            r.memory_mb
            for r in self.runs
            if not r.is_closed and r.node_id == node_id
        )

    def running_count(self, node_id: str) -> int:
        return sum(
            1 for r in self.runs if not r.is_closed and r.node_id == node_id
        )

    def can_place(self, job: Job, node: Node) -> bool:
        if not node.available:
            return False
        if self.used_memory(node.id) + job.memory_mb > node.memory_mb:
            return False
        if self.running_count(node.id) + 1 > node.max_running:
            return False
        return True

    # ---- 单个时间步的调度 ----------------------------------------------

    def complete(self, now: int) -> List[str]:
        """第一阶段：正常完成到期任务（只释放一次）。返回完成的 job_id。"""
        finished: List[str] = []
        for job in list(self.jobs.values()):
            if (
                job.state is JobState.RUNNING
                and job.start_time is not None
                and now >= job.start_time + job.duration
            ):
                self._release(job, now, "completed")
                job.state = JobState.COMPLETED
                finished.append(job.id)
        return finished

    def schedule_pending(self, now: int) -> List[str]:
        """第二阶段：维护预留并放置作业。返回本步启动的作业。"""
        started: List[str] = []
        # 1) 老化触发预留（含恢复加载后的状态）
        threshold = self.policy.reserve_threshold
        if threshold is not None:
            for job in self.jobs.values():
                if job.state is JobState.QUEUED and now - job.submit_time >= threshold:
                    job.state = JobState.RESERVED
                    job.reserved_at = now
                    self.reservation_order.append(job.id)
            # 目标节点不可用时改挂到其它可用节点，保留进入预留的时间
            for job_id in self.reservation_order:
                job = self.jobs[job_id]
                if job.state is not JobState.RESERVED:
                    continue
                target = self.reservation_target.get(job_id)
                if target is None or not self.nodes[target].available:
                    new_target = self._pick_reservation_target(job)
                    if new_target is not None:
                        self.reservation_target[job_id] = new_target.id

        # 2) 预留任务优先在冻结的目标节点上启动（先进入预留者先启动）
        for job_id in list(self.reservation_order):
            job = self.jobs[job_id]
            if job.state is not JobState.RESERVED:
                self._drop_reservation(job_id)
                continue
            node = self.nodes.get(self.reservation_target.get(job_id, ""))
            if node is not None and self.can_place(job, node):
                self._start(job, node, now)
                self._drop_reservation(job_id)
                started.append(job_id)

        # 3) 普通任务保守回填：不得使用任何预留目标节点（冻结资源）
        frozen = {
            nid
            for jid, nid in self.reservation_target.items()
            if self.jobs[jid].state is JobState.RESERVED
        }
        queued = [j for j in self.jobs.values() if j.state is JobState.QUEUED]
        for job in self.policy.backfill_order(queued, now):
            node = self._best_fit_node(job, frozen)
            if node is not None:
                self._start(job, node, now)
                started.append(job.id)
        return started

    def tick(self, now: int) -> List[str]:
        """单步推进：完成到期任务 → 维护预留 → 放置作业。

        直接调用时本 tick 没有外部事件；有外部事件时模拟器会在
        complete() 与 schedule_pending() 之间插入提交/取消/节点事件。
        """
        self.complete(now)
        return self.schedule_pending(now)

    def _pick_reservation_target(self, job: Job) -> Optional[Node]:
        """选物理上能放下该任务的最小容量可用节点（预留后该节点被冻结）。"""
        candidates = [
            n
            for n in self.nodes.values()
            if n.available and n.memory_mb >= job.memory_mb
        ]
        # 尽量选还没有被其它预留占用的节点，避免互相阻塞
        targeted = set(self.reservation_target.values())
        fresh = [n for n in candidates if n.id not in targeted]
        pool = fresh or candidates
        return min(pool, key=lambda n: (n.memory_mb, n.id)) if pool else None

    def _best_fit_node(self, job: Job, forbidden: set) -> Optional[Node]:
        """最佳匹配：放完后剩余内存最少的节点，减少碎片；绝不超卖。"""
        feasible: List[Tuple[int, str, Node]] = []
        for node in self.nodes.values():
            if node.id in forbidden or not self.can_place(job, node):
                continue
            leftover = node.memory_mb - self.used_memory(node.id) - job.memory_mb
            feasible.append((leftover, node.id, node))
        return min(feasible, key=lambda x: (x[0], x[1]))[2] if feasible else None

    # ---- 启动 / 释放（幂等）--------------------------------------------

    def _start(self, job: Job, node: Node, now: int) -> None:
        if job.state not in (JobState.QUEUED, JobState.RESERVED):
            raise RuntimeError(f"实验 {job.id} 状态 {job.state} 不可启动")
        if not self.can_place(job, node):
            raise RuntimeError(f"节点 {node.id} 容量不足，拒绝安排 {job.id}")
        self.runs.append(
            RunRecord(
                job_id=job.id,
                node_id=node.id,
                memory_mb=job.memory_mb,
                start_time=now,
            )
        )
        job.state = JobState.RUNNING
        job.node_id = node.id
        job.start_time = now

    def _release(self, job: Job, now: int, result: str) -> None:
        """释放资源。取消/失败/节点故障与正常完成共用同一入口，保证只释放一次。"""
        if job.state is not JobState.RUNNING:
            return
        run = self._open_run(job.id)
        if run is None:
            # 防御性断言：运行中的作业必然存在未封口的运行记录
            raise RuntimeError(f"实验 {job.id} 运行中但找不到占用记录")
        run.end_time = now
        run.result = result
        job.node_id = None
        job.finish_time = now
        job.release_count += 1

    def _drop_reservation(self, job_id: str) -> None:
        self.reservation_target.pop(job_id, None)
        if job_id in self.reservation_order:
            self.reservation_order.remove(job_id)

    # ---- 快照持久化 -----------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "policy": self.policy.to_dict()
            if isinstance(self.policy, FairAgingPolicy)
            else {"name": self.policy.name},
            "nodes": [
                {
                    "id": n.id,
                    "memory_mb": n.memory_mb,
                    "max_running": n.max_running,
                    "available": n.available,
                }
                for n in self.nodes.values()
            ],
            "jobs": [
                {
                    "id": j.id,
                    "memory_mb": j.memory_mb,
                    "duration": j.duration,
                    "submit_time": j.submit_time,
                    "priority": j.priority,
                    "state": j.state.value,
                    "node_id": j.node_id,
                    "start_time": j.start_time,
                    "finish_time": j.finish_time,
                    "reserved_at": j.reserved_at,
                    "release_count": j.release_count,
                }
                for j in self.jobs.values()
            ],
            "runs": [
                {
                    "job_id": r.job_id,
                    "node_id": r.node_id,
                    "memory_mb": r.memory_mb,
                    "start_time": r.start_time,
                    "end_time": r.end_time,
                    "result": r.result,
                }
                for r in self.runs
            ],
            "reservation_order": list(self.reservation_order),
            "reservation_target": dict(self.reservation_target),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Scheduler":
        policy = FairAgingPolicy.from_dict(data["policy"])
        sched = cls(policy=policy)
        for nd in data["nodes"]:
            node = Node(**nd)
            sched.nodes[node.id] = node
        for jd in data["jobs"]:
            jd = dict(jd)
            jd["state"] = JobState(jd["state"])
            job = Job(**jd)
            sched.jobs[job.id] = job
        for rd in data["runs"]:
            sched.runs.append(RunRecord(**rd))
        sched.reservation_order = list(data.get("reservation_order", []))
        sched.reservation_target = dict(data.get("reservation_target", {}))
        return sched
