"""虚拟计算节点、实验需求与运行记录的数据模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED)


class BlockReason(str, Enum):
    """排队/等待任务当前无法启动的原因，三者互斥、按此优先级判定。"""

    MAINTENANCE = "maintenance"        # 所有容量足够的节点都在维护/不可用
    BUDGET = "budget"                  # 集群调度预算（同时在跑阶段数）已用尽
    RESOURCE_SHORTAGE = "resource"     # 资源不足：等已在跑任务释放


@dataclass(frozen=True)
class Stage:
    """实验的一个执行阶段：同时占用多类资源，持续 duration 个时间单位。

    - memory_mb 是内存资源；resources 声明额外的具名资源（如 cpu、gpu），
      全部为整数份额，独占设备用 1 份表示独占。
    - 阶段资源是"全有或全无"：任何一类资源不满足都不会启动，等待期间
      不持有任何资源。
    - safe_point=True 表示本阶段结束边界是声明的安全点，高优先级任务只能在
      这些边界要求让出资源；False 表示该边界不可被打断接续。
    - migratable=True 表示节点维护开始时本阶段可以安全迁移到其他节点
      （保留阶段进度）；False 表示必须在本节点跑到阶段结束。
    """

    memory_mb: int
    duration: int
    safe_point: bool = True
    resources: Optional[dict[str, int]] = None
    migratable: bool = False

    def __post_init__(self) -> None:
        if self.memory_mb <= 0:
            raise ValueError("阶段内存必须为正数")
        if self.duration <= 0:
            raise ValueError("阶段时长必须为正数")
        for key, amount in (self.resources or {}).items():
            if key in ("memory_mb", "slots"):
                raise ValueError(f"resources 不能重复声明保留资源 {key}")
            if not isinstance(amount, int) or amount <= 0:
                raise ValueError(f"资源 {key} 的份额必须为正整数")

    def requirement(self) -> dict[str, int]:
        """本阶段的完整资源需求：内存 + 具名资源，全满足才可启动。"""
        req = {"memory_mb": self.memory_mb}
        req.update(self.resources or {})
        return req


def normalize_requirement(requirement):
    """兼容旧的单内存接口：int 视为 {"memory_mb": int}。"""
    if isinstance(requirement, int):
        return {"memory_mb": requirement}
    return dict(requirement)


@dataclass(frozen=True)
class Node:
    """虚拟计算节点：内存 + 并发槽 + 可选的具名资源（cpu、独占设备等）。"""

    node_id: str
    memory_mb: int
    max_jobs: int
    capacity: Optional[dict[str, int]] = None

    def __post_init__(self) -> None:
        if self.memory_mb <= 0:
            raise ValueError(f"节点 {self.node_id} 内存必须为正数")
        if self.max_jobs <= 0:
            raise ValueError(f"节点 {self.node_id} 并发上限必须为正数")
        for key, amount in (self.capacity or {}).items():
            if key in ("memory_mb", "slots"):
                raise ValueError(f"capacity 不能重复声明保留资源 {key}")
            if not isinstance(amount, int) or amount <= 0:
                raise ValueError(f"节点 {self.node_id} 资源 {key} 容量必须为正整数")

    def capacity_map(self) -> dict[str, int]:
        cap = {"memory_mb": self.memory_mb}
        cap.update(self.capacity or {})
        return cap


@dataclass(frozen=True)
class ExperimentRequest:
    """实验需求。experiment_id 是稳定身份，队列策略调整不改变它。"""

    experiment_id: str
    memory_mb: int
    duration: int
    submit_time: int = 0
    name: str = ""
    stages: Optional[tuple[Stage, ...]] = None
    priority: int = 0

    def __post_init__(self) -> None:
        if self.stages is not None:
            if not self.stages:
                raise ValueError(f"实验 {self.experiment_id} 至少需要一个阶段")
            for stage in self.stages:
                if not isinstance(stage, Stage):
                    raise TypeError("stages 必须是 Stage 列表")
        elif self.memory_mb <= 0:
            raise ValueError(f"实验 {self.experiment_id} 内存必须为正数")
        if self.stages is None and self.duration <= 0:
            raise ValueError(f"实验 {self.experiment_id} 运行时长必须为正数")
        if self.submit_time < 0:
            raise ValueError(f"实验 {self.experiment_id} 提交时间不能为负")
        if self.priority < 0:
            raise ValueError(f"实验 {self.experiment_id} 优先级不能为负")

    @property
    def stage_list(self) -> tuple[Stage, ...]:
        """统一阶段视图：单阶段旧接口自动包装为一个安全点阶段。"""
        if self.stages is not None:
            return self.stages
        return (Stage(self.memory_mb, self.duration, safe_point=True),)


@dataclass
class RunEvent:
    time: int
    kind: str
    detail: str = ""

    def to_dict(self) -> dict:
        return {"time": self.time, "kind": self.kind, "detail": self.detail}

    @classmethod
    def from_dict(cls, data: dict) -> "RunEvent":
        return cls(time=data["time"], kind=data["kind"], detail=data.get("detail", ""))


@dataclass
class RunRecord:
    """一次实验运行的全生命周期记录。"""

    experiment_id: str
    status: RunStatus = RunStatus.PENDING
    node_id: Optional[str] = None
    submit_time: int = 0
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    stage_index: int = 0
    stage_started_at: Optional[int] = None
    block_reason: Optional[BlockReason] = None
    events: list[RunEvent] = field(default_factory=list)

    def log(self, time: int, kind: str, detail: str = "") -> None:
        self.events.append(RunEvent(time, kind, detail))

    @property
    def wait_time(self) -> Optional[int]:
        if self.start_time is None:
            return None
        return self.start_time - self.submit_time

    def to_dict(self) -> dict:
        return {
            "experiment_id": self.experiment_id,
            "status": self.status.value,
            "node_id": self.node_id,
            "submit_time": self.submit_time,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "stage_index": self.stage_index,
            "stage_started_at": self.stage_started_at,
            "block_reason": self.block_reason.value if self.block_reason else None,
            "events": [e.to_dict() for e in self.events],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RunRecord":
        return cls(
            experiment_id=data["experiment_id"],
            status=RunStatus(data["status"]),
            node_id=data.get("node_id"),
            submit_time=data["submit_time"],
            start_time=data.get("start_time"),
            end_time=data.get("end_time"),
            stage_index=data.get("stage_index", 0),
            stage_started_at=data.get("stage_started_at"),
            block_reason=BlockReason(data["block_reason"]) if data.get("block_reason") else None,
            events=[RunEvent.from_dict(e) for e in data.get("events", [])],
        )


@dataclass(frozen=True)
class Outage:
    """节点临时不可用区间 [start, end)。"""

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError("不可用区间结束时间必须晚于开始时间")

    def covers(self, time: int) -> bool:
        return self.start <= time < self.end


@dataclass(frozen=True)
class MaintenanceWindow:
    """节点计划维护区间 [start, end)。

    与 Outage（突发故障，杀掉在跑任务）不同：维护是计划事件——
    - 区间内节点不接受新放置；
    - 开始时，当前阶段声明 migratable=True 的任务迁移到其他节点（保留
      阶段进度），无处可去则释放并显式等待；
    - migratable=False 的任务绝不打断，必须在本节点跑到阶段结束。
    """

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError("维护区间结束时间必须晚于开始时间")

    def covers(self, time: int) -> bool:
        return self.start <= time < self.end


class NodeRuntime:
    """节点的运行时占用状态。

    多类资源与并发槽在任务真正 start 时才占用（全有或全无）；
    release 幂等：取消、失败重复触发只释放一次。
    """

    def __init__(self, node: Node) -> None:
        self.node = node
        self.used: dict[str, int] = {}
        self.running: dict[str, dict[str, int]] = {}
        self.available = True
        self.in_maintenance = False

    @property
    def used_memory(self) -> int:
        return self.used.get("memory_mb", 0)

    def free_memory(self) -> int:
        return self.node.memory_mb - self.used.get("memory_mb", 0)

    def free_slots(self) -> int:
        return self.node.max_jobs - len(self.running)

    def free(self, resource: str) -> int:
        return self.node.capacity_map().get(resource, 0) - self.used.get(resource, 0)

    def can_fit(self, requirement) -> bool:
        if not self.available or self.in_maintenance:
            return False
        if self.free_slots() < 1:
            return False
        req = normalize_requirement(requirement)
        return all(amount <= self.free(res) for res, amount in req.items())

    def allocate(self, experiment_id: str, requirement) -> None:
        req = normalize_requirement(requirement)
        if experiment_id in self.running:
            raise RuntimeError(f"实验 {experiment_id} 已在节点 {self.node.node_id} 占用资源")
        if not self.can_fit(req):
            raise RuntimeError(
                f"节点 {self.node.node_id} 无法容纳 {req}："
                f"剩余内存 {self.free_memory()}，剩余并发槽 {self.free_slots()}，"
                f"维护中={self.in_maintenance}"
            )
        for res, amount in req.items():
            self.used[res] = self.used.get(res, 0) + amount
        self.running[experiment_id] = req

    def release(self, experiment_id: str) -> bool:
        """释放资源；返回是否真正执行释放（保证只释放一次）。"""
        req = self.running.pop(experiment_id, None)
        if req is None:
            return False
        for res, amount in req.items():
            self.used[res] -= amount
        return True

    def to_dict(self) -> dict:
        return {
            "used": dict(self.used),
            "running": {eid: dict(req) for eid, req in self.running.items()},
            "available": self.available,
            "in_maintenance": self.in_maintenance,
        }


class Cluster:
    """虚拟集群：管理节点与放置可行性。"""

    def __init__(self, nodes: list[Node]) -> None:
        if not nodes:
            raise ValueError("集群至少需要一个节点")
        ids = [n.node_id for n in nodes]
        if len(set(ids)) != len(ids):
            raise ValueError("节点 ID 必须唯一")
        self.nodes: dict[str, Node] = {n.node_id: n for n in nodes}
        self.runtime: dict[str, NodeRuntime] = {n.node_id: NodeRuntime(n) for n in nodes}

    def runtime_for(self, node_id: str) -> NodeRuntime:
        return self.runtime[node_id]

    def find_placement(
        self,
        requirement,
        exclude: Optional[set[str]] = None,
        reserved: Optional[dict[str, dict[str, int]]] = None,
        reserved_slots: Optional[dict[str, int]] = None,
    ) -> Optional[str]:
        """返回可放置节点 ID；不可行时返回 None，绝不安排超卖容量。

        全类资源同时满足才放置（全有或全无）。best-fit：可用内存最接近
        需求的节点优先，减少碎片扩散。reserved_* 是为饥饿大任务保留的
        容量，其他任务不得占用。维护中/不可用节点一律跳过。
        """
        req = normalize_requirement(requirement)
        exclude = exclude or set()
        reserved = reserved or {}
        reserved_slots = reserved_slots or {}
        candidates: list[tuple[int, int, str]] = []
        for node_id, runtime in self.runtime.items():
            if node_id in exclude or not runtime.available or runtime.in_maintenance:
                continue
            held = reserved.get(node_id, {})
            held_slots = reserved_slots.get(node_id, 0)
            usable_slots = runtime.node.max_jobs - len(runtime.running) - held_slots
            if usable_slots < 1:
                continue
            usable_memory = runtime.node.memory_mb - runtime.used_memory - held.get("memory_mb", 0)
            fits = usable_memory >= req.get("memory_mb", 0)
            for res, amount in req.items():
                if res == "memory_mb":
                    continue
                free_res = runtime.node.capacity_map().get(res, 0) - runtime.used.get(res, 0) - held.get(res, 0)
                if free_res < amount:
                    fits = False
                    break
            if fits:
                candidates.append((usable_memory, usable_slots, node_id))
        if not candidates:
            return None
        # first-fit 按可用容量升序：在所有"立刻放得下"的节点里选剩余最紧的，
        # 避免把任务排给当前容量不足、只能靠未来释放的节点。
        candidates.sort(key=lambda c: (c[0], c[1], c[2]))
        return candidates[0][2]

    def total_free_memory(self) -> int:
        return sum(rt.free_memory() for rt in self.runtime.values() if rt.available)
