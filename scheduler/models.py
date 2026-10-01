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


@dataclass(frozen=True)
class Stage:
    """实验的一个执行阶段。

    memory_mb 与 resources 会合并成一个原子资源需求，调度时必须一次全部
    满足，不能只拿到部分资源就启动。

    safe_point=True 表示本阶段结束边界可以让出；migratable=True 表示遇到
    节点维护时可带剩余进度迁移。二者都为 False 时只能在原节点维护结束后
    继续，且其他任务不能在阶段边界抢占它。
    """

    memory_mb: int
    duration: int
    safe_point: bool = True
    resources: dict[str, int] = field(default_factory=dict)
    migratable: bool = False

    def __post_init__(self) -> None:
        if self.memory_mb <= 0:
            raise ValueError("阶段内存必须为正数")
        if self.duration <= 0:
            raise ValueError("阶段时长必须为正数")
        for name, amount in self.resources.items():
            if amount <= 0:
                raise ValueError(f"阶段资源 {name} 需求量必须为正数")

    @property
    def resource_request(self) -> dict[str, int]:
        return {"memory_mb": self.memory_mb, "slots": 1, **self.resources}


@dataclass(frozen=True)
class Node:
    """虚拟计算节点：内置内存/并发槽，并可声明设备等命名资源容量。"""

    node_id: str
    memory_mb: int
    max_jobs: int
    capacities: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.memory_mb <= 0:
            raise ValueError(f"节点 {self.node_id} 内存必须为正数")
        if self.max_jobs <= 0:
            raise ValueError(f"节点 {self.node_id} 并发上限必须为正数")
        for name, amount in self.capacities.items():
            if amount <= 0:
                raise ValueError(f"节点 {self.node_id} 资源 {name} 容量必须为正数")

    @property
    def resource_capacity(self) -> dict[str, int]:
        return {"memory_mb": self.memory_mb, "slots": self.max_jobs, **self.capacities}


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
    stage_remaining: Optional[int] = None
    pinned_node_id: Optional[str] = None
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
            "stage_remaining": self.stage_remaining,
            "pinned_node_id": self.pinned_node_id,
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
            stage_remaining=data.get("stage_remaining"),
            pinned_node_id=data.get("pinned_node_id"),
            events=[RunEvent.from_dict(e) for e in data.get("events", [])],
        )


@dataclass(frozen=True)
class Outage:
    """节点维护区间 [start, end)。"""

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError("不可用区间结束时间必须晚于开始时间")

    def covers(self, time: int) -> bool:
        return self.start <= time < self.end


class NodeRuntime:
    """节点的运行时占用状态。

    内存与并发槽在任务真正 start 时才占用；
    release 幂等：取消、失败重复触发只释放一次。
    """

    def __init__(self, node: Node) -> None:
        self.node = node
        self.used: dict[str, int] = {name: 0 for name in node.resource_capacity}
        self.allocations: dict[str, dict[str, int]] = {}
        self.available = True

    @property
    def used_memory(self) -> int:
        return self.used.get("memory_mb", 0)

    @property
    def running(self) -> dict[str, int]:
        return {eid: demand.get("memory_mb", 0) for eid, demand in self.allocations.items()}

    def free_memory(self) -> int:
        return self.node.memory_mb - self.used_memory

    def free_slots(self) -> int:
        return self.node.max_jobs - len(self.running)

    def free_resource(self, name: str) -> int:
        return self.node.resource_capacity.get(name, 0) - self.used.get(name, 0)

    def can_fit(self, memory_mb: Optional[int] = None, resources: Optional[dict[str, int]] = None) -> bool:
        demand = self._demand(memory_mb, resources)
        return self.available and all(amount <= self.free_resource(name) for name, amount in demand.items())

    @staticmethod
    def _demand(memory_mb: Optional[int], resources: Optional[dict[str, int]]) -> dict[str, int]:
        demand = dict(resources or {})
        if memory_mb is not None:
            demand["memory_mb"] = memory_mb
        demand.setdefault("slots", 1)
        return demand

    def allocate(self, experiment_id: str, memory_mb, resources: Optional[dict[str, int]] = None) -> None:
        if isinstance(memory_mb, dict):
            demand = dict(memory_mb)
        else:
            demand = self._demand(memory_mb, resources)
        if experiment_id in self.running:
            raise RuntimeError(f"实验 {experiment_id} 已在节点 {self.node.node_id} 占用资源")
        missing = [name for name in demand if name not in self.node.resource_capacity]
        if missing:
            raise RuntimeError(f"节点 {self.node.node_id} 不存在资源 {','.join(sorted(missing))}")
        if not self.can_fit(resources=demand):
            raise RuntimeError(
                f"节点 {self.node.node_id} 无法容纳 {demand}：剩余 {self.free_resources()}"
            )
        for name, amount in demand.items():
            self.used[name] = self.used.get(name, 0) + amount
        self.allocations[experiment_id] = demand

    def release(self, experiment_id: str) -> bool:
        """释放资源；返回是否真正执行释放（保证只释放一次）。"""
        demand = self.allocations.pop(experiment_id, None)
        if demand is None:
            return False
        for name, amount in demand.items():
            self.used[name] -= amount
        return True

    def free_resources(self) -> dict[str, int]:
        return {name: self.free_resource(name) for name in self.node.resource_capacity}

    def to_dict(self) -> dict:
        return {
            "used_memory": self.used_memory,
            "used": dict(self.used),
            "running": dict(self.allocations),
            "available": self.available,
        }

    def load_dict(self, data: dict) -> None:
        self.available = data.get("available", True)
        self.used = {name: 0 for name in self.node.resource_capacity}
        self.allocations = {}
        for eid, demand in data.get("running", {}).items():
            self.allocate(eid, demand)


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
        memory_mb: int,
        exclude: Optional[set[str]] = None,
        reserved_memory: Optional[dict[str, int]] = None,
        reserved_slots: Optional[dict[str, int]] = None,
        resources: Optional[dict[str, int]] = None,
    ) -> Optional[str]:
        """返回可放置节点 ID；不可行时返回 None，绝不安排超卖容量。

        best-fit：可用内存最接近需求的节点优先，减少碎片扩散。
        reserved_* 是为饥饿大任务保留的容量，其他任务不得占用。
        """
        exclude = exclude or set()
        reserved_memory = reserved_memory or {}
        reserved_slots = reserved_slots or {}
        candidates: list[tuple[int, int, str]] = []
        for node_id, runtime in self.runtime.items():
            if node_id in exclude or not runtime.available:
                continue
            held_mem = reserved_memory.get(node_id, 0)
            held_slots = reserved_slots.get(node_id, 0)
            usable_memory = runtime.node.memory_mb - runtime.used_memory - held_mem
            usable_slots = runtime.node.max_jobs - len(runtime.running) - held_slots
            named_demand = resources or {}
            named_ok = all(
                runtime.node.capacities.get(name, 0) - runtime.used.get(name, 0) >= amount
                for name, amount in named_demand.items()
            )
            if memory_mb <= usable_memory and usable_slots >= 1 and named_ok:
                candidates.append((usable_memory, usable_slots, node_id))
        if not candidates:
            return None
        # first-fit 按可用容量升序：在所有"立刻放得下"的节点里选剩余最紧的，
        # 避免把任务排给当前容量不足、只能靠未来释放的节点。
        candidates.sort(key=lambda c: (c[0], c[1], c[2]))
        return candidates[0][2]

    def total_free_memory(self) -> int:
        return sum(rt.free_memory() for rt in self.runtime.values() if rt.available)
