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
    """实验的一个执行阶段：占用 memory_mb，持续 duration 个时间单位。

    safe_point=True 表示本阶段结束边界是声明的安全点，高优先级任务只能在
    这些边界要求让出资源；False 表示该边界不可被打断接续。
    """

    memory_mb: int
    duration: int
    safe_point: bool = True

    def __post_init__(self) -> None:
        if self.memory_mb <= 0:
            raise ValueError("阶段内存必须为正数")
        if self.duration <= 0:
            raise ValueError("阶段时长必须为正数")


@dataclass(frozen=True)
class Node:
    """虚拟计算节点：只有内存与并发槽两类资源。"""

    node_id: str
    memory_mb: int
    max_jobs: int

    def __post_init__(self) -> None:
        if self.memory_mb <= 0:
            raise ValueError(f"节点 {self.node_id} 内存必须为正数")
        if self.max_jobs <= 0:
            raise ValueError(f"节点 {self.node_id} 并发上限必须为正数")


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


class NodeRuntime:
    """节点的运行时占用状态。

    内存与并发槽在任务真正 start 时才占用；
    release 幂等：取消、失败重复触发只释放一次。
    """

    def __init__(self, node: Node) -> None:
        self.node = node
        self.used_memory = 0
        self.running: dict[str, int] = {}
        self.available = True

    def free_memory(self) -> int:
        return self.node.memory_mb - self.used_memory

    def free_slots(self) -> int:
        return self.node.max_jobs - len(self.running)

    def can_fit(self, memory_mb: int) -> bool:
        return self.available and memory_mb <= self.free_memory() and self.free_slots() >= 1

    def allocate(self, experiment_id: str, memory_mb: int) -> None:
        if experiment_id in self.running:
            raise RuntimeError(f"实验 {experiment_id} 已在节点 {self.node.node_id} 占用资源")
        if not self.can_fit(memory_mb):
            raise RuntimeError(
                f"节点 {self.node.node_id} 无法容纳 {memory_mb}MB："
                f"剩余内存 {self.free_memory()}，剩余并发槽 {self.free_slots()}"
            )
        self.used_memory += memory_mb
        self.running[experiment_id] = memory_mb

    def release(self, experiment_id: str) -> bool:
        """释放资源；返回是否真正执行释放（保证只释放一次）。"""
        memory = self.running.pop(experiment_id, None)
        if memory is None:
            return False
        self.used_memory -= memory
        return True

    def to_dict(self) -> dict:
        return {
            "used_memory": self.used_memory,
            "running": dict(self.running),
            "available": self.available,
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
        memory_mb: int,
        exclude: Optional[set[str]] = None,
        reserved_memory: Optional[dict[str, int]] = None,
        reserved_slots: Optional[dict[str, int]] = None,
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
            if memory_mb <= usable_memory and usable_slots >= 1:
                candidates.append((usable_memory, usable_slots, node_id))
        if not candidates:
            return None
        # first-fit 按可用容量升序：在所有"立刻放得下"的节点里选剩余最紧的，
        # 避免把任务排给当前容量不足、只能靠未来释放的节点。
        candidates.sort(key=lambda c: (c[0], c[1], c[2]))
        return candidates[0][2]

    def total_free_memory(self) -> int:
        return sum(rt.free_memory() for rt in self.runtime.values() if rt.available)
