"""虚拟计算节点、实验需求与运行记录的数据模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class JobState(str, Enum):
    QUEUED = "queued"                # 已提交，等待资源
    RESERVED = "reserved"            # 触发公平保底，资源被冻结预留
    RUNNING = "running"              # 已开始，占用节点资源
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"            # 任何节点都放不下，提交即拒绝


TERMINAL_STATES = {
    JobState.COMPLETED,
    JobState.FAILED,
    JobState.CANCELLED,
    JobState.REJECTED,
}


@dataclass
class Node:
    """虚拟计算节点：内存容量与并发槽位是两类独立资源。"""

    id: str
    memory_mb: int
    max_running: int = 1
    available: bool = True           # False 表示临时不可用（维护/故障）

    def validate(self) -> None:
        if self.memory_mb <= 0:
            raise ValueError(f"节点 {self.id} 内存容量必须为正")
        if self.max_running <= 0:
            raise ValueError(f"节点 {self.id} 并发槽位必须为正")


@dataclass
class Job:
    """实验需求。身份由 id 唯一确定，队列策略变化不改变 id。"""

    id: str
    memory_mb: int
    duration: int                    # 模拟运行时长（tick 数）
    submit_time: int
    priority: int = 0                # 基础优先级，越大越优先
    state: JobState = JobState.QUEUED
    node_id: Optional[str] = None
    start_time: Optional[int] = None
    finish_time: Optional[int] = None
    reserved_at: Optional[int] = None
    release_count: int = 0           # 释放次数，正常生命周期中只能为 0 或 1

    def validate(self) -> None:
        if self.memory_mb <= 0:
            raise ValueError(f"实验 {self.id} 内存需求必须为正")
        if self.duration <= 0:
            raise ValueError(f"实验 {self.id} 运行时长必须为正")


@dataclass
class RunRecord:
    """一次实际占用资源的运行记录：开始时创建，释放时封口。"""

    job_id: str
    node_id: str
    memory_mb: int
    start_time: int
    end_time: Optional[int] = None
    result: Optional[str] = None     # completed / cancelled / failed / node_down

    @property
    def is_closed(self) -> bool:
        return self.end_time is not None
