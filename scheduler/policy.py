"""队列策略。策略只决定排队顺序与公平保底，不改变实验身份。"""

from __future__ import annotations

from typing import Iterable, List, Optional

from .models import Job


class QueuePolicy:
    name = "base"
    reserve_threshold: Optional[int] = None

    def backfill_order(self, jobs: Iterable[Job], now: int) -> List[Job]:
        raise NotImplementedError


class FIFOQueuePolicy(QueuePolicy):
    """先到先服务，无预留。仅用于对照，持续小任务流可饿死大任务。"""

    name = "fifo"

    def backfill_order(self, jobs: Iterable[Job], now: int) -> List[Job]:
        return sorted(jobs, key=lambda j: (j.submit_time, j.id))


class FairAgingPolicy(QueuePolicy):
    """明确的公平策略：老化加权 + 防饥饿预留 + 保守回填。

    1. 老化加权：动态分 = priority + 等待时长 * aging_factor，等待越久越靠前；
    2. 防饥饿预留：等待达到 reserve_threshold 个 tick 的任务进入预留队列，
       按进入预留的先后为其选定目标节点，该节点释放出的资源只留给预留任务；
    3. 保守回填：未预留的小任务只能使用“非预留目标节点”的空闲资源，
       因此无论小任务来得多密集，目标节点上的在跑任务陆续结束后，
       大任务必然能在有限时间内启动，不会被无限期插队。
    """

    def __init__(self, reserve_threshold: int = 4, aging_factor: float = 1.0):
        if reserve_threshold <= 0:
            raise ValueError("reserve_threshold 必须为正")
        self.name = "fair-aging"
        self.reserve_threshold = reserve_threshold
        self.aging_factor = aging_factor

    def backfill_order(self, jobs: Iterable[Job], now: int) -> List[Job]:
        def score(job: Job) -> float:
            wait = now - job.submit_time
            return -(job.priority + wait * self.aging_factor)

        return sorted(jobs, key=lambda j: (score(j), j.submit_time, j.id))

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "reserve_threshold": self.reserve_threshold,
            "aging_factor": self.aging_factor,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "QueuePolicy":
        if data.get("name") == "fifo":
            return FIFOQueuePolicy()
        return cls(
            reserve_threshold=int(data["reserve_threshold"]),
            aging_factor=float(data.get("aging_factor", 1.0)),
        )
