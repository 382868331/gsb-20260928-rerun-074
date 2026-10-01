"""多类资源、节点维护窗口与迁移限制、阻塞原因区分的演示。"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scheduler import (
    ExperimentRequest,
    MaintenanceWindow,
    Node,
    Scheduler,
    Stage,
)


def main() -> None:
    scheduler = Scheduler(
        [
            Node("n1", 100, 2, capacity={"gpu": 1}),
            Node("n2", 120, 2, capacity={"gpu": 1}),
        ],
        starvation_threshold=10,
        budget=4,  # 集群级同时在跑阶段数上限
    )
    # train 阶段0 同时占内存 + 独占 GPU，可迁移；阶段1 只要内存，不可迁移
    scheduler.submit(ExperimentRequest(
        "train", 90, 3,
        stages=(
            Stage(90, 4, resources={"gpu": 1}, safe_point=True, migratable=True),
            Stage(30, 3, safe_point=True, migratable=False),
        ),
    ))
    # render 也要独占 GPU：必须等 train 阶段0 释放，不能部分拿到就启动
    scheduler.submit(ExperimentRequest(
        "render", 10, 2, submit_time=1,
        stages=(Stage(10, 2, resources={"gpu": 1}),),
    ))
    scheduler.add_maintenance("n1", MaintenanceWindow(start=2, end=5))

    scheduler.run_until(end_time=2)
    train = scheduler.records["train"]
    print("t=2 train 事件:", [(e.time, e.kind, e.detail) for e in train.events])

    with tempfile.TemporaryDirectory() as tmp:
        state = Path(tmp) / "maint.json"
        scheduler.save_state(state)
        resumed = Scheduler.load_state(state)
        resumed.run_until()

    for eid in ("train", "render"):
        rec = resumed.records[eid]
        print(f"{eid}: 终态={rec.status.value} 结束={rec.end_time}")
        print("  ", [(e.time, e.kind) for e in rec.events])
    assert resumed.records["train"].status.value == "completed"
    assert resumed.records["render"].status.value == "completed"


if __name__ == "__main__":
    main()
