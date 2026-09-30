"""多阶段资源需求、阶段间显式等待与安全点让出的演示。"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scheduler import ExperimentRequest, Node, Scheduler, Stage


def main() -> None:
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=10)
    # low 是 3 阶段实验：大->小->大；只在阶段 0 结束处声明安全点
    scheduler.submit(ExperimentRequest(
        "low", 100, 10,
        stages=(
            Stage(90, 4, safe_point=True),
            Stage(20, 3, safe_point=False),
            Stage(90, 3, safe_point=True),
        ),
    ))
    # 高优任务 t=2 到达：只能在 low 的 t=4 安全点接续让出
    scheduler.submit(ExperimentRequest("urgent", 90, 2, submit_time=2, priority=10))
    scheduler.run_until(end_time=4)
    print("t=4 low 事件:", [(e.time, e.kind, e.detail) for e in scheduler.records["low"].events])

    with tempfile.TemporaryDirectory() as tmp:
        state = Path(tmp) / "multi.json"
        # 在阶段间显式等待处快照，恢复后不重算已执行阶段
        scheduler.save_state(state)
        resumed = Scheduler.load_state(state)
        resumed.run_until()

    low = resumed.records["low"]
    urgent = resumed.records["urgent"]
    print(f"urgent 启动={urgent.start_time}（安全点让出），终态={urgent.status.value}")
    print("low 阶段启动时刻:", [e.time for e in low.events if e.kind == "stage_started"])
    print(f"low 终态={low.status.value}，结束={low.end_time}")
    assert urgent.start_time == 4
    assert low.status.value == "completed"


if __name__ == "__main__":
    main()
