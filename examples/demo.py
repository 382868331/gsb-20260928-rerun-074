"""可直接运行的演示：碎片、节点临时不可用、防饥饿与状态续跑。"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import tempfile

from scheduler import ExperimentRequest, Node, Scheduler
from scheduler.models import Outage


def main() -> None:
    scheduler = Scheduler(
        [Node("n1", 100, 2), Node("n2", 100, 2)],
        starvation_threshold=5,
    )
    # 小任务先占满 4 个并发槽
    for i in range(4):
        scheduler.submit(ExperimentRequest(f"occ-{i}", memory_mb=40, duration=3))
    # 大任务随后到达，必须等待释放；期间小任务不断到来
    scheduler.submit(ExperimentRequest("big", memory_mb=100, duration=3, submit_time=1))
    for t in range(1, 10):
        scheduler.submit(
            ExperimentRequest(f"small-{t}", memory_mb=25, duration=2, submit_time=t)
        )
    scheduler.add_outage("n1", Outage(start=12, end=15))

    with tempfile.TemporaryDirectory() as tmp:
        state = Path(tmp) / "snapshot.json"
        scheduler.run_until(end_time=4)
        scheduler.save_state(state)
        print(f"t=4 快照已保存，big 状态: {scheduler.records['big'].status.value}")
        resumed = Scheduler.load_state(state)
        resumed.run_until()

    big = resumed.records["big"]
    print(
        f"big 启动时刻={big.start_time}，等待={big.wait_time}，"
        f"终态={big.status.value}，节点={big.node_id}"
    )
    assert 0 < (big.wait_time or 0) <= 10


if __name__ == "__main__":
    main()
