"""防饥饿性质检查：节点被占满后大任务到达，随机小任务持续洪泛，等待有界。"""

import random

from scheduler import ExperimentRequest, Node, Scheduler


def test_big_job_wait_is_bounded_under_random_small_arrivals():
    random.seed(7)
    max_wait_seen = 0
    for scenario in range(8):
        scheduler = Scheduler(
            [Node("n1", 100, 2), Node("n2", 100, 2)],
            starvation_threshold=5,
        )
        # t=0 用 4 个小任务占满两个节点的全部并发槽
        for i in range(4):
            scheduler.submit(
                ExperimentRequest(f"occ{scenario}_{i}", 40, random.randint(2, 4), 0)
            )
        # 大任务在容量全被占用时到达
        scheduler.submit(ExperimentRequest("big", 100, 2, submit_time=1))
        # 随后小任务持续洪泛到达
        for t in range(1, 60):
            for k in range(random.randint(1, 2)):
                scheduler.submit(
                    ExperimentRequest(f"s{scenario}_{t}_{k}", 25, random.randint(1, 3), t)
                )
        scheduler.run_until()
        wait = scheduler.records["big"].wait_time
        assert wait is not None
        assert wait <= 5 + 4  # 阈值 + 占槽任务的最大时长
        max_wait_seen = max(max_wait_seen, wait)
    assert max_wait_seen > 0
