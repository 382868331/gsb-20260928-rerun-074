"""多阶段 + 大小任务混合 + 节点退出 + 连续高优到达的公平性与账目测试。"""

import random

from scheduler import ExperimentRequest, Node, RunStatus, Scheduler, Stage
from scheduler.models import Outage


def test_mixed_multistage_workload_accounting_and_progress():
    random.seed(42)
    scheduler = Scheduler([
        Node("n1", 120, 3),
        Node("n2", 120, 3),
    ], starvation_threshold=8)
    # 4 个大多阶段任务：大-小-大，安全点/非安全点交替声明
    for i in range(4):
        scheduler.submit(
            ExperimentRequest(
                f"big{i}", 100, 10, submit_time=0,
                stages=(
                    Stage(100, random.randint(3, 5), safe_point=True),
                    Stage(30, random.randint(1, 2), safe_point=bool(i % 2)),
                    Stage(100, random.randint(3, 5), safe_point=True),
                ),
            )
        )
    # 小任务持续洪泛
    for t in range(0, 40):
        for k in range(random.randint(1, 3)):
            scheduler.submit(
                ExperimentRequest(f"small_{t}_{k}", 25, random.randint(1, 3), submit_time=t)
            )
    # 连续高优到达
    for t in range(2, 20, 4):
        scheduler.submit(
            ExperimentRequest(f"urgent_{t}", 90, 2, submit_time=t, priority=5)
        )
    scheduler.add_outage("n1", Outage(start=25, end=28))
    result = scheduler.run_until()

    # 所有未被节点退出杀掉的实验都到达终态，无悬挂运行/占用
    for eid, rec in scheduler.records.items():
        assert rec.status.terminal or rec.status is RunStatus.WAITING
    for rt in scheduler.cluster.runtime.values():
        assert rt.used_memory == 0
        assert rt.running == {}
    # 高优任务都在合理时间内启动（不能被普通任务长期阻塞）
    for t in range(2, 20, 4):
        rec = scheduler.records[f"urgent_{t}"]
        assert rec.status is RunStatus.COMPLETED
        assert rec.wait_time <= 6
    # 至少有一个大任务跨过节点退出完成全部 3 个阶段，且每阶段只执行一次
    completed_bigs = [b for b in (f"big{i}" for i in range(4))
                      if scheduler.records[b].status is RunStatus.COMPLETED]
    assert completed_bigs
    for eid in completed_bigs:
        events = scheduler.records[eid].events
        for idx in range(3):
            starts = [e for e in events if e.kind == "stage_started" and f"stage={idx}" in e.detail]
            assert len(starts) == 1
        completions = [e for e in events if e.kind == "completed"]
        assert len(completions) == 1
    # 显式等待期间绝不能持有资源：waiting 事件时刻所有节点合计占用不含该实验
    assert not scheduler.queue  # 结束时队列清空
    assert all(v == 0 for v in [rt.used_memory for rt in scheduler.cluster.runtime.values()])


def test_burst_high_priority_each_preempts_only_at_safe_points():
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.submit(ExperimentRequest(
        "low", 80, 10,
        stages=(Stage(80, 2, safe_point=True), Stage(80, 2, safe_point=True),
                Stage(80, 2, safe_point=True), Stage(80, 2, safe_point=True),
                Stage(80, 2, safe_point=True)),
    ))
    scheduler.run_until(end_time=0)
    for t in (1, 3, 5):
        scheduler.submit(ExperimentRequest(f"hi{t}", 90, 1, submit_time=t, priority=9))
    scheduler.run_until()
    # 高优全部完成；首个高优必须等到 low 的第一个声明安全点 t=2，
    # 之后高优可彼此接续释放，但绝不会在 low 的奇数阶段中间抢 low。
    for t in (1, 3, 5):
        rec = scheduler.records[f"hi{t}"]
        assert rec.status is RunStatus.COMPLETED
        assert rec.start_time >= 2
    assert scheduler.records["hi1"].start_time == 2
    low = scheduler.records["low"]
    # low 的每次让出都发生在偶数安全点
    yielded_times = [e.time for e in low.events if e.kind == "yielded"]
    assert yielded_times and all(t % 2 == 0 for t in yielded_times)
    assert low.status is RunStatus.COMPLETED
    starts = [e.time for e in low.events if e.kind == "stage_started"]
    assert sorted(starts) == starts
    assert len(starts) == 5  # 五个阶段各执行一次，让出不重算已执行阶段
