"""多阶段资源需求、可中断等待、安全点让出与快照阶段进度的测试。"""

from scheduler import ExperimentRequest, Node, RunStatus, Scheduler, Stage
from scheduler.models import Outage


def staged(eid, stages, submit=0, priority=0):
    return ExperimentRequest(
        eid, memory_mb=stages[0].memory_mb, duration=stages[0].duration,
        submit_time=submit, priority=priority, stages=tuple(stages),
    )


def test_stage_end_releases_unneeded_capacity():
    # m: 阶段0 占 90MB（t0-3），阶段1 只需 10MB（t3-6）。
    # 阶段0 结束后释放 80MB，小任务 b(80MB) 必须能在 t=3 启动。
    scheduler = Scheduler([Node("n1", 100, 3)])
    scheduler.submit(staged("m", [Stage(90, 3), Stage(10, 3)]))
    scheduler.submit(ExperimentRequest("b", 80, 2, submit_time=1))
    scheduler.run_until()
    m, b = scheduler.records["m"], scheduler.records["b"]
    assert m.status is RunStatus.COMPLETED
    assert b.start_time == 3
    assert b.status is RunStatus.COMPLETED
    kinds = [e.kind for e in m.events]
    assert kinds.count("stage_started") == 2


def test_next_stage_shortage_enters_explicit_waiting_without_holding():
    # m: 90MB t0-3 后第二阶段要 90MB；b 在 t=1 占用 90MB 到 t=5。
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.submit(staged("m", [Stage(90, 3), Stage(90, 2)]))
    scheduler.submit(ExperimentRequest("b", 90, 2, submit_time=1, priority=1))
    result = scheduler.run_until(end_time=3)
    assert scheduler.records["m"].status is RunStatus.WAITING
    assert scheduler.records["m"].stage_index == 1
    assert "m" not in scheduler.cluster.runtime_for("n1").running
    assert scheduler.cluster.runtime_for("n1").used_memory == 90  # 只剩 b
    assert "m" in result.waiting
    scheduler.run_until()
    assert scheduler.records["m"].status is RunStatus.COMPLETED
    stage_starts = [e.time for e in scheduler.records["m"].events if e.kind == "stage_started"]
    assert stage_starts == [0, 5]


def test_high_priority_yields_only_at_declared_safe_point():
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.submit(staged("low", [Stage(80, 2, safe_point=True), Stage(80, 3, safe_point=False)]))
    scheduler.run_until(end_time=0)
    scheduler.submit(staged("hi", [Stage(90, 2)], submit=1, priority=10))
    scheduler.run_until()
    low, hi = scheduler.records["low"], scheduler.records["hi"]
    assert hi.start_time == 2
    assert hi.status is RunStatus.COMPLETED
    assert low.status is RunStatus.COMPLETED
    assert any(e.kind == "yielded" and e.time == 2 for e in low.events)
    second_start = [e.time for e in low.events if e.kind == "stage_started"]
    assert second_start == [0, 4]
    assert low.end_time == 7


def test_non_safe_boundary_is_not_preempted():
    # low 的 t=2 边界声明为非安全点：不允许在该边界让出接续。
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.submit(staged("low", [Stage(80, 2, safe_point=False), Stage(80, 3, safe_point=True)]))
    scheduler.run_until(end_time=0)
    scheduler.submit(staged("hi", [Stage(90, 2)], submit=1, priority=10))
    scheduler.run_until()
    assert not any(e.kind == "yielded" for e in scheduler.records["low"].events)
    assert scheduler.records["hi"].start_time >= 2
    assert scheduler.records["low"].status is RunStatus.COMPLETED


def test_resource_accounting_after_node_exit_and_recovery():
    scheduler = Scheduler([Node("n1", 100, 2), Node("n2", 100, 2)], starvation_threshold=100)
    scheduler.submit(staged("m", [Stage(90, 8), Stage(90, 2)]))
    scheduler.run_until(end_time=0)
    node = scheduler.records["m"].node_id
    scheduler.add_outage(node, Outage(start=2, end=4))
    scheduler.submit(ExperimentRequest("small", 30, 2, submit_time=1))
    scheduler.run_until()
    assert scheduler.records["m"].status is RunStatus.FAILED
    assert scheduler.cluster.runtime_for(node).used_memory == 0
    assert scheduler.cluster.runtime_for(node).running == {}
    assert scheduler.records["small"].status is RunStatus.COMPLETED


def test_cancel_during_explicit_wait_releases_nothing_and_keeps_stage_index():
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.submit(staged("m", [Stage(90, 3), Stage(90, 2)]))
    scheduler.submit(ExperimentRequest("b", 90, 2, submit_time=1, priority=1))
    scheduler.run_until(end_time=3)
    assert scheduler.records["m"].status is RunStatus.WAITING
    scheduler.request_cancel("m", at_time=4)
    scheduler.run_until()
    rec = scheduler.records["m"]
    assert rec.status is RunStatus.CANCELLED
    assert rec.end_time == 4
    assert scheduler.cluster.runtime_for("n1").used_memory == 0


def test_yield_request_dropped_when_requester_cancelled():
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.submit(staged("low", [Stage(80, 4, safe_point=True), Stage(80, 3)]))
    scheduler.run_until(end_time=0)
    scheduler.submit(staged("hi", [Stage(90, 1)], submit=1, priority=10))
    scheduler.run_until(end_time=1)
    assert "low" in scheduler._yield_requested
    scheduler.request_cancel("hi", at_time=2)
    scheduler.run_until()
    low = scheduler.records["low"]
    assert low.status is RunStatus.COMPLETED
    # 请求者取消后 low 不再需要让出；它在 t=4 安全点正常继续第二阶段
    assert not any(e.kind == "yielded" for e in low.events)
    starts = [e.time for e in low.events if e.kind == "stage_started"]
    assert starts == [0, 4]


def test_snapshot_resume_keeps_stage_progress(tmp_path):
    # 在第二阶段显式等待期间快照：恢复后只能从阶段1接续，阶段0绝不重算。
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.submit(staged("m", [Stage(90, 3), Stage(90, 2)]))
    scheduler.submit(ExperimentRequest("b", 90, 4, submit_time=1, priority=1))
    scheduler.run_until(end_time=3)
    assert scheduler.records["m"].status is RunStatus.WAITING
    path = tmp_path / "state.json"
    scheduler.save_state(path)

    resumed = Scheduler.load_state(path)
    assert resumed.now == 3
    rec = resumed.records["m"]
    assert rec.stage_index == 1
    assert rec.status is RunStatus.WAITING
    assert "m" not in resumed.cluster.runtime_for("n1").running
    resumed.run_until()
    events = resumed.records["m"].events
    stage0 = [e for e in events if e.kind == "stage_started" and "stage=0" in e.detail]
    stage1 = [e for e in events if e.kind == "stage_started" and "stage=1" in e.detail]
    assert len(stage0) == 1
    assert len(stage1) == 1
    assert stage1[0].time == 7
    assert resumed.records["m"].status is RunStatus.COMPLETED


def test_snapshot_resume_preserves_yield_request(tmp_path):
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.submit(staged("low", [Stage(80, 3, safe_point=True), Stage(80, 3)]))
    scheduler.run_until(end_time=0)
    scheduler.submit(staged("hi", [Stage(90, 1)], submit=1, priority=10))
    scheduler.run_until(end_time=1)
    assert "low" in scheduler._yield_requested
    path = tmp_path / "state.json"
    scheduler.save_state(path)

    resumed = Scheduler.load_state(path)
    resumed.run_until()
    assert resumed.records["hi"].start_time == 3
    assert any(e.kind == "yielded" and e.time == 3 for e in resumed.records["low"].events)
