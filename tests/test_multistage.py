"""多阶段资源需求、可中断等待与安全点抢占的行为测试。"""

from scheduler import ExperimentRequest, Node, RunStatus, Scheduler, StageSpec


def test_stage_releases_then_explicit_wait_does_not_hold_capacity():
    # 阶段1(40MB,2t) 结束后阶段2需要 100MB；同节点只跑着 60MB 任务，
    # 放不下 -> 进入明确等待，且等待期间账目上不持有任何容量。
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest(
        "m", 40, 4, stages=(StageSpec(40, 2), StageSpec(100, 2))))
    scheduler.submit(ExperimentRequest("q", 60, 4))
    scheduler.run_until()
    m = scheduler.records["m"]
    assert [e.kind for e in m.events].count("waiting") >= 1
    assert m.start_time == 0 and m.end_time == 6
    assert scheduler.cluster.runtime_for("n1").used_memory == 0
    assert scheduler.cluster.runtime_for("n1").running == {}


def test_waiting_releases_capacity_for_other_experiment():
    # 阶段2 需要整节点；同节点另一任务占据剩余容量，阶段1结束后必须明确
    # 等待。其释放的 40MB 不留作无用占用：阻塞者结束后等待任务的阶段2
    # 立刻在同一时刻接续，且账目最终归零。
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest(
        "m", 40, 4, stages=(StageSpec(40, 2), StageSpec(100, 2))))
    scheduler.submit(ExperimentRequest("blocker", 60, 4))
    scheduler.submit(ExperimentRequest("w", 40, 1, submit_time=2))
    scheduler.run_until()
    m = scheduler.records["m"]
    assert m.status is RunStatus.COMPLETED
    assert any(e.kind == "waiting" for e in m.events)
    assert m.end_time == 6
    assert scheduler.records["w"].status is RunStatus.COMPLETED
    assert scheduler.records["blocker"].status is RunStatus.COMPLETED
    assert scheduler.cluster.runtime_for("n1").used_memory == 0


def test_stage_progress_advances_once():
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest(
        "m", 6, duration=6, stages=(
            StageSpec(40, 2), StageSpec(60, 2), StageSpec(30, 2))))
    scheduler.run_until()
    m = scheduler.records["m"]
    assert m.status is RunStatus.COMPLETED
    assert [e.kind for e in m.events].count("stage_completed") == 3
    assert m.current_stage == 3


def test_preemption_only_at_declared_safe_point():
    scheduler = Scheduler([Node("n1", 100, 1)])
    scheduler.submit(ExperimentRequest(
        "victim", 100, 6, stages=(StageSpec(100, 6, safe_point_offsets=(2, 4)),)))
    scheduler.submit(ExperimentRequest("hp", 100, 1, submit_time=1, priority=10))
    scheduler.run_until()
    hp, victim = scheduler.records["hp"], scheduler.records["victim"]
    assert hp.start_time == 2
    assert any(e.kind == "yielded" and e.time == 2 for e in victim.events)
    # 已执行的 2 个单位不重算：让出2 + hp占1 + 剩余4 = 第7时刻完成
    assert victim.end_time == 7
    assert victim.current_stage == 1


def test_no_inner_safe_point_waits_for_stage_boundary():
    scheduler = Scheduler([Node("n1", 100, 1)])
    scheduler.submit(ExperimentRequest("victim", 100, 6))
    scheduler.submit(ExperimentRequest("hp", 100, 1, submit_time=1, priority=10))
    scheduler.run_until()
    assert scheduler.records["hp"].start_time == 6
    # 阶段边界（=完成点）前绝不被打断
    assert not any(e.kind == "yielded" for e in scheduler.records["victim"].events)


def test_multiple_victims_wait_until_collective_release():
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest(
        "v1", 50, 6, stages=(StageSpec(50, 6, safe_point_offsets=(2,)),)))
    scheduler.submit(ExperimentRequest(
        "v2", 50, 6, stages=(StageSpec(50, 6, safe_point_offsets=(4,)),)))
    scheduler.submit(ExperimentRequest("hp", 100, 1, submit_time=1, priority=10))
    scheduler.run_until()
    assert scheduler.records["hp"].start_time == 4
    v1 = scheduler.records["v1"]
    # v1 早在 t2 到达自身安全点并停下，但必须等到 t4 才重新可调度
    assert any(e.kind == "yielded" and e.time == 2 for e in v1.events)
    assert not any(e.kind == "stage_started" and 2 < e.time < 5 for e in v1.events)
    # 已执行的 2 个单位不重算：续跑剩余 4 个单位
    assert v1.end_time == 9


def test_normal_priority_cannot_preempt():
    scheduler = Scheduler([Node("n1", 100, 1)])
    scheduler.submit(ExperimentRequest(
        "victim", 100, 6, stages=(StageSpec(100, 6, safe_point_offsets=(2,)),)))
    scheduler.submit(ExperimentRequest("peer", 100, 1, submit_time=1, priority=0))
    scheduler.run_until()
    assert scheduler.records["peer"].start_time == 6
    assert not any(e.kind == "yielded" for e in scheduler.records["victim"].events)


def test_node_exit_fails_running_and_pending_routes_elsewhere():
    scheduler = Scheduler([Node("n1", 100, 1), Node("n2", 100, 1)])
    scheduler.submit(ExperimentRequest("a", 80, 10))
    scheduler.submit(ExperimentRequest("b", 80, 2))
    scheduler.run_until(end_time=0)
    dead = scheduler.records["a"].node_id
    scheduler.remove_node(dead, 2)
    scheduler.run_until()
    assert scheduler.records["a"].status is RunStatus.FAILED
    assert scheduler.records["b"].node_id != dead
    assert scheduler.cluster.runtime_for(dead).available is False
    assert all(rt.used_memory == 0 for rt in scheduler.cluster.runtime.values())


def test_snapshot_resume_keeps_held_yield(tmp_path):
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest(
        "v1", 50, 6, stages=(StageSpec(50, 6, safe_point_offsets=(2,)),)))
    scheduler.submit(ExperimentRequest(
        "v2", 50, 6, stages=(StageSpec(50, 6, safe_point_offsets=(4,)),)))
    scheduler.submit(ExperimentRequest("hp", 100, 1, submit_time=1, priority=10))
    scheduler.run_until(end_time=2)
    assert "v1" in scheduler._held
    state = tmp_path / "state.json"
    scheduler.save_state(state)
    resumed = Scheduler.load_state(state)
    assert "v1" in resumed._held
    resumed.run_until()
    assert resumed.records["hp"].start_time == 4
    assert resumed.records["v1"].end_time == 9


def test_snapshot_resume_preserves_stage_progress_no_recompute(tmp_path):
    scheduler = Scheduler([Node("n1", 100, 1)])
    scheduler.submit(ExperimentRequest(
        "victim", 100, 6, stages=(StageSpec(100, 6, safe_point_offsets=(2, 4)),)))
    scheduler.submit(ExperimentRequest("hp", 100, 2, submit_time=1, priority=10))
    scheduler.run_until(end_time=3)
    state = tmp_path / "state.json"
    scheduler.save_state(state)
    resumed = Scheduler.load_state(state)
    resumed.run_until()
    victim = resumed.records["victim"]
    assert victim.end_time == 8  # 2 已执行 + 2 被 hp 占 + 4 剩余
    assert victim.status is RunStatus.COMPLETED
    assert [e.kind for e in victim.events].count("stage_completed") == 1
    assert resumed.records["hp"].status is RunStatus.COMPLETED


def test_snapshot_resume_keeps_waiting_stage(tmp_path):
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest(
        "m", 40, 4, stages=(StageSpec(40, 2), StageSpec(100, 2))))
    scheduler.submit(ExperimentRequest("blocker", 60, 4))
    scheduler.run_until(end_time=2)
    assert scheduler.records["m"].status is RunStatus.WAITING
    state = tmp_path / "state.json"
    scheduler.save_state(state)
    resumed = Scheduler.load_state(state)
    assert resumed.records["m"].current_stage == 1
    resumed.run_until()
    m = resumed.records["m"]
    assert m.status is RunStatus.COMPLETED
    assert m.end_time == 6
    assert [e.kind for e in m.events].count("stage_completed") == 2


def test_repeated_high_priority_arrivals_all_finish():
    scheduler = Scheduler(
        [Node("n1", 100, 2), Node("n2", 100, 2)], starvation_threshold=5)
    for i in range(4):
        scheduler.submit(ExperimentRequest(f"low{i}", 40, 4))
    for t in range(1, 6):
        scheduler.submit(
            ExperimentRequest(f"hp{t}", 60, 1, submit_time=t, priority=10))
    scheduler.run_until()
    for i in range(4):
        assert scheduler.records[f"low{i}"].status is RunStatus.COMPLETED
    for t in range(1, 6):
        assert scheduler.records[f"hp{t}"].status is RunStatus.COMPLETED
    assert all(rt.used_memory == 0 for rt in scheduler.cluster.runtime.values())
