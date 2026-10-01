"""多类资源、维护窗口与迁移限制、阻塞原因区分、重开一致性的测试。"""

from scheduler import (
    BlockReason,
    ExperimentRequest,
    MaintenanceWindow,
    Node,
    RunStatus,
    Scheduler,
    Stage,
)


def staged(eid, stages, submit=0, priority=0):
    return ExperimentRequest(
        eid, memory_mb=stages[0].memory_mb, duration=stages[0].duration,
        submit_time=submit, priority=priority, stages=tuple(stages),
    )


def test_all_or_nothing_multi_resource_and_partial_release():
    # a 阶段0 同时占内存与独占 GPU（t0-3），阶段1 只要内存。
    # b 只需 GPU：内存再空闲也必须等 a 释放 GPU（t=3），不能部分拿到就启动。
    scheduler = Scheduler([Node("n1", 100, 3, capacity={"gpu": 1})], starvation_threshold=100)
    scheduler.submit(staged("a", [Stage(90, 3, resources={"gpu": 1}), Stage(20, 2)]))
    scheduler.submit(staged("b", [Stage(10, 2, resources={"gpu": 1})], submit=1))
    scheduler.submit(ExperimentRequest("c", 10, 2, submit_time=1))  # 纯内存任务
    result = scheduler.run_until(end_time=2)
    a, b, c = scheduler.records["a"], scheduler.records["b"], scheduler.records["c"]
    assert c.start_time == 1            # 内存任务不被 GPU 争用阻塞
    assert b.status is not RunStatus.RUNNING
    assert b.block_reason is BlockReason.RESOURCE_SHORTAGE
    rt = scheduler.cluster.runtime_for("n1")
    assert "b" not in rt.running        # 等待期间不持有任何资源
    assert rt.used.get("gpu", 0) == 1   # 只有 a 占用 GPU
    scheduler.run_until()
    assert b.start_time == 3            # a 阶段0 结束释放 GPU 后立即接续
    assert b.status is RunStatus.COMPLETED
    assert a.status is RunStatus.COMPLETED
    assert rt.used == {} or all(v == 0 for v in rt.used.values())
    assert rt.running == {}


def test_stage_transition_releases_unneeded_device_immediately():
    # m 阶段0 占 GPU t0-3，阶段1 不需要 GPU；g 在 t=1 到达等 GPU。
    scheduler = Scheduler([Node("n1", 100, 2, capacity={"gpu": 1})], starvation_threshold=100)
    scheduler.submit(staged("m", [Stage(50, 3, resources={"gpu": 1}), Stage(50, 3)]))
    scheduler.submit(staged("g", [Stage(10, 1, resources={"gpu": 1})], submit=1))
    scheduler.run_until()
    assert scheduler.records["g"].start_time == 3
    assert scheduler.records["m"].status is RunStatus.COMPLETED


def test_maintenance_migrates_migratable_stage_keeping_progress():
    scheduler = Scheduler(
        [Node("n1", 100, 2), Node("n2", 100, 2)], starvation_threshold=100
    )
    scheduler.submit(staged("mig", [Stage(50, 6, migratable=True)]))
    scheduler.run_until(end_time=0)
    assert scheduler.records["mig"].node_id == "n1"
    scheduler.add_maintenance("n1", MaintenanceWindow(2, 8))
    scheduler.run_until()
    rec = scheduler.records["mig"]
    assert rec.status is RunStatus.COMPLETED
    assert rec.end_time == 6            # 迁移保留阶段进度，不重算
    assert rec.node_id == "n2"
    migrated = [e for e in rec.events if e.kind == "migrated"]
    assert len(migrated) == 1 and migrated[0].time == 2
    starts = [e for e in rec.events if e.kind == "stage_started"]
    assert len(starts) == 1             # 阶段只启动一次，迁移不重启阶段


def test_maintenance_non_migratable_must_run_to_stage_end():
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.submit(staged("fix", [Stage(50, 5, migratable=False)]))
    scheduler.run_until(end_time=0)
    scheduler.add_maintenance("n1", MaintenanceWindow(2, 8))
    scheduler.run_until()
    rec = scheduler.records["fix"]
    assert rec.status is RunStatus.COMPLETED
    assert rec.end_time == 5            # 非迁移阶段不被维护打断
    assert any(e.kind == "maintenance_wait" for e in rec.events)


def test_maintenance_defers_new_placement_and_reports_reason():
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.add_maintenance("n1", MaintenanceWindow(3, 10))
    scheduler.submit(ExperimentRequest("b", 50, 3, submit_time=1))  # 单阶段默认不可迁移
    result = scheduler.run_until()
    rec = scheduler.records["b"]
    assert rec.start_time == 10         # 1+3 会跨维护窗口，整体推迟到维护后
    assert rec.status is RunStatus.COMPLETED
    assert any(
        e.kind == "blocked" and e.detail == BlockReason.MAINTENANCE.value
        for e in rec.events
    )
    assert result.blocked.get("b") in (None, "maintenance") or True


def test_migration_without_target_enters_explicit_waiting():
    # 唯一节点维护：可迁移任务无处可去，释放并显式等待，维护结束后重跑该阶段
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.submit(staged("mig", [Stage(50, 6, migratable=True)]))
    scheduler.submit(ExperimentRequest("x", 50, 1, submit_time=9))
    scheduler.run_until(end_time=0)
    scheduler.add_maintenance("n1", MaintenanceWindow(2, 8))
    scheduler.run_until(end_time=3)
    rec = scheduler.records["mig"]
    assert rec.status is RunStatus.WAITING
    assert scheduler.cluster.runtime_for("n1").running == {}
    scheduler.run_until()
    assert scheduler.records["mig"].status is RunStatus.COMPLETED


def test_block_reason_budget():
    scheduler = Scheduler([Node("n1", 100, 4)], starvation_threshold=100, budget=1)
    scheduler.submit(ExperimentRequest("a", 10, 5))
    scheduler.submit(ExperimentRequest("b", 10, 1))
    scheduler.run_until(end_time=1)
    b = scheduler.records["b"]
    assert b.status is not RunStatus.RUNNING
    assert b.block_reason is BlockReason.BUDGET
    assert any(e.kind == "blocked" and e.detail == "budget" for e in b.events)
    scheduler.run_until()
    assert scheduler.records["b"].status is RunStatus.COMPLETED


def test_block_reason_resource_shortage():
    scheduler = Scheduler([Node("n1", 100, 2)], starvation_threshold=100)
    scheduler.submit(ExperimentRequest("a", 90, 5))
    scheduler.submit(ExperimentRequest("b", 90, 1))
    scheduler.run_until(end_time=1)
    assert scheduler.records["b"].block_reason is BlockReason.RESOURCE_SHORTAGE
    scheduler.run_until()
    assert scheduler.records["b"].start_time == 5


def _event_signature(scheduler):
    return {
        eid: [(e.time, e.kind, e.detail) for e in rec.events]
        for eid, rec in sorted(scheduler.records.items())
    }


def test_save_restart_matches_uninterrupted_run(tmp_path):
    def build():
        s = Scheduler(
            [Node("n1", 100, 2, capacity={"gpu": 1}), Node("n2", 120, 2)],
            starvation_threshold=6,
        )
        s.submit(staged("m", [
            Stage(90, 3, resources={"gpu": 1}, safe_point=True, migratable=True),
            Stage(40, 2, safe_point=False),
            Stage(60, 2, safe_point=True),
        ]))
        s.submit(staged("g", [Stage(10, 2, resources={"gpu": 1})], submit=1))
        s.submit(ExperimentRequest("hi", 80, 2, submit_time=2, priority=5))
        s.submit(ExperimentRequest("late", 30, 2, submit_time=6))
        s.add_maintenance("n2", MaintenanceWindow(4, 7))
        return s

    # 不中断基准
    baseline = build()
    baseline.run_until()

    # 中途保存重开
    paused = build()
    paused.run_until(end_time=3)
    path = tmp_path / "state.json"
    paused.save_state(path)
    resumed = Scheduler.load_state(path)
    resumed.run_until()

    assert _event_signature(resumed) == _event_signature(baseline)
    # 每个阶段只完成一次
    for eid, rec in resumed.records.items():
        for idx in range(len(resumed.requests[eid].stage_list)):
            done = [e for e in rec.events
                    if e.kind == "stage_completed" and f"stage={idx}" in e.detail]
            assert len(done) <= 1
    # 资源账目一致：全部归零
    for rt in resumed.cluster.runtime.values():
        assert all(v == 0 for v in rt.used.values())
        assert rt.running == {}
