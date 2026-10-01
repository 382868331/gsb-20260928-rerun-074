"""多资源原子申请、维护迁移/续跑、阻塞分类与预算限制测试。"""

from scheduler import ExperimentRequest, Node, RunStatus, Scheduler, Stage
from scheduler.models import Outage
from scheduler.queue import BUDGET_LIMIT, MAINTENANCE, RESOURCE_SHORTAGE


def stage_req(eid, resources, duration=2, migratable=False):
    return ExperimentRequest(
        eid,
        memory_mb=resources.pop("memory_mb", 10),
        duration=duration,
        stages=(Stage(10, duration, resources=resources, migratable=migratable),),
    )


def test_stage_requires_all_resource_types_atomically():
    scheduler = Scheduler([Node("n1", 100, 3, capacities={"gpu": 1, "device": 1})])
    scheduler.submit(stage_req("holder", {"gpu": 1}, duration=4))
    scheduler.run_until(end_time=0)
    scheduler.submit(stage_req("partial", {"gpu": 1, "device": 1}, duration=1))
    scheduler.run_until(end_time=0)
    assert scheduler.records["partial"].status is RunStatus.PENDING
    assert "partial" not in scheduler.cluster.runtime_for("n1").allocations
    assert scheduler.last_blockages["partial"] == RESOURCE_SHORTAGE
    scheduler.run_until()
    assert scheduler.records["partial"].start_time == 4


def test_maintenance_migratable_stage_moves_without_replaying_completed_work(tmp_path):
    scheduler = Scheduler([
        Node("n1", 100, 2, capacities={"device": 1}),
        Node("n2", 100, 2, capacities={"device": 1}),
    ], starvation_threshold=100)
    scheduler.submit(ExperimentRequest(
        "job", 10, 10,
        stages=(Stage(10, 8, resources={"device": 1}, migratable=True),),
    ))
    scheduler.run_until(end_time=0)
    original = scheduler.records["job"].node_id
    scheduler.add_outage(original, Outage(3, 6))
    scheduler.run_until(end_time=2)
    scheduler.save_state(tmp_path / "state.json")
    resumed = Scheduler.load_state(tmp_path / "state.json")
    resumed.run_until(end_time=3)
    assert resumed.records["job"].node_id != original
    resumed.run_until()
    rec = resumed.records["job"]
    assert rec.status is RunStatus.COMPLETED
    assert rec.node_id != original
    assert rec.end_time == 8
    starts = [e for e in rec.events if e.kind == "stage_started"]
    resumes = [e for e in rec.events if e.kind == "stage_resumed"]
    assert len(starts) == 1
    assert len(resumes) == 1
    assert all(rt.used["device"] == 0 for rt in resumed.cluster.runtime.values())


def test_non_migratable_stage_pins_until_maintenance_end():
    scheduler = Scheduler([
        Node("n1", 100, 2, capacities={"device": 1}),
        Node("n2", 100, 2, capacities={"device": 1}),
    ], starvation_threshold=100)
    scheduler.submit(stage_req("pinned", {"device": 1}, duration=8, migratable=False))
    scheduler.run_until(end_time=0)
    original = scheduler.records["pinned"].node_id
    scheduler.add_outage(original, Outage(3, 6))
    scheduler.run_until()
    rec = scheduler.records["pinned"]
    assert rec.status is RunStatus.COMPLETED
    assert rec.node_id == original
    assert rec.end_time == 11
    assert any(e.kind == "maintenance_pinned_wait" for e in rec.events)
    assert not any(e.kind == "maintenance_migratable_wait" for e in rec.events)


def test_blockage_distinguishes_resource_maintenance_and_budget():
    scheduler = Scheduler([
        Node("n1", 100, 1, capacities={"device": 1}),
        Node("n2", 100, 1, capacities={"special": 1}),
    ], starvation_threshold=100)
    scheduler.submit(stage_req("holder", {"device": 1}, duration=4))
    scheduler.add_outage("n2", Outage(0, 5))
    scheduler.submit(stage_req("maintenance", {"special": 1}, duration=1))
    scheduler.submit(stage_req("resource", {"device": 1}, duration=1))
    scheduler.run_until(end_time=0)
    reasons = dict(scheduler.last_blockages)
    assert reasons["maintenance"] == MAINTENANCE
    assert reasons["resource"] == RESOURCE_SHORTAGE

    scheduler.submit(stage_req("budget", {"device": 1}, duration=1))
    scheduler.run_until(end_time=0, schedule_budget=0)
    assert scheduler.last_blockages["budget"] == BUDGET_LIMIT
    assert scheduler.records["budget"].status is RunStatus.PENDING
