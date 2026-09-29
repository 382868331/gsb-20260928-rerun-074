import pytest

from scheduler import (
    Cluster,
    ExperimentRequest,
    FairQueuePolicy,
    Node,
    RunStatus,
    Scheduler,
)
from scheduler.models import NodeRuntime, Outage
from scheduler.simulator import CapacityInfeasibleError


def make_big_cluster():
    return Scheduler([
        Node("n1", memory_mb=100, max_jobs=3),
        Node("n2", memory_mb=100, max_jobs=3),
    ], starvation_threshold=5)


def req(eid, mem, dur, submit=0):
    return ExperimentRequest(eid, mem, dur, submit)


def test_basic_placement_memory_and_slots():
    scheduler = Scheduler([Node("n1", 40, 2)])
    scheduler.submit(req("a", 30, 2))
    scheduler.submit(req("b", 20, 2))
    result = scheduler.run_until()
    assert result.started == ["a", "b"]
    rt = scheduler.cluster.runtime_for("n1")
    assert rt.used_memory == 0 and rt.running == {}
    assert scheduler.records["a"].status is RunStatus.COMPLETED


def test_infeasible_capacity_rejected():
    scheduler = Scheduler([Node("n1", 50, 2)])
    with pytest.raises(CapacityInfeasibleError):
        scheduler.submit(req("huge", 51, 1))


def test_fragmentation_blocks_until_free_then_best_fit():
    # n1 剩余 60 且有槽，n2 剩余 40；55MB 任务只能等/放 n1，不能超卖
    scheduler = Scheduler([Node("n1", 100, 2), Node("n2", 100, 1)])
    scheduler.submit(req("occ1", 60, 4))  # 占 n1 40 剩余
    scheduler.submit(req("occ2", 60, 4))  # 占 n2 40 剩余
    scheduler.run_until(end_time=0)
    scheduler.submit(req("big", 55, 2))
    result = scheduler.run_until()
    rec = scheduler.records["big"]
    assert rec.node_id == "n1"
    assert rec.start_time == 4  # 等 occ1 释放后才启动
    assert rec.status is RunStatus.COMPLETED


def test_release_is_idempotent_cancel_and_fail_once():
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(req("a", 60, 10))
    scheduler.run_until(end_time=0)
    rt = scheduler.cluster.runtime_for("n1")
    assert rt.release("a") is True
    assert rt.release("a") is False  # 重复释放不二次扣减
    assert rt.used_memory == 0


def test_cancel_running_releases_once():
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(req("a", 80, 10))
    scheduler.run_until(end_time=0)
    scheduler.request_cancel("a", at_time=3)
    scheduler.run_until()
    assert scheduler.records["a"].status is RunStatus.CANCELLED
    assert scheduler.cluster.runtime_for("n1").used_memory == 0
    # 再推进时间不会出现重复终态事件
    scheduler.run_until()
    assert scheduler.records["a"].end_time == 3


def test_cancel_pending_never_allocates():
    scheduler = Scheduler([Node("n1", 50, 1)])
    scheduler.submit(req("a", 50, 5))
    scheduler.submit(req("b", 50, 5))
    scheduler.request_cancel("b", at_time=1)
    scheduler.run_until()
    assert scheduler.records["b"].status is RunStatus.CANCELLED
    assert scheduler.records["b"].start_time is None


def test_big_job_not_starved_by_small_jobs():
    scheduler = make_big_cluster()
    scheduler.submit(req("big", 100, 3))  # 需要整节点
    scheduler.run_until(end_time=0)
    # 小任务持续到来，每次 30MB，足以在无保护时不断填满节点
    for t in range(0, 12):
        scheduler.submit(req(f"s{t}", 30, 2, submit=t))
    scheduler.run_until()
    big = scheduler.records["big"]
    assert big.status is RunStatus.COMPLETED
    # 阈值 5：最迟在一个空闲槽/内存释放后被启动，等待有界
    assert big.wait_time <= 7


def test_node_outage_moves_work_and_fails_running():
    scheduler = Scheduler([
        Node("n1", 100, 2),
        Node("n2", 100, 2),
    ], starvation_threshold=50)
    scheduler.submit(req("a", 80, 10))
    scheduler.submit(req("b", 60, 3), )
    scheduler.run_until(end_time=0)
    node_a = scheduler.records["a"].node_id
    scheduler.add_outage(node_a, Outage(start=2, end=5))
    scheduler.run_until()
    assert scheduler.records["a"].status is RunStatus.FAILED
    recovered = scheduler.cluster.runtime_for(node_a)
    assert recovered.available is True and recovered.used_memory == 0


def test_pending_job_avoids_unavailable_node():
    scheduler = Scheduler([
        Node("n1", 100, 2),
        Node("n2", 100, 2),
    ], starvation_threshold=100)
    scheduler.add_outage("n1", Outage(start=0, end=10))
    scheduler.submit(req("a", 80, 2))
    scheduler.run_until()
    assert scheduler.records["a"].node_id == "n2"


def test_identity_stable_under_queue_policy():
    scheduler = make_big_cluster()
    scheduler.submit(req("big", 100, 2))
    scheduler.submit(req("s0", 10, 1, submit=1))
    scheduler.run_until()
    assert set(scheduler.records) == {"big", "s0"}
    assert scheduler.records["big"].experiment_id == "big"


def test_save_and_resume(tmp_path):
    scheduler = make_big_cluster()
    scheduler.submit(req("long", 80, 20))
    scheduler.submit(req("later", 40, 2, submit=8))
    scheduler.run_until(end_time=5)
    path = tmp_path / "state.json"
    scheduler.save_state(path)

    resumed = Scheduler.load_state(path)
    assert resumed.now == 5
    assert resumed.records["long"].status is RunStatus.RUNNING
    assert resumed.cluster.runtime_for(resumed.records["long"].node_id).used_memory == 80
    resumed.run_until()
    assert resumed.records["long"].status is RunStatus.COMPLETED
    assert resumed.records["later"].status is RunStatus.COMPLETED
    assert resumed.records["later"].start_time == 8


def test_resume_preserves_identity_and_events(tmp_path):
    scheduler = make_big_cluster()
    scheduler.submit(req("a", 30, 2))
    scheduler.run_until()
    path = tmp_path / "state.json"
    scheduler.save_state(path)
    resumed = Scheduler.load_state(path)
    assert resumed.records["a"].events[0].kind == "submitted"
    assert resumed.records["a"].events[-1].kind == "completed"
