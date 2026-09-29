import json
from pathlib import Path

import pytest

from scheduler import (
    FIFOQueuePolicy,
    FairAgingPolicy,
    Job,
    JobState,
    Node,
    Scheduler,
    Simulator,
)


def make_cluster(policy=None):
    sched = Scheduler(policy=policy or FairAgingPolicy(reserve_threshold=3))
    sched.add_node(Node("n1", memory_mb=1000, max_running=2))
    sched.add_node(Node("n2", memory_mb=500, max_running=1))
    return sched


def test_memory_and_concurrency_limits():
    """内存与并发槽位双重限制，绝不安排不存在的容量。"""
    sched = make_cluster()
    sched.submit(Job("a", 600, 5, submit_time=0))
    sched.submit(Job("b", 500, 5, submit_time=0))
    started = sched.tick(0)
    assert set(started) == {"a", "b"}  # n1 剩 400MB/1槽，n2 已满

    # c 要 500MB：n1 只剩 400MB（碎片），n2 满 -> 无法放置
    sched.submit(Job("c", 500, 1, submit_time=1))
    assert sched.tick(1) == []
    assert sched.used_memory("n1") == 600

    # 超过集群物理上限 -> 提交即拒绝
    rejected = sched.submit(Job("huge", 2000, 1, submit_time=1))
    assert rejected is JobState.REJECTED


def test_best_fit_reduces_fragmentation():
    """最佳匹配：400MB 任务应优先进入 500MB 节点，给大节点留整空间。"""
    sched = make_cluster()
    sched.submit(Job("small", 400, 2, submit_time=0))
    started = sched.tick(0)
    assert started == ["small"]
    assert sched.jobs["small"].node_id == "n2"


def test_release_happens_exactly_once_on_cancel_and_failure():
    """取消与失败各只释放一次；未开始的取消不产生占用记录。"""
    sched = Scheduler(policy=FairAgingPolicy(reserve_threshold=100))
    sched.add_node(Node("n1", memory_mb=1000, max_running=2))
    sched.submit(Job("a", 600, 10, submit_time=0))
    sched.submit(Job("q", 400, 10, submit_time=0))  # n2 放不下（仅 500 但 a 占 n1）
    sched.tick(0)

    # 在跑时取消
    sched.cancel("a", 2)
    assert sched.jobs["a"].state is JobState.CANCELLED
    assert sched.jobs["a"].release_count == 1
    assert sched.used_memory("n1") == 400  # q 仍在跑，只释放了 a 的 600MB

    # 再次取消/完成不再释放
    sched.cancel("a", 3)
    assert sched.jobs["a"].release_count == 1
    closed = [r for r in sched.runs if r.job_id == "a"]
    assert len(closed) == 1 and closed[0].result == "cancelled"

    # 排队任务取消：不占资源
    sched.submit(Job("w", 300, 10, submit_time=2))  # n1 剩 500MB/1槽，可放
    sched.tick(2)
    sched.submit(Job("q2", 500, 10, submit_time=2))  # 放不下，排队
    sched.cancel("q", 2)
    sched.cancel("q2", 2)
    assert sched.jobs["q2"].state is JobState.CANCELLED
    assert sched.jobs["q2"].release_count == 0
    assert all(r.job_id != "q2" for r in sched.runs)

    # 节点故障杀掉在跑任务，同样只释放一次
    sched.submit(Job("b", 300, 10, submit_time=2))
    sched.tick(2)
    killed = sched.set_node_available("n1", False, 3, kill_running=True)
    assert set(killed) == {"b", "w"}
    assert sched.jobs["b"].state is JobState.FAILED
    assert sched.jobs["b"].release_count == 1
    assert sched.jobs["w"].release_count == 1
    assert sched.used_memory("n1") == 0


def test_big_job_not_starved_by_continuous_small_jobs():
    """公平策略核心：持续小任务流不能让大任务无限等待。"""
    sched = Scheduler(policy=FairAgingPolicy(reserve_threshold=3))
    sched.add_node(Node("n1", memory_mb=1000, max_running=2))
    sim = Simulator(sched)

    sim.schedule_submit(0, "seed", 500, duration=8)
    sim.schedule_submit(1, "BIG", 1000, duration=3)
    for t in range(2, 20):
        sim.schedule_submit(t, f"s{t}", 500, duration=2)
    sim.run_until(25)

    big = sched.jobs["BIG"]
    assert big.state is JobState.COMPLETED
    assert big.start_time <= 8


def test_fifo_without_reservation_starves_big_job():
    """对照：无预留冻结时，持续小任务会让需要整节点的大任务饿死。"""
    sched = Scheduler(policy=FIFOQueuePolicy())
    sched.add_node(Node("n1", memory_mb=1000, max_running=2))
    sim = Simulator(sched)
    sim.schedule_submit(0, "seed", 500, duration=20)
    sim.schedule_submit(1, "BIG", 1000, duration=3)
    for t in range(2, 20):
        sim.schedule_submit(t, f"s{t}", 500, duration=2)
    sim.run_until(20)
    assert sched.jobs["BIG"].state is JobState.QUEUED
    assert sched.jobs["BIG"].start_time is None


def test_node_temporarily_unavailable_then_recovers():
    """节点 cordon 期间不放新任务，恢复后排队任务继续；预留会改挂节点。"""
    sched = make_cluster()
    sim = Simulator(sched)
    sim.schedule_submit(0, "x", 500, duration=12)  # 占 n2
    sim.schedule_node_status(1, "n2", available=False)
    sim.schedule_submit(2, "y", 500, duration=2)  # n2 维护，只能去 n1
    sim.run_until(3)
    assert sched.jobs["y"].node_id == "n1"

    sim.schedule_node_status(13, "n2", available=True)
    sim.schedule_submit(13, "z", 450, duration=1)
    sim.run_until(14)
    assert sched.jobs["z"].start_time == 13
    assert sched.jobs["z"].node_id == "n2"


def test_save_and_resume_matches_continuation(tmp_path: Path):
    """存盘后接续模拟，结果与一次性跑完一致。"""

    def build(sim: Simulator) -> None:
        sim.schedule_submit(0, "BIG", 800, duration=3)
        for t in range(0, 10):
            sim.schedule_submit(t, f"s{t}", 400, duration=2)
        sim.schedule_node_status(6, "n2", available=False)
        sim.schedule_node_status(9, "n2", available=True)
        sim.schedule_cancel(7, "s8")

    sched_a = make_cluster()
    sim_a = Simulator(sched_a)
    build(sim_a)
    sim_a.run_until(14)

    sched_b = make_cluster()
    sim_b = Simulator(sched_b)
    build(sim_b)
    sim_b.run_until(6)
    snapshot = tmp_path / "state.json"
    sim_b.save(snapshot)

    # 快照内容自洽
    raw = json.loads(snapshot.read_text(encoding="utf-8"))
    assert raw["now"] == 6
    assert {"id", "state", "node_id"} <= set(raw["scheduler"]["jobs"][0])

    sim_c = Simulator.load(snapshot)
    sim_c.run_until(14)

    for jid, job_a in sim_a.scheduler.jobs.items():
        job_c = sim_c.scheduler.jobs[jid]
        assert (
            job_c.state,
            job_c.start_time,
            job_c.finish_time,
            job_c.node_id,
            job_c.release_count,
        ) == (
            job_a.state,
            job_a.start_time,
            job_a.finish_time,
            job_a.node_id,
            job_a.release_count,
        ), jid


def test_job_identity_unchanged_under_policy_change():
    """切换队列策略不改变实验身份（id 不变），且可跨策略恢复状态。"""
    sched = make_cluster()
    sched.submit(Job("exp-1", 300, 2, submit_time=0, priority=5))
    sched.tick(0)
    data = sched.to_dict()
    # 恢复后换成纯 FIFO 策略，作业身份保持
    data["policy"] = {"name": "fifo"}
    restored = Scheduler.from_dict(data)
    assert "exp-1" in restored.jobs
    assert restored.jobs["exp-1"].priority == 5
    assert isinstance(restored.policy, FIFOQueuePolicy)
