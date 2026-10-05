from scheduler import (
    ExperimentRequest,
    ExpiryPolicy,
    Node,
    Outage,
    ReservationStatus,
    Scheduler,
    Stage,
)
from scheduler.models import RunStatus


def _gpu_stage(mem, dur):
    return Stage(mem, dur, resources={"gpu": 1})


def _late_job_scheduler(policy, check_in_deadline=3):
    scheduler = Scheduler([Node("n1", 100, 4, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest("job", 10, 4, submit_time=5, stages=(_gpu_stage(10, 4),)))
    report = scheduler.reserve_deadline(
        "job", 12, earliest_start=2, check_in_deadline=check_in_deadline, expiry_policy=policy,
    )
    assert report.ok
    scheduler.submit(ExperimentRequest("other", 10, 6, submit_time=4, stages=(_gpu_stage(10, 6),)))
    return scheduler, report.reservations[0]


def test_check_in_within_window_keeps_promise():
    scheduler = Scheduler([Node("n1", 100, 1, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest("job", 10, 2, stages=(_gpu_stage(10, 2),)))
    report = scheduler.reserve_deadline("job", 10, earliest_start=4, check_in_deadline=6)
    reservation = report.reservations[0]
    assert reservation.check_in_deadline == 6
    arrival = scheduler.notify_arrival("job")
    assert arrival.ok and arrival.within_window and not arrival.late
    scheduler.run_until(end_time=4)
    assert scheduler.records["job"].status is RunStatus.RUNNING
    scheduler.run_until()
    assert scheduler.reservations.get(reservation.reservation_id).status is ReservationStatus.CONSUMED


def test_release_policy_frees_capacity_and_late_job_cannot_reclaim():
    scheduler, reservation = _late_job_scheduler(ExpiryPolicy.RELEASE)
    scheduler.run_until()
    stored = scheduler.reservations.get(reservation.reservation_id)
    assert stored.status is ReservationStatus.CANCELLED
    assert stored.breach_reason == "no_show"
    expired = [e for e in scheduler.records["job"].events if e.kind == "reservation_expired"]
    assert expired and expired[0].time == 4
    # 释放后 other 立即占用空出的容量，迟到任务不能夺回
    assert scheduler.records["other"].start_time == 4
    assert scheduler.records["other"].end_time == 10
    assert scheduler.records["job"].start_time == 10
    assert scheduler.records["job"].start_time >= scheduler.records["other"].end_time


def test_hold_policy_keeps_promise_for_late_arrival():
    scheduler, reservation = _late_job_scheduler(ExpiryPolicy.HOLD)
    scheduler.run_until()
    stored = scheduler.reservations.get(reservation.reservation_id)
    # 窗口 [2,7)：到期不释放，容量宁可闲置也不给别人，直到窗口结束才违约
    assert stored.status is ReservationStatus.BREACHED
    hold_events = [e for e in scheduler.records["job"].events if e.kind == "reservation_hold_expired"]
    assert len(hold_events) == 1 and hold_events[0].time == 4
    assert scheduler.records["other"].start_time == 7
    assert scheduler.records["job"].start_time == 13
    view = scheduler.reservation_capacity_view(5)["n1"]
    assert view["promised"]["gpu"] == 1
    assert view["actual"]["gpu"] == 0


def test_checked_in_reservation_does_not_expire():
    scheduler, reservation = _late_job_scheduler(ExpiryPolicy.RELEASE)
    scheduler.run_until(end_time=2)
    arrival = scheduler.notify_arrival("job")
    assert arrival.ok and arrival.within_window
    scheduler.run_until()
    stored = scheduler.reservations.get(reservation.reservation_id)
    # 已核对到场：不按 no_show 释放；但迟到到无法装进窗口时仍记违约
    assert stored.status is ReservationStatus.BREACHED
    expired = [e for e in scheduler.records["job"].events if e.kind == "reservation_expired"]
    assert not expired
    assert scheduler.records["other"].start_time == 7


def test_duplicate_arrival_notification_is_idempotent_across_snapshot(tmp_path):
    scheduler, _ = _late_job_scheduler(ExpiryPolicy.RELEASE)
    scheduler.run_until(end_time=2)
    first = scheduler.notify_arrival("job")
    assert first.ok and not first.duplicate
    path = tmp_path / "state.json"
    scheduler.save_state(path)
    scheduler = Scheduler.load_state(path)
    second = scheduler.notify_arrival("job")
    assert second.duplicate
    events = [e for e in scheduler.records["job"].events if e.kind == "arrival_notified"]
    assert len(events) == 1
    scheduler.run_until()
    expired = [e for e in scheduler.records["job"].events if e.kind == "reservation_expired"]
    assert not expired


def _delay_scenario():
    scheduler = Scheduler([Node("n1", 100, 4, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest(
        "multi", 10, 9,
        stages=(_gpu_stage(10, 4), _gpu_stage(10, 3), _gpu_stage(10, 2)),
    ))
    reserved = scheduler.reserve_deadline("multi", 20, earliest_start=0)
    assert reserved.ok
    scheduler.submit(ExperimentRequest("other", 10, 3, stages=(_gpu_stage(10, 3),)))
    other_res = scheduler.reserve_deadline("other", 20, earliest_start=10)
    assert other_res.ok
    scheduler.run_until(end_time=1)
    assert scheduler.records["multi"].status is RunStatus.RUNNING
    return scheduler, reserved.reservations, other_res.reservations[0]


def test_evaluate_delay_strategies_compares_three_options():
    scheduler, reservations, other_res = _delay_scenario()
    result = scheduler.evaluate_delay_strategies("multi", delay=3)
    assert result["ok"]
    strategies = result["strategies"]
    assert set(strategies) == {"keep", "cancel_affected", "reapply"}

    keep = strategies["keep"]["stages"]
    assert keep[1]["reservation_status"] == "breached"
    assert keep[2]["reservation_status"] == "breached"
    # 阶段 2 等 other 的已确认窗口结束才能启动，逐段列出实际等待
    assert keep[2]["wait"] == keep[2]["actual_start"] - keep[2]["promised"][0]
    assert keep[2]["actual_start"] >= other_res.end

    cancelled = strategies["cancel_affected"]["stages"]
    assert cancelled[1]["reservation_status"] == "cancelled"
    assert cancelled[2]["reservation_status"] == "cancelled"

    reapply = strategies["reapply"]
    assert reapply["conflicts"], "重新申请受 other 窗口阻挡应逐段列出冲突"
    conflict_stages = {c["stage_index"] for c in reapply["conflicts"]}
    assert 1 in conflict_stages

    for name, report in strategies.items():
        assert report["others_windows_unchanged"], name

    # 评估不修改当前模拟
    assert scheduler.now == 1
    for item in reservations[1:]:
        assert scheduler.reservations.get(item.reservation_id).status is ReservationStatus.CONFIRMED
    assert scheduler.records["multi"].status is RunStatus.RUNNING


def test_snapshot_near_expiry_restores_consistent_records(tmp_path):
    def build():
        return _late_job_scheduler(ExpiryPolicy.RELEASE)[0]

    uninterrupted = build()
    uninterrupted.run_until(end_time=3)  # 临近到场窗口到期
    path = tmp_path / "state.json"
    uninterrupted.save_state(path)
    resumed = Scheduler.load_state(path)
    uninterrupted.run_until()
    resumed.run_until()
    assert resumed.reservations.to_list() == uninterrupted.reservations.to_list()
    for eid in ("job", "other"):
        before = [(e.time, e.kind, e.detail) for e in uninterrupted.records[eid].events]
        after = [(e.time, e.kind, e.detail) for e in resumed.records[eid].events]
        assert after == before
    for time in range(0, 13):
        assert resumed.reservation_capacity_view(time) == uninterrupted.reservation_capacity_view(time)


def test_delay_notify_and_maintenance_adjust_consistent_after_restore(tmp_path):
    scheduler, reservations, _ = _delay_scenario()
    scheduler.request_delay("multi", 2)
    scheduler.adjust_reservation(reservations[1].reservation_id, 7, 10)
    scheduler.add_outage("n1", Outage(20, 22))
    scheduler.notify_arrival("multi")
    path = tmp_path / "state.json"
    scheduler.save_state(path)
    resumed = Scheduler.load_state(path)
    scheduler.run_until()
    resumed.run_until()
    assert resumed.reservations.to_list() == scheduler.reservations.to_list()
    for eid in ("multi", "other"):
        before = [(e.time, e.kind, e.detail) for e in scheduler.records[eid].events]
        after = [(e.time, e.kind, e.detail) for e in resumed.records[eid].events]
        assert after == before
    for time in range(0, 25):
        for view in resumed.reservation_capacity_view(time).values():
            assert all(amount >= 0 for amount in view["available"].values()), time
