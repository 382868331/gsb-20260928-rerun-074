from scheduler import ExperimentRequest, Node, Outage, ReservationStatus, Scheduler, Stage
from scheduler.models import RunStatus


def _gpu_scheduler():
    return Scheduler([Node("n1", 100, 1, capacities={"gpu": 1})])


def _single_stage(eid, duration, **kwargs):
    return ExperimentRequest(eid, 10, duration, stages=(Stage(10, duration, resources={"gpu": 1}),), **kwargs)


def test_checkin_within_window_consumes_promise():
    scheduler = _gpu_scheduler()
    scheduler.submit(_single_stage("job", 2))
    report = scheduler.reserve_stage("job", 0, 10, earliest_start=4, checkin_window=2)
    assert report.ok
    reservation = report.reservation
    assert reservation.checkin_deadline == 6
    assert scheduler.notify_arrival("job")
    assert not scheduler.notify_arrival("job")
    scheduler.run_until(end_time=4)
    assert scheduler.records["job"].status is RunStatus.RUNNING
    view = scheduler.reservation_capacity_view(4)
    assert view["n1"]["promised"]["gpu"] == 0
    assert view["n1"]["actual"]["gpu"] == 1
    scheduler.run_until()
    stored = scheduler.reservations.get(reservation.reservation_id)
    assert stored.status is ReservationStatus.CONSUMED
    assert stored.checked_in


def test_expired_window_releases_capacity_when_policy_is_release():
    scheduler = _gpu_scheduler()
    scheduler.submit(_single_stage("job", 4, submit_time=10))
    report = scheduler.reserve_stage("job", 0, 10, earliest_start=2, checkin_window=2, release_on_expiry=True)
    reservation = report.reservation
    scheduler.run_until(end_time=6)
    stored = scheduler.reservations.get(reservation.reservation_id)
    assert stored.status is ReservationStatus.EXPIRED
    view = scheduler.reservation_capacity_view(4)
    assert view["n1"]["promised"]["gpu"] == 0
    kinds = [event.kind for event in scheduler.records["job"].events]
    assert "reservation_expired" in kinds
    scheduler.run_until()
    assert scheduler.records["job"].status is RunStatus.COMPLETED
    assert scheduler.records["job"].start_time == 10


def test_expired_window_is_held_when_policy_is_keep():
    scheduler = _gpu_scheduler()
    scheduler.submit(_single_stage("job", 4, submit_time=7))
    report = scheduler.reserve_stage("job", 0, 8, earliest_start=2, checkin_window=2, release_on_expiry=False)
    reservation = report.reservation
    scheduler.run_until(end_time=5)
    stored = scheduler.reservations.get(reservation.reservation_id)
    assert stored.status is ReservationStatus.HELD
    view = scheduler.reservation_capacity_view(4)
    assert view["n1"]["promised"]["gpu"] == 1
    scheduler.run_until()
    assert scheduler.reservations.get(reservation.reservation_id).status is ReservationStatus.CONSUMED
    assert scheduler.records["job"].status is RunStatus.COMPLETED
    assert scheduler.records["job"].start_time == 7


def test_late_task_cannot_reclaim_capacity_given_to_others():
    scheduler = _gpu_scheduler()
    scheduler.submit(_single_stage("job", 2, submit_time=6))
    report = scheduler.reserve_stage("job", 0, 10, earliest_start=0, checkin_window=1, release_on_expiry=True)
    scheduler.submit(_single_stage("other", 6, priority=1))
    scheduler.run_until()
    assert scheduler.reservations.get(report.reservation.reservation_id).status is ReservationStatus.EXPIRED
    other = scheduler.records["other"]
    job = scheduler.records["job"]
    assert other.status is RunStatus.COMPLETED
    assert job.status is RunStatus.COMPLETED
    assert job.start_time >= other.end_time


def test_delay_strategies_compare_keep_cancel_and_rebook():
    scheduler = _gpu_scheduler()
    scheduler.submit(ExperimentRequest(
        "multi", 10, 6,
        stages=(
            Stage(10, 2, resources={"gpu": 1}, migratable=False),
            Stage(10, 2, resources={"gpu": 1}),
            Stage(10, 2, resources={"gpu": 1}),
        ),
    ))
    report = scheduler.reserve_deadline("multi", 20, earliest_start=0)
    assert report.ok
    scheduler.submit(_single_stage("other", 2))
    other_report = scheduler.reserve_stage("other", 0, 30, earliest_start=10)
    assert other_report.ok
    scheduler.add_outage("n1", Outage(1, 3))
    scheduler.run_until(end_time=3)
    # 维护与自身后续承诺让 multi 延迟，stage2 的承诺窗口已不可达
    assert scheduler.records["multi"].status is RunStatus.PENDING

    comparison = scheduler.evaluate_delay_strategies("multi", end_time=40)
    assert comparison["ok"]
    affected_stages = [item["stage_index"] for item in comparison["affected"]]
    assert affected_stages == [2]

    keep = comparison["scenarios"]["keep"]
    cancel = comparison["scenarios"]["cancel_affected"]
    rebook = comparison["scenarios"]["rebook"]
    assert cancel["cancelled"] == [item["reservation_id"] for item in comparison["affected"]]
    assert len(rebook["rebooked"]) == 1
    assert rebook["conflicts"] == []
    for scenario in (keep, cancel, rebook):
        assert scenario["statuses"]["multi"] == "completed"
        assert scenario["statuses"]["other"] == "completed"
        assert 2 in scenario["stage_waits"]
    # 保留旧承诺的方案让 stage2 等得最久
    assert keep["stage_waits"][2] > cancel["stage_waits"][2]

    scheduler.run_until(end_time=40)
    stored_other = scheduler.reservations.get(other_report.reservation.reservation_id)
    assert (stored_other.start, stored_other.end) == (
        other_report.reservation.start,
        other_report.reservation.end,
    )


def test_save_restore_near_expiry_keeps_promise_and_actual_consistent(tmp_path):
    def build():
        scheduler = _gpu_scheduler()
        scheduler.submit(_single_stage("x", 4))
        scheduler.reserve_stage("x", 0, 8, earliest_start=2, checkin_window=3, release_on_expiry=False)
        scheduler.submit(_single_stage("y", 2))
        scheduler.reserve_stage("y", 0, 12, earliest_start=8)
        return scheduler

    baseline = build()
    assert baseline.notify_arrival("x")
    assert not baseline.notify_arrival("x")
    baseline.run_until(end_time=4)
    baseline.add_outage("n1", Outage(8, 9))
    baseline.run_until(end_time=20)

    restored = build()
    assert restored.notify_arrival("x")
    restored.run_until(end_time=4)
    state_path = tmp_path / "state.json"
    restored.save_state(state_path)
    restored = Scheduler.load_state(state_path)
    assert not restored.notify_arrival("x")
    restored.add_outage("n1", Outage(8, 9))
    restored.run_until(end_time=20)

    for eid in ("x", "y"):
        expected = [(e.time, e.kind, e.detail) for e in baseline.records[eid].events]
        actual = [(e.time, e.kind, e.detail) for e in restored.records[eid].events]
        assert actual == expected
    assert restored.reservations.to_list() == baseline.reservations.to_list()
    for moment in (0, 2, 4, 6, 8, 12, 20):
        assert restored.reservation_capacity_view(moment) == baseline.reservation_capacity_view(moment)
    checkins = [e for e in restored.records["x"].events if e.kind == "checkin_notified"]
    assert len(checkins) == 1
