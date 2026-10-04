from scheduler import ExperimentRequest, Node, Outage, ReservationStatus, Scheduler, Stage
from scheduler.models import RunStatus


def test_reservation_separates_promise_from_actual_use():
    scheduler = Scheduler([Node("n1", 100, 1, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest("job", 10, 2, stages=(Stage(10, 2, resources={"gpu": 1}),)))
    report = scheduler.reserve_deadline("job", 10, earliest_start=4)
    assert report.ok
    reservation = report.reservations[0]
    assert reservation.start == 4
    view = scheduler.reservation_capacity_view(4)
    assert view["n1"]["promised"]["gpu"] == 1
    assert view["n1"]["actual"]["gpu"] == 0
    assert view["n1"]["available"]["gpu"] == 0
    scheduler.run_until(end_time=0)
    assert scheduler.records["job"].status is RunStatus.PENDING
    scheduler.run_until(end_time=4)
    assert scheduler.records["job"].status is RunStatus.RUNNING
    assert scheduler.cluster.runtime_for("n1").used["gpu"] == 1
    view = scheduler.reservation_capacity_view(4)
    assert view["n1"]["promised"]["gpu"] == 0
    assert view["n1"]["actual"]["gpu"] == 1
    scheduler.run_until()
    assert scheduler.reservations.get(reservation.reservation_id).status is ReservationStatus.CONSUMED


def test_confirmed_reservation_is_not_replaced_by_later_job():
    scheduler = Scheduler([Node("n1", 100, 1, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest("first", 10, 2, stages=(Stage(10, 2, resources={"gpu": 1}),)))
    scheduler.reserve_deadline("first", 6, earliest_start=4)
    scheduler.submit(ExperimentRequest("late", 10, 2, submit_time=0, stages=(Stage(10, 2, resources={"gpu": 1}),)))
    scheduler.run_until(end_time=4)
    assert scheduler.records["first"].status is RunStatus.RUNNING
    assert scheduler.records["late"].end_time is not None
    assert scheduler.records["late"].end_time <= 4
    scheduler.run_until()
    assert scheduler.records["first"].start_time == 4


def test_multi_stage_deadline_conflict_is_reported_without_fake_schedule():
    scheduler = Scheduler([Node("n1", 100, 1, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest(
        "job", 10, 10,
        stages=(
            Stage(10, 4, resources={"gpu": 1}),
            Stage(10, 4, resources={"gpu": 1}),
        ),
    ))
    scheduler.add_outage("n1", Outage(5, 9))
    report = scheduler.reserve_deadline("job", 10, earliest_start=0)
    assert not report.ok
    assert report.conflicts[0].stage_index == 1
    assert report.conflicts[0].deadline == 10
    assert scheduler.reservations.list(ReservationStatus.CONFIRMED) == []
    assert scheduler.records["job"].status is RunStatus.PENDING


def test_cancel_partial_reservation_releases_capacity_and_reports_feasibility():
    scheduler = Scheduler([Node("n1", 100, 1, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest(
        "multi", 10, 4,
        stages=(Stage(10, 2, resources={"gpu": 1}), Stage(10, 2, resources={"gpu": 1})),
    ))
    report = scheduler.reserve_deadline("multi", 8, earliest_start=0)
    assert report.ok
    first, second = report.reservations
    scheduler.submit(ExperimentRequest("other", 10, 2, priority=1, stages=(Stage(10, 2, resources={"gpu": 1}),)))
    cancel_report = scheduler.cancel_reservation(second.reservation_id)
    assert cancel_report.ok
    scheduler.run_until(end_time=2)
    scheduler.run_until()
    assert scheduler.records["other"].start_time == 2
    assert scheduler.reservations.get(second.reservation_id).status is ReservationStatus.CANCELLED
    assert scheduler.reservations.get(first.reservation_id).status is ReservationStatus.CONSUMED


def test_maintenance_extension_marks_original_promise_breached():
    scheduler = Scheduler([Node("n1", 100, 1, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest("job", 10, 2, stages=(Stage(10, 2, resources={"gpu": 1}),)))
    reservation = scheduler.reserve_deadline("job", 8, earliest_start=4).reservations[0]
    scheduler.add_outage("n1", Outage(4, 6))
    scheduler.run_until()
    stored = scheduler.reservations.get(reservation.reservation_id)
    assert stored.status is ReservationStatus.BREACHED
    assert "maintenance" in stored.breach_reason
    breach_events = [e for e in scheduler.records["job"].events if e.kind == "reservation_breached"]
    assert breach_events and breach_events[0].time == 4


def test_early_finish_cancels_future_stage_reservation_and_runs_waiting_job():
    scheduler = Scheduler([Node("n1", 100, 1, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest(
        "multi", 10, 6,
        stages=(Stage(10, 2, resources={"gpu": 1}), Stage(10, 2, resources={"gpu": 1})),
    ))
    report = scheduler.reserve_deadline("multi", 8, earliest_start=0)
    second = report.reservations[1]
    scheduler.submit(ExperimentRequest("other", 10, 2, stages=(Stage(10, 2, resources={"gpu": 1}),)))
    scheduler.run_until(end_time=0)
    assert scheduler.records["multi"].status is RunStatus.RUNNING
    scheduler.request_early_finish("multi", 1)
    scheduler.run_until()
    assert scheduler.records["multi"].status is RunStatus.COMPLETED
    assert scheduler.records["multi"].end_time == 1
    assert scheduler.reservations.get(second.reservation_id).status is ReservationStatus.CANCELLED
    assert scheduler.records["other"].start_time == 1


def test_reservation_lifecycle_persists_across_snapshot(tmp_path):
    scheduler = Scheduler([Node("n1", 100, 1, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest("job", 10, 2, stages=(Stage(10, 2, resources={"gpu": 1}),)))
    reservation = scheduler.reserve_deadline("job", 8, earliest_start=4).reservations[0]
    path = tmp_path / "state.json"
    scheduler.save_state(path)
    resumed = Scheduler.load_state(path)
    assert resumed.reservations.get(reservation.reservation_id).start == 4
    resumed.run_until(end_time=4)
    assert resumed.records["job"].status is RunStatus.RUNNING
    resumed.run_until()
    assert resumed.reservations.get(reservation.reservation_id).status is ReservationStatus.CONSUMED


def test_evaluate_reservation_change_is_readonly():
    scheduler = Scheduler([Node("n1", 100, 1, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest("a", 10, 2, stages=(Stage(10, 2, resources={"gpu": 1}),)))
    reservation = scheduler.reserve_deadline("a", 8, earliest_start=4).reservations[0]
    scheduler.submit(ExperimentRequest("b", 10, 2, priority=1, stages=(Stage(10, 2, resources={"gpu": 1}),)))
    result = scheduler.evaluate_reservation_change(cancel_ids=[reservation.reservation_id])
    assert result["ok"] is True
    assert result["after"]["statuses"]["a"] == "completed"
    assert scheduler.reservations.get(reservation.reservation_id).status is ReservationStatus.CONFIRMED


def _run_protected_window_scenario(resume_path=None):
    scheduler = Scheduler([Node("n1", 100, 4, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest("reserved", 70, 3))
    reservation = scheduler.reserve_deadline("reserved", 10, earliest_start=7).reservations[0]
    scheduler.submit(ExperimentRequest("big", 70, 12))
    scheduler.submit(ExperimentRequest(
        "gpu_only", 10, 2,
        stages=(Stage(10, 2, resources={"gpu": 1}),),
    ))
    scheduler.run_until(end_time=0)
    assert scheduler.records["big"].status is RunStatus.PENDING
    assert scheduler.last_blockages["big"] == "resource_shortage"
    assert scheduler.records["gpu_only"].status is RunStatus.RUNNING
    if resume_path is not None:
        scheduler.save_state(resume_path)
        scheduler = Scheduler.load_state(resume_path)
    scheduler.run_until()
    return scheduler, reservation


def test_start_eligibility_protects_full_reserved_window():
    scheduler, reservation = _run_protected_window_scenario()
    assert scheduler.records["reserved"].start_time == 7
    assert scheduler.records["reserved"].end_time == 10
    assert scheduler.records["big"].start_time == 10
    assert scheduler.records["big"].end_time == 22
    assert scheduler.records["gpu_only"].start_time == 0
    assert scheduler.reservations.get(reservation.reservation_id).status is ReservationStatus.CONSUMED
    for time in range(0, 23):
        view = scheduler.reservation_capacity_view(time)["n1"]
        assert all(amount >= 0 for amount in view["available"].values()), time


def test_start_eligibility_protects_window_across_snapshot(tmp_path):
    scheduler, reservation = _run_protected_window_scenario(resume_path=tmp_path / "state.json")
    assert scheduler.records["reserved"].start_time == 7
    assert scheduler.records["big"].start_time == 10
    assert scheduler.records["big"].end_time == 22
    started = [e for e in scheduler.records["big"].events if e.kind == "stage_started"]
    assert len(started) == 1 and started[0].time == 10


def test_interleaved_maintenance_cancel_and_adjust_timeline(tmp_path):
    scheduler = Scheduler([Node("n1", 100, 4), Node("n2", 100, 4)])
    scheduler.add_outage("n2", Outage(2, 6))
    scheduler.submit(ExperimentRequest("reserved", 70, 3))
    reservation = scheduler.reserve_deadline("reserved", 10, earliest_start=7).reservations[0]
    scheduler.submit(ExperimentRequest("big", 70, 12))
    scheduler.submit(ExperimentRequest("spare", 50, 2))
    spare_res = scheduler.reserve_deadline("spare", 12, earliest_start=10).reservations[0]
    scheduler.run_until(end_time=0)
    assert scheduler.last_blockages["big"] == "resource_shortage"
    path = tmp_path / "state.json"
    scheduler.save_state(path)
    scheduler = Scheduler.load_state(path)
    cancel_report = scheduler.cancel_reservation(spare_res.reservation_id)
    assert cancel_report.ok
    assert scheduler.reservations.get(spare_res.reservation_id).status is ReservationStatus.CANCELLED
    view = scheduler.reservation_capacity_view(11)["n1"]
    assert view["promised"]["memory_mb"] == 0
    adjust_report = scheduler.adjust_reservation(reservation.reservation_id, 8, 11)
    assert adjust_report.ok
    moved = adjust_report.reservations[0]
    assert (moved.start, moved.end) == (8, 11)
    scheduler.run_until()
    assert scheduler.records["reserved"].start_time == 8
    assert scheduler.records["reserved"].end_time == 11
    assert scheduler.records["big"].node_id == "n2"
    assert scheduler.records["big"].start_time == 6
    assert scheduler.records["big"].end_time == 18
    assert scheduler.records["spare"].status is RunStatus.COMPLETED
    assert scheduler.reservations.get(moved.reservation_id).status is ReservationStatus.CONSUMED
    for time in range(0, 19):
        for node_view in scheduler.reservation_capacity_view(time).values():
            assert all(amount >= 0 for amount in node_view["available"].values()), time
