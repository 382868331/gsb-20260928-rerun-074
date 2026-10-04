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


def test_future_reservation_protects_full_execution_interval():
    # 容量100，7-10已承诺70；70x12的任务不能只因0时刻空闲就启动
    scheduler = Scheduler([Node("n1", 100, 4)])
    scheduler.submit(ExperimentRequest("reserved", 70, 3))
    report = scheduler.reserve_deadline("reserved", 10, earliest_start=7)
    assert report.ok
    scheduler.submit(ExperimentRequest("long", 70, 12))
    scheduler.run_until(end_time=0)
    assert scheduler.records["long"].status is RunStatus.PENDING
    assert scheduler.last_blockages["long"] == "reservation_conflict"
    scheduler.run_until(end_time=7)
    assert scheduler.records["reserved"].status is RunStatus.RUNNING
    assert scheduler.records["long"].status is RunStatus.PENDING
    assert scheduler.last_blockages["long"] == "reservation_conflict"
    scheduler.run_until()
    assert scheduler.records["reserved"].start_time == 7
    assert scheduler.records["reserved"].end_time == 10
    assert scheduler.records["long"].start_time == 10
    assert scheduler.records["long"].end_time == 22


def test_disjoint_stage_resources_do_not_overblock():
    # 预约只承诺 gpu；纯内存长任务与后续纯内存阶段不应被它阻塞
    scheduler = Scheduler([Node("n1", 100, 4, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest("gpujob", 10, 2, stages=(Stage(10, 2, resources={"gpu": 1}),)))
    report = scheduler.reserve_deadline("gpujob", 8, earliest_start=4)
    assert report.ok
    scheduler.submit(ExperimentRequest("memjob", 60, 10))
    scheduler.submit(ExperimentRequest(
        "multi", 10, 4,
        stages=(Stage(10, 2, resources={"gpu": 1}), Stage(50, 2)),
    ))
    scheduler.run_until(end_time=0)
    assert scheduler.records["memjob"].status is RunStatus.RUNNING
    assert scheduler.records["multi"].status is RunStatus.RUNNING
    scheduler.run_until()
    assert scheduler.records["memjob"].start_time == 0
    assert scheduler.records["multi"].start_time == 0
    assert scheduler.records["gpujob"].start_time == 4
    assert scheduler.records["gpujob"].end_time == 6
    # multi 的纯内存阶段等 memjob 释放内存后接续，与 gpu 预约无关
    assert scheduler.records["multi"].end_time == 12


def test_interleaved_maintenance_adjust_cancel_and_snapshot(tmp_path):
    scheduler = Scheduler([Node("n1", 100, 4, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest(
        "multi", 10, 4,
        stages=(Stage(10, 2, resources={"gpu": 1}), Stage(60, 2)),
    ))
    report = scheduler.reserve_deadline("multi", 12, earliest_start=2)
    assert report.ok
    first, second = report.reservations
    assert (first.start, first.end) == (2, 4)
    assert (second.start, second.end) == (4, 6)
    # 维护窗口与阶段0预约重叠 -> 预约保留为 breached 并记录区间
    scheduler.add_outage("n1", Outage(2, 4))
    stored = scheduler.reservations.get(first.reservation_id)
    assert stored.status is ReservationStatus.BREACHED
    assert "maintenance=[2,4)" in stored.breach_reason
    # 调整阶段0预约到维护之后，旧承诺取消、生成新 ID
    adjusted = scheduler.adjust_reservation(first.reservation_id, 4, 6)
    assert adjusted.ok
    new_first = adjusted.reservations[0]
    assert new_first.replaces == first.reservation_id
    assert scheduler.reservations.get(first.reservation_id).status is ReservationStatus.CANCELLED
    # 取消阶段1尚未执行的预约 -> 承诺立即释放
    cancel = scheduler.cancel_reservation(second.reservation_id)
    assert cancel.ok
    view = scheduler.reservation_capacity_view(5)
    assert view["n1"]["promised"]["memory_mb"] == 10
    assert view["n1"]["promised"]["gpu"] == 1
    # 纯内存补充任务与承诺不相交，在维护前立即启动并完成
    scheduler.submit(ExperimentRequest("filler", 40, 1))
    scheduler.run_until(end_time=0)
    assert scheduler.records["filler"].status is RunStatus.RUNNING
    # 维护中途保存重开，预约与事件流水完整恢复
    scheduler.run_until(end_time=3)
    path = tmp_path / "state.json"
    scheduler.save_state(path)
    resumed = Scheduler.load_state(path)
    assert resumed.reservations.get(new_first.reservation_id).status is ReservationStatus.CONFIRMED
    resumed.run_until(end_time=5)
    assert resumed.records["multi"].status is RunStatus.RUNNING
    assert resumed.records["multi"].start_time == 4
    view = resumed.reservation_capacity_view()
    assert view["n1"]["actual"]["gpu"] == 1
    assert view["n1"]["promised"]["gpu"] == 0
    resumed.run_until()
    # 账目核对：阶段0消费预约 4-6，阶段1 接续到 8，filler 维护前已完成
    assert resumed.records["multi"].end_time == 8
    assert resumed.records["filler"].end_time == 1
    assert resumed.reservations.get(new_first.reservation_id).status is ReservationStatus.CONSUMED
    started = [e for e in resumed.records["multi"].events if e.kind == "stage_started"]
    assert len(started) == 2
