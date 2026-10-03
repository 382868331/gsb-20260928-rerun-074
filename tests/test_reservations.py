"""资源预约：承诺可见、不可静默挤掉、可撤回/调整、违约可审计、不重复扣账。"""

import pytest

from scheduler import (
    ExperimentRequest,
    Node,
    ReservationConflictError,
    ReservationStatus,
    RunStatus,
    Scheduler,
    Stage,
)
from scheduler.models import Outage
from scheduler.queue import RESOURCE_SHORTAGE


def test_reservation_visible_before_start_and_blocks_latecomers():
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest("big", 60, 5))
    reservation = scheduler.reserve("big", deadline=20)

    # 任务尚未开始：承诺与可分配容量已可见
    view = scheduler.capacity_view()["n1"]
    assert view["used"].get("memory_mb", 0) == 0  # 预约不等于占用
    assert view["reserved_now"]["memory_mb"] == 60
    assert view["allocatable_now"]["memory_mb"] == 40
    assert len(view["windows"]) == 1

    scheduler.submit(ExperimentRequest("late", 60, 2))
    scheduler.run_until(end_time=0)
    # 后到任务不能无说明地挤掉已确认预约：被阻塞且原因可查
    assert scheduler.records["late"].status is RunStatus.PENDING
    assert scheduler.last_blockages["late"] == RESOURCE_SHORTAGE
    # 实际占用只计一次：big 在跑占 60MB，窗口已消费，不重复扣账
    assert scheduler.cluster.runtime_for("n1").used_memory == 60
    assert scheduler.capacity_view()["n1"]["reserved_now"] == {}

    scheduler.run_until()
    late = scheduler.records["late"]
    assert late.start_time == 5  # 窗口 [0,5) 结束后才启动
    assert late.status is RunStatus.COMPLETED
    assert reservation.status is ReservationStatus.FULFILLED
    kinds = [e.kind for e in reservation.events]
    assert "reserved" in kinds and "window_consumed" in kinds


def test_future_window_defends_whole_interval():
    # big 尚未到达，预约窗口 [10,15)；late 若 t=0 启动会穿过窗口，必须让位。
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest("big", 60, 5, submit_time=10))
    scheduler.reserve("big", deadline=30, not_before=10)
    scheduler.submit(ExperimentRequest("late", 60, 20))
    scheduler.run_until(end_time=9)
    assert scheduler.records["late"].status is RunStatus.PENDING
    assert scheduler.last_blockages["late"] == RESOURCE_SHORTAGE
    scheduler.run_until()
    big, late = scheduler.records["big"], scheduler.records["late"]
    assert big.start_time == 10
    assert late.start_time == 15  # 窗口结束后接续，而非静默挤占
    assert big.status is RunStatus.COMPLETED


def test_infeasible_deadline_reports_conflict_scope_not_fake_schedule():
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest("x", 80, 10))
    with pytest.raises(ReservationConflictError) as excinfo:
        scheduler.reserve("x", deadline=5)
    conflict = excinfo.value.conflicts[0]
    assert conflict["stage_index"] == 0
    assert conflict["earliest_end"] == 10
    assert conflict["deadline"] == 5
    assert scheduler.reservations == {}  # 没有伪造可行日程

    scheduler.submit(ExperimentRequest(
        "chain", 80, 4, stages=(Stage(80, 4), Stage(90, 4)),
    ))
    with pytest.raises(ReservationConflictError) as excinfo:
        scheduler.reserve("chain", deadline=6)
    conflict = excinfo.value.conflicts[0]
    assert conflict["stage_index"] == 1
    assert conflict["earliest_end"] == 8


def test_withdraw_releases_capacity_and_reports_impact():
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest("big", 60, 5, submit_time=10))
    scheduler.reserve("big", deadline=30, not_before=10)
    scheduler.submit(ExperimentRequest("late", 60, 20))

    impact = scheduler.assess_withdrawal("res-big")
    assert impact["before"]["late"] == RESOURCE_SHORTAGE
    assert "late" not in impact["after"]
    assert impact["changed"]["late"] == (RESOURCE_SHORTAGE, None)
    # assess 不实际撤回
    assert scheduler.reservations["res-big"].status is ReservationStatus.CONFIRMED

    impact = scheduler.withdraw_reservation("res-big")
    assert impact["changed"]["late"] == (RESOURCE_SHORTAGE, None)
    reservation = scheduler.reservations["res-big"]
    assert reservation.status is ReservationStatus.WITHDRAWN
    assert all(w.released for w in reservation.windows)

    scheduler.run_until()
    # 承诺撤回后，late 立即启动，big 到达时只能等 late 结束
    assert scheduler.records["late"].start_time == 0
    assert scheduler.records["big"].start_time == 20


def test_adjust_reservation_replans_and_keeps_original_on_failure():
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest("big", 60, 5, submit_time=10))
    reservation = scheduler.reserve("big", deadline=30, not_before=10)
    original_window = reservation.active_windows[0]
    assert (original_window.start, original_window.end) == (10, 15)

    report = scheduler.adjust_reservation("res-big", stages=[Stage(50, 3)], deadline=25)
    assert report["reservation"] is reservation
    assert set(report) >= {"before", "after", "changed"}
    assert reservation.deadline == 25
    assert original_window.released
    new_window = reservation.active_windows[0]
    assert (new_window.start, new_window.end) == (10, 13)
    assert new_window.demand["memory_mb"] == 50
    assert any(e.kind == "adjusted" for e in reservation.events)

    # 不可行的调整：抛冲突且原承诺保持不变
    with pytest.raises(ReservationConflictError):
        scheduler.adjust_reservation("res-big", deadline=11)
    assert reservation.active_windows[0] is new_window
    assert reservation.deadline == 25


def test_multi_resource_reservation_blocks_named_capacity():
    scheduler = Scheduler([Node("n1", 100, 2, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest(
        "gpu-job", 10, 4, stages=(Stage(10, 4, resources={"gpu": 1}),),
    ))
    scheduler.reserve("gpu-job", deadline=20)
    scheduler.submit(ExperimentRequest(
        "gpu-late", 10, 2, stages=(Stage(10, 2, resources={"gpu": 1}),),
    ))
    scheduler.run_until(end_time=0)
    assert scheduler.records["gpu-late"].status is RunStatus.PENDING
    scheduler.run_until()
    assert scheduler.records["gpu-late"].start_time == 4


def test_stage_handoff_does_not_double_count():
    # 阶段间释放再申请：stage0 90MB [0,3)，stage1 20MB；b(80MB) 在 t=3 与 stage1 并存。
    scheduler = Scheduler([Node("n1", 100, 3)])
    scheduler.submit(ExperimentRequest(
        "m", 90, 3, stages=(Stage(90, 3), Stage(20, 3)),
    ))
    reservation = scheduler.reserve("m", deadline=20)
    scheduler.submit(ExperimentRequest("b", 80, 2, submit_time=1))
    scheduler.run_until()
    assert scheduler.records["b"].start_time == 3
    assert scheduler.records["m"].status is RunStatus.COMPLETED
    # 全程占用从未超过容量（allocate 超卖会直接抛错），且窗口全部消费
    assert all(w.consumed for w in reservation.windows)


def test_save_load_with_interleaved_maintenance_early_finish_and_withdrawal(tmp_path):
    scheduler = Scheduler([Node("n1", 100, 3)])
    scheduler.submit(ExperimentRequest("quitter", 40, 10))
    scheduler.submit(ExperimentRequest("late", 60, 6))
    scheduler.submit(ExperimentRequest("big", 60, 5, submit_time=10))
    scheduler.submit(ExperimentRequest("other", 50, 7, submit_time=5))
    scheduler.run_until(end_time=0)  # quitter + late 在跑

    big_res = scheduler.reserve("big", deadline=30, not_before=10)
    other_res = scheduler.reserve("other", deadline=40, not_before=5)
    scheduler.add_outage("n1", Outage(12, 20))  # 维护延长，冲击 big 的窗口
    assert big_res.status is ReservationStatus.BREACHED
    assert any("maintenance" in reason for reason in big_res.breach_reasons)

    scheduler.request_cancel("quitter", 2)  # 任务提前结束
    scheduler.run_until(end_time=2)
    assert scheduler.records["quitter"].status is RunStatus.CANCELLED

    scheduler.withdraw_reservation(other_res.reservation_id)  # 预约取消
    assert other_res.status is ReservationStatus.WITHDRAWN

    path = tmp_path / "state.json"
    scheduler.save_state(path)
    loaded = Scheduler.load_state(path)
    # 接续后三条账目分别可核对
    assert loaded.reservations[big_res.reservation_id].status is ReservationStatus.BREACHED
    assert loaded.reservations[big_res.reservation_id].breach_reasons == big_res.breach_reasons
    assert loaded.reservations[other_res.reservation_id].status is ReservationStatus.WITHDRAWN

    loaded.run_until()
    big = loaded.records["big"]
    assert big.status is RunStatus.COMPLETED
    assert big.end_time == 23  # 10 启动，12 维护中断，20 续跑剩余 3
    assert [e.kind for e in big.events].count("stage_started") == 1
    assert any(e.kind == "reservation_breached" for e in big.events)
    assert loaded.records["other"].status is RunStatus.COMPLETED
    assert loaded.records["late"].end_time == 6
    # 资源最终账目清零：承诺与实际占用都没有残留
    assert all(rt.used_memory == 0 for rt in loaded.cluster.runtime.values())
    assert all(not res.active_windows for res in loaded.reservations.values())


def test_reserve_validation():
    scheduler = Scheduler([Node("n1", 100, 2)])
    scheduler.submit(ExperimentRequest("a", 10, 2))
    scheduler.reserve("a", deadline=10)
    with pytest.raises(ValueError):
        scheduler.reserve("a", deadline=20)  # 已有活动预约
    with pytest.raises(ValueError):
        scheduler.reserve("ghost", deadline=10)  # 未知实验
    with pytest.raises(ValueError):
        scheduler.withdraw_reservation("res-ghost")
