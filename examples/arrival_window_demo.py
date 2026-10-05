"""到场窗口与多阶段延迟传播对比示例。"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scheduler import ExperimentRequest, ExpiryPolicy, Node, Scheduler, Stage


def gpu_stage(mem, dur):
    return Stage(mem, dur, resources={"gpu": 1})


def arrival_window_demo():
    scheduler = Scheduler([Node("n1", 100, 4, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest("late_job", 10, 4, submit_time=5, stages=(gpu_stage(10, 4),)))
    report = scheduler.reserve_deadline(
        "late_job", 12, earliest_start=2, check_in_deadline=3,
        expiry_policy=ExpiryPolicy.RELEASE,
    )
    reservation = report.reservations[0]
    print(f"预约窗口 [{reservation.start},{reservation.end}) 到场截止 {reservation.check_in_deadline}")
    scheduler.submit(ExperimentRequest("standby", 10, 6, submit_time=4, stages=(gpu_stage(10, 6),)))
    scheduler.run_until()
    for event in scheduler.records["late_job"].events:
        print(f"  t={event.time} {event.kind} {event.detail}")
    print(f"standby 启动于 {scheduler.records['standby'].start_time}，late_job 排队到 {scheduler.records['late_job'].start_time}")


def delay_strategies_demo():
    scheduler = Scheduler([Node("n1", 100, 4, capacities={"gpu": 1})])
    scheduler.submit(ExperimentRequest(
        "multi", 10, 9,
        stages=(gpu_stage(10, 4), gpu_stage(10, 3), gpu_stage(10, 2)),
    ))
    scheduler.reserve_deadline("multi", 20, earliest_start=0)
    scheduler.submit(ExperimentRequest("other", 10, 3, stages=(gpu_stage(10, 3),)))
    scheduler.reserve_deadline("other", 20, earliest_start=10)
    scheduler.run_until(end_time=1)
    result = scheduler.evaluate_delay_strategies("multi", delay=3)
    for name, report in result["strategies"].items():
        print(f"方案 {name}: 状态={report['experiment_status']} 他人窗口未动={report['others_windows_unchanged']}")
        for stage in report["stages"]:
            print(
                f"  阶段{stage['stage_index']} 承诺={stage['promised']} 状态={stage['reservation_status']}"
                f" 实际启动={stage['actual_start']} 等待={stage['wait']} 冲突={len(stage['conflicts'])}"
            )


if __name__ == "__main__":
    print("== 到场窗口（RELEASE 到期释放） ==")
    arrival_window_demo()
    print("== 多阶段延迟传播三方案对比 ==")
    delay_strategies_demo()
