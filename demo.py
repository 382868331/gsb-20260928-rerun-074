"""场景演示：资源碎片、节点临时不可用、大任务防饿死、存盘接续。

运行：python demo.py
"""

from __future__ import annotations

from pathlib import Path

from scheduler import FairAgingPolicy, Node, Simulator, Scheduler


def print_log(title: str, events: list) -> None:
    print(f"--- {title} ---")
    for e in events:
        if e["event"] == "start":
            print(f"t={e['time']:>2}  启动 {e['job_id']} -> {e['node_id']}")
        elif e["event"] == "finish":
            print(
                f"t={e['time']:>2}  结束 {e['job_id']} ({e['state']}@{e['node_id']})"
            )
        elif e["event"] == "submit":
            print(f"t={e['time']:>2}  提交 {e['job_id']} [{e['state']}]")
        elif e["event"] == "node_status":
            print(
                f"t={e['time']:>2}  节点 {e['node_id']} "
                f"{'恢复' if e['available'] else '临时不可用'}"
                + (f"，杀掉 {e['killed']}" if e.get("killed") else "")
            )
        elif e["event"] == "cancel":
            print(f"t={e['time']:>2}  取消 {e['job_id']} -> {e['state']}")


def main() -> None:
    # 集群：n1 1000MB/2 槽（小任务会把它切成碎片），n2 500MB/1 槽用于节点维护演示。
    sched = Scheduler(policy=FairAgingPolicy(reserve_threshold=3, aging_factor=1.0))
    sched.add_node(Node("n1", memory_mb=1000, max_running=2))
    sched.add_node(Node("n2", memory_mb=500, max_running=1))
    sim = Simulator(sched)

    # 大任务 1000MB（需要整台 n1）：t1 到达时 n1 已有长期小任务钉住一个槽，
    # 之后每 tick 都来 500MB 小任务续上另一个槽，朴素回填会让它无限等待
    sim.schedule_submit(0, "anchor", memory_mb=800, duration=6)
    sim.schedule_submit(1, "BIG", memory_mb=1000, duration=3)
    for i in range(2, 9):
        sim.schedule_submit(i, f"s{i}", memory_mb=200, duration=2)

    # t9 取消一个排队小任务，演示未开始任务的取消（不产生资源释放）
    sim.schedule_cancel(9, "s8")

    # 不存在的容量：1200MB 超过最大节点 1000MB，提交即拒绝
    sim.schedule_submit(2, "TOO_BIG", memory_mb=1200, duration=1)

    # 节点临时不可用：t6 cordon n2（在跑的继续），t10 恢复
    sim.schedule_node_status(6, "n2", available=False)
    sim.schedule_node_status(10, "n2", available=True)

    # 前半段运行后存盘
    half = sim.run_until(6)
    print_log("t0-t5：碎片 + 大任务老化进入预留（节点被冻结）", half)
    print("当前未完成任务的可行放置计划：")
    for item in sim.feasible_plan():
        print("   ", item)

    state_path = Path("state_snapshot.json")
    sim.save(state_path)
    print(f"调度状态已保存到 {state_path}，模拟时钟 t={sim.now}")

    # 新进程视角：从快照接续模拟
    sim2 = Simulator.load(state_path)
    rest = sim2.run_until(12)
    print_log("t6-t12：从快照接续，大任务必然启动、节点不可用与恢复", rest)

    # 结果核对：每个作业至多释放一次；BIG 一定在某个有限时刻启动
    jobs = sim2.scheduler.jobs
    over_release = [j.id for j in jobs.values() if j.release_count > 1]
    big = jobs["BIG"]
    print("--- 校验 ---")
    print("TOO_BIG 状态:", jobs["TOO_BIG"].state.value)
    print("BIG 启动时刻:", big.start_time, "完成时刻:", big.finish_time)
    print("释放超过一次的作业:", over_release or "无")
    assert big.start_time is not None, "大任务被小任务饿死！"
    assert not over_release, "存在重复释放！"
    state_path.unlink()


if __name__ == "__main__":
    main()
