# 离线实验资源调度系统

纯 Python（仅标准库）的离散事件模拟器：管理虚拟计算节点、实验需求和运行
记录，不依赖任何真实集群。

## 资源模型

- 节点只有两类资源：内存（MB）与并发槽（max_jobs）。
- 任务在真正 start 时才占用资源；排队期间不预占。
- 完成、失败、取消走同一条幂等释放路径，NodeRuntime.release 对同一实验
  重复调用第二次返回 False，保证只释放一次。

## 作业放置（第一版）

Cluster.find_placement 校验内存与并发槽，容量不足返回 None，绝不安排超卖
容量。需求超过任何单机空容量的实验在提交时抛出 CapacityInfeasibleError，
不会进入队列。

## 同一实验的多阶段资源需求与可中断等待

1. ExperimentRequest 支持 `stages=(Stage(mem, dur, safe_point), ...)`；不传时
   旧的 memory/duration 单阶段接口完全兼容，自动视为一个安全点阶段。
2. 每个阶段只在自己运行时占用资源；阶段结束走与失败/取消同一条幂等释放路径，
   下一阶段不再需要的容量立即归还集群，不会“占着无用容量”阻塞其他实验。
3. 下一阶段容量不足时，记录进入明确的 RunStatus.WAITING，`node_id` 置空、
   占用归零并按 `stage_index` 重新入队；容量满足后从中断的阶段接续。
4. `Stage.safe_point` 声明该阶段结束边界是否安全点：高优先级（priority>0）
   任务容量不足时只能请求在跑的低优先级任务在其声明安全点让出，非安全边界
   绝不抢占；被让出任务在安全点释放并进入 WAITING，已完成阶段不会重算。
5. 非安全边界处若下一阶段在同节点放得下则直接接续（不被抢占），放不下才
   释放并显式等待。
6. 快照保存 `stage_index`、阶段开始时刻与未决让出标记；恢复后从当前阶段
   继续，事件流水保证每个 `stage_started` 只出现一次。

示例：

    ExperimentRequest(
        "exp", 90, 10,
        stages=(Stage(90, 4, safe_point=True), Stage(20, 3, safe_point=False)),
        priority=0,
    )

## 公平策略：老化提权 + 饥饿预留

1. experiment_id 是实验的稳定身份，队列策略只改变服务顺序。
2. 基准为 FIFO；等待超过 starvation_threshold 后，每多等 1 个时间单位
   优先级分数减 1（老化提权）。
3. 每个 tick 用前瞻模拟（FairQueuePolicy.plan_targets）为排队任务确定
   最早启动节点；饥饿任务的目标容量被硬预留，持续到来的小任务无法占用。
4. 因此大任务的等待上界约为 starvation_threshold 加已在跑任务的剩余
   时长，不会被小任务无限饿死。tests/test_fairness.py 用随机到达模式验证。

## 节点临时不可用

add_outage(node_id, Outage(start, end))：

- 区间开始时该节点不再接受新任务，正在运行的任务失败并释放资源；
- 排队任务的前瞻规划自动绕开不可用节点；
- 区间结束后节点恢复，继续参与放置。

## 状态保存与接续模拟

Scheduler.save_state(path) 保存时钟、节点、需求、运行记录（含完整事件
流水）与未来事件堆；Scheduler.load_state(path) 后可直接 run_until 继续
模拟，实验身份与在跑任务的占用都会恢复。

## 运行
    .venv/bin/python examples/demo.py
    .venv/bin/python examples/multistage_demo.py
    .venv/bin/python -m pytest -q
