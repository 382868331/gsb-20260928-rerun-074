# 离线实验资源调度系统

纯 Python（仅标准库）的离散事件模拟器：管理虚拟计算节点、实验需求和运行
记录，不依赖任何真实集群。

## 资源模型

- 节点资源：内存（MB）、并发槽（max_jobs）与可选具名资源
  （capacity={"cpu": 8, "gpu": 1}，独占设备用 1 份表示）。
- 阶段可同时声明多类资源（Stage(resources={"gpu": 1})），调度是
  全有或全无：任何一类不满足都不启动，等待期间不持有任何资源。
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

## 节点维护窗口与迁移限制

add_maintenance(node_id, MaintenanceWindow(start, end)) 登记计划维护：

- 区间内节点不接受新放置；前瞻规划自动绕开窗口，声明
  migratable=False 的阶段若会跨窗口运行则整体推迟到维护结束后。
- 窗口开始时，当前阶段 migratable=True 的任务迁移到其他节点，保留
  阶段进度（结束时刻不变，阶段不重算）；无处可去则释放全部资源并
  显式等待，绝不占着已不需要的容量。
- migratable=False 的阶段绝不打断：留在本节点跑到阶段结束
  （maintenance_wait 事件），维护只为它推迟生效。

## 阻塞原因区分

排队任务每个 tick 判定互斥的阻塞原因（RunRecord.block_reason 与
blocked 事件，SimulationResult.blocked 汇总）：

1. BlockReason.MAINTENANCE：所有容量足够的节点都在维护/不可用，
   或前瞻启动时刻被维护窗口推迟；
2. BlockReason.BUDGET：Scheduler(budget=N) 的集群级同时在跑阶段数
   已达上限；
3. BlockReason.RESOURCE_SHORTAGE：资源不足，等已在跑任务释放。

判定优先级：维护 > 预算 > 资源不足。

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
流水）、维护窗口、调度预算与未来事件堆；Scheduler.load_state(path) 后
可直接 run_until 继续模拟，实验身份与在跑任务的占用都会恢复。
tests/test_multiresource.py 验证：中途保存重开的事件流水与资源账目和
不中断运行完全一致，每个阶段只完成一次。

## 运行
    .venv/bin/python examples/demo.py
    .venv/bin/python examples/multistage_demo.py
    .venv/bin/python examples/maintenance_demo.py
    .venv/bin/python -m pytest -q
