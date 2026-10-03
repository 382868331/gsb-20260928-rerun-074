# 离线实验资源调度系统

纯 Python（仅标准库）的离散事件模拟器：管理虚拟计算节点、实验需求和运行
记录，不依赖任何真实集群。

## 资源模型

- 节点内置内存（MB）与并发槽（max_jobs），还可通过
  `Node(..., capacities={"gpu": 1, "device": 2})` 声明独占设备等命名资源。
- 阶段用 `Stage(..., resources={"gpu": 1})` 声明多类资源；内存、槽位和所有
  命名资源是同一个原子需求，全部满足才启动，不会只拿到一部分就执行。
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

## 节点维护与迁移限制

add_outage(node_id, Outage(start, end))：

- `Stage(migratable=True)`：维护开始时立即释放当前节点的全部资源，带着阶段
  剩余时长迁移到其他满足完整资源组合的节点；阶段不重算。
- `Stage(migratable=False)`：维护开始时也释放资源，避免维护等待期间占着容量，
  但任务钉在原节点，只能在维护结束后用剩余时长续跑，不能迁到其他节点。
- `safe_point=False` 且未进入维护的阶段不会在边界被高优任务抢占；下一阶段资源
  不足或节点维护时先释放，再进入显式等待。
- 排队任务的前瞻规划自动绕开维护节点。
- 区间结束后节点恢复，继续参与放置。

## 阻塞原因与调度预算

`scheduler.last_blockages` 保存最近一次调度尝试中每个未启动实验的原因：

- `resource_shortage`：当前没有任何节点拥有或能在未来释放完整资源组合；
- `maintenance`：容量存在，但唯一可行节点处于维护窗口；
- `budget_limit`：本次 `run_until(..., schedule_budget=n)` 最多只允许新启动 n 个阶段。

## 资源预约：承诺与实际占用分离

`Scheduler.reserve(experiment_id, deadline, not_before=None)` 为一组有共同
交付期限的阶段建立资源预约：

1. 预约是承诺不是占用：ReservedWindow 只在前瞻规划中阻塞其他实验，真正
   start 时才走 NodeRuntime.allocate；`capacity_view()` 在任务未开始时
   就能看到各节点已承诺（windows / reserved_now）与仍可分配
   （allocatable_now）的容量。
2. 后到任务不能无说明地挤掉已确认预约：队列前瞻把他人窗口当作区间级硬
   约束（开始负增量、结束正增量），被挡住的任务在 `last_blockages` 中
   留下明确原因；属主自己的窗口不参与自我阻塞。
3. 窗口被消费（consumed）后即停止阻塞：承诺转为实际占用的瞬间只计一次账，
   阶段间释放再申请、多类资源同时需要都不会重复扣账。
4. 期限内排不下全部阶段时抛出 ReservationConflictError，conflicts 携带
   冲突范围（阶段、各节点最早可排时刻、期限差距），绝不伪造可行日程。
5. `withdraw_reservation(rid)` / `adjust_reservation(rid, stages=, deadline=)`
   撤回或调整未消费部分，返回 {before, after, changed} 报告对其他排队任务
   阻塞原因的影响；`assess_withdrawal(rid)` 只评估不执行。调整不可行时
   原承诺保持不变。
6. 维护计划改变（add_outage 与窗口重叠）或实际启动晚于窗口时，预约被标记
   BREACHED 并记录 breach_reasons；承诺（Reservation.events）、实际执行
   （RunRecord.events）与违约原因三条账目分别保存，随时可独立核对。
7. 预约随 save_state/load_state 持久化：维护延长、任务提前结束、预约取消
   交错的场景保存后接续，原承诺、实际执行与违约原因仍然各自可查。

## 状态保存与接续模拟

Scheduler.save_state(path) 保存时钟、节点、命名资源占用、维护窗口、调度预算、
需求、运行记录（含完整事件流水）、阶段剩余时长与未来事件堆；
Scheduler.load_state(path) 后可直接 run_until 继续模拟。保存重开后每个
stage_started 只出现一次，迁移续跑记录为 stage_resumed，资源最终账目与
不中断运行一致。

## 运行
    .venv/bin/python examples/demo.py
    .venv/bin/python examples/multistage_demo.py
    .venv/bin/python -m pytest -q
