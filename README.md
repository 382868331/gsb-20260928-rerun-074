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

## 资源预约与承诺核对

`reserve_deadline(experiment_id, deadline, earliest_start=...)` 为共同交付期限
建立预约；多阶段任务会按阶段顺序检查完整窗口。任何阶段不能在期限前完成时，
返回 `ReservationReport(ok=False, conflicts=[...])`，冲突中给出阶段、请求时间
范围、期限、节点和原因，不会伪造可行日程。

- 预约与真实占用分开记账：未开始的容量只出现在 `promised`，真正启动后预约
  变为 `consumed`，容量转入 `actual`，同一资源不会被预约和运行重复扣账。
- `reservation_capacity_view(time)` 返回每个节点的 `capacity`、`promised`、
  `actual` 和剩余 `available`，任务尚未开始也能查看已承诺容量。
- 后到任务可以使用预约前的空闲窗口，但前瞻会检查整个运行区间，不能侵入已
  确认预约。
- `cancel_reservation(id)` 可撤回单个阶段预约；`adjust_reservation(id,start,end)`
  调整预约，旧承诺会标记为 `cancelled` 并生成新 ID，释放容量可重新分配。
- `evaluate_reservation_change(cancel_ids=[...], adjustments={...})` 不修改当前
  模拟，而是在快照副本中比较变更前后的任务终态、启动/结束时间和等待原因。
- 维护窗口与已确认预约重叠时，预约保留为 `breached` 并记录具体维护区间；
  受影响承诺不会被后到任务静默替换。
- 任务提前结束或取消时，尚未消费的后续预约会取消；预约、真实执行和违约事件
  分别写入任务事件流水，快照中也会完整保存。

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
