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

## 多阶段资源需求与可中断等待

`ExperimentRequest(stages=(StageSpec(mem, dur, safe_point_offsets=...), ...))`
描述同一实验按顺序执行的多个阶段：

- 阶段结束即释放该阶段的内存与槽位；下一阶段若当前放不下，任务进入
  `WAITING`（明确等待），不保留任何旧阶段占用，因此不会占着无用容量
  阻塞其他实验。容量归还后在同一 tick 重新放置。
- `RunRecord.current_stage` 是已完成阶段数，`stage_elapsed` 是当前阶段
  已执行单位；快照保存/恢复这两项，已执行的阶段和时间不会再算一次。

## 高优先抢占：仅在声明的安全点接续

`priority` 越大越优先。高优先任务当前无法放置时，调度器只挑选优先级更
低的在跑任务，在它们 `StageSpec.safe_point_offsets` 声明的阶段内安全点
（阶段结束恒为隐式安全点）让出；无安全点时必须等到阶段边界。

- 受害者在安全点释放资源并转为 `WAITING`，保留 `stage_elapsed`，高优先
  任务完成后只跑剩余时间。
- 同优先级或更高优先级任务不会被抢占；抢占请求只登记最近的安全点。

`Scheduler.remove_node(node_id, at_time)` 安排节点永久退出：时刻到达后
该节点不再接受放置，在跑任务失败并释放账目，排队任务由前瞻规划绕行到
其他节点。

## 状态保存与接续模拟

Scheduler.save_state(path) 保存时钟、节点、需求（含阶段定义与优先级）、
运行记录（含阶段进度与完整事件流水）、未来事件堆与失效事件序列号；
Scheduler.load_state(path) 后可直接 run_until 继续模拟，实验身份、在跑
占用、WAITING 阶段位置与已登记的安全点让出都会恢复。

## 运行
    .venv/bin/python examples/demo.py
    .venv/bin/python -m pytest -q
