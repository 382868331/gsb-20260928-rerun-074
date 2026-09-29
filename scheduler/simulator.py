"""确定性事件驱动模拟器：提交/取消/节点事件按 tick 重放，支持存盘接续。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import Job, JobState
from .scheduler import Scheduler


class Simulator:
    def __init__(self, scheduler: Optional[Scheduler] = None):
        self.scheduler = scheduler or Scheduler()
        self.now = 0
        # 未来事件，元素 (tick, seq, type, payload)，保证确定性顺序
        self.events: List[tuple] = []
        self._seq = 0
        self.log: List[Dict[str, Any]] = []

    # ---- 事件编排 -------------------------------------------------------

    def schedule_submit(
        self, at: int, job_id: str, memory_mb: int, duration: int, priority: int = 0
    ) -> None:
        self._add(
            at,
            "submit",
            {
                "id": job_id,
                "memory_mb": memory_mb,
                "duration": duration,
                "submit_time": at,
                "priority": priority,
            },
        )

    def schedule_cancel(self, at: int, job_id: str) -> None:
        self._add(at, "cancel", {"id": job_id})

    def schedule_node_status(
        self, at: int, node_id: str, available: bool, kill_running: bool = False
    ) -> None:
        self._add(
            at,
            "node_status",
            {
                "node_id": node_id,
                "available": available,
                "kill_running": kill_running,
            },
        )

    def _add(self, at: int, kind: str, payload: dict) -> None:
        if at < self.now:
            raise ValueError(f"不能在过去的 {at} 时刻安排事件（当前 {self.now}）")
        self.events.append((at, self._seq, kind, payload))
        self._seq += 1
        self.events.sort(key=lambda e: (e[0], e[1]))

    # ---- 推进 -----------------------------------------------------------

    def run_until(self, end: int) -> List[Dict[str, Any]]:
        """从当前时钟推进到 end（不含）。已处理的时钟点可从快照恢复后续跑。

        每个 tick 的处理顺序：外部事件（提交/取消/节点）→ 完成释放 → 放置。
        提交先于完成可让 t 时刻提交的作业本 tick 启动；
        完成先于放置可让刚释放的容量同 tick 被利用。
        """
        if end < self.now:
            raise ValueError(f"不能回退时钟：{end} < {self.now}")
        new_log: List[Dict[str, Any]] = []
        while self.now < end:
            tick_no = self.now
            due = [e for e in self.events if e[0] == tick_no]
            for _, _, kind, payload in due:
                self._handle(kind, payload, new_log)
            self.events = [e for e in self.events if e not in due]

            finished = self.scheduler.complete(tick_no)
            started = self.scheduler.schedule_pending(tick_no)

            for jid in finished:
                run = self._last_run(jid)
                new_log.append(
                    {
                        "time": tick_no,
                        "event": "finish",
                        "job_id": jid,
                        "state": JobState.COMPLETED.value,
                        "node_id": run.node_id if run else None,
                        "result": run.result if run else None,
                    }
                )
            for jid in started:
                new_log.append(
                    {
                        "time": tick_no,
                        "event": "start",
                        "job_id": jid,
                        "node_id": self.scheduler.jobs[jid].node_id,
                    }
                )
            self.now += 1
        self.log.extend(new_log)
        return new_log

    def _last_run(self, job_id: str):
        run = None
        for r in self.scheduler.runs:
            if r.job_id == job_id:
                run = r
        return run

    def _handle(self, kind: str, payload: dict, new_log: list) -> None:
        if kind == "submit":
            job = Job(**payload)
            state = self.scheduler.submit(job)
            new_log.append(
                {
                    "time": self.now,
                    "event": "submit",
                    "job_id": job.id,
                    "state": state.value,
                }
            )
        elif kind == "cancel":
            if payload["id"] in self.scheduler.jobs:
                state = self.scheduler.cancel(payload["id"], self.now)
                new_log.append(
                    {
                        "time": self.now,
                        "event": "cancel",
                        "job_id": payload["id"],
                        "state": state.value,
                    }
                )
        elif kind == "node_status":
            killed = self.scheduler.set_node_available(
                payload["node_id"],
                payload["available"],
                self.now,
                payload.get("kill_running", False),
            )
            new_log.append(
                {
                    "time": self.now,
                    "event": "node_status",
                    "node_id": payload["node_id"],
                    "available": payload["available"],
                    "killed": killed,
                }
            )

    # ---- 计划预览（不改变状态）------------------------------------------

    def feasible_plan(self) -> List[Dict[str, Any]]:
        """列出当前所有未完成任务可放置的节点；放不下的给出 reason。"""
        sched = self.scheduler
        plan = []
        for job in sched.jobs.values():
            if job.state in (JobState.QUEUED, JobState.RESERVED):
                options = [
                    n.id for n in sched.nodes.values() if sched.can_place(job, n)
                ]
                entry = {"job_id": job.id, "state": job.state.value}
                if options:
                    entry["place_on"] = options
                else:
                    cap = max(n.memory_mb for n in sched.nodes.values())
                    if job.memory_mb > cap:
                        entry["reason"] = "exceeds_cluster_capacity"
                    elif not any(n.available for n in sched.nodes.values()):
                        entry["reason"] = "all_nodes_unavailable"
                    else:
                        entry["reason"] = "insufficient_free_capacity"
                plan.append(entry)
        return plan

    # ---- 持久化 ---------------------------------------------------------

    def save(self, path: str | Path) -> None:
        data = {
            "now": self.now,
            "seq": self._seq,
            "scheduler": self.scheduler.to_dict(),
            "events": [
                {"at": at, "kind": kind, "payload": payload}
                for at, _, kind, payload in self.events
            ],
            "log": self.log,
        }
        Path(path).write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    @classmethod
    def load(cls, path: str | Path) -> "Simulator":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        sim = cls(Scheduler.from_dict(data["scheduler"]))
        sim.now = int(data["now"])
        sim._seq = int(data.get("seq", 0))
        sim.log = list(data.get("log", []))
        for ed in data.get("events", []):
            sim._add(int(ed["at"]), ed["kind"], ed["payload"])
        return sim
