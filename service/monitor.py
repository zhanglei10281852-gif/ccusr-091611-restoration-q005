"""通道处理状态机（核心安全逻辑）。

不变量：
1. 任何状态变化先写事件日志，再更新内存投影；投影可由事件流完整重建。
2. observed_at 高水位之外的迟到遥测一律丢弃，处理阶段只许前进不许回退。
3. 联锁等级单调升级，只能被对应角色的显式动作降低。
4. emergency_lock 在一个处理回合内不可解除；supervisor_unlock 开启新回合。
5. FORCE_RELAYS_OPEN 每回合只真正下发一次（DeviceGateway 硬闸门 + 事件重放恢复）。
"""
from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from .device import DeviceGateway

LEVELS = [None, "advisory", "operator_reset", "supervisor_reset", "emergency_lock"]
LEVEL_RANK = {v: i for i, v in enumerate(LEVELS)}
ENERGIZED_STATES = {"ramping", "steady"}

READING_KEEP = 60
WINDOW_SNAPSHOT = 120.0  # 审计快照窗口（秒）


class CommandRejected(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def load_recipes(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))["recipes"][0]


@dataclass
class Reading:
    signal: str
    value: float
    raw_value: float
    unit: str
    observed_at: datetime
    received_at: datetime
    calibration_id: str | None


@dataclass
class ChannelState:
    channel_id: str
    recipe: dict
    state: str = "prepared"
    mode: str = "normal"
    interlock: str | None = None
    round_id: int = 1
    high_water: datetime | None = None
    last_contact: datetime | None = None
    entered_at: datetime | None = None
    resume_to: str | None = None
    precheck_passed_at: datetime | None = None
    maintenance_entered_at: datetime | None = None
    hard_tripped: bool = False
    rinse_started_at: datetime | None = None
    steady_started_at: datetime | None = None
    readings: dict[str, deque] = field(default_factory=dict)
    interlock_reasons: list[dict] = field(default_factory=list)
    transitions: list[dict] = field(default_factory=list)
    actions: list[dict] = field(default_factory=list)
    device_frames: list[dict] = field(default_factory=list)
    opened: bool = False

    def latest(self, signal: str) -> Reading | None:
        q = self.readings.get(signal)
        return q[-1] if q else None


class Monitor:
    def __init__(self, channel_id: str, recipe: dict, store, gateway: DeviceGateway, clock,
                 calibrations=None, replay: bool = False):
        self.s = ChannelState(channel_id=channel_id, recipe=recipe)
        self.store = store
        self.gateway = gateway
        self.clock = clock
        self.cal = calibrations
        self.replaying = replay

    # ---------------------------------------------------------------- 事件
    def _append(self, event_type: str, payload: dict) -> int:
        seq = self.store.append(self.s.channel_id, event_type, payload, self.clock.now())
        return seq

    def _transition(self, to: str, reason: str, trigger: str, reading_window=True):
        if self.s.state == to:
            return
        frm = self.s.state
        seq = self._append("StateTransitioned", {
            "round_id": self.s.round_id, "from": frm, "to": to, "reason": reason,
            "trigger": trigger, "at": self.clock.now().isoformat(),
            "reading_window": self.reading_window() if reading_window else None,
            "resume_to": self.s.resume_to if to == "paused" else None,
        })
        self._apply_transition(seq, frm, to, reason, trigger)

    def _apply_transition(self, seq, frm, to, reason, trigger):
        self.s.transitions.append({
            "seq": seq, "at": self.clock.now().isoformat(), "round_id": self.s.round_id,
            "from": frm, "to": to, "reason": reason, "trigger": trigger,
            "reading_window": self.reading_window(),
        })
        now = self.clock.now()
        self.s.entered_at = now
        self.s.state = to
        if to == "steady" and self.s.steady_started_at is None:
            self.s.steady_started_at = now
        if to == "rinsing":
            self.s.rinse_started_at = now

    def _send(self, frame: str, args: dict, cause: str, cause_seq: int | None = None):
        if self.replaying:
            return None, None
        sent, physically = self.gateway.send(frame, self.s.channel_id, args, self.clock.now(),
                                             cause_seq or 0, cause,
                                             maintenance=self.s.mode == "maintenance")
        if not physically:
            # 网关一次动作闸门命中：首帧事件已在日志中，不重复记录、不重复下发。
            self._append("DeviceFrameSuppressed", {"frame": frame, "cause": cause,
                                                   "reason": "already-sent-this-round",
                                                   "at": self.clock.now().isoformat()})
            return None, sent
        seq = self._append("DeviceFrameSent", {
            "frame": frame, "args": args, "cause": cause, "at": self.clock.now().isoformat(),
        })
        sent.cause_seq = cause_seq or seq
        self.s.device_frames.append({"seq": seq, "frame": frame, "args": args, "cause": cause,
                                     "at": self.clock.now().isoformat()})
        return seq, sent

    def _raise(self, level: str, reasons: list[str], trigger: str):
        assert level in ("advisory", "operator_reset", "supervisor_reset", "emergency_lock")
        if LEVEL_RANK[level] <= LEVEL_RANK[self.s.interlock]:
            return  # 联锁只升不降
        seq = self._append("InterlockRaised", {
            "round_id": self.s.round_id, "level": level, "reasons": reasons, "trigger": trigger,
            "at": self.clock.now().isoformat(), "reading_window": self.reading_window(),
        })
        self.s.interlock = level
        self.s.interlock_reasons.append({"seq": seq, "level": level, "reasons": reasons,
                                         "at": self.clock.now().isoformat(), "active": True})
        if level in ("operator_reset", "supervisor_reset", "emergency_lock"):
            if self.s.state in ENERGIZED_STATES:
                self.s.resume_to = self.s.state
                self._send("SUSPEND_OUTPUT", {}, f"interlock:{level}")
                self._transition("paused", f"联锁 {level}：{';'.join(reasons)}", "interlock")
        if level in ("supervisor_reset", "emergency_lock"):
            if level == "emergency_lock":
                self.s.hard_tripped = True
            self._send("FORCE_RELAYS_OPEN", {}, f"interlock:{level}")
            self._transition("locked", f"联锁 {level}：{';'.join(reasons)}", "interlock")

    def _clear_interlock(self, level: str, by: str):
        seq = self._append("InterlockCleared", {
            "level": level, "by": by, "at": self.clock.now().isoformat()})
        self.s.interlock = None
        for r in reversed(self.s.interlock_reasons):
            if r["active"]:
                r["active"] = False
                r["cleared_by"] = by
                r["cleared_seq"] = seq
                break

    # ------------------------------------------------------------- 读窗口
    def reading_window(self) -> dict:
        out: dict[str, list] = {}
        bound = self.clock.now() - timedelta(seconds=WINDOW_SNAPSHOT)
        for sig, q in self.s.readings.items():
            rows = [r for r in q if r.observed_at >= bound] or list(q)[-3:]
            out[sig] = [{
                "value": r.value, "raw_value": r.raw_value, "observed_at": r.observed_at.isoformat(),
                "received_at": r.received_at.isoformat(), "calibration_id": r.calibration_id,
            } for r in rows[-10:]]
        return {"signals": out, "window_s": WINDOW_SNAPSHOT,
                "evaluated_at": self.clock.now().isoformat()}

    # ------------------------------------------------------------- 开通道
    def open(self, recipe_id: str):
        if self.s.opened:
            raise CommandRejected("channel already opened")
        seq = self._append("ChannelOpened", {"recipe_id": recipe_id, "at": self.clock.now().isoformat()})
        self.s.opened = True
        self.s.entered_at = self.clock.now()

    def seed_state(self, state: str, at: datetime):
        """测试/场景重放：把通道置于指定阶段（事件留痕）。"""
        self.s.opened = True
        self._append("ChannelOpened", {"recipe_id": self.s.recipe["recipe_id"],
                                       "at": at.isoformat(), "seed": True})
        self.s.entered_at = at
        if state != "prepared":
            seq = self.store.append(self.s.channel_id, "StateTransitioned", {
                "round_id": 1, "from": "prepared", "to": state, "reason": "scenario seed",
                "trigger": "seed", "at": at.isoformat(), "reading_window": None,
            }, at)
            self.s.transitions.append({"seq": seq, "at": at.isoformat(), "round_id": 1,
                                       "from": "prepared", "to": state, "reason": "scenario seed",
                                       "trigger": "seed", "reading_window": None})
            self.s.state = state
            self.s.entered_at = at
            if state == "steady":
                self.s.steady_started_at = at

    # ------------------------------------------------------------- 遥测
    def ingest(self, rows: list[dict]) -> list[dict]:
        results = []
        for row in rows:
            results.append(self._ingest_one(row))
        if not self.replaying:
            self.poll_timeouts()
        return results

    def _ingest_one(self, row: dict) -> dict:
        mid = row["message_id"]
        observed_at = datetime.fromisoformat(row["observed_at"])
        received_at = (datetime.fromisoformat(row["received_at"]) if row.get("received_at")
                       else self.clock.now())
        if self.store.message_seen(mid):
            self._append("TelemetryDropped", {"message_id": mid, "reason": "duplicate"})
            return {"message_id": mid, "accepted": False, "reason": "duplicate"}
        if self.s.high_water is not None and observed_at < self.s.high_water:
            self.store.mark_message(mid, self.s.channel_id,
                                    self._append("TelemetryDropped",
                                                 {"message_id": mid, "reason": "stale",
                                                  "observed_at": observed_at.isoformat(),
                                                  "high_water": self.s.high_water.isoformat()}),
                                    self.clock.now())
            return {"message_id": mid, "accepted": False, "reason": "stale"}
        value, cal_id = (self.cal.calibrate(self.s.channel_id, row["signal"], float(row["value"]),
                                            observed_at) if self.cal else (float(row["value"]), None))
        seq = self._append("TelemetryAccepted", {
            "message_id": mid, "signal": row["signal"], "raw_value": row["value"], "value": value,
            "unit": row["unit"], "observed_at": observed_at.isoformat(),
            "received_at": received_at.isoformat(), "calibration_id": cal_id,
        })
        self.store.mark_message(mid, self.s.channel_id, seq, self.clock.now())
        r = Reading(row["signal"], value, float(row["value"]), row["unit"], observed_at,
                    received_at, cal_id)
        q = self.s.readings.setdefault(r.signal, deque(maxlen=READING_KEEP))
        q.append(r)
        self.s.high_water = max(self.s.high_water or observed_at, observed_at)
        self.s.last_contact = max(self.s.last_contact or received_at, received_at)
        if not self.replaying:
            self._evaluate(r)
            self._evaluate_phase_progress()
        return {"message_id": mid, "accepted": True, "value": value, "calibration_id": cal_id}

    # ------------------------------------------------------------- 阈值
    def _evaluate(self, r: Reading):
        st = self.s.recipe["steady"]
        if r.signal == "current":
            self._band(r.value, st["current_warn_a"], st["current_operator_reset_a"],
                       st["current_emergency_lock_a"], "电流(A)", instant_lock=True)
        elif r.signal == "voltage":
            self._band(r.value, st["voltage_warn_v"], st["voltage_operator_reset_v"],
                       st["voltage_emergency_lock_v"], "电压(V)")
        elif r.signal == "temperature":
            self._band(r.value, st["temperature_warn_c"], st["temperature_operator_reset_c"],
                       st["temperature_emergency_lock_c"], "溶液温度(°C)")
        elif r.signal == "chloride":
            self._chloride_rate()

    def _band(self, v, warn, reset, emergency, label, instant_lock=False):
        reasons = []
        if v >= emergency:
            self._raise("emergency_lock", [f"{label} {v:g} ≥ 极端限值 {emergency:g}"], "telemetry")
            return
        if v >= reset:
            # 电流瞬时越过安全线：夜间事故场景 —— 即使短时自行恢复也锁通道，须主管解除。
            level = "supervisor_reset" if instant_lock else "operator_reset"
            self._raise(level, [f"{label} {v:g} ≥ 重置限值 {reset:g}"], "telemetry")
            return
        if v >= warn:
            self._raise("advisory", [f"{label} {v:g} ≥ 告警限值 {warn:g}"], "telemetry")

    def _chloride_rate(self):
        q = self.s.readings.get("chloride")
        if not q or len(q) < 2:
            return
        latest = q[-1]
        floor = latest.observed_at - timedelta(hours=1)
        earlier = next((x for x in q if x.observed_at <= floor), None)
        if not earlier:
            return
        hours = (latest.observed_at - earlier.observed_at).total_seconds() / 3600.0
        if hours <= 0:
            return
        rate = (latest.value - earlier.value) / hours
        limit = self.s.recipe["steady"]["chloride_rate_warn_mg_l_per_h"]
        if rate > limit:
            self._raise("advisory", [f"氯离子上升速率 {rate:.0f} mg/L/h > {limit:g}"], "telemetry")

    def _healthy_now(self) -> bool:
        st = self.s.recipe["steady"]
        c = self.s.latest("current")
        if c and c.value >= st["current_warn_a"]:
            return False
        v = self.s.latest("voltage")
        if v and v.value >= st["voltage_warn_v"]:
            return False
        t = self.s.latest("temperature")
        if t and t.value >= st["temperature_warn_c"]:
            return False
        return True

    # ----------------------------------------------------- 阶段自动推进
    def _evaluate_phase_progress(self):
        st = self.s.state
        now = self.clock.now()
        if st == "ramping" and self.s.entered_at:
            ramp = self.s.recipe["ramp"]
            elapsed = (now - self.s.entered_at).total_seconds()
            cur = self.s.latest("current")
            if elapsed >= ramp["duration_s"] and cur and cur.value >= ramp["target_current_a"] - ramp["current_tolerance_a"]:
                self._send("HOLD_CURRENT", {"setpoint_a": self.s.recipe["steady"]["current_setpoint_a"]},
                           "ramp-complete")
                self._transition("steady", "升流结束，进入恒流稳态", "phase")
        elif st == "rinsing" and self.s.rinse_started_at:
            dur = (now - self.s.rinse_started_at).total_seconds()
            if dur >= self.s.recipe["rinse"]["duration_s"]:
                self._send("STOP_RINSE", {}, "rinse-complete")
                self._transition("completed", "冲洗结束，处理回合完成", "phase")

    def completion_criteria_met(self) -> tuple[bool, str]:
        steady = self.s.recipe["steady"]
        if self.s.steady_started_at is None:
            return False, "尚未进入稳态"
        if (self.clock.now() - self.s.steady_started_at).total_seconds() < steady["completion_min_h"] * 3600:
            return False, f"稳态处理未满 {steady['completion_min_h']} 小时"
        floor = self.clock.now() - timedelta(seconds=steady["completion_window_s"])
        q = self.s.readings.get("chloride")
        recent = [r for r in (q or []) if r.observed_at >= floor]
        if not recent:
            return False, "完成判定窗口内无氯离子读数"
        if any(r.value > steady["completion_chloride_mg_l"] for r in recent):
            return False, f"窗口内仍有读数 > {steady['completion_chloride_mg_l']} mg/L"
        return True, "满足结束条件"

    # ------------------------------------------------------------- 超时
    def poll_timeouts(self):
        if self.replaying or not self.s.opened:
            return
        now = self.clock.now()
        t = self.s.recipe["timeouts"]
        # 传感器失联
        if self.s.last_contact is not None:
            silent_for = (now - self.s.last_contact).total_seconds()
            if silent_for > t["silence_s"]:
                self._on_silence(silent_for)
        if self.s.state == "ramping" and self.s.entered_at:
            if (now - self.s.entered_at).total_seconds() > t["ramp_max_s"]:
                self._raise("operator_reset", [f"升流超过 {t['ramp_max_s']}s 未达稳态"], "timeout")
        if self.s.state == "paused" and self.s.entered_at:
            if (self.s.entered_at and self.s.interlock is None
                    and (now - self.s.entered_at).total_seconds() > t["paused_max_s"]):
                self._raise("advisory", [f"暂停已超过 {t['paused_max_s']}s"], "timeout")
        if self.s.state == "rinsing" and self.s.rinse_started_at:
            if (now - self.s.rinse_started_at).total_seconds() > t["rinse_max_s"]:
                self._raise("advisory", [f"冲洗超过 {t['rinse_max_s']}s"], "timeout")
        if self.s.mode == "maintenance" and self.s.maintenance_entered_at:
            mp = self.s.recipe  # maintenance max 单独文件给出
            if (now - self.s.maintenance_entered_at).total_seconds() > 14400:
                self._raise("advisory", ["维护模式超过 4 小时"], "timeout")
        self._evaluate_phase_progress()

    def _on_silence(self, silent_for: float):
        reason = (f"传感器/网关心跳失联(silence) {silent_for:.0f}s"
                  f"（阈值 {self.s.recipe['timeouts']['silence_s']}s）")
        if self.s.mode == "maintenance":
            # 维护模式：只记录，采用不同的安全策略——不联锁、不暂停、不下发帧。
            self._append("MaintenanceSilenceNoted", {"silent_for_s": round(silent_for, 1)})
            self.s.last_contact = self.clock.now()  # 避免重复记录，维护期间节流
            return
        if self.s.state in ENERGIZED_STATES:
            self._raise("operator_reset", [reason], "silence")
        else:
            self._append("SensorSilenceNoted", {"silent_for_s": round(silent_for, 1),
                                                "state": self.s.state})
            self.s.last_contact = self.clock.now()

    # ------------------------------------------------------------- 命令
    def command(self, command: str, role: str = "operator", idempotency_key: str | None = None,
                payload: dict | None = None, badge: str | None = None) -> dict:
        payload = payload or {}
        cached = self.store.get_command(idempotency_key) if idempotency_key else None
        if cached is not None:
            return {**cached["result"], "replayed": True, "idempotency_key": idempotency_key}
        try:
            result = self._dispatch(command, role, payload, badge)
            accepted = True
            reason = None
        except CommandRejected as exc:
            result = {"accepted": False, "reason": exc.reason}
            accepted = False
            reason = exc.reason
        seq = self._append("OperatorAction", {
            "command": command, "role": role, "badge": badge,
            "idempotency_key": idempotency_key, "accepted": accepted, "reason": reason,
            "result": result, "at": self.clock.now().isoformat(),
        })
        self.s.actions.append({"seq": seq, "command": command, "role": role, "accepted": accepted,
                               "reason": reason, "at": self.clock.now().isoformat()})
        if idempotency_key:
            self.store.save_command(idempotency_key, self.s.channel_id, command, result,
                                    seq if accepted else None, self.clock.now())
        if not self.replaying:
            self.poll_timeouts()
        return {"accepted": accepted, "reason": reason, **(result if accepted else {})}

    def _require_state(self, *allowed):
        if self.s.state not in allowed:
            raise CommandRejected(f"命令在状态 {self.s.state} 下不可用（允许：{','.join(allowed)}）")

    def _dispatch(self, command, role, payload, badge) -> dict:
        if command == "precheck":
            self._require_state("prepared")
            ok, why = self._precheck_pass()
            if not ok:
                raise CommandRejected(f"预检未通过：{why}")
            self.s.precheck_passed_at = self.clock.now()
            self._append("PrecheckPassed", {"at": self.clock.now().isoformat(),
                                            "reading_window": self.reading_window()})
            return {"precheck": "passed"}

        if command == "start":
            self._require_state("prepared")
            grace = self.s.recipe["timeouts"]["precheck_grace_s"]
            if not self.s.precheck_passed_at:
                raise CommandRejected("必须先通过预检")
            if (self.clock.now() - self.s.precheck_passed_at).total_seconds() > grace:
                raise CommandRejected(f"预检已超过 {grace}s 宽限期，需重新预检")
            ramp = self.s.recipe["ramp"]
            self._send("ENABLE_OUTPUT", {"current_limit_a": ramp["instant_current_hard_max_a"]}, "start")
            self._transition("ramping", "操作员启动升流", "operator")
            return {"state": "ramping"}

        if command == "pause":
            self._require_state("ramping", "steady")
            self.s.resume_to = self.s.state
            self._send("SUSPEND_OUTPUT", {}, "operator-pause")
            self._transition("paused", "操作员手动暂停", "operator")
            return {"state": "paused"}

        if command == "resume":
            self._require_state("paused")
            if self.s.interlock is not None:
                raise CommandRejected(f"联锁 {self.s.interlock} 未清除，禁止恢复")
            if self.s.mode == "maintenance":
                raise CommandRejected("维护模式中，禁止恢复带电运行")
            if not self._fresh_healthy():
                raise CommandRejected("恢复前需要最新的正常读数窗口（失联或读数越限）")
            target = self.s.resume_to or "steady"
            if target == "ramping":
                self._send("ENABLE_OUTPUT",
                           {"current_limit_a": self.s.recipe["ramp"]["instant_current_hard_max_a"]},
                           "resume")
            else:
                self._send("HOLD_CURRENT",
                           {"setpoint_a": self.s.recipe["steady"]["current_setpoint_a"]}, "resume")
            self._transition(target, "操作员确认恢复", "operator")
            return {"state": target}

        if command == "acknowledge":
            if self.s.interlock is None:
                raise CommandRejected("当前无活动联锁")
            if self.s.interlock in ("supervisor_reset", "emergency_lock"):
                raise CommandRejected(f"{self.s.interlock} 不能由普通确认解除，须主管解锁")
            if self.s.interlock == "operator_reset" and not self._healthy_now():
                raise CommandRejected("越限读数尚未恢复正常，确认无效")
            level = self.s.interlock
            self._clear_interlock(level, role)
            return {"cleared": level, "state": self.s.state}

        if command == "supervisor_unlock":
            if role != "supervisor":
                raise CommandRejected("需要主管角色")
            self._require_state("locked")
            if self.s.interlock == "emergency_lock" or self.s.hard_tripped:
                raise CommandRejected("emergency_lock 永久闭锁本回合，不能解锁；只能紧急停机后结束")
            self._clear_interlock("supervisor_reset", role)
            # 开启新处理回合：复位设备一次动作闸门与阶段计时。
            self.s.round_id += 1
            self.s.hard_tripped = False
            self.s.steady_started_at = None
            self.s.precheck_passed_at = None
            if not self.replaying:
                self.gateway.reset_round(self.s.channel_id)
            self._send("UNLOCK_RELAYS", {"supervisor_badge": badge}, "supervisor-unlock")
            self._append("RoundStarted", {"round_id": self.s.round_id,
                                          "at": self.clock.now().isoformat()})
            self._transition("prepared", "主管解锁，开始新处理回合", "supervisor")
            return {"state": "prepared", "round_id": self.s.round_id}

        if command == "enter_maintenance":
            self._require_state("paused", "prepared")
            if self.s.mode == "maintenance":
                raise CommandRejected("已处于维护模式")
            self.s.mode = "maintenance"
            self.s.maintenance_entered_at = self.clock.now()
            self._append("MaintenanceEntered", {"at": self.clock.now().isoformat(), "badge": badge})
            return {"mode": "maintenance"}

        if command == "exit_maintenance":
            if self.s.mode != "maintenance":
                raise CommandRejected("当前不在维护模式")
            if self.s.interlock is not None:
                raise CommandRejected(f"维护期间发生过联锁（{self.s.interlock}），退出被拒绝，须主管处理")
            ok, why = self._precheck_pass()
            if not ok:
                raise CommandRejected(f"退出维护需重新通过预检：{why}")
            self.s.mode = "normal"
            self.s.maintenance_entered_at = None
            self._append("MaintenanceExited", {"at": self.clock.now().isoformat(), "badge": badge})
            return {"mode": "normal", "state": self.s.state}

        if command == "emergency_stop":
            if self.s.state == "completed":
                raise CommandRejected("处理已结束")
            # 设备硬帧由网关每回合去重；此处无论是否换 idempotency_key 都只产生一次帧。
            already = (self.s.hard_tripped or
                       (not self.replaying and self.gateway.hard_trip_done(self.s.channel_id)))
            self.s.hard_tripped = True
            if LEVEL_RANK["emergency_lock"] > LEVEL_RANK[self.s.interlock]:
                self._raise("emergency_lock", ["操作员紧急停机"], "operator")
            elif not already:
                self._send("FORCE_RELAYS_OPEN", {}, "emergency-stop")
            # 已停机过：换键/网络重试不再产生任何帧事件，只在操作记录留痕。
            if self.s.state != "locked":
                self._transition("locked", "紧急停机", "operator")
            return {"state": "locked", "interlock": "emergency_lock",
                    "hard_frame_already_sent": self.gateway.hard_trip_done(self.s.channel_id)
                    if not self.replaying else True}

        if command == "begin_rinse":
            self._require_state("steady")
            ok, why = self.completion_criteria_met()
            if not ok:
                raise CommandRejected(f"不满足冲洗条件：{why}")
            self._send("START_RINSE", {}, "begin-rinse")
            self._transition("rinsing", "除氯达标，开始冲洗", "operator")
            return {"state": "rinsing"}

        if command == "finish":
            if self.s.state == "completed":
                return {"state": "completed", "noop": True}
            if self.s.state == "locked" and self.s.hard_tripped:
                self._transition("completed", "紧急停机后结束本回合（异常结束）", "operator")
                return {"state": "completed", "abnormal": True}
            raise CommandRejected("只能在冲洗完成或紧急停机闭锁后结束")

        raise CommandRejected(f"未知命令 {command}")

    def _fresh_healthy(self) -> bool:
        if self.s.last_contact is None:
            return False
        if (self.clock.now() - self.s.last_contact).total_seconds() > self.s.recipe["timeouts"]["silence_s"]:
            return False
        return self._healthy_now()

    def _precheck_pass(self) -> tuple[bool, str]:
        pre = self.s.recipe["precheck"]
        floor = self.clock.now() - timedelta(seconds=pre["window_seconds"])
        for sig in pre["required_readings"]:
            r = self.s.latest(sig)
            if not r or r.observed_at < floor:
                return False, f"{sig} 在 {pre['window_seconds']}s 窗口内无有效读数"
            if sig == "heartbeat" and r.value != 1:
                return False, "网关心跳异常"
        temp = self.s.latest("temperature")
        lo, hi = pre["temperature_window_c"]
        if not (lo <= temp.value <= hi):
            return False, f"温度 {temp.value:g}°C 不在预检窗口 [{lo},{hi}]"
        cl = self.s.latest("chloride")
        if cl.value > pre["chloride_initial_max_mg_l"]:
            return False, f"初始氯离子 {cl.value:g} 超过 {pre['chloride_initial_max_mg_l']}"
        return True, "通过"

    # ------------------------------------------------------------- 快照
    def snapshot(self) -> dict:
        return {
            "channel_id": self.s.channel_id, "state": self.s.state, "mode": self.s.mode,
            "interlock": self.s.interlock, "round_id": self.s.round_id,
            "hard_tripped": self.s.hard_tripped,
            "entered_at": self.s.entered_at.isoformat() if self.s.entered_at else None,
            "last_contact": self.s.last_contact.isoformat() if self.s.last_contact else None,
            "high_water": self.s.high_water.isoformat() if self.s.high_water else None,
            "precheck_passed_at": self.s.precheck_passed_at.isoformat()
            if self.s.precheck_passed_at else None,
            "latest": {sig: {"value": q[-1].value, "observed_at": q[-1].observed_at.isoformat(),
                             "calibration_id": q[-1].calibration_id}
                       for sig, q in self.s.readings.items()},
            "resume": {"allowed": self.s.state == "paused" and self.s.interlock is None
                       and self._fresh_healthy()},
            "completion": self.completion_criteria_met(),
        }

    def audit(self) -> dict:
        return {
            "channel_id": self.s.channel_id, "round_id": self.s.round_id,
            "transitions": self.s.transitions,
            "interlocks": self.s.interlock_reasons,
            "operator_actions": self.s.actions,
            "device_frames": self.s.device_frames,
        }
