"""处理监控服务：遥测摄入、阶段机、安全联锁、命令幂等、审计与崩溃恢复。

安全不变式：
- 乱序/重复遥测被门槛隔离，处理阶段只前进不回退；
- 严重越限锁定通道（locked），普通确认（acknowledge）不能解除，需主管复位；
- 传感器失联 = 自动保护性暂停 + 可升级联锁；维护模式 = 计划性暂停监测并阻塞命令；
- 所有命令/动作按幂等键只执行一次；
- 恢复时先重放日志重建安全状态，complete_recovery 之后才接受新命令。
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Optional

from .channel import ChannelRuntime
from .clock import Clock
from .config import DomainConfig
from .models import (
    CONSOLE_ACTIONS,
    Interlock,
    InterlockLevel,
    Phase,
    Role,
    TelemetryEvent,
    TreatmentPlan,
    level_from_name,
    parse_time,
)
from .rules import classify
from .telemetry import TelemetryGate

# 遥测驱动联锁评估的阶段
_EVALUATED_PHASES = (Phase.RAMPING, Phase.STEADY, Phase.PAUSED, Phase.RINSING)
# 允许暂停命令的阶段
_PAUSABLE_PHASES = (Phase.RAMPING, Phase.STEADY, Phase.RINSING)
# 允许进入维护的阶段
_MAINTENANCE_ENTRY_PHASES = (Phase.PREPARED, Phase.PAUSED, Phase.COMPLETED)


class TreatmentMonitorService:
    STATUS_ACTIVE = "active"
    STATUS_RECOVERING = "recovering"

    CURRENT_ZERO_TOLERANCE_A = 0.2

    def __init__(self, config: DomainConfig, clock: Clock, store):
        self.config = config
        self.clock = clock
        self.store = store
        self.status = self.STATUS_ACTIVE
        self.channels: dict[str, ChannelRuntime] = {}
        self.idempotency: dict[str, dict] = {}
        self.audit: list[dict] = []
        self._audit_seq = 0
        self._gate = TelemetryGate(config)
        self._replaying = False
        self._replay_now = None

    # ------------------------------------------------------------------
    # 时间与日志
    # ------------------------------------------------------------------
    def _now(self):
        return self._replay_now if self._replay_now is not None else self.clock.now()

    def _journal(self, record: dict) -> None:
        if not self._replaying:
            self.store.append(record)

    def _audit(self, entry: dict) -> None:
        self._audit_seq += 1
        entry["seq"] = self._audit_seq
        self.audit.append(entry)

    def _channel(self, channel_id: str) -> ChannelRuntime:
        rt = self.channels.get(channel_id)
        if rt is None:
            rt = ChannelRuntime(channel_id)
            self.channels[channel_id] = rt
        return rt

    # ------------------------------------------------------------------
    # 处理方案
    # ------------------------------------------------------------------
    def register_plan(self, plan_dict: dict) -> dict:
        if self.status != self.STATUS_ACTIVE:
            return {"status": "rejected", "reason": "service_recovering"}
        try:
            plan = TreatmentPlan.from_dict(plan_dict)
        except ValueError as exc:
            return {"status": "rejected", "reason": str(exc)}
        if not self.config.known_channel(plan.channel_id):
            return {"status": "rejected", "reason": f"未知通道 {plan.channel_id}"}
        rt = self._channel(plan.channel_id)
        if rt.plan is not None and rt.phase not in (Phase.PREPARED, Phase.COMPLETED):
            return {"status": "rejected", "reason": "treatment_in_progress", "message": "处理进行中，不能更换方案"}
        self._journal({"kind": "plan", "at": self._now().isoformat(), "plan": plan_dict})
        self._register_plan(plan)
        return {"status": "ok", "plan_id": plan.plan_id}

    def _register_plan(self, plan: TreatmentPlan) -> None:
        rt = self._channel(plan.channel_id)
        rt.reset_for_new_plan(plan)
        self._audit({
            "kind": "plan_registered",
            "channel_id": plan.channel_id,
            "at": self._now().isoformat(),
            "plan_id": plan.plan_id,
            "artifact_id": plan.artifact_id,
        })

    # ------------------------------------------------------------------
    # 遥测摄入
    # ------------------------------------------------------------------
    def ingest_telemetry(self, event_dict: dict) -> dict:
        if self.status != self.STATUS_ACTIVE:
            return {"status": "rejected", "reason": "service_recovering"}
        try:
            event = TelemetryEvent.from_dict(
                event_dict, self.config.required_telemetry_fields, self.config.known_signals
            )
        except ValueError as exc:
            return {"status": "rejected", "reason": str(exc)}
        if not self.config.known_channel(event.channel_id):
            return {"status": "rejected", "reason": f"未知通道 {event.channel_id}"}
        rt = self._channel(event.channel_id)
        try:
            gate = self._gate.accept(rt, event)
        except ValueError as exc:
            return {"status": "rejected", "reason": str(exc)}
        if gate.status != "accepted":
            # 乱序/重复：隔离记录，不进入控制逻辑，不回退阶段
            record = {
                "kind": "telemetry_quarantined",
                "at": self._now().isoformat(),
                "channel_id": event.channel_id,
                "message_id": event.message_id,
                "status": gate.status,
                "reason": gate.reason,
            }
            self._journal(record)
            self._audit(dict(record))
            return {"status": gate.status, "reason": gate.reason}
        self._journal({"kind": "telemetry", "at": self._now().isoformat(), "event": event_dict})
        self._after_accepted_reading(rt, gate.reading)
        return {
            "status": "accepted",
            "message_id": event.message_id,
            "corrected_value": gate.reading.value,
            "calibration_valid": gate.reading.calibration_valid,
        }

    def _after_accepted_reading(self, rt: ChannelRuntime, reading) -> None:
        if reading.signal == "heartbeat":
            rt.last_heartbeat_at = reading.observed_at
            rt.sensor_lost_since = None
        else:
            calibration_code = f"calibration_expired_{reading.signal}"
            if reading.calibration_valid:
                self._close_advisory(rt, calibration_code)
            else:
                self._open_interlock(
                    rt, calibration_code, InterlockLevel.ADVISORY,
                    f"{reading.signal} 传感器校准缺失或已过有效期",
                )
            if not rt.maintenance and rt.phase in _EVALUATED_PHASES and rt.plan is not None:
                self._evaluate_reading(rt, reading)
                self._evaluate_progression(rt, reading)
        self.evaluate_timeouts()

    def _evaluate_reading(self, rt: ChannelRuntime, reading) -> None:
        rule = self.config.phase_rule(rt.phase.value, reading.signal)
        if not rule:
            return
        violation = classify(rt, rule, reading)
        warn_code = f"{reading.signal}_warn"
        if violation is None:
            self._close_advisory(rt, warn_code)
            return
        details = {"value": reading.value, "limit": violation.limit, "message_id": reading.message_id}
        if violation.severity == "warn":
            self._open_interlock(rt, warn_code, InterlockLevel.ADVISORY, violation.detail, details=details)
        elif violation.severity == "alarm":
            self._open_interlock(rt, f"{reading.signal}_alarm", InterlockLevel.OPERATOR_RESET, violation.detail, details=details)
        else:
            level = level_from_name(violation.severe_interlock or self.config.default_severe_interlock)
            self._open_interlock(rt, f"{reading.signal}_severe", level, violation.detail, details=details)

    def _evaluate_progression(self, rt: ChannelRuntime, reading) -> None:
        """升流 → 稳态：电流在目标带内保持 settle_s。只前进，不回退。"""
        if rt.phase != Phase.RAMPING or reading.signal != "current" or rt.plan is None:
            return
        plan = rt.plan
        if abs(reading.value - plan.target_current_A) > plan.steady_band_A:
            rt.band_since = None
            return
        if rt.band_since is None:
            rt.band_since = reading.observed_at
        if (reading.observed_at - rt.band_since).total_seconds() >= plan.settle_s:
            self._transition(
                rt, Phase.STEADY,
                trigger={"kind": "telemetry", "signal": "current"},
                reasons=[f"电流在目标带 {plan.steady_band_A}A 内保持 {plan.settle_s}s"],
            )

    # ------------------------------------------------------------------
    # 联锁
    # ------------------------------------------------------------------
    def _open_interlock(
        self,
        rt: ChannelRuntime,
        code: str,
        level: InterlockLevel,
        message: str,
        *,
        details: Optional[dict] = None,
        actor: Optional[dict] = None,
        trigger: Optional[dict] = None,
    ) -> Interlock:
        existing = rt.open_interlock(code)
        if existing is not None:
            if details:
                existing.details.update(details)
            if level > existing.level:
                existing.level = level
                self._audit({
                    "kind": "interlock_escalated",
                    "channel_id": rt.channel_id,
                    "at": self._now().isoformat(),
                    "interlock": existing.to_dict(),
                })
                self._apply_interlock_policy(rt, existing, actor=actor, trigger=trigger)
            return existing
        interlock = Interlock(
            interlock_id=rt.next_interlock_id(),
            channel_id=rt.channel_id,
            code=code,
            level=level,
            message=message,
            opened_at=self._now(),
            details=dict(details or {}),
        )
        rt.interlocks.append(interlock)
        self._audit({
            "kind": "interlock_opened",
            "channel_id": rt.channel_id,
            "at": self._now().isoformat(),
            "interlock": interlock.to_dict(),
        })
        self._apply_interlock_policy(rt, interlock, actor=actor, trigger=trigger)
        return interlock

    def _apply_interlock_policy(
        self, rt: ChannelRuntime, interlock: Interlock, *, actor=None, trigger=None
    ) -> None:
        """联锁策略：主管级及以上锁定通道；操作员级自动保护性暂停；提示级只记录。"""
        if rt.phase in (Phase.COMPLETED, Phase.LOCKED):
            return
        trig = trigger or {"kind": "interlock", "code": interlock.code}
        if interlock.level >= InterlockLevel.SUPERVISOR_RESET:
            rt.interrupted_from = rt.phase
            self._effect(rt, "set_current", 0.0, f"interlock:{interlock.code}")
            self._transition(rt, Phase.LOCKED, trigger=trig, actor=actor, reasons=[interlock.message])
        elif interlock.level == InterlockLevel.OPERATOR_RESET and rt.phase in _PAUSABLE_PHASES:
            rt.paused_from = rt.phase
            rt.paused_since = self._now()
            self._effect(rt, "set_current", 0.0, f"interlock:{interlock.code}")
            self._transition(rt, Phase.PAUSED, trigger=trig, actor=actor, reasons=[interlock.message])

    def _close_interlock(self, rt: ChannelRuntime, interlock: Interlock, actor_id: str) -> None:
        interlock.open = False
        interlock.cleared_at = self._now()
        interlock.cleared_by = actor_id
        self._audit({
            "kind": "interlock_cleared",
            "channel_id": rt.channel_id,
            "at": self._now().isoformat(),
            "interlock_id": interlock.interlock_id,
            "code": interlock.code,
            "cleared_by": actor_id,
        })

    def _close_advisory(self, rt: ChannelRuntime, code: str) -> None:
        interlock = rt.open_interlock(code)
        if interlock is not None and interlock.level == InterlockLevel.ADVISORY:
            self._close_interlock(rt, interlock, "auto")

    def _maybe_unlock(self, rt: ChannelRuntime, actor: Optional[dict]) -> None:
        """锁定通道在主管级及以上联锁全部解除后，回到安全可恢复状态。"""
        if rt.phase != Phase.LOCKED:
            return
        if any(i.level >= InterlockLevel.SUPERVISOR_RESET for i in rt.open_interlocks()):
            return
        interrupted = rt.interrupted_from
        if interrupted in _PAUSABLE_PHASES:
            rt.paused_from = interrupted
            rt.paused_since = self._now()
            target = Phase.PAUSED
        elif interrupted == Phase.PAUSED:
            target = Phase.PAUSED
        elif interrupted == Phase.PREPARED:
            target = Phase.PREPARED
        else:
            target = Phase.PAUSED
        rt.interrupted_from = None
        self._transition(rt, target, trigger={"kind": "interlock_reset"}, actor=actor, reasons=["联锁全部解除"])

    def _effect(self, rt: ChannelRuntime, effect_type: str, value: float, reason: str) -> None:
        effect = {"type": effect_type, "value": value, "at": self._now().isoformat(), "reason": reason}
        rt.effects.append(effect)
        self._audit({"kind": "effect", "channel_id": rt.channel_id, "at": effect["at"], "effect": dict(effect)})

    # ------------------------------------------------------------------
    # 状态转换与读数窗口
    # ------------------------------------------------------------------
    def _transition(
        self,
        rt: ChannelRuntime,
        to_phase: Phase,
        *,
        trigger: dict,
        actor: Optional[dict] = None,
        reasons: Optional[list] = None,
    ) -> dict:
        entry = {
            "kind": "transition",
            "channel_id": rt.channel_id,
            "at": self._now().isoformat(),
            "from_phase": rt.phase.value,
            "to_phase": to_phase.value,
            "trigger": trigger,
            "actor": actor,
            "reading_window": self._window_snapshot(rt),
            "open_interlocks": [i.code for i in rt.open_interlocks()],
            "reasons": list(reasons or []),
        }
        rt.phase = to_phase
        rt.window.clear()
        self._audit(entry)
        return entry

    @staticmethod
    def _window_snapshot(rt: ChannelRuntime) -> dict:
        observed = [r.observed_at for r in rt.window]
        return {
            "message_ids": [r.message_id for r in rt.window],
            "observed_from": min(observed).isoformat() if observed else None,
            "observed_to": max(observed).isoformat() if observed else None,
            "latest": {
                signal: {
                    "value": r.value,
                    "raw_value": r.raw_value,
                    "unit": r.unit,
                    "observed_at": r.observed_at.isoformat(),
                    "message_id": r.message_id,
                    "calibration_valid": r.calibration_valid,
                }
                for signal, r in rt.latest.items()
            },
        }

    # ------------------------------------------------------------------
    # 命令与控制台动作（统一幂等调用）
    # ------------------------------------------------------------------
    def _validate_invocation(self, inv: dict, allowed_types) -> Optional[str]:
        if inv.get("type") not in allowed_types:
            return f"未知类型: {inv.get('type')}"
        if not inv.get("channel_id") or not self.config.known_channel(inv["channel_id"]):
            return f"未知通道: {inv.get('channel_id')}"
        if not inv.get("idempotency_key"):
            return "缺少幂等键"
        if not inv.get("actor"):
            return "缺少操作人"
        if inv.get("role") not in (Role.OPERATOR.value, Role.SUPERVISOR.value):
            return f"非法角色: {inv.get('role')}"
        return None

    def _invoke(self, invocation: dict, handler, journal_kind: str) -> dict:
        """幂等 → 写前日志 → 执行 → 缓存 → 审计。网络重试命中缓存，只执行一次。"""
        key = invocation["idempotency_key"]
        if key in self.idempotency:
            return {**self.idempotency[key], "deduplicated": True}
        self._journal({"kind": journal_kind, "at": self._now().isoformat(), journal_kind: invocation})
        result = handler(invocation)
        self.idempotency[key] = result
        self._audit({
            "kind": journal_kind,
            "channel_id": invocation["channel_id"],
            "at": self._now().isoformat(),
            "invocation": {
                "type": invocation["type"],
                "actor": invocation["actor"],
                "role": invocation["role"],
                "idempotency_key": key,
            },
            "result": result.get("status"),
            "reason": result.get("reason"),
        })
        return result

    def execute_command(self, command: dict) -> dict:
        """设备命令：start/pause/resume/emergency_stop/begin_rinse/finish。"""
        if self.status != self.STATUS_ACTIVE:
            return {"status": "rejected", "reason": "service_recovering"}
        error = self._validate_invocation(command, self.config.device_commands)
        if error:
            return {"status": "rejected", "reason": error}
        return self._invoke(command, self._apply_command, "command")

    def execute_action(self, action: dict) -> dict:
        """控制台动作：acknowledge_interlock/supervisor_reset/enter_maintenance/exit_maintenance。"""
        if self.status != self.STATUS_ACTIVE:
            return {"status": "rejected", "reason": "service_recovering"}
        error = self._validate_invocation(action, CONSOLE_ACTIONS)
        if error:
            return {"status": "rejected", "reason": error}
        return self._invoke(action, self._apply_action, "action")

    def _apply_command(self, command: dict) -> dict:
        rt = self._channel(command["channel_id"])
        ctype = command["type"]
        actor = {"id": command["actor"], "role": command["role"], "idempotency_key": command["idempotency_key"]}
        if rt.maintenance and ctype != "emergency_stop":
            return {"status": "rejected", "reason": "maintenance_mode", "message": "维护模式下工艺命令被阻塞"}
        handler = {
            "start": self._cmd_start,
            "pause": self._cmd_pause,
            "resume": self._cmd_resume,
            "begin_rinse": self._cmd_begin_rinse,
            "finish": self._cmd_finish,
            "emergency_stop": self._cmd_emergency_stop,
        }[ctype]
        return handler(rt, actor)

    # ---- 设备命令 ----
    def _cmd_start(self, rt: ChannelRuntime, actor: dict) -> dict:
        if rt.phase != Phase.PREPARED:
            return {"status": "rejected", "reason": "invalid_state", "message": f"当前阶段 {rt.phase.value} 不能启动"}
        if rt.plan is None:
            return {"status": "rejected", "reason": "no_plan", "message": "尚未登记处理方案"}
        precheck = self._precheck(rt)
        if precheck["failures"]:
            return {"status": "rejected", "reason": "precheck_failed", "failures": precheck["failures"]}
        rt.ramp_started_at = self._now()
        rt.band_since = None
        self._transition(rt, Phase.RAMPING, trigger={"kind": "command", "command": "start"}, actor=actor, reasons=["预检通过"])
        return {"status": "ok", "phase": rt.phase.value}

    def _cmd_pause(self, rt: ChannelRuntime, actor: dict) -> dict:
        if rt.phase not in _PAUSABLE_PHASES:
            return {"status": "rejected", "reason": "invalid_state", "message": f"当前阶段 {rt.phase.value} 不能暂停"}
        rt.paused_from = rt.phase
        rt.paused_since = self._now()
        self._effect(rt, "set_current", 0.0, "command:pause")
        self._transition(rt, Phase.PAUSED, trigger={"kind": "command", "command": "pause"}, actor=actor)
        return {"status": "ok", "phase": rt.phase.value}

    def _cmd_resume(self, rt: ChannelRuntime, actor: dict) -> dict:
        if rt.phase != Phase.PAUSED:
            return {"status": "rejected", "reason": "invalid_state", "message": f"当前阶段 {rt.phase.value} 不能继续"}
        blocking = rt.blocking_interlocks()
        if blocking:
            return {"status": "rejected", "reason": "interlocks_open", "codes": [i.code for i in blocking]}
        if rt.last_heartbeat_at is None or (self._now() - rt.last_heartbeat_at).total_seconds() > self.config.heartbeat_timeout_s:
            return {"status": "rejected", "reason": "sensor_lost", "message": "心跳缺失或过期"}
        if rt.paused_from is None:
            return {"status": "rejected", "reason": "invalid_state", "message": "无可恢复阶段"}
        target = rt.paused_from
        self._transition(rt, target, trigger={"kind": "command", "command": "resume"}, actor=actor)
        return {"status": "ok", "phase": rt.phase.value}

    def _cmd_begin_rinse(self, rt: ChannelRuntime, actor: dict) -> dict:
        if rt.phase != Phase.STEADY:
            return {"status": "rejected", "reason": "invalid_state", "message": f"当前阶段 {rt.phase.value} 不能开始冲洗"}
        plan = rt.plan
        reading = rt.latest.get("chloride")
        problems = []
        if reading is None:
            problems.append("无氯离子读数")
        else:
            if not reading.calibration_valid:
                problems.append("氯离子传感器校准失效")
            if (self._now() - reading.observed_at).total_seconds() > self.config.reading_freshness_s:
                problems.append("氯离子读数过旧")
            if reading.value > plan.chloride_target_mg_L:
                problems.append(f"氯离子 {reading.value:.2f}mg/L 高于目标 {plan.chloride_target_mg_L}mg/L")
        if problems:
            return {"status": "rejected", "reason": "chloride_not_ready", "problems": problems}
        rt.rinse_started_at = self._now()
        self._transition(rt, Phase.RINSING, trigger={"kind": "command", "command": "begin_rinse"}, actor=actor,
                         reasons=[f"氯离子达标 {reading.value:.2f}mg/L"])
        return {"status": "ok", "phase": rt.phase.value}

    def _cmd_finish(self, rt: ChannelRuntime, actor: dict) -> dict:
        if rt.phase != Phase.RINSING:
            return {"status": "rejected", "reason": "invalid_state", "message": f"当前阶段 {rt.phase.value} 不能结束"}
        plan = rt.plan
        if (self._now() - rt.rinse_started_at).total_seconds() < plan.rinse_min_s:
            return {"status": "rejected", "reason": "rinse_too_short", "message": "冲洗时长不足"}
        current = rt.latest.get("current")
        if current is None or current.value > self.CURRENT_ZERO_TOLERANCE_A:
            return {"status": "rejected", "reason": "current_not_zero", "message": "电流未降到零"}
        self._effect(rt, "set_current", 0.0, "command:finish")
        self._transition(rt, Phase.COMPLETED, trigger={"kind": "command", "command": "finish"}, actor=actor)
        return {"status": "ok", "phase": rt.phase.value}

    def _cmd_emergency_stop(self, rt: ChannelRuntime, actor: dict) -> dict:
        if rt.phase == Phase.COMPLETED:
            return {"status": "rejected", "reason": "invalid_state", "message": "处理已结束"}
        if rt.phase == Phase.LOCKED:
            # 已处于安全态：同一语义不重复产生效果
            return {"status": "ok", "note": "already_locked", "phase": Phase.LOCKED.value}
        self._open_interlock(
            rt, "emergency_stop", InterlockLevel.EMERGENCY_LOCK, "紧急停机",
            actor=actor, trigger={"kind": "command", "command": "emergency_stop"},
        )
        return {"status": "ok", "phase": rt.phase.value}

    # ---- 控制台动作 ----
    def _apply_action(self, action: dict) -> dict:
        rt = self._channel(action["channel_id"])
        atype = action["type"]
        actor = {"id": action["actor"], "role": action["role"], "idempotency_key": action["idempotency_key"]}
        if atype == "acknowledge_interlock":
            # 普通确认：只能解除 advisory / operator_reset，严重联锁保持
            cleared, remaining = [], []
            for interlock in rt.open_interlocks():
                if interlock.level <= InterlockLevel.OPERATOR_RESET:
                    self._close_interlock(rt, interlock, actor["id"])
                    cleared.append(interlock.code)
                else:
                    remaining.append(interlock.code)
            self._maybe_unlock(rt, actor)
            if remaining:
                return {
                    "status": "denied",
                    "reason": "存在需主管复位的联锁，普通确认不能解除",
                    "cleared": cleared,
                    "remaining": remaining,
                }
            return {"status": "ok", "cleared": cleared}
        if atype == "supervisor_reset":
            if action["role"] != Role.SUPERVISOR.value:
                return {"status": "rejected", "reason": "permission_denied", "message": "需要主管角色"}
            cleared = []
            for interlock in rt.open_interlocks():
                self._close_interlock(rt, interlock, actor["id"])
                cleared.append(interlock.code)
            self._maybe_unlock(rt, actor)
            return {"status": "ok", "cleared": cleared}
        if atype == "enter_maintenance":
            if action["role"] != Role.SUPERVISOR.value:
                return {"status": "rejected", "reason": "permission_denied", "message": "需要主管角色"}
            if rt.phase not in _MAINTENANCE_ENTRY_PHASES:
                return {"status": "rejected", "reason": "invalid_state", "message": "仅预检/暂停/结束阶段可进入维护"}
            rt.maintenance = True
            self._audit({"kind": "maintenance", "channel_id": rt.channel_id, "at": self._now().isoformat(),
                         "mode": "enter", "actor": actor})
            return {"status": "ok", "maintenance": True}
        if atype == "exit_maintenance":
            if action["role"] != Role.SUPERVISOR.value:
                return {"status": "rejected", "reason": "permission_denied", "message": "需要主管角色"}
            rt.maintenance = False
            self._audit({"kind": "maintenance", "channel_id": rt.channel_id, "at": self._now().isoformat(),
                         "mode": "exit", "actor": actor})
            # 维护期间的静默不计入失联升级：失联计时从退出维护这一刻重新开始
            now = self._now()
            if rt.last_heartbeat_at is not None and (now - rt.last_heartbeat_at).total_seconds() > self.config.heartbeat_timeout_s:
                rt.sensor_lost_since = now
            else:
                rt.sensor_lost_since = None
            # 退出维护立即恢复安全评估（失联/超时等）
            self.evaluate_timeouts()
            return {"status": "ok", "maintenance": False}
        return {"status": "rejected", "reason": f"未知动作: {atype}"}

    # ------------------------------------------------------------------
    # 预检与超时
    # ------------------------------------------------------------------
    def _precheck(self, rt: ChannelRuntime) -> dict:
        now = self._now()
        checks, failures = [], []
        for signal in self.config.required_signals:
            reading = rt.latest.get(signal)
            if reading is None:
                checks.append({"signal": signal, "ok": False, "detail": "无读数"})
                failures.append(f"{signal}: 无读数")
            elif (now - reading.observed_at).total_seconds() > self.config.reading_freshness_s:
                checks.append({"signal": signal, "ok": False, "detail": "读数过旧"})
                failures.append(f"{signal}: 读数过旧")
            elif not reading.calibration_valid:
                checks.append({"signal": signal, "ok": False, "detail": "校准失效"})
                failures.append(f"{signal}: 校准失效")
            else:
                checks.append({"signal": signal, "ok": True, "value": reading.value})
        heartbeat_ok = rt.last_heartbeat_at is not None and (
            now - rt.last_heartbeat_at
        ).total_seconds() <= self.config.heartbeat_timeout_s
        checks.append({"signal": "heartbeat", "ok": heartbeat_ok})
        if not heartbeat_ok:
            failures.append("heartbeat: 心跳缺失或过期")
        for interlock in rt.blocking_interlocks():
            failures.append(f"联锁未解除: {interlock.code}")
        if rt.maintenance:
            failures.append("维护模式")
        return {"checks": checks, "failures": failures}

    def precheck_report(self, channel_id: str) -> dict:
        rt = self._channel(channel_id)
        report = self._precheck(rt)
        report["channel_id"] = channel_id
        report["phase"] = rt.phase.value
        return report

    def evaluate_timeouts(self) -> None:
        """按当前时钟评估超时：心跳失联（可升级）、升流超时、暂停过久。内部调用，不写日志。"""
        now = self._now()
        for rt in self.channels.values():
            if rt.maintenance or rt.phase in (Phase.PREPARED, Phase.LOCKED, Phase.COMPLETED):
                continue
            plan = rt.plan
            # 传感器失联策略：自动保护性暂停；持续失联升级为主管级锁定
            if rt.phase in (Phase.RAMPING, Phase.STEADY, Phase.PAUSED, Phase.RINSING) and rt.last_heartbeat_at is not None:
                silence = (now - rt.last_heartbeat_at).total_seconds()
                if silence > self.config.heartbeat_timeout_s:
                    if rt.sensor_lost_since is None:
                        # 失联起点 = 最后心跳 + 超时阈值（停机恢复后能正确累计）
                        rt.sensor_lost_since = rt.last_heartbeat_at + timedelta(seconds=self.config.heartbeat_timeout_s)
                    self._open_interlock(
                        rt, "sensor_lost", InterlockLevel.OPERATOR_RESET,
                        f"心跳失联 {silence:.0f}s，已保护性暂停",
                        details={"silence_s": silence},
                    )
                    if (now - rt.sensor_lost_since).total_seconds() > self.config.heartbeat_escalation_s:
                        self._open_interlock(
                            rt, "sensor_lost_prolonged", InterlockLevel.SUPERVISOR_RESET,
                            "传感器长时间失联，升级锁定",
                        )
            # 升流超时
            if rt.phase == Phase.RAMPING and plan and rt.ramp_started_at:
                if (now - rt.ramp_started_at).total_seconds() > plan.ramp_timeout_s:
                    self._open_interlock(rt, "ramp_timeout", InterlockLevel.OPERATOR_RESET, "升流超时")
            # 暂停过久（提示）
            if rt.phase == Phase.PAUSED and plan and rt.paused_since:
                if (now - rt.paused_since).total_seconds() > plan.paused_max_s:
                    self._open_interlock(rt, "paused_too_long", InterlockLevel.ADVISORY, "暂停超时，需决定继续或冲洗")

    def tick(self) -> dict:
        """时钟推进入口：模拟时钟前进后调用，触发超时评估。"""
        if self.status != self.STATUS_ACTIVE:
            return {"status": "rejected", "reason": "service_recovering"}
        self._journal({"kind": "tick", "at": self._now().isoformat()})
        self.evaluate_timeouts()
        return {"status": "ok"}

    # ------------------------------------------------------------------
    # 恢复
    # ------------------------------------------------------------------
    @classmethod
    def recover(cls, config: DomainConfig, clock: Clock, store) -> "TreatmentMonitorService":
        """从日志重建。重建期间状态为 recovering，拒绝新命令与新遥测。"""
        svc = cls(config, clock, store)
        svc.status = cls.STATUS_RECOVERING
        svc._replaying = True
        for record in store.read_all():
            svc._replay_record(record)
        svc._replaying = False
        svc._replay_now = None
        return svc

    def _replay_record(self, record: dict) -> None:
        self._replay_now = parse_time(record["at"])
        kind = record["kind"]
        if kind == "plan":
            self._register_plan(TreatmentPlan.from_dict(record["plan"]))
        elif kind == "telemetry":
            event = TelemetryEvent.from_dict(
                record["event"], self.config.required_telemetry_fields, self.config.known_signals
            )
            rt = self._channel(event.channel_id)
            gate = self._gate.accept(rt, event)
            if gate.status == "accepted":
                self._after_accepted_reading(rt, gate.reading)
        elif kind == "telemetry_quarantined":
            self._audit(dict(record))
        elif kind == "command":
            self._invoke(record["command"], self._apply_command, "command")
        elif kind == "action":
            self._invoke(record["action"], self._apply_action, "action")
        elif kind == "tick":
            self.evaluate_timeouts()

    def complete_recovery(self) -> dict:
        """恢复收口：先按当前时钟重建安全状态（如停机期间的心跳失联），再接受新命令。"""
        if self.status != self.STATUS_RECOVERING:
            return {"status": "rejected", "reason": "not_recovering"}
        self._journal({"kind": "tick", "at": self._now().isoformat()})
        self.evaluate_timeouts()
        self.status = self.STATUS_ACTIVE
        self._audit({"kind": "recovery_completed", "at": self._now().isoformat()})
        return {"status": "ok"}

    # ------------------------------------------------------------------
    # 主管视图
    # ------------------------------------------------------------------
    def get_channel_view(self, channel_id: str) -> Optional[dict]:
        rt = self.channels.get(channel_id)
        if rt is None:
            return None
        return {
            "channel_id": channel_id,
            "phase": rt.phase.value,
            "maintenance": rt.maintenance,
            "plan_id": rt.plan.plan_id if rt.plan else None,
            "artifact_id": rt.plan.artifact_id if rt.plan else None,
            "open_interlocks": [i.to_dict() for i in rt.open_interlocks()],
            "latest_readings": {
                signal: {
                    "value": r.value,
                    "raw_value": r.raw_value,
                    "unit": r.unit,
                    "observed_at": r.observed_at.isoformat(),
                    "calibration_valid": r.calibration_valid,
                }
                for signal, r in rt.latest.items()
            },
            "last_heartbeat_at": rt.last_heartbeat_at.isoformat() if rt.last_heartbeat_at else None,
            "paused_from": rt.paused_from.value if rt.paused_from else None,
            "effects": [dict(e) for e in rt.effects],
            "high_water": {signal: t.isoformat() for signal, t in rt.high_water.items()},
        }

    def get_service_view(self) -> dict:
        return {
            "status": self.status,
            "channels": {cid: rt.phase.value for cid, rt in self.channels.items()},
        }

    def get_transition_log(self, channel_id: Optional[str] = None) -> list[dict]:
        """每次状态转换的读数窗口、联锁原因与人工操作。"""
        return [
            e for e in self.audit
            if e["kind"] == "transition" and (channel_id is None or e["channel_id"] == channel_id)
        ]

    def get_audit(self, channel_id: Optional[str] = None) -> list[dict]:
        return [e for e in self.audit if channel_id is None or e.get("channel_id") == channel_id]
