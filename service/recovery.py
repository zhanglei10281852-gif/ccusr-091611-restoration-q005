"""崩溃恢复：启动时先重放事件日志重建安全状态，然后才开放命令入口。

重放期间 Monitor.replaying=True：
- 不向设备网关下发任何帧（设备状态按事件流中的 DeviceFrameSent 重建）；
- 不重新评估阈值/超时（联锁结论已在事件流中，避免对历史数据二次反应）；
- 消息去重表与命令幂等表由 SQLite 持久化，天然恢复。
"""
from __future__ import annotations

from collections import deque
from datetime import datetime

from .monitor import Monitor, Reading, READING_KEEP


def restore_channel(monitor: Monitor) -> Monitor:
    s = monitor.s
    forced_open = False  # 本回合继电器是否已被硬断开；RoundStarted 时复位
    for ev in monitor.store.events(s.channel_id):
        p = ev["payload"]
        t = ev["event_type"]
        if t == "ChannelOpened":
            s.opened = True
            s.entered_at = datetime.fromisoformat(p["at"])
        elif t == "StateTransitioned":
            s.round_id = p.get("round_id", s.round_id)
            s.transitions.append({"seq": ev["seq"], "at": p["at"], "round_id": s.round_id,
                                  "from": p["from"], "to": p["to"], "reason": p["reason"],
                                  "trigger": p.get("trigger", "replay"),
                                  "reading_window": p.get("reading_window")})
            if p.get("resume_to"):
                s.resume_to = p["resume_to"]
            s.state = p["to"]
            s.entered_at = datetime.fromisoformat(p["at"])
            if p["to"] == "steady" and s.steady_started_at is None:
                s.steady_started_at = datetime.fromisoformat(p["at"])
            if p["to"] == "rinsing":
                s.rinse_started_at = datetime.fromisoformat(p["at"])
        elif t == "TelemetryAccepted":
            r = Reading(p["signal"], p["value"], p["raw_value"], p["unit"],
                        datetime.fromisoformat(p["observed_at"]),
                        datetime.fromisoformat(p["received_at"]),
                        p.get("calibration_id"))
            s.readings.setdefault(r.signal, deque(maxlen=READING_KEEP)).append(r)
            ts = r.observed_at
            s.high_water = max(s.high_water or ts, ts)
            s.last_contact = max(s.last_contact or r.received_at, r.received_at)
        elif t == "InterlockRaised":
            s.interlock = p["level"]
            s.interlock_reasons.append({"seq": ev["seq"], "level": p["level"],
                                        "reasons": p["reasons"], "at": p["at"], "active": True})
            if p["level"] == "emergency_lock":
                s.hard_tripped = True
        elif t == "InterlockCleared":
            s.interlock = None
            for row in reversed(s.interlock_reasons):
                if row["active"]:
                    row["active"] = False
                    row["cleared_by"] = p.get("by")
                    row["cleared_seq"] = ev["seq"]
                    break
        elif t == "DeviceFrameSent":
            s.device_frames.append({"seq": ev["seq"], "frame": p["frame"], "args": p.get("args", {}),
                                    "cause": p.get("cause", ""), "at": p["at"]})
            if p["frame"] == "FORCE_RELAYS_OPEN":
                forced_open = True
        elif t == "OperatorAction":
            s.actions.append({"seq": ev["seq"], "command": p["command"], "role": p.get("role"),
                              "accepted": p.get("accepted"), "reason": p.get("reason"),
                              "at": p["at"]})
            if p.get("accepted") and p["command"] == "emergency_stop":
                s.hard_tripped = True
        elif t == "PrecheckPassed":
            s.precheck_passed_at = datetime.fromisoformat(p["at"])
        elif t == "MaintenanceEntered":
            s.mode = "maintenance"
            s.maintenance_entered_at = datetime.fromisoformat(p["at"])
        elif t == "MaintenanceExited":
            s.mode = "normal"
            s.maintenance_entered_at = None
        elif t == "RoundStarted":
            s.round_id = p["round_id"]
            forced_open = False
    # 设备网关硬闸门按事件流重建：本回合一经硬断开（无论 supervisor/emergency），
    # 重启后不得再发第二次 FORCE_RELAYS_OPEN。
    if forced_open:
        monitor.gateway.restore_hard_trip(s.channel_id)
    return monitor
