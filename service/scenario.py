"""故障样例回放器：按 domain/fault_samples.json 的步骤驱动服务，供测试与演示。"""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Optional

from .clock import ManualClock
from .models import parse_time
from .monitor import TreatmentMonitorService


class ScenarioRunner:
    def __init__(self, service: TreatmentMonitorService, clock: ManualClock):
        self.service = service
        self.clock = clock
        self.auto_hb: Optional[dict] = None
        self._hb_last = None
        self._hb_seq = 0
        self.channel_id: Optional[str] = None
        self.results: list[tuple[str, Any]] = []

    def _inject_heartbeats_until(self, moment) -> None:
        if self.auto_hb is None:
            return
        every = timedelta(seconds=float(self.auto_hb["every_s"]))
        channel = self.auto_hb["channel_id"]
        cursor = self._hb_last if self._hb_last is not None else self.clock.now()
        nxt = cursor + every
        while nxt <= moment:
            self._hb_seq += 1
            self.clock.set(nxt)
            self.service.ingest_telemetry({
                "message_id": f"auto-hb-{channel}-{self._hb_seq}",
                "channel_id": channel,
                "signal": "heartbeat",
                "value": 1,
                "unit": "none",
                "observed_at": nxt.isoformat(),
                "received_at": nxt.isoformat(),
            })
            self._hb_last = nxt
            nxt = nxt + every

    def _sync_clock(self, moment) -> None:
        self._inject_heartbeats_until(moment)
        self.clock.set(moment)

    def run(self, scenario: dict) -> list[tuple[str, Any]]:
        for step in scenario["steps"]:
            self._run_step(step)
        return self.results

    def _run_step(self, step: dict) -> None:
        if "set_clock" in step:
            self._sync_clock(parse_time(step["set_clock"]))
        elif "advance_s" in step:
            self._sync_clock(self.clock.now() + timedelta(seconds=float(step["advance_s"])))
        elif "plan" in step:
            self.channel_id = step["plan"]["channel_id"]
            self.results.append(("plan", self.service.register_plan(step["plan"])))
        elif "auto_heartbeat" in step:
            if step.get("at"):
                self._sync_clock(parse_time(step["at"]))
            self.auto_hb = step["auto_heartbeat"]
            # 启用时从当前时钟起算注入节奏；停用时清空
            self._hb_last = self.clock.now() if self.auto_hb else None
        elif "telemetry" in step:
            event = step["telemetry"]
            self._sync_clock(parse_time(event["received_at"]))
            self.results.append(("telemetry", self.service.ingest_telemetry(event)))
        elif "command" in step:
            command = step["command"]
            self._sync_clock(parse_time(command["at"]))
            payload = {k: v for k, v in command.items() if k != "at"}
            self.results.append(("command", self.service.execute_command(payload)))
        elif "action" in step:
            action = step["action"]
            self._sync_clock(parse_time(action["at"]))
            payload = {k: v for k, v in action.items() if k != "at"}
            self.results.append(("action", self.service.execute_action(payload)))
        elif "tick" in step:
            self._sync_clock(parse_time(step["tick"]["at"]))
            self.results.append(("tick", self.service.tick()))
        elif "expect" in step:
            self.results.append(("expect", self.check_expectation(step["expect"])))

    def check_expectation(self, expect: dict) -> dict:
        view = self.service.get_channel_view(self.channel_id)
        mismatches = []
        if "phase" in expect and view["phase"] != expect["phase"]:
            mismatches.append(f"phase 期望 {expect['phase']} 实际 {view['phase']}")
        if "open_interlocks" in expect:
            actual = sorted(i["code"] for i in view["open_interlocks"])
            if actual != sorted(expect["open_interlocks"]):
                mismatches.append(f"open_interlocks 期望 {sorted(expect['open_interlocks'])} 实际 {actual}")
        if "effects_count" in expect and len(view["effects"]) != expect["effects_count"]:
            mismatches.append(f"effects 期望 {expect['effects_count']} 实际 {len(view['effects'])}")
        if "maintenance" in expect and view["maintenance"] != expect["maintenance"]:
            mismatches.append(f"maintenance 期望 {expect['maintenance']} 实际 {view['maintenance']}")
        return {"passed": not mismatches, "mismatches": mismatches}
