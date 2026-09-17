"""验收：重放 domain/faults.json 中的全部故障样例并核对期望处置。"""
import json
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.helpers import build_monitor, tel, steady_fixture, T0, ROOT

SCENARIOS = json.loads((ROOT / "domain" / "faults.json").read_text(encoding="utf-8"))["scenarios"]

UNITS = {"current": "A", "voltage": "V", "chloride": "mg/L",
         "temperature": "C", "heartbeat": "status"}


def replay(scn):
    mon, clock, store, gw = build_monitor(scn["start_state"])
    base = clock.now()
    if scn["start_state"] in ("steady", "ramping", "paused"):
        steady_fixture(mon, clock)
    last_t = 0
    for ev in scn["events"]:
        clock.advance(ev["t_s"] - last_t)
        last_t = ev["t_s"]
        if ev["kind"] == "telemetry":
            obs = base + timedelta(seconds=ev.get("observed_t_s", ev["t_s"]))
            row = tel(ev["signal"], ev["value"], clock, f"{scn['scenario_id']}-{ev['t_s']}",
                      unit=UNITS[ev["signal"]], observed_at=obs)
            mon.ingest([row])
        elif ev["kind"] == "clock_tick":
            clock.advance(0)
            mon.poll_timeouts()
        elif ev["kind"] == "command":
            mon.command(ev["command"], role=ev.get("role", "operator"),
                        idempotency_key=ev.get("idempotency_key", f"{scn['scenario_id']}-{ev['t_s']}"),
                        badge=ev.get("badge"))
    return mon, clock, store, gw


def check(scn, mon, gw):
    exp = scn["expect"]
    if "final_state" in exp:
        assert mon.s.state == exp["final_state"], f"{scn['scenario_id']}: state {mon.s.state}"
    if "interlock" in exp:
        assert mon.s.interlock == exp["interlock"], f"{scn['scenario_id']}: {mon.s.interlock}"
    if "mode" in exp:
        assert mon.s.mode == exp["mode"]
    if "device_frames" in exp:
        sent_frames = [f.frame for f in gw.sent]
        for f in exp["device_frames"]:
            assert f in sent_frames, f"{scn['scenario_id']}: 缺少设备帧 {f}"
    if "device_frames_during_silence" in exp:
        assert len(gw.sent) == 0 or all(
            f.frame not in ("SUSPEND_OUTPUT", "FORCE_RELAYS_OPEN") for f in gw.sent)
    if "device_frame_count" in exp:
        for frame, n in exp["device_frame_count"].items():
            assert len([f for f in gw.sent if f.frame == frame]) == n, scn["scenario_id"]
    if "dropped_messages" in exp:
        drops = [e for e in store_events(mon) if e["event_type"] == "TelemetryDropped"]
        assert len(drops) == exp["dropped_messages"], scn["scenario_id"]
    if "permanent_for_round" in exp and exp["permanent_for_round"]:
        assert mon.command("supervisor_unlock", "supervisor", "x", badge="b")["accepted"] is False
    if "operator_ack_clears" in exp and exp["operator_ack_clears"] is False:
        assert mon.command("acknowledge", "operator", "ack-x")["accepted"] is False
    if "reason_contains" in exp:
        joined = " ".join(r["reasons"][0] for r in mon.s.interlock_reasons)
        assert exp["reason_contains"] in joined


def store_events(mon):
    return list(mon.store.events("cell-3"))


def test_all_fault_scenarios():
    for scn in SCENARIOS:
        mon, clock, store, gw = replay(scn)
        check(scn, mon, gw)
