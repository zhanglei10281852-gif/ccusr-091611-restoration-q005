import json
from datetime import datetime
from pathlib import Path

root = Path(__file__).resolve().parents[1]
contract = json.loads((root / "domain" / "contract.json").read_text(encoding="utf-8"))
telemetry = json.loads((root / "examples" / "telemetry.json").read_text(encoding="utf-8"))
required = set(contract["required_telemetry_fields"])
assert telemetry and all(required <= set(row) for row in telemetry)
assert all(row["signal"] in contract["telemetry_types"] for row in telemetry)
assert all(isinstance(row["value"], (int, float)) for row in telemetry)
for row in telemetry:
    datetime.fromisoformat(row["observed_at"])
    datetime.fromisoformat(row["received_at"])

# ---- 扩展校验：协议、方案、校准、故障样例彼此自洽 ----
protocol = json.loads((root / "domain" / "device_protocol.json").read_text(encoding="utf-8"))
recipes = json.loads((root / "domain" / "recipes.json").read_text(encoding="utf-8"))
calibrations = json.loads((root / "domain" / "calibrations.json").read_text(encoding="utf-8"))
faults = json.loads((root / "domain" / "faults.json").read_text(encoding="utf-8"))

frame_names = {f["frame"] for f in protocol["downlink_frames"]}
for f in ("ENABLE_OUTPUT", "HOLD_CURRENT", "SUSPEND_OUTPUT", "START_RINSE",
          "STOP_RINSE", "FORCE_RELAYS_OPEN", "UNLOCK_RELAYS"):
    assert f in frame_names

recipe = recipes["recipes"][0]
for key in ("precheck", "ramp", "steady", "rinse", "timeouts"):
    assert key in recipe
assert recipe["steady"]["current_warn_a"] < recipe["steady"]["current_operator_reset_a"] \
    < recipe["steady"]["current_emergency_lock_a"], "电流阈值必须单调递增"
for sig in recipe["precheck"]["required_readings"]:
    assert sig in contract["telemetry_types"]

for c in calibrations["calibrations"]:
    assert c["signal"] in contract["telemetry_types"]
    datetime.fromisoformat(c["valid_from"])
    datetime.fromisoformat(c["valid_until"])

for scn in faults["scenarios"]:
    assert scn["start_state"] in contract["treatment_states"]
    for ev in scn["events"]:
        if ev["kind"] == "telemetry":
            assert ev["signal"] in contract["telemetry_types"]
        elif ev["kind"] == "command":
            assert ev["command"] in contract["command_types"]
    assert scn["expect"].get("final_state") in contract["treatment_states"]

print("安全联锁契约、设备协议、方案曲线、校准记录与故障样例格式有效")
