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

# 设备协议：通道与信号定义完整，心跳参数为正数
protocol = json.loads((root / "domain" / "device_protocol.json").read_text(encoding="utf-8"))
assert protocol["channels"], "设备协议缺少通道定义"
assert set(protocol["signals"]) == set(contract["telemetry_types"]), "协议信号与契约不一致"
assert all(protocol["heartbeat"][k] > 0 for k in ("interval_s", "timeout_s", "escalation_s"))
assert set(protocol["required_signals_for_treatment"]) <= set(contract["telemetry_types"])

# 阈值曲线：只引用契约中的阶段、信号与联锁等级
thresholds = json.loads((root / "domain" / "thresholds.json").read_text(encoding="utf-8"))
assert thresholds["default_severe_interlock"] in contract["interlock_levels"]
for phase, signals in thresholds["phases"].items():
    assert phase in contract["treatment_states"], f"未知阶段 {phase}"
    for signal, rule in signals.items():
        assert signal in contract["telemetry_types"], f"未知信号 {signal}"
        if "severe_interlock" in rule:
            assert rule["severe_interlock"] in contract["interlock_levels"]

# 校准记录：引用的通道与信号存在，有效期可解析
calibration = json.loads((root / "domain" / "calibration.json").read_text(encoding="utf-8"))
channel_ids = {c["channel_id"] for c in protocol["channels"]}
for record in calibration["records"]:
    assert record["channel_id"] in channel_ids, f"未知通道 {record['channel_id']}"
    assert record["signal"] in contract["telemetry_types"]
    assert datetime.fromisoformat(record["calibrated_at"]) < datetime.fromisoformat(record["valid_until"])

# 故障样例：命令/动作类型合法，遥测字段完整
samples = json.loads((root / "domain" / "fault_samples.json").read_text(encoding="utf-8"))
actions = {"acknowledge_interlock", "supervisor_reset", "enter_maintenance", "exit_maintenance"}
for scenario in samples["scenarios"]:
    assert scenario["steps"], f"样例 {scenario['name']} 缺少步骤"
    for step in scenario["steps"]:
        if "telemetry" in step:
            assert required <= set(step["telemetry"]), f"样例 {scenario['name']} 遥测缺字段"
        if "command" in step:
            assert step["command"]["type"] in contract["command_types"]
            assert step["command"]["idempotency_key"]
        if "action" in step:
            assert step["action"]["type"] in actions
            assert step["action"]["idempotency_key"]

print("安全联锁契约、设备协议、阈值曲线、校准记录与故障样例格式有效")
