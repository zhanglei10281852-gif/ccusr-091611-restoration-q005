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
print("安全联锁契约与遥测样例格式有效")
