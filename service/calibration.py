"""传感器校准：按通道/信号/时间查生效记录，把示值换算为工程值。"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


class CalibrationTable:
    def __init__(self, calibrations: list[dict]):
        self._rows = calibrations

    @classmethod
    def load(cls, path: str | Path) -> "CalibrationTable":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(data["calibrations"])

    def calibrate(self, channel_id: str, signal: str, value: float, at: datetime):
        """返回 (工程值, 记录id或None)。无生效记录时按原值（gain=1）。"""
        for row in self._rows:
            if row["channel_id"] != channel_id or row["signal"] != signal:
                continue
            valid_from = datetime.fromisoformat(row["valid_from"])
            valid_until = datetime.fromisoformat(row["valid_until"])
            if valid_from <= at < valid_until:
                cal = float(row.get("gain", 1.0)) * value + float(row.get("offset_" + _unit_key(signal), 0.0))
                return round(cal, 6), row["record_id"]
        return value, None


def _unit_key(signal: str) -> str:
    return {"current": "a", "voltage": "v", "chloride": "mg_l", "temperature": "c"}.get(signal, "")
