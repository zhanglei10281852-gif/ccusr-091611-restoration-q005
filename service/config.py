"""领域配置：从仓库读取契约、设备协议、阈值曲线与校准记录。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from .models import parse_time


class DomainConfig:
    def __init__(self, root: Path):
        self.root = Path(root)
        domain = self.root / "domain"
        self.contract: dict[str, Any] = json.loads((domain / "contract.json").read_text(encoding="utf-8"))
        self.protocol: dict[str, Any] = json.loads((domain / "device_protocol.json").read_text(encoding="utf-8"))
        self.thresholds: dict[str, Any] = json.loads((domain / "thresholds.json").read_text(encoding="utf-8"))
        calibration = json.loads((domain / "calibration.json").read_text(encoding="utf-8"))
        self._calibration: dict[tuple[str, str], dict[str, Any]] = {}
        for record in calibration["records"]:
            self._calibration[(record["channel_id"], record["signal"])] = record

    @property
    def required_telemetry_fields(self) -> set[str]:
        return set(self.contract["required_telemetry_fields"])

    @property
    def known_signals(self) -> set[str]:
        return set(self.contract["telemetry_types"])

    @property
    def device_commands(self) -> set[str]:
        return set(self.contract["command_types"])

    def known_channel(self, channel_id: str) -> bool:
        return any(c["channel_id"] == channel_id for c in self.protocol["channels"])

    def channel_ids(self) -> list[str]:
        return [c["channel_id"] for c in self.protocol["channels"]]

    def expected_unit(self, signal: str) -> Optional[str]:
        spec = self.protocol["signals"].get(signal)
        return spec["unit"] if spec else None

    @property
    def heartbeat_timeout_s(self) -> float:
        return float(self.protocol["heartbeat"]["timeout_s"])

    @property
    def heartbeat_escalation_s(self) -> float:
        return float(self.protocol["heartbeat"]["escalation_s"])

    @property
    def required_signals(self) -> list[str]:
        return list(self.protocol["required_signals_for_treatment"])

    @property
    def reading_freshness_s(self) -> float:
        return float(self.protocol["reading_freshness_s"])

    def calibration_for(self, channel_id: str, signal: str) -> Optional[dict[str, Any]]:
        return self._calibration.get((channel_id, signal))

    def apply_calibration(self, channel_id: str, signal: str, raw_value: float, observed_at) -> tuple[float, bool]:
        """返回 (修正值, 校准时效是否有效)。无校准记录视为未校准。"""
        record = self.calibration_for(channel_id, signal)
        if record is None:
            return raw_value, False
        valid = parse_time(record["calibrated_at"]) <= observed_at <= parse_time(record["valid_until"])
        corrected = raw_value * float(record["gain"]) + float(record["offset"])
        return corrected, valid

    def phase_rule(self, phase: str, signal: str) -> Optional[dict[str, Any]]:
        return self.thresholds["phases"].get(phase, {}).get(signal)

    @property
    def default_severe_interlock(self) -> str:
        return self.thresholds["default_severe_interlock"]
