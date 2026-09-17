"""遥测门槛：去重、乱序隔离、校准修正。

保证：重复（同 message_id）与乱序（observed_at 不晚于该信号高水位）的读数
永远不会进入控制逻辑，因此处理阶段不会因为遥测到达顺序而回退。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .channel import ChannelRuntime
from .config import DomainConfig
from .models import Reading, TelemetryEvent


@dataclass
class GateResult:
    status: str  # accepted | duplicate | out_of_order
    reading: Optional[Reading] = None
    reason: Optional[str] = None


class TelemetryGate:
    def __init__(self, config: DomainConfig):
        self.config = config

    def accept(self, rt: ChannelRuntime, event: TelemetryEvent) -> GateResult:
        # 1) 幂等去重：同一 message_id 只处理一次
        if event.message_id in rt.seen_messages:
            return GateResult(status="duplicate", reason="message_id 已处理")

        # 2) 单位校验
        expected_unit = self.config.expected_unit(event.signal)
        if expected_unit is not None and event.unit != expected_unit:
            raise ValueError(f"信号 {event.signal} 单位应为 {expected_unit}，收到 {event.unit}")

        # 3) 乱序隔离：observed_at 必须严格晚于该 (通道, 信号) 的高水位
        high_water = rt.high_water.get(event.signal)
        if high_water is not None and event.observed_at <= high_water:
            return GateResult(
                status="out_of_order",
                reason=f"observed_at {event.observed_at.isoformat()} 不晚于高水位 {high_water.isoformat()}",
            )

        # 4) 校准修正
        corrected, calibration_valid = self.config.apply_calibration(
            event.channel_id, event.signal, event.value, event.observed_at
        )

        rt.seen_messages.add(event.message_id)
        rt.high_water[event.signal] = event.observed_at
        reading = Reading(
            message_id=event.message_id,
            signal=event.signal,
            raw_value=event.value,
            value=corrected,
            unit=event.unit,
            observed_at=event.observed_at,
            received_at=event.received_at,
            calibration_valid=calibration_valid,
        )
        rt.latest[event.signal] = reading
        rt.window.append(reading)
        return GateResult(status="accepted", reading=reading)
