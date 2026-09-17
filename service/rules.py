"""阈值曲线评估：按当前阶段对修正后的读数分级（warn / alarm / severe）。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from .channel import ChannelRuntime
from .models import Reading


@dataclass
class Violation:
    severity: str  # warn | alarm | severe
    detail: str
    limit: float
    severe_interlock: Optional[str] = None


def classify(rt: ChannelRuntime, rule: dict[str, Any], reading: Reading) -> Optional[Violation]:
    """返回该读数触发的最高严重度；未越限返回 None。"""
    kind = rule.get("kind")
    value = reading.value
    plan = rt.plan

    severe_above = rule.get("severe_above_A") or rule.get("severe_above")
    severe_interlock = rule.get("severe_interlock")

    if kind == "ramp_curve":
        assert plan is not None and rt.ramp_started_at is not None
        elapsed = max(0.0, (reading.observed_at - rt.ramp_started_at).total_seconds())
        limit = min(
            plan.ramp_rate_A_per_s * elapsed + rule["margin_A"],
            plan.target_current_A * rule["ceiling_factor"],
        )
        over = value - limit
        if severe_above is not None and value > severe_above:
            return Violation("severe", f"电流 {value:.2f}A 超过严重线 {severe_above}A", severe_above, severe_interlock)
        if over > rule["alarm_over_limit_A"]:
            return Violation("alarm", f"电流 {value:.2f}A 超出升流曲线 {limit:.2f}A（超 {over:.2f}A）", limit)
        if over > rule["warn_over_limit_A"]:
            return Violation("warn", f"电流 {value:.2f}A 略高于升流曲线 {limit:.2f}A", limit)
        return None

    if kind == "deviation":
        assert plan is not None
        deviation = abs(value - plan.target_current_A)
        if severe_above is not None and value > severe_above:
            return Violation("severe", f"电流 {value:.2f}A 超过严重线 {severe_above}A", severe_above, severe_interlock)
        if deviation > rule["alarm_deviation_A"]:
            return Violation("alarm", f"电流偏离目标 {deviation:.2f}A（告警带 {rule['alarm_deviation_A']}A）", rule["alarm_deviation_A"])
        if deviation > rule["warn_deviation_A"]:
            return Violation("warn", f"电流偏离目标 {deviation:.2f}A（提示带 {rule['warn_deviation_A']}A）", rule["warn_deviation_A"])
        return None

    if kind == "above":
        if severe_above is not None and value > severe_above:
            return Violation("severe", f"{reading.signal} {value} 超过严重线 {severe_above}", severe_above, severe_interlock)
        alarm_above = rule.get("alarm_above")
        if alarm_above is not None and value > alarm_above:
            return Violation("alarm", f"{reading.signal} {value} 超过告警线 {alarm_above}", alarm_above)
        warn_above = rule.get("warn_above")
        if warn_above is not None and value > warn_above:
            return Violation("warn", f"{reading.signal} {value} 超过提示线 {warn_above}", warn_above)
        return None

    return None
