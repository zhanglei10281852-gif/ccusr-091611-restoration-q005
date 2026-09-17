"""领域模型：阶段、联锁等级、遥测事件、处理方案、联锁记录。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


class Phase(str, enum.Enum):
    """处理阶段，取值与 domain/contract.json 的 treatment_states 一致。"""

    PREPARED = "prepared"      # 预检
    RAMPING = "ramping"        # 升流
    STEADY = "steady"          # 稳态
    PAUSED = "paused"          # 暂停
    LOCKED = "locked"          # 安全锁定
    RINSING = "rinsing"        # 冲洗
    COMPLETED = "completed"    # 结束


class InterlockLevel(enum.IntEnum):
    """联锁等级，数值越大需要的复位权限越高。取值与 contract.json 的 interlock_levels 一致。"""

    ADVISORY = 1          # 提示，不阻断
    OPERATOR_RESET = 2    # 操作员确认即可解除
    SUPERVISOR_RESET = 3  # 需主管复位
    EMERGENCY_LOCK = 4    # 紧急锁定，需主管复位


_LEVEL_NAMES = {
    "advisory": InterlockLevel.ADVISORY,
    "operator_reset": InterlockLevel.OPERATOR_RESET,
    "supervisor_reset": InterlockLevel.SUPERVISOR_RESET,
    "emergency_lock": InterlockLevel.EMERGENCY_LOCK,
}
_LEVEL_LABELS = {v: k for k, v in _LEVEL_NAMES.items()}


def level_from_name(name: str) -> InterlockLevel:
    if name not in _LEVEL_NAMES:
        raise ValueError(f"未知联锁等级: {name}")
    return _LEVEL_NAMES[name]


def level_name(level: InterlockLevel) -> str:
    return _LEVEL_LABELS[level]


class Role(str, enum.Enum):
    OPERATOR = "operator"
    SUPERVISOR = "supervisor"


SIGNALS = ("current", "voltage", "chloride", "temperature", "heartbeat")

DEVICE_COMMANDS = ("start", "pause", "resume", "emergency_stop", "begin_rinse", "finish")

CONSOLE_ACTIONS = (
    "acknowledge_interlock",  # 普通确认：只能解除 advisory / operator_reset
    "supervisor_reset",       # 主管复位：可解除全部等级
    "enter_maintenance",      # 进入维护模式（计划性，暂停失联监测）
    "exit_maintenance",       # 退出维护模式（立即恢复安全评估）
)


def parse_time(text: str) -> datetime:
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        raise ValueError(f"时间缺少时区: {text}")
    return moment


@dataclass(frozen=True)
class TelemetryEvent:
    message_id: str
    channel_id: str
    signal: str
    value: float
    unit: str
    observed_at: datetime
    received_at: datetime

    @classmethod
    def from_dict(cls, raw: dict[str, Any], required_fields: set[str], known_signals: set[str]) -> "TelemetryEvent":
        missing = required_fields - set(raw)
        if missing:
            raise ValueError(f"遥测缺少字段: {sorted(missing)}")
        if raw["signal"] not in known_signals:
            raise ValueError(f"未知信号类型: {raw['signal']}")
        if not isinstance(raw["value"], (int, float)) or isinstance(raw["value"], bool):
            raise ValueError("value 必须是数值")
        return cls(
            message_id=str(raw["message_id"]),
            channel_id=str(raw["channel_id"]),
            signal=str(raw["signal"]),
            value=float(raw["value"]),
            unit=str(raw["unit"]),
            observed_at=parse_time(str(raw["observed_at"])),
            received_at=parse_time(str(raw["received_at"])),
        )


@dataclass(frozen=True)
class TreatmentPlan:
    """器物级处理方案：目标电流、升流速率、氯离子目标与各阶段时限。"""

    plan_id: str
    artifact_id: str
    channel_id: str
    target_current_A: float
    ramp_rate_A_per_s: float
    steady_band_A: float
    chloride_target_mg_L: float
    settle_s: float
    ramp_timeout_s: float
    rinse_min_s: float
    paused_max_s: float
    artifact_name: str = ""

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "TreatmentPlan":
        required = {
            "plan_id", "artifact_id", "channel_id", "target_current_A",
            "ramp_rate_A_per_s", "steady_band_A", "chloride_target_mg_L",
            "settle_s", "ramp_timeout_s", "rinse_min_s", "paused_max_s",
        }
        missing = required - set(raw)
        if missing:
            raise ValueError(f"处理方案缺少字段: {sorted(missing)}")
        return cls(
            plan_id=str(raw["plan_id"]),
            artifact_id=str(raw["artifact_id"]),
            channel_id=str(raw["channel_id"]),
            target_current_A=float(raw["target_current_A"]),
            ramp_rate_A_per_s=float(raw["ramp_rate_A_per_s"]),
            steady_band_A=float(raw["steady_band_A"]),
            chloride_target_mg_L=float(raw["chloride_target_mg_L"]),
            settle_s=float(raw["settle_s"]),
            ramp_timeout_s=float(raw["ramp_timeout_s"]),
            rinse_min_s=float(raw["rinse_min_s"]),
            paused_max_s=float(raw["paused_max_s"]),
            artifact_name=str(raw.get("artifact_name", "")),
        )


@dataclass
class Reading:
    """经过校准修正、被门槛接受的读数。"""

    message_id: str
    signal: str
    raw_value: float
    value: float  # 校准修正后的值
    unit: str
    observed_at: datetime
    received_at: datetime
    calibration_valid: bool


@dataclass
class Interlock:
    interlock_id: str
    channel_id: str
    code: str
    level: InterlockLevel
    message: str
    opened_at: datetime
    open: bool = True
    cleared_at: Optional[datetime] = None
    cleared_by: Optional[str] = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "interlock_id": self.interlock_id,
            "channel_id": self.channel_id,
            "code": self.code,
            "level": level_name(self.level),
            "message": self.message,
            "opened_at": self.opened_at.isoformat(),
            "open": self.open,
            "cleared_at": self.cleared_at.isoformat() if self.cleared_at else None,
            "cleared_by": self.cleared_by,
            "details": self.details,
        }
