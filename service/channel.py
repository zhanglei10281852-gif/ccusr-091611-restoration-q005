"""通道运行时：单个电解槽的全部可变状态。"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from .models import Interlock, InterlockLevel, Phase, Reading, TreatmentPlan


class ChannelRuntime:
    def __init__(self, channel_id: str):
        self.channel_id = channel_id
        self.phase: Phase = Phase.PREPARED
        self.plan: Optional[TreatmentPlan] = None
        self.maintenance: bool = False

        # 阶段记忆：暂停/锁定前所处的阶段，用于恢复
        self.paused_from: Optional[Phase] = None
        self.interrupted_from: Optional[Phase] = None

        # 阶段计时
        self.ramp_started_at: Optional[datetime] = None
        self.band_since: Optional[datetime] = None  # 电流进入目标带的观测时刻
        self.rinse_started_at: Optional[datetime] = None
        self.paused_since: Optional[datetime] = None

        # 遥测门槛状态
        self.seen_messages: set[str] = set()
        self.high_water: dict[str, datetime] = {}  # 每个信号的观测时间高水位
        self.latest: dict[str, Reading] = {}
        self.window: list[Reading] = []  # 距上次状态转换以来的读数窗口

        # 安全状态
        self.interlocks: list[Interlock] = []
        self.effects: list[dict] = []  # 服务向设备下发的安全动作记录（如电流归零）
        self.last_heartbeat_at: Optional[datetime] = None
        self.sensor_lost_since: Optional[datetime] = None
        self._interlock_seq = 0

    # ---- 联锁 ----
    def open_interlocks(self) -> list[Interlock]:
        return [i for i in self.interlocks if i.open]

    def open_interlock(self, code: str) -> Optional[Interlock]:
        for interlock in self.interlocks:
            if interlock.open and interlock.code == code:
                return interlock
        return None

    def highest_open_level(self) -> Optional[InterlockLevel]:
        levels = [i.level for i in self.open_interlocks()]
        return max(levels) if levels else None

    def blocking_interlocks(self) -> list[Interlock]:
        """需要确认/复位的联锁（advisory 不阻断工艺操作）。"""
        return [i for i in self.open_interlocks() if i.level >= InterlockLevel.OPERATOR_RESET]

    def next_interlock_id(self) -> str:
        self._interlock_seq += 1
        return f"{self.channel_id}-il-{self._interlock_seq}"

    # ---- 新方案 ----
    def reset_for_new_plan(self, plan: TreatmentPlan) -> None:
        self.plan = plan
        self.phase = Phase.PREPARED
        self.paused_from = None
        self.interrupted_from = None
        self.ramp_started_at = None
        self.band_since = None
        self.rinse_started_at = None
        self.paused_since = None
        self.sensor_lost_since = None
