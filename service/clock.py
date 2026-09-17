"""可注入时钟：生产用系统时钟，测试/回放用模拟时钟。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


class Clock:
    """时钟接口。服务内所有时间判断都经过该接口，保证可测试、可回放。"""

    def now(self) -> datetime:  # pragma: no cover - 接口定义
        raise NotImplementedError


class SystemClock(Clock):
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class ManualClock(Clock):
    """模拟时钟：只能显式推进，用于超时测试与故障样例回放。"""

    def __init__(self, start: datetime):
        if start.tzinfo is None:
            raise ValueError("ManualClock 需要带时区的初始时间")
        self._now = start

    def now(self) -> datetime:
        return self._now

    def set(self, moment: datetime) -> None:
        if moment.tzinfo is None:
            raise ValueError("需要带时区的时间")
        self._now = moment

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)
