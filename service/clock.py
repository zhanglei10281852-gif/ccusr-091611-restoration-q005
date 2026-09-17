"""时钟：生产用真实时钟，测试与超时推演用可推进的模拟时钟。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


class Clock:
    def now(self) -> datetime:
        raise NotImplementedError

    def advance(self, seconds: float) -> datetime:  # 真实时钟不可推进
        raise NotImplementedError("real clock cannot be advanced")


class RealClock(Clock):
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class SimClock(Clock):
    """模拟时钟。测试通过 advance 推进，超时与失联判定据此触发。"""

    def __init__(self, start: datetime | None = None):
        self._now = start or datetime(2026, 9, 15, 20, 0, tzinfo=timezone.utc)
        if self._now.tzinfo is None:
            self._now = self._now.replace(tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> datetime:
        self._now += timedelta(seconds=seconds)
        return self._now
