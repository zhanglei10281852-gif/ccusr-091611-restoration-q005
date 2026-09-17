"""设备网关：向电源/循环泵下发逻辑帧。

关键安全语义：FORCE_RELAYS_OPEN（紧急停机硬帧）每个通道每个处理回合
只允许真正下发一次——即使网络抖动带来并发/重试调用。普通帧则由
事件日志保证“同一状态转换只产生一帧”。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime


class DeviceError(RuntimeError):
    pass


@dataclass
class DeviceFrame:
    frame: str
    channel_id: str
    args: dict
    sent_at: datetime
    cause_seq: int
    cause: str


class DeviceGateway:
    """测试/无硬件环境下的内存网关；生产可替换为串口/网口实现。

    维护模式下拒绝一切带电帧（precharged frames），由 Monitor 先过滤，
    网关再做一道防线。
    """

    ENERGIZED_FRAMES = {"ENABLE_OUTPUT", "HOLD_CURRENT", "UNLOCK_RELAYS"}

    def __init__(self):
        self._lock = threading.Lock()
        self._hard_trip: dict[str, bool] = {}
        self.sent: list[DeviceFrame] = []

    def reset_round(self, channel_id: str):
        """新处理回合（主管解锁后重新开始）时复位一次动作闸门。"""
        with self._lock:
            self._hard_trip.pop(channel_id, None)

    def restore_hard_trip(self, channel_id: str):
        """进程恢复时按事件流重建闸门：本回合一经硬停机，重启后仍拒绝第二帧。"""
        with self._lock:
            self._hard_trip[channel_id] = True

    def hard_trip_done(self, channel_id: str) -> bool:
        return self._hard_trip.get(channel_id, False)

    def send(self, frame: str, channel_id: str, args: dict, sent_at: datetime,
             cause_seq: int, cause: str, maintenance: bool = False) -> tuple[DeviceFrame, bool]:
        """返回 (帧, 是否真正下发)。重复硬帧返回首帧且 physically_sent=False。"""
        with self._lock:
            if maintenance and frame in self.ENERGIZED_FRAMES:
                raise DeviceError(f"maintenance mode refuses energized frame {frame}")
            if frame == "FORCE_RELAYS_OPEN":
                # 紧急停机帧：幂等闸门。第一次真正下发，之后全部短路。
                if self._hard_trip.get(channel_id):
                    # 重启恢复后 sent 列表为空：首帧只存在于事件日志，同样短路返回 None。
                    first = next((f for f in reversed(self.sent)
                                  if f.channel_id == channel_id and f.frame == frame), None)
                    return first, False
                self._hard_trip[channel_id] = True
            df = DeviceFrame(frame=frame, channel_id=channel_id, args=dict(args),
                             sent_at=sent_at, cause_seq=cause_seq, cause=cause)
            self.sent.append(df)
            return df, True

    def frames_for(self, channel_id: str) -> list[DeviceFrame]:
        return [f for f in self.sent if f.channel_id == channel_id]
