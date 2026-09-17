"""正常工艺全流程：预检 -> 升流 -> 稳态达标 -> 冲洗 -> 结束（模拟时钟）。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.helpers import build_monitor, tel, steady_fixture


def _feed(mon, clock, prefix, cl=40.0):
    rows = [
        tel("current", 0.80, clock, f"{prefix}-cur"),
        tel("voltage", 12.0, clock, f"{prefix}-vol"),
        tel("chloride", cl, clock, f"{prefix}-cl"),
        tel("temperature", 28.0, clock, f"{prefix}-temp"),
        tel("heartbeat", 1, clock, f"{prefix}-hb"),
    ]
    mon.ingest(rows)


def test_happy_path_to_completion():
    mon, clock, store, gw = build_monitor("prepared")

    # 预检 + 启动
    _feed(mon, clock, "pc", cl=900)
    assert mon.command("precheck", "operator", "pc")["accepted"]
    assert mon.command("start", "operator", "st")["accepted"]
    assert mon.s.state == "ramping"

    # 升流 1800s 后自动进入稳态
    for i in range(7):
        clock.advance(300)
        _feed(mon, clock, f"r{i}", cl=800 - i * 80)
    assert mon.s.state == "steady"

    # 稳态不足 24h 不允许冲洗
    blocked = mon.command("begin_rinse", "operator", "rinse-early")
    assert not blocked["accepted"]

    # 推进 24 小时，期间持续上送低氯离子读数（每 30 分钟一批）
    for i in range(49):
        clock.advance(1800)
        _feed(mon, clock, f"s{i}", cl=30.0)
    assert mon.s.state == "steady"  # 无联锁
    ok = mon.command("begin_rinse", "operator", "rinse-1")
    assert ok["accepted"] and mon.s.state == "rinsing"

    # 冲洗期间温度仍受监测；推进 3600s 自动结束
    clock.advance(3601)
    mon.poll_timeouts()
    assert mon.s.state == "completed"
    assert [f.frame for f in gw.sent][-2:] == ["START_RINSE", "STOP_RINSE"]

    # 结束后急停无效
    assert mon.command("emergency_stop", "operator", "late")["accepted"] is False


def test_resume_blocked_by_stale_window_then_allowed():
    mon, clock, store, gw = build_monitor("steady")
    assert mon.command("pause", "operator", "p1")["accepted"]
    assert mon.s.state == "paused"
    # 只有旧读数时恢复被拒
    clock.advance(120)
    assert mon.command("resume", "operator", "res-bad")["accepted"] is False
    # 新鲜正常读数后可恢复
    _feed(mon, clock, "fresh")
    r = mon.command("resume", "operator", "res-ok")
    assert r["accepted"] and r["state"] == "steady"
