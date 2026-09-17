"""安全不变量单元测试。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from service.recovery import restore_channel
from service.monitor import Monitor
from tests.helpers import build_monitor, tel, steady_fixture, T0, ROOT


# ---------------------------------------------------------- 乱序与重复
def test_duplicate_message_has_no_effect_twice():
    mon, clock, store, gw = build_monitor("steady")
    r1 = mon.ingest([tel("current", 1.24, clock, "m1")])
    assert r1[0]["accepted"] is True
    assert mon.s.state == "locked"
    # 完全重复的 message_id：拒绝，且不产生新事件效果
    r2 = mon.ingest([tel("current", 1.24, clock, "m1")])
    assert r2[0]["accepted"] is False and r2[0]["reason"] == "duplicate"
    drops = [e for e in store.events("cell-3") if e["event_type"] == "TelemetryDropped"]
    assert len(drops) == 1 and drops[0]["payload"]["reason"] == "duplicate"


def test_stale_reading_cannot_regress_state():
    mon, clock, store, gw = build_monitor("steady")
    mon.ingest([tel("current", 1.24, clock, "m1", observed_at=clock.now())])
    assert mon.s.state == "locked"
    clock.advance(5)
    late = tel("current", 0.80, clock, "m2",
               observed_at=clock.now().replace(microsecond=0) - __import__("datetime").timedelta(seconds=6))
    r = mon.ingest([late])
    assert r[0]["accepted"] is False and r[0]["reason"] == "stale"
    assert mon.s.state == "locked"  # 阶段不回退


def test_out_of_order_delivery_still_processed_in_observation_order():
    # examples/telemetry.json 形态：msg-81 先到（21:00:02），msg-80 后到（21:00:01）
    mon, clock, store, gw = build_monitor("prepared")
    later = tel("heartbeat", 1, clock, "msg-81",
                observed_at=T0.replace(year=2026, month=9, day=15, hour=21, minute=0, second=2))
    earlier = tel("heartbeat", 1, clock, "msg-80",
                  observed_at=T0.replace(year=2026, month=9, day=15, hour=21, minute=0, second=1))
    mon.ingest([later, earlier])
    assert mon.s.high_water.isoformat().endswith("21:00:02+08:00")
    # 再来一条更早的：必须丢弃
    old = tel("heartbeat", 1, clock, "msg-79",
              observed_at=T0.replace(year=2026, month=9, day=15, hour=20, minute=59, second=59))
    assert mon.ingest([old])[0]["reason"] == "stale"


# ---------------------------------------------------------- 联锁分级
def test_advisory_is_cleared_by_acknowledge():
    mon, clock, store, gw = build_monitor("steady")
    mon.ingest([tel("current", 1.05, clock, "m1")])  # >= warn 1.00
    assert mon.s.interlock == "advisory" and mon.s.state == "steady"
    res = mon.command("acknowledge", role="operator", idempotency_key="ack-1")
    assert res["accepted"] and res["cleared"] == "advisory"
    assert mon.s.interlock is None


def test_operator_reset_pauses_and_requires_recovery_before_ack():
    mon, clock, store, gw = build_monitor("steady")
    mon.ingest([tel("temperature", 42.5, clock, "m1")])  # >= 42.0 operator_reset
    assert mon.s.state == "paused" and mon.s.interlock == "operator_reset"
    assert any(f["frame"] == "SUSPEND_OUTPUT" for f in mon.s.device_frames)
    # 读数仍越限时确认无效
    bad = mon.command("acknowledge", role="operator", idempotency_key="ack-bad")
    assert not bad["accepted"]
    # 读数恢复 + 仍然失联窗口内：可确认，然后 resume
    clock.advance(10)
    mon.ingest([
        tel("current", 0.80, clock, "m2"), tel("voltage", 12, clock, "m3"),
        tel("temperature", 30.0, clock, "m4"), tel("heartbeat", 1, clock, "m5"),
    ])
    ok = mon.command("acknowledge", role="operator", idempotency_key="ack-ok")
    assert ok["accepted"]
    res = mon.command("resume", role="operator", idempotency_key="res-1")
    assert res["accepted"] and res["state"] == "steady"


def test_current_spike_locks_and_ordinary_ack_cannot_clear():
    mon, clock, store, gw = build_monitor("steady")
    mon.ingest([tel("current", 0.80, clock, "m0")])
    mon.ingest([tel("current", 1.24, clock, "m1")])  # 夜间事故：短时越过安全线
    mon.ingest([tel("current", 0.81, clock, "m2")])  # 自行恢复
    assert mon.s.state == "locked" and mon.s.interlock == "supervisor_reset"
    ack = mon.command("acknowledge", role="operator", idempotency_key="ack-1")
    assert not ack["accepted"] and "主管" in ack["reason"]
    # 操作员角色调用主管解锁也被拒
    fake = mon.command("supervisor_unlock", role="operator", idempotency_key="sup-x",
                       badge="op-1")
    assert not fake["accepted"]
    # 真正主管解锁 -> 新回合 -> prepared
    sup = mon.command("supervisor_unlock", role="supervisor", idempotency_key="sup-1",
                      badge="mgr-9")
    assert sup["accepted"] and mon.s.state == "prepared" and mon.s.round_id == 2


def test_emergency_lock_is_permanent_for_the_round():
    mon, clock, store, gw = build_monitor("steady")
    mon.ingest([tel("current", 1.40, clock, "m1")])
    assert mon.s.interlock == "emergency_lock" and mon.s.state == "locked"
    assert mon.command("acknowledge", "operator", "a1")["accepted"] is False
    assert mon.command("supervisor_unlock", "supervisor", "s1", badge="mgr")["accepted"] is False
    # 只能紧急停机后异常结束
    fin = mon.command("finish", "supervisor", "f1")
    assert fin["accepted"] and fin.get("abnormal") and mon.s.state == "completed"


# ---------------------------------------------------------- 失联与维护
def test_silence_while_energized_suspends():
    mon, clock, store, gw = build_monitor("steady")
    mon.ingest([tel("heartbeat", 1, clock, "hb0")])
    clock.advance(75)  # 超过 silence_s=60
    mon.poll_timeouts()
    assert mon.s.state == "paused" and mon.s.interlock == "operator_reset"
    reasons = " ".join(r["reasons"][0] for r in mon.s.interlock_reasons)
    assert "失联" in reasons


def test_silence_during_maintenance_does_not_interlock():
    mon, clock, store, gw = build_monitor("paused")
    mon.ingest([tel("heartbeat", 1, clock, "hb0")])
    assert mon.command("enter_maintenance", "operator", "mnt-1")["accepted"]
    frames_before = len(gw.sent)
    clock.advance(600)
    mon.poll_timeouts()
    assert mon.s.mode == "maintenance" and mon.s.interlock is None
    assert mon.s.state == "paused"
    assert len(gw.sent) == frames_before  # 维护期间失联不下发任何帧
    noted = [e for e in store.events("cell-3") if e["event_type"] == "MaintenanceSilenceNoted"]
    assert noted  # 但留下了记录


def test_maintenance_exit_requires_fresh_precheck():
    mon, clock, store, gw = build_monitor("paused")
    steady_fixture(mon, clock)
    mon.command("enter_maintenance", "operator", "mnt-1")
    clock.advance(300)  # 读数窗口过期
    blocked = mon.command("exit_maintenance", "operator", "mnt-2")
    assert not blocked["accepted"] and "预检" in blocked["reason"]
    steady_fixture(mon, clock, prefix="m2")
    assert mon.command("exit_maintenance", "operator", "mnt-3")["accepted"]


# ---------------------------------------------------------- 紧急停机幂等
def test_estop_hard_frame_sent_exactly_once_under_retries():
    mon, clock, store, gw = build_monitor("steady")
    r1 = mon.command("emergency_stop", "operator", "estop-1")
    assert r1["accepted"]
    r2 = mon.command("emergency_stop", "operator", "estop-1")  # 同键网络重试
    r3 = mon.command("emergency_stop", "operator", "estop-2")  # 换键重试
    assert r2["replayed"] is True
    hard = [f for f in gw.sent if f.frame == "FORCE_RELAYS_OPEN"]
    assert len(hard) == 1
    assert mon.s.state == "locked"


def test_estop_still_single_frame_after_process_restart():
    import tempfile
    from service.clock import SimClock
    from service.calibration import CalibrationTable
    from service.device import DeviceGateway
    from service.monitor import Monitor, load_recipes
    from service.store import EventStore

    recipe = load_recipes(ROOT / "domain" / "recipes.json")
    cal = CalibrationTable.load(ROOT / "domain" / "calibrations.json")
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "m.db"

        # —— 第一个进程：建库、进稳态、紧急停机 ——
        clock = SimClock(T0)
        store = EventStore(db)
        gw = DeviceGateway()
        mon = Monitor("cell-3", recipe, store, gw, clock, cal)
        mon.open(recipe["recipe_id"])
        mon.seed_state("steady", clock.now())
        mon.command("emergency_stop", "operator", "estop-1")
        assert len([f for f in gw.sent if f.frame == "FORCE_RELAYS_OPEN"]) == 1
        store.commit(); store.close()

        # —— 进程重启：全新网关，仅靠事件流恢复 ——
        clock2 = SimClock(T0)
        store_b = EventStore(db)
        gw_b = DeviceGateway()
        mon_b = Monitor("cell-3", recipe, store_b, gw_b, clock2, cal, replay=True)
        restore_channel(mon_b)
        mon_b.replaying = False
        assert mon_b.s.state == "locked" and mon_b.s.hard_tripped is True
        # 恢复后又来一次停机请求（新键）：不得再下发硬帧
        mon_b.command("emergency_stop", "operator", "estop-2")
        assert len([f for f in gw_b.sent if f.frame == "FORCE_RELAYS_OPEN"]) == 0
        store_b.close()


# ---------------------------------------------------------- 模拟时钟超时
def test_simulated_clock_drives_expected_timeouts():
    mon, clock, store, gw = build_monitor("prepared")
    steady_fixture(mon, clock)
    assert mon.command("precheck", "operator", "pc-1")["accepted"]
    assert mon.command("start", "operator", "st-1")["accepted"]
    assert mon.s.state == "ramping"
    # 升流期间持续上送电流，推进超过 1800s
    for i in range(7):
        clock.advance(300)
        mon.ingest([
            tel("current", 0.80, clock, f"c{i}"), tel("heartbeat", 1, clock, f"h{i}"),
            tel("voltage", 12, clock, f"v{i}"), tel("temperature", 28, clock, f"t{i}"),
            tel("chloride", 400, clock, f"l{i}"),
        ])
    assert mon.s.state == "steady", mon.s.state
    # 稳态失联超时
    clock.advance(91)
    mon.poll_timeouts()
    assert mon.s.state == "paused"


# ---------------------------------------------------------- 命令状态门控
def test_commands_only_fire_from_valid_states():
    mon, clock, store, gw = build_monitor("prepared")
    # 未预检不能 start
    assert mon.command("start", "operator", "x1")["accepted"] is False
    # locked 中不能 pause
    mon2, c2, s2, g2 = build_monitor("locked")
    assert mon2.command("pause", "operator", "x2")["accepted"] is False
    # completed 通道紧急停机被拒
    mon3, c3, s3, g3 = build_monitor("completed")
    assert mon3.command("emergency_stop", "operator", "x3")["accepted"] is False


def test_command_idempotency_returns_first_result():
    mon, clock, store, gw = build_monitor("steady")
    r1 = mon.command("pause", "operator", "pause-1")
    r2 = mon.command("pause", "operator", "pause-1")
    assert r1["state"] == "paused" and r2["replayed"] is True
    assert len([f for f in gw.sent if f.frame == "SUSPEND_OUTPUT"]) == 1


# ---------------------------------------------------------- 审计轨迹
def test_audit_carries_windows_reasons_and_human_actions():
    mon, clock, store, gw = build_monitor("steady")
    mon.ingest([tel("current", 1.24, clock, "m1")])
    mon.command("acknowledge", "operator", "ack-1")
    audit = mon.audit()
    tr = audit["transitions"][-1]
    assert tr["to"] == "locked" and tr["reading_window"]["signals"]["current"]
    assert audit["interlocks"][-1]["reasons"]
    assert any(a["command"] == "acknowledge" for a in audit["operator_actions"])
    assert any(f["frame"] == "FORCE_RELAYS_OPEN" for f in audit["device_frames"])


# ---------------------------------------------------------- 校准
def test_telemetry_is_calibrated_before_evaluation():
    mon, clock, store, gw = build_monitor("steady")
    # 夜间记录 msg-80 示值 0.51 -> 校正 0.51*0.9886-0.0024=0.501786（记录中标注 0.502）
    res = mon.ingest([tel("current", 0.51, clock, "msg-80")])
    assert round(res[0]["value"], 3) == 0.502
    assert abs(res[0]["value"] - 0.501786) < 1e-6
    assert res[0]["calibration_id"] == "cal-2026-09-01-psu"
