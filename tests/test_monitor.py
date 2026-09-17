"""处理监控服务测试：阶段机、遥测门槛、联锁等级、失联/维护策略、幂等、超时、恢复。"""
from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from service import (
    DomainConfig,
    JournalStore,
    ManualClock,
    MemoryStore,
    ScenarioRunner,
    TreatmentMonitorService,
)

ROOT = Path(__file__).resolve().parents[1]
TZ = timezone(timedelta(hours=8))
T0 = datetime(2026, 9, 15, 21, 0, 0, tzinfo=TZ)

PLAN = {
    "plan_id": "plan-test",
    "artifact_id": "bronze-test",
    "artifact_name": "测试器物",
    "channel_id": "cell-3",
    "target_current_A": 2.0,
    "ramp_rate_A_per_s": 0.05,
    "steady_band_A": 0.1,
    "chloride_target_mg_L": 0.5,
    "settle_s": 10,
    "ramp_timeout_s": 120,
    "rinse_min_s": 60,
    "paused_max_s": 1800,
}


def make_service(clock=None, store=None):
    config = DomainConfig(ROOT)
    clock = clock or ManualClock(T0)
    store = store or MemoryStore()
    return TreatmentMonitorService(config, clock, store), clock, store


class Driver:
    """测试驱动助手：发送遥测/心跳/命令并推进模拟时钟。"""

    def __init__(self, service, clock, channel="cell-3"):
        self.svc = service
        self.clock = clock
        self.channel = channel
        self.seq = 0

    def _mid(self, prefix):
        self.seq += 1
        return f"{prefix}-{self.seq}"

    def at(self, seconds):
        self.clock.set(T0 + timedelta(seconds=seconds))
        return self.clock.now()

    def telemetry(self, signal, value, seconds, unit=None, channel=None, mid=None):
        moment = self.at(seconds)
        units = {"current": "A", "voltage": "V", "chloride": "mg/L", "temperature": "C", "heartbeat": "none"}
        return self.svc.ingest_telemetry({
            "message_id": mid or self._mid(signal),
            "channel_id": channel or self.channel,
            "signal": signal,
            "value": value,
            "unit": unit or units[signal],
            "observed_at": moment.isoformat(),
            "received_at": moment.isoformat(),
        })

    def heartbeat(self, seconds, channel=None):
        return self.telemetry("heartbeat", 1, seconds, channel=channel)

    def command(self, ctype, seconds, key=None, actor="op-1", role="operator"):
        self.at(seconds)
        return self.svc.execute_command({
            "type": ctype,
            "channel_id": self.channel,
            "actor": actor,
            "role": role,
            "idempotency_key": key or self._mid(ctype),
        })

    def action(self, atype, seconds, key=None, actor="sup-1", role="supervisor"):
        self.at(seconds)
        return self.svc.execute_action({
            "type": atype,
            "channel_id": self.channel,
            "actor": actor,
            "role": role,
            "idempotency_key": key or self._mid(atype),
        })

    def to_steady(self, plan=None):
        """登记方案、预检读数、启动并升流到稳态。返回各步骤结果。"""
        self.svc.register_plan(plan or dict(PLAN))
        self.telemetry("current", 0.0, 1)
        self.telemetry("voltage", 8.0, 2)
        self.telemetry("chloride", 1.5, 3)
        self.telemetry("temperature", 24.0, 4)
        self.heartbeat(4)
        started = self.command("start", 5)
        assert started["status"] == "ok", started
        for seconds, amps in ((15, 0.5), (25, 1.0), (35, 1.5), (45, 2.0), (55, 2.0)):
            self.heartbeat(seconds)
            self.telemetry("current", amps, seconds)
        return started


class TestPhaseMachine(unittest.TestCase):
    def test_happy_path_full_cycle(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "steady")

        self.assertEqual(d.command("pause", 60)["status"], "ok")
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "paused")
        self.assertEqual(d.command("resume", 65, actor="op-1")["status"], "ok")
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "steady")

        # 氯离子未达标时不能冲洗
        d.heartbeat(66)
        d.telemetry("chloride", 0.9, 66)
        rejected = d.command("begin_rinse", 67)
        self.assertEqual(rejected["status"], "rejected")
        self.assertEqual(rejected["reason"], "chloride_not_ready")

        d.heartbeat(68)
        d.telemetry("chloride", 0.4, 68)
        self.assertEqual(d.command("begin_rinse", 69)["status"], "ok")
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "rinsing")

        # 冲洗时长不足不能结束
        self.assertEqual(d.command("finish", 80)["reason"], "rinse_too_short")
        d.heartbeat(130)
        d.telemetry("current", 0.0, 130)
        self.assertEqual(d.command("finish", 130)["status"], "ok")
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "completed")

    def test_commands_only_valid_from_proper_states(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        # steady 状态下不能再次 start / 不能 resume / 不能 finish
        self.assertEqual(d.command("start", 60)["reason"], "invalid_state")
        self.assertEqual(d.command("resume", 61)["reason"], "invalid_state")
        self.assertEqual(d.command("finish", 62)["reason"], "invalid_state")
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "steady")

    def test_start_requires_precheck(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        svc.register_plan(dict(PLAN))
        # 缺少读数与心跳，预检失败
        result = d.command("start", 5)
        self.assertEqual(result["reason"], "precheck_failed")
        self.assertTrue(any("无读数" in f for f in result["failures"]))

    def test_transition_log_carries_window_interlocks_and_actor(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        log = svc.get_transition_log("cell-3")
        # prepared→ramping 转换带操作人与预检原因
        start_entry = next(e for e in log if e["to_phase"] == "ramping")
        self.assertEqual(start_entry["actor"]["id"], "op-1")
        self.assertIn("预检通过", start_entry["reasons"])
        # ramping→steady 转换带读数窗口（含稳态保持的电流读数）
        steady_entry = next(e for e in log if e["to_phase"] == "steady")
        window = steady_entry["reading_window"]
        self.assertTrue(window["message_ids"])
        self.assertIn("current", window["latest"])
        self.assertEqual(window["latest"]["current"]["value"], 2.0)
        self.assertIsNotNone(window["observed_from"])
        self.assertIsNotNone(window["observed_to"])


class TestTelemetryGate(unittest.TestCase):
    def test_out_of_order_and_duplicate_never_regress(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "steady")

        # 稳态下重发最近一条读数 → 重复；补发旧观测时间的读数 → 乱序隔离
        d.heartbeat(60)
        fresh = {
            "message_id": "cur-60", "channel_id": "cell-3", "signal": "current", "value": 2.0,
            "unit": "A", "observed_at": (T0 + timedelta(seconds=60)).isoformat(),
            "received_at": (T0 + timedelta(seconds=60)).isoformat(),
        }
        self.assertEqual(svc.ingest_telemetry(fresh)["status"], "accepted")
        self.assertEqual(svc.ingest_telemetry(fresh)["status"], "duplicate")
        stale = dict(fresh, message_id="late-1", value=9.9,
                     observed_at=(T0 + timedelta(seconds=30)).isoformat())
        self.assertEqual(svc.ingest_telemetry(stale)["status"], "out_of_order")
        # 即使乱序读数严重越限也被隔离：阶段不回退、不触发联锁
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "steady")
        self.assertEqual(svc.get_channel_view("cell-3")["open_interlocks"], [])

        # 仓库中的乱序样例（msg-81 先于 msg-80 到达）：在新通道上验证门槛行为
        rows = json.loads((ROOT / "examples" / "telemetry.json").read_text(encoding="utf-8"))
        first = svc.ingest_telemetry(dict(rows[0], channel_id="cell-1"))   # msg-81 observed 21:00:02
        self.assertEqual(first["status"], "accepted")
        second = svc.ingest_telemetry(dict(rows[1], channel_id="cell-1"))  # msg-80 observed 21:00:01，乱序
        self.assertEqual(second["status"], "out_of_order")
        again = svc.ingest_telemetry(dict(rows[0], channel_id="cell-1"))   # msg-81 重复
        self.assertEqual(again["status"], "duplicate")
        # 隔离记录进入审计（重复 2 条 + 乱序 2 条）
        quarantined = [e for e in svc.get_audit() if e["kind"] == "telemetry_quarantined"]
        self.assertEqual(len(quarantined), 4)

    def test_invalid_telemetry_rejected(self):
        svc, clock, _ = make_service()
        result = svc.ingest_telemetry({"message_id": "x", "channel_id": "cell-3"})
        self.assertEqual(result["status"], "rejected")
        bad_unit = {
            "message_id": "u1", "channel_id": "cell-3", "signal": "current", "value": 1.0,
            "unit": "mA", "observed_at": T0.isoformat(), "received_at": T0.isoformat(),
        }
        self.assertEqual(svc.ingest_telemetry(bad_unit)["status"], "rejected")
        unknown_channel = dict(bad_unit, unit="A", channel_id="cell-99", message_id="u2")
        self.assertEqual(svc.ingest_telemetry(unknown_channel)["status"], "rejected")


class TestInterlocks(unittest.TestCase):
    def test_severe_overcurrent_locks_and_operator_ack_cannot_clear(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        d.heartbeat(60)
        d.telemetry("current", 3.4, 60)  # 严重越限
        view = svc.get_channel_view("cell-3")
        self.assertEqual(view["phase"], "locked")
        self.assertEqual(view["open_interlocks"][0]["code"], "current_severe")
        self.assertEqual(view["open_interlocks"][0]["level"], "emergency_lock")
        self.assertEqual(len(view["effects"]), 1)  # 电流归零

        # 普通确认不能解除
        denied = d.action("acknowledge_interlock", 70, actor="op-1", role="operator")
        self.assertEqual(denied["status"], "denied")
        self.assertIn("current_severe", denied["remaining"])
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "locked")

        # 主管复位后解除并回到可恢复状态
        reset = d.action("supervisor_reset", 80, actor="sup-1", role="supervisor")
        self.assertEqual(reset["status"], "ok")
        view = svc.get_channel_view("cell-3")
        self.assertEqual(view["phase"], "paused")
        self.assertEqual(view["open_interlocks"], [])
        self.assertEqual(view["paused_from"], "steady")

    def test_supervisor_reset_requires_supervisor_role(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        d.telemetry("current", 3.4, 60)
        denied = d.action("supervisor_reset", 70, actor="op-1", role="operator")
        self.assertEqual(denied["reason"], "permission_denied")
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "locked")

    def test_alarm_auto_pauses_and_operator_ack_clears(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        d.heartbeat(60)
        d.telemetry("current", 2.4, 60)  # 偏离 0.4A > 告警带 0.3A
        view = svc.get_channel_view("cell-3")
        self.assertEqual(view["phase"], "paused")  # 自动保护性暂停
        self.assertEqual(view["open_interlocks"][0]["code"], "current_alarm")
        self.assertEqual(view["open_interlocks"][0]["level"], "operator_reset")

        # 联锁未确认前不能继续
        self.assertEqual(d.command("resume", 65)["reason"], "interlocks_open")
        ack = d.action("acknowledge_interlock", 66, actor="op-1", role="operator")
        self.assertEqual(ack["status"], "ok")
        d.heartbeat(67)
        self.assertEqual(d.command("resume", 68)["status"], "ok")
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "steady")

    def test_warn_advisory_auto_clears(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        d.heartbeat(60)
        d.telemetry("current", 2.2, 60)  # 偏离 0.2A > 提示带 0.15A
        codes = [i["code"] for i in svc.get_channel_view("cell-3")["open_interlocks"]]
        self.assertIn("current_warn", codes)
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "steady")  # 提示不阻断
        d.heartbeat(65)
        d.telemetry("current", 2.0, 65)  # 回到正常
        self.assertEqual(svc.get_channel_view("cell-3")["open_interlocks"], [])

    def test_calibration_expired_advisory_and_rinse_guard(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock, channel="cell-2")
        svc.register_plan(dict(PLAN, plan_id="plan-cell2", channel_id="cell-2"))
        d.telemetry("current", 0.0, 1, channel="cell-2")
        d.telemetry("voltage", 8.0, 2, channel="cell-2")
        # cell-2 氯离子传感器校准已过期（2026-06-01 到期）
        result = d.telemetry("chloride", 1.5, 3, channel="cell-2")
        self.assertFalse(result["calibration_valid"])
        codes = [i["code"] for i in svc.get_channel_view("cell-2")["open_interlocks"]]
        self.assertIn("calibration_expired_chloride", codes)
        # 预检拦截校准失效
        d.telemetry("temperature", 24.0, 4, channel="cell-2")
        d.heartbeat(4, channel="cell-2")
        self.assertEqual(d.command("start", 5)["reason"], "precheck_failed")


class TestSensorLossVsMaintenance(unittest.TestCase):
    def test_sensor_loss_auto_pauses_then_escalates(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()  # 最后心跳在 55s
        d.at(71)  # 16s 无心跳 > 15s 超时
        svc.tick()
        view = svc.get_channel_view("cell-3")
        self.assertEqual(view["phase"], "paused")  # 失联策略：保护性暂停
        self.assertEqual(view["open_interlocks"][0]["code"], "sensor_lost")

        d.at(71 + 601)  # 失联持续超过升级阈值
        svc.tick()
        view = svc.get_channel_view("cell-3")
        self.assertEqual(view["phase"], "locked")  # 升级锁定
        codes = sorted(i["code"] for i in view["open_interlocks"])
        self.assertEqual(codes, ["sensor_lost", "sensor_lost_prolonged"])
        # 普通确认不能解除主管级锁定
        self.assertEqual(d.action("acknowledge_interlock", 700, actor="op-1", role="operator")["status"], "denied")
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "locked")

    def test_sensor_recovery_requires_acknowledge_before_resume(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        d.at(75)
        svc.tick()
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "paused")
        d.heartbeat(80)  # 心跳恢复，但联锁仍需确认
        self.assertEqual(d.command("resume", 81)["reason"], "interlocks_open")
        d.action("acknowledge_interlock", 82, actor="op-1", role="operator")
        self.assertEqual(d.command("resume", 83)["status"], "ok")

    def test_maintenance_mode_uses_different_policy(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        d.command("pause", 60)
        # 进入维护需要主管角色
        self.assertEqual(
            d.action("enter_maintenance", 61, actor="op-1", role="operator")["reason"],
            "permission_denied",
        )
        self.assertEqual(d.action("enter_maintenance", 62)["status"], "ok")
        # 维护期间：工艺命令被阻塞
        self.assertEqual(d.command("resume", 63)["reason"], "maintenance_mode")
        # 维护期间：失联监测暂停（2 小时无心跳也不产生联锁）
        d.at(62 + 7200)
        svc.tick()
        view = svc.get_channel_view("cell-3")
        self.assertEqual(view["phase"], "paused")
        self.assertEqual(view["open_interlocks"], [])
        # 退出维护：立即恢复安全评估，失联联锁出现
        self.assertEqual(d.action("exit_maintenance", 62 + 7210)["status"], "ok")
        codes = [i["code"] for i in svc.get_channel_view("cell-3")["open_interlocks"]]
        self.assertIn("sensor_lost", codes)


class TestEmergencyStopIdempotency(unittest.TestCase):
    def test_emergency_stop_executes_once_under_network_retry(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        first = d.command("emergency_stop", 60, key="estop-1")
        self.assertEqual(first["status"], "ok")
        self.assertEqual(svc.get_channel_view("cell-3")["phase"], "locked")
        # 网络重试：同一幂等键
        retry = d.command("emergency_stop", 61, key="estop-1")
        self.assertTrue(retry["deduplicated"])
        # 已锁定后新的急停键也不再产生效果
        again = d.command("emergency_stop", 62, key="estop-2")
        self.assertEqual(again.get("note"), "already_locked")
        view = svc.get_channel_view("cell-3")
        self.assertEqual(len(view["effects"]), 1)  # 电流归零只执行一次
        locked_transitions = [e for e in svc.get_transition_log() if e["to_phase"] == "locked"]
        self.assertEqual(len(locked_transitions), 1)

    def test_duplicate_command_key_returns_cached_result(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        first = d.command("pause", 60, key="pause-1")
        second = d.command("pause", 61, key="pause-1")
        self.assertTrue(second["deduplicated"])
        self.assertEqual(first["status"], second["status"])
        paused = [e for e in svc.get_transition_log() if e["to_phase"] == "paused"]
        self.assertEqual(len(paused), 1)


class TestSimulatedClockTimeouts(unittest.TestCase):
    def test_ramp_timeout(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        svc.register_plan(dict(PLAN))
        d.telemetry("current", 0.0, 1)
        d.telemetry("voltage", 8.0, 2)
        d.telemetry("chloride", 1.5, 3)
        d.telemetry("temperature", 24.0, 4)
        d.heartbeat(4)
        d.command("start", 5)
        # 升流停滞：电流一直不到目标，超过 ramp_timeout_s=120
        for seconds in (30, 60, 90):
            d.heartbeat(seconds)
            d.telemetry("current", 0.5, seconds)
        d.heartbeat(126)
        d.at(126)
        svc.tick()
        view = svc.get_channel_view("cell-3")
        self.assertEqual(view["phase"], "paused")
        self.assertIn("ramp_timeout", [i["code"] for i in view["open_interlocks"]])

    def test_paused_too_long_advisory(self):
        svc, clock, _ = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        d.command("pause", 60)
        d.heartbeat(60 + 1801)
        d.at(60 + 1801)
        svc.tick()
        codes = [i["code"] for i in svc.get_channel_view("cell-3")["open_interlocks"]]
        self.assertIn("paused_too_long", codes)


class TestRecovery(unittest.TestCase):
    def test_recovery_rebuilds_safe_state_before_accepting_commands(self):
        svc, clock, store = make_service()
        d = Driver(svc, clock)
        d.to_steady()
        d.command("emergency_stop", 60, key="estop-1")

        # 进程重启：从日志恢复
        clock2 = ManualClock(T0 + timedelta(seconds=90))
        svc2 = TreatmentMonitorService.recover(DomainConfig(ROOT), clock2, store)
        self.assertEqual(svc2.status, "recovering")
        # 恢复完成前拒绝新命令与新遥测
        self.assertEqual(
            svc2.execute_command({"type": "pause", "channel_id": "cell-3", "actor": "op-1",
                                  "role": "operator", "idempotency_key": "x-1"})["reason"],
            "service_recovering",
        )
        self.assertEqual(svc2.ingest_telemetry({
            "message_id": "m1", "channel_id": "cell-3", "signal": "current", "value": 0.0,
            "unit": "A", "observed_at": clock2.now().isoformat(), "received_at": clock2.now().isoformat(),
        })["reason"], "service_recovering")
        # 安全状态已重建：锁定与急停联锁都在
        view = svc2.get_channel_view("cell-3")
        self.assertEqual(view["phase"], "locked")
        self.assertEqual(view["open_interlocks"][0]["code"], "emergency_stop")
        # 幂等注册表也重建了：重试急停命中缓存
        self.assertEqual(svc2.complete_recovery()["status"], "ok")
        retry = svc2.execute_command({"type": "emergency_stop", "channel_id": "cell-3", "actor": "op-1",
                                      "role": "operator", "idempotency_key": "estop-1"})
        self.assertTrue(retry["deduplicated"])
        self.assertEqual(len(svc2.get_channel_view("cell-3")["effects"]), 1)
        # 恢复后可继续操作
        reset = svc2.execute_action({"type": "supervisor_reset", "channel_id": "cell-3", "actor": "sup-1",
                                     "role": "supervisor", "idempotency_key": "reset-1"})
        self.assertEqual(reset["status"], "ok")
        self.assertEqual(svc2.get_channel_view("cell-3")["phase"], "paused")

    def test_recovery_applies_downtime_sensor_loss(self):
        svc, clock, store = make_service()
        d = Driver(svc, clock)
        d.to_steady()  # 最后心跳 55s
        # 停机 1 小时后恢复：心跳早已失联，complete_recovery 先重建安全状态
        clock2 = ManualClock(T0 + timedelta(hours=1))
        svc2 = TreatmentMonitorService.recover(DomainConfig(ROOT), clock2, store)
        svc2.complete_recovery()
        view = svc2.get_channel_view("cell-3")
        codes = sorted(i["code"] for i in view["open_interlocks"])
        self.assertEqual(codes, ["sensor_lost", "sensor_lost_prolonged"])
        self.assertEqual(view["phase"], "locked")  # 失联超升级阈值，直接锁定

    def test_journal_store_file_roundtrip(self):
        # 真实文件日志：写入 → 崩溃 → 从文件重放
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "journal.jsonl"
            store = JournalStore(path)
            svc, clock, _ = make_service(store=store)
            d = Driver(svc, clock)
            d.to_steady()
            d.command("pause", 60, key="pause-file")
            store.close()

            svc2 = TreatmentMonitorService.recover(
                DomainConfig(ROOT), ManualClock(T0 + timedelta(seconds=70)), JournalStore(path)
            )
            self.assertEqual(svc2.status, "recovering")
            svc2.complete_recovery()
            view = svc2.get_channel_view("cell-3")
            self.assertEqual(view["phase"], "paused")
            # 幂等注册表从文件重建
            retry = svc2.execute_command({"type": "pause", "channel_id": "cell-3", "actor": "op-1",
                                          "role": "operator", "idempotency_key": "pause-file"})
            self.assertTrue(retry["deduplicated"])


class TestFaultSamples(unittest.TestCase):
    def test_all_fault_samples_replay_to_expected_state(self):
        samples = json.loads((ROOT / "domain" / "fault_samples.json").read_text(encoding="utf-8"))
        for scenario in samples["scenarios"]:
            with self.subTest(scenario=scenario["name"]):
                svc, clock, _ = make_service()
                runner = ScenarioRunner(svc, clock)
                results = runner.run(scenario)
                expectations = [r for kind, r in results if kind == "expect"]
                self.assertTrue(expectations, "样例缺少 expect")
                for expectation in expectations:
                    self.assertTrue(expectation["passed"], expectation["mismatches"])

    def test_spike_sample_transition_log_shows_spike_reading(self):
        samples = json.loads((ROOT / "domain" / "fault_samples.json").read_text(encoding="utf-8"))
        scenario = next(s for s in samples["scenarios"] if s["name"] == "overnight_current_spike")
        svc, clock, _ = make_service()
        runner = ScenarioRunner(svc, clock)
        results = runner.run(scenario)
        # 普通确认被拒绝
        ack = next(r for kind, r in results if kind == "action" and r.get("status") == "denied")
        self.assertIn("current_severe", ack["remaining"])
        # 锁定转换的读数窗口包含越限读数
        locked = next(e for e in svc.get_transition_log("cell-3") if e["to_phase"] == "locked")
        self.assertIn("s1-c-008", locked["reading_window"]["message_ids"])
        self.assertEqual(locked["open_interlocks"], ["current_severe"])
        # 越限后回落的读数被接受但阶段不回退
        after = [e for e in svc.get_transition_log("cell-3") if e["from_phase"] == "locked" and e["to_phase"] != "locked"]
        self.assertEqual(after[0]["to_phase"], "paused")  # 只能经主管复位离开锁定

    def test_estop_sample_deduplicates_retry(self):
        samples = json.loads((ROOT / "domain" / "fault_samples.json").read_text(encoding="utf-8"))
        scenario = next(s for s in samples["scenarios"] if s["name"] == "estop_network_retry")
        svc, clock, _ = make_service()
        runner = ScenarioRunner(svc, clock)
        results = runner.run(scenario)
        commands = [r for kind, r in results if kind == "command"]
        estops = commands[-3:]
        self.assertEqual(estops[0]["status"], "ok")
        self.assertTrue(estops[1]["deduplicated"])
        self.assertEqual(estops[2].get("note"), "already_locked")


if __name__ == "__main__":
    unittest.main()
