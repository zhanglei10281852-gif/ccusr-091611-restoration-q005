"""端到端演示：2026-09-15 夜间电流越线事故若由本系统处置会怎样。

运行：python tools/demo_night_incident.py
演示内容：
  1) 升流 -> 稳态的正常流程（模拟时钟推进）；
  2) 电流短时越过安全线后自行恢复：通道锁定，阶段不回退，普通确认无效；
  3) 同键/换键的紧急停机重试：硬帧全程只下发一次；
  4) 模拟进程重启：先重放重建安全状态，命令入口才恢复；
  5) 主管审计报告（读数窗口/联锁原因/人工操作）。
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from service.app import Service
from service.clock import SimClock
from tests.helpers import T0


def row(svc, cid, sig, val, mid, unit):
    ts = svc.clock.now().isoformat()
    return {"message_id": mid, "channel_id": cid, "signal": sig, "value": val,
            "unit": unit, "observed_at": ts, "received_at": ts}


def banner(s):
    print("\n" + "─" * 72 + f"\n{s}\n" + "─" * 72)


def main() -> int:
    td = tempfile.mkdtemp()
    db = str(Path(td) / "demo.db")
    svc = Service(db, clock=SimClock(T0))
    cid = "cell-3"
    svc.open_channel(cid)

    def cmd(c, key, role="operator", badge=None):
        r = svc.command(cid, {"command": c, "idempotency_key": key, "role": role,
                              "badge": badge})
        print(f"  命令 {c:18s} -> {r}")
        return r

    banner("① 预检窗口读数正常，预检并启动升流")
    feed = [("current", 0.10, "A"), ("voltage", 4.0, "V"), ("chloride", 900, "mg/L"),
            ("temperature", 22.0, "C"), ("heartbeat", 1, "status")]
    svc.ingest([row(svc, cid, s, v, f"pc-{s}", u) for s, v, u in feed])
    cmd("precheck", "k-precheck")
    cmd("start", "k-start")

    banner("② 模拟时钟推进 1800s，电流爬升到目标值，自动进入稳态")
    for i in range(7):
        svc.clock.advance(300)
        svc.ingest([
            row(svc, cid, "current", 0.80, f"r-c{i}", "A"),
            row(svc, cid, "voltage", 12, f"r-v{i}", "V"),
            row(svc, cid, "chloride", 600 - i * 20, f"r-l{i}", "mg/L"),
            row(svc, cid, "temperature", 28, f"r-t{i}", "C"),
            row(svc, cid, "heartbeat", 1, f"r-h{i}", "status"),
        ])
    print(f"  当前阶段：{svc.get(cid).s.state}")

    banner("③ 夜间事故重演：电流 0.80 -> 1.24（越线）-> 0.81（自行恢复）")
    svc.ingest([row(svc, cid, "current", 0.80, "night-0", "A")])
    svc.clock.advance(2)
    svc.ingest([row(svc, cid, "current", 1.24, "night-1", "A")])
    svc.clock.advance(2)
    svc.ingest([row(svc, cid, "current", 0.81, "night-2", "A")])
    mon = svc.get(cid)
    print(f"  当前阶段：{mon.s.state}，联锁：{mon.s.interlock}")
    print("  设备帧：", [f["frame"] for f in mon.s.device_frames])

    banner("④ 迟到的“正常”旧读数（乱序遥测）不能把阶段回退")
    old_ts = (svc.clock.now()).isoformat()
    stale = {"message_id": "night-late", "channel_id": cid, "signal": "current",
             "value": 0.50, "unit": "A", "observed_at": old_ts,
             "received_at": svc.clock.now().isoformat()}
    # 构造一条 observed_at 早于高水位的消息
    from datetime import timedelta
    stale["observed_at"] = (svc.clock.now() - timedelta(seconds=10)).isoformat()
    print("  入库结果：", svc.ingest([stale]))
    print(f"  阶段仍为：{mon.s.state}")

    banner("⑤ 普通确认不能解除；主管解锁才开启新回合")
    cmd("acknowledge", "k-ack-op")
    cmd("supervisor_unlock", "k-sup-op", role="operator")  # 角色不足
    cmd("supervisor_unlock", "k-sup", role="supervisor", badge="SUP-007")
    print(f"  当前阶段：{mon.s.state}，回合：{mon.s.round_id}")

    banner("⑥ 紧急停机网络抖动：同键重试 + 换键重试，硬帧每回合仅一次")
    # 重新走一轮到稳态
    svc.ingest([row(svc, cid, s, v, f"pc2-{s}", u) for s, v, u in feed])
    cmd("precheck", "k-pc2"); cmd("start", "k-start2")
    svc.clock.advance(1801)
    svc.ingest([row(svc, cid, "current", 0.80, "r2-c", "A"),
                row(svc, cid, "heartbeat", 1, "r2-h", "status")])
    cmd("emergency_stop", "k-estop")
    cmd("emergency_stop", "k-estop")       # 同键重试
    cmd("emergency_stop", "k-estop-new")   # 换键重试
    hard = [f for f in svc.gateway.sent if f.frame == "FORCE_RELAYS_OPEN"]
    print(f"  FORCE_RELAYS_OPEN 实际下发次数：{len(hard)}（跨两个回合各 1 次）")

    banner("⑦ 进程重启：新 Service 先重放事件流，ready 后才接受命令")
    svc.shutdown()
    svc2 = Service(db, clock=svc.clock)
    mon2 = svc2.get(cid)
    print(f"  重建状态：{mon2.s.state} / {mon2.s.interlock} / round {mon2.s.round_id}")
    print(f"  设备硬闸门随恢复重建：{svc2.gateway.hard_trip_done(cid)}")
    r = svc2.command(cid, {"command": "emergency_stop", "idempotency_key": "k-after-restart"})
    print(f"  重启后再次急停（不产生第二帧）：{r}")

    banner("⑧ 主管审计报告")
    from tools.audit_report import render_text
    print(render_text(cid, mon2.audit(), mon2.s.state))
    svc2.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
