"""主管审计报告：列出每个通道每次状态转换所用的读数窗口、联锁原因与人工操作。

用法：
  python tools/audit_report.py --db data/monitor.db
  python tools/audit_report.py --db data/monitor.db --channel cell-3 --json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from service.app import Service
from service.clock import RealClock


def render_text(channel_id: str, audit: dict, latest_state: str) -> str:
    lines = [f"通道 {channel_id}（当前阶段：{latest_state}，处理回合：{audit['round_id']}）",
             "=" * 72]
    lines.append("一、状态转换与所用读数窗口")
    for tr in audit["transitions"]:
        lines.append(f"  [{tr['at']}] {tr['from']} -> {tr['to']}  "
                     f"触发={tr['trigger']}  原因={tr['reason']}")
        win = tr.get("reading_window")
        if win and win.get("signals"):
            for sig, rows in win["signals"].items():
                vals = ", ".join(f"{r['value']:g}@{r['observed_at'][11:19]}" for r in rows)
                lines.append(f"      读数窗口[{sig}] {vals}")
    lines.append("")
    lines.append("二、联锁记录（等级、原因、是否仍活动、解除人）")
    if not audit["interlocks"]:
        lines.append("  （无）")
    for lk in audit["interlocks"]:
        status = "活动" if lk["active"] else f"已解除 by {lk.get('cleared_by')}"
        lines.append(f"  [{lk['at']}] {lk['level']}  {status}")
        for r in lk["reasons"]:
            lines.append(f"      原因：{r}")
    lines.append("")
    lines.append("三、人工操作")
    if not audit["operator_actions"]:
        lines.append("  （无）")
    for a in audit["operator_actions"]:
        ok = "接受" if a["accepted"] else f"拒绝（{a['reason']}）"
        lines.append(f"  [{a['at']}] {a['role']:10s} {a['command']:18s} {ok}")
    lines.append("")
    lines.append("四、下发设备帧")
    for f in audit["device_frames"]:
        lines.append(f"  [{f['at']}] {f['frame']:18s} 诱因={f['cause']}")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="电化学处理安全审计报告")
    ap.add_argument("--db", default=str(Path(__file__).resolve().parents[1] / "data" / "monitor.db"))
    ap.add_argument("--channel", default=None)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if not Path(args.db).exists():
        print(f"事件库不存在：{args.db}", file=sys.stderr)
        return 2
    svc = Service(args.db, clock=RealClock())
    cids = [args.channel] if args.channel else list(svc.channels)
    if args.json:
        out = {cid: {"snapshot": svc.get(cid).snapshot(), "audit": svc.get(cid).audit()}
               for cid in cids}
        print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
        return 0
    for cid in cids:
        mon = svc.get(cid)
        print(render_text(cid, mon.audit(), mon.s.state))
        print()
    svc.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
