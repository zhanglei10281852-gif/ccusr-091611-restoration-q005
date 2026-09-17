"""回放故障样例并打印主管视图：状态转换、读数窗口、联锁原因与人工操作。

用法：
    python tools/replay_scenario.py                      # 回放全部样例
    python tools/replay_scenario.py --scenario estop_network_retry
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from service import DomainConfig, ManualClock, MemoryStore, ScenarioRunner, TreatmentMonitorService  # noqa: E402


def replay(scenario: dict) -> TreatmentMonitorService:
    clock = ManualClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    service = TreatmentMonitorService(DomainConfig(ROOT), clock, MemoryStore())
    runner = ScenarioRunner(service, clock)
    results = runner.run(scenario)
    for kind, result in results:
        if kind == "expect":
            status = "通过" if result["passed"] else f"失败: {result['mismatches']}"
            print(f"  [期望校验] {status}")
    return service


def print_supervisor_view(service: TreatmentMonitorService, channel_id: str) -> None:
    view = service.get_channel_view(channel_id)
    print(f"\n== 通道 {channel_id} 最终状态: {view['phase']}  维护模式: {view['maintenance']}")
    for interlock in view["open_interlocks"]:
        print(f"  未解除联锁: {interlock['code']} ({interlock['level']}) {interlock['message']}")
    print(f"  安全动作: {len(view['effects'])} 次")
    for effect in view["effects"]:
        print(f"    - {effect['at']} {effect['type']}={effect['value']} 原因 {effect['reason']}")
    print("\n== 状态转换日志（读数窗口 / 联锁 / 人工操作）")
    for entry in service.get_transition_log(channel_id):
        actor = entry["actor"]
        actor_text = f"{actor['id']}({actor['role']})" if actor else "自动"
        window = entry["reading_window"]
        print(
            f"  #{entry['seq']} {entry['at']}  {entry['from_phase']} → {entry['to_phase']}"
            f"  触发 {entry['trigger']}  操作 {actor_text}"
        )
        print(
            f"      读数窗口 {window['observed_from']} ~ {window['observed_to']}"
            f"  消息 {window['message_ids']}"
        )
        if entry["open_interlocks"]:
            print(f"      联锁: {entry['open_interlocks']}")
        for reason in entry["reasons"]:
            print(f"      原因: {reason}")


def main() -> None:
    parser = argparse.ArgumentParser(description="回放故障样例")
    parser.add_argument("--scenario", help="只回放指定样例")
    args = parser.parse_args()

    samples = json.loads((ROOT / "domain" / "fault_samples.json").read_text(encoding="utf-8"))
    scenarios = samples["scenarios"]
    if args.scenario:
        scenarios = [s for s in scenarios if s["name"] == args.scenario]
        if not scenarios:
            sys.exit(f"未找到样例 {args.scenario}")

    for scenario in scenarios:
        print(f"\n===== 样例 {scenario['name']}: {scenario['description']}")
        service = replay(scenario)
        channel_id = next(
            step["plan"]["channel_id"] for step in scenario["steps"] if "plan" in step
        )
        print_supervisor_view(service, channel_id)


if __name__ == "__main__":
    main()
