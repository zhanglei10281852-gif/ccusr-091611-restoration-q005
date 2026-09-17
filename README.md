# 青铜器电化学处理安全联锁

该项目定义电化学处理阶段、遥测信号与设备控制命令，并实现完整的处理监控服务：接收电源与溶液传感器事件，按器物级方案驱动预检、升流、稳态、暂停、冲洗、结束各阶段，为操作员提供只能由有效状态触发的控制动作。安全事件与普通工艺事件使用不同等级，任何命令都带有幂等键，便于在网络重试和进程恢复时识别同一操作。

## 仓库布局

- `domain/contract.json` — 阶段、信号、命令、联锁等级的领域契约
- `domain/device_protocol.json` — 设备协议：通道、信号单位、心跳参数、命令幂等规则
- `domain/thresholds.json` — 阈值曲线：各阶段 warn/alarm/severe 限值与联锁等级映射
- `domain/calibration.json` — 传感器校准记录（增益/偏移/有效期）
- `domain/fault_samples.json` — 故障样例：夜间越流、传感器失联、急停重试、维护窗口
- `examples/telemetry.json` — 包含乱序采集时间的读数资料
- `examples/treatment_plan.json` — 器物级处理方案样例
- `service/` — 监控服务实现（见下）
- `tests/` — 单元与场景测试
- `tools/validate_contract.py` — 校验契约与全部领域数据格式
- `tools/replay_scenario.py` — 回放故障样例并打印主管视图

## 服务设计（service/）

- `monitor.py` — `TreatmentMonitorService`：遥测摄入、阶段机、联锁策略、命令、审计、恢复
- `telemetry.py` — 遥测门槛：按 `message_id` 去重、按 `(通道, 信号)` 的 `observed_at` 高水位隔离乱序
- `rules.py` — 阈值曲线评估（升流曲线 / 目标带偏差 / 上限）
- `channel.py` — 单通道运行时状态
- `clock.py` — 可注入时钟：`SystemClock` 与 `ManualClock`（超时测试用模拟时钟推进）
- `store.py` — JSONL 写前日志（`JournalStore`，写盘后 fsync）与内存存储
- `scenario.py` — 故障样例回放器

### 关键安全语义

- **阶段不回退**：乱序（`observed_at` 不晚于高水位）与重复（同 `message_id`）遥测被隔离记录，绝不进入控制逻辑；阶段机只前进。
- **联锁四级**：`advisory`（提示，读数恢复正常后自动解除）→ `operator_reset`（自动保护性暂停，操作员确认可解除）→ `supervisor_reset` / `emergency_lock`（锁定通道，**普通确认不能解除**，需主管复位）。
- **失联 vs 维护**：传感器失联（心跳超时）是非计划事件——自动保护性暂停并升级锁定；维护模式是计划事件——主管进入后暂停失联监测、阻塞工艺命令，退出时立即恢复安全评估且维护期间的静默不计入失联升级。
- **急停幂等**：`emergency_stop` 携带幂等键，网络重试命中缓存只执行一次；已锁定后新的急停键也不再产生重复效果。
- **审计**：每次状态转换记录读数窗口（消息 ID、观测时间范围、各信号最新值）、联锁原因与人工操作（操作人/角色/幂等键），`get_transition_log()` 供主管回看。
- **恢复**：`TreatmentMonitorService.recover()` 重放日志重建状态，期间拒绝新命令与新遥测；`complete_recovery()` 先按当前时钟评估超时（如停机期间的心跳失联）重建安全状态，再接受新命令。

## 运行

```bash
python tools/validate_contract.py            # 校验领域数据
python -m unittest discover -s tests         # 全部测试（23 项）
python tools/replay_scenario.py              # 回放全部故障样例
python tools/replay_scenario.py --scenario overnight_current_spike
```

## 使用示例

```python
from pathlib import Path
from datetime import datetime, timezone
from service import DomainConfig, ManualClock, JournalStore, TreatmentMonitorService

config = DomainConfig(Path("."))
clock = ManualClock(datetime(2026, 9, 15, 21, 0, tzinfo=timezone.utc))
store = JournalStore("var/journal.jsonl")          # 崩溃后可据此恢复
svc = TreatmentMonitorService(config, clock, store)

svc.register_plan({...})                            # 器物级方案（examples/treatment_plan.json）
svc.ingest_telemetry({...})                         # 电源/溶液传感器事件
svc.execute_command({"type": "start", "channel_id": "cell-3",
                     "actor": "op-1", "role": "operator", "idempotency_key": "k-1"})
svc.tick()                                          # 时钟推进，评估超时
svc.get_transition_log("cell-3")                    # 主管回看：读数窗口/联锁/人工操作

# 进程重启后：
svc2 = TreatmentMonitorService.recover(config, clock, store)   # 状态 recovering，拒绝新命令
svc2.complete_recovery()                                       # 先重建安全状态，再接受命令
```
