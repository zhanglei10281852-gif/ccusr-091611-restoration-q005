# 青铜器电化学处理安全联锁监控服务

针对电化学除氯处理的安全监控服务：接收电源与溶液传感器遥测，按器物级方案驱动
**预检 → 升流 → 稳态 → 暂停 → 冲洗 → 结束** 全流程，为操作员提供只能由有效状态
触发的控制动作，并为主管保留每次状态转换的完整审计轨迹。

针对夜间记录暴露的问题——"电流短时越过安全线、设备自行恢复、无人确认风险、继续运行还是
停机只靠口头判断"——本系统把以下规则固化为不可绕过的状态机：

- **乱序/重复遥测不回退处理阶段**：以 `observed_at` 为处理顺序，高水位之前的消息丢弃；
  `message_id` 去重表持久化，重复消息绝不产生第二次效果。
- **严重越限锁定通道**：电流瞬时越过重置限值即升为 `supervisor_reset` 并 `FORCE_RELAYS_OPEN`，
  即使读数随后自行恢复也不回到稳态；普通操作员确认无法解除，仅主管解锁。
- **传感器失联与维护模式采用不同安全策略**：带电状态下失联自动软暂停并升联锁；维护模式下
  失联只记录、不联锁、不下发任何帧，但阈值越限仍然联锁。
- **紧急停机只执行一次**：同键重放返回首次结果；换键重试命中设备网关的"每回合一帧"硬闸门；
  进程重启后闸门随事件流重建，网络抖动不可能导致第二次硬停机。
- **恢复先于命令**：进程启动先重放事件日志重建全部通道的安全状态，`ready` 后才开放命令入口。

## 目录结构

```
domain/                     # 可从仓库读取的领域资料
  contract.json             # 状态/遥测/命令/联锁等级契约
  device_protocol.json      # 电源与循环泵逻辑帧、定序规则、安全模式
  recipes.json              # 器物级方案：预检条件、阈值曲线、超时、联锁/维护策略
  calibrations.json         # 传感器校准记录（增益/偏置/有效期）
  faults.json               # 故障样例与期望处置（验收测试据此重放）
examples/telemetry.json     # 乱序采集时间的读数样例
service/
  clock.py                  # RealClock / SimClock（模拟时钟推进触发超时）
  calibration.py            # 按通道/信号/时间应用校准
  store.py                  # SQLite 事件库（去重表 + 幂等命令表）
  device.py                 # 设备网关：硬停机帧每回合仅一次的闸门
  monitor.py                # 核心：通道状态机、阈值评估、联锁升级、命令门控
  recovery.py               # 事件流重放，重建安全状态与设备闸门
  app.py                    # HTTP 服务（标准库，启动即恢复，未就绪返回 503）
tests/                      # 22 个测试：安全不变量、故障样例、HTTP、全流程
tools/
  validate_contract.py      # 契约/协议/方案/校准/故障样例自洽性校验
  run_tests.py              # 零依赖测试运行器
  audit_report.py           # 主管审计报告（读数窗口/联锁原因/人工操作）
  demo_night_incident.py    # 夜间事故端到端处置演示
```

## 快速开始

需要 Python 3.11+，无第三方依赖。

```bash
python tools/validate_contract.py     # 校验领域资料
python tools/run_tests.py             # 运行全部 22 个测试
python tools/demo_night_incident.py   # 观看夜间事故在本系统下的处置
python -m service.app --port 8080     # 启动 HTTP 服务（默认 data/monitor.db）
```

## 一次完整处理

```
POST /channels                      {"channel_id":"cell-3"}              # 开通道（prepared）
POST /telemetry                     [预检窗口五类读数]
POST /channels/cell-3/commands      {"command":"precheck","idempotency_key":"k1"}
POST /channels/cell-3/commands      {"command":"start","idempotency_key":"k2"}   # → ramping
# 持续遥测；升流 1800s 达标自动 → steady；稳态满 24h 且氯离子窗口达标后：
POST /channels/cell-3/commands      {"command":"begin_rinse",...}        # → rinsing
# 冲洗 3600s 自动 → completed
GET  /channels/cell-3               # 当前快照（阶段/联锁/最新读数/可否恢复）
GET  /channels/cell-3/audit         # 全部状态转换、联锁、人工操作、设备帧
```

命令只能在有效状态触发（如未预检不能 `start`、`locked` 下不能 `pause`、`completed` 后
急停被拒）；非法触发返回结构化拒绝 `{"accepted": false, "reason": ...}` 而非崩溃。
所有命令必须带 `idempotency_key`，同键重放永远返回首次结果。角色字段 `role` 取
`operator` / `supervisor`，主管解锁还需 `badge`。

## 联锁等级（只升不降，由对应角色动作解除）

| 等级 | 触发 | 处置 | 解除 |
|---|---|---|---|
| advisory | 单点轻度越限（warn） | 记录通知，状态不变 | 操作员 acknowledge |
| operator_reset | 电压/温度明显越限；带电失联；升流超时 | 自动 SUSPEND_OUTPUT → paused | 读数恢复正常后操作员确认 |
| supervisor_reset | **电流瞬时越过重置限值**（夜间事故情形）；重复 operator_reset | FORCE_RELAYS_OPEN → locked | 仅主管 supervisor_unlock，开启新处理回合 |
| emergency_lock | 电流/电压/温度达极端限值；操作员急停；极端失联 | 永久闭锁本回合并硬断开，禁止再合闸 | 不可解除，只能结束回合 |

## 关键设计：事件溯源

所有 `StateTransitioned / InterlockRaised / InterlockCleared / DeviceFrameSent /
OperatorAction / TelemetryAccepted|Dropped / Maintenance*` 先写 SQLite 仅追加日志，
内存投影随时可由事件流完整重建。每次状态转换事件都附带当时的**读数窗口快照**
（校准后工程值、原始示值、观测/接收时间、校准记录号），配合联锁原因与人工操作，
构成主管可回看的完整证据链。审计报告：

```bash
python tools/audit_report.py --db data/monitor.db            # 文本
python tools/audit_report.py --db data/monitor.db --json     # JSON
```

接口字段与 HTTP 语义见 [docs/api.md](docs/api.md)。
