# HTTP API 参考

所有请求/响应均为 JSON；写操作需要 `Content-Type: application/json`。
时间字段一律 ISO 8601 带时区（如 `2026-09-15T21:00:02+08:00`）。

## 启动与就绪

进程启动后先重放事件日志重建所有通道的安全状态；未就绪时所有 POST 返回 `503`：

```json
{"error": "service recovering, safety state not rebuilt yet"}
```

`GET /health` — 健康/就绪检查

```json
{"ready": true, "channels": ["cell-3"], "time": "2026-09-15T13:00:00+00:00"}
```

`POST /admin/recover` — 手动触发一次全量重放（一般不需要）。

## 通道与快照

### `POST /channels`
```json
{"channel_id": "channel-7"}
```
开启一个新处理通道，进入 `prepared`。已存在则 `409`。

### `GET /channels/{channel_id}`
返回当前安全快照：

```json
{
  "channel_id": "cell-3",
  "state": "steady",
  "mode": "normal",
  "interlock": null,
  "round_id": 1,
  "hard_tripped": false,
  "entered_at": "2026-09-15T21:30:00+08:00",
  "last_contact": "2026-09-15T21:35:00+08:00",
  "high_water": "2026-09-15T21:35:00+08:00",
  "precheck_passed_at": "2026-09-15T21:00:00+08:00",
  "latest": {"current": {"value": 0.78848, "observed_at": "...", "calibration_id": "cal-2026-09-01-psu"}},
  "resume": {"allowed": true},
  "completion": [true, "满足结束条件"]
}
```

### `GET /channels/{channel_id}/audit`
仅追加事件流的投影：每次状态转换（含**触发时的读数窗口**）、联锁升降级（含原因、
是否仍活动、解除人）、全部人工命令（接受/拒绝及理由）、实际下发的设备帧。

## 遥测

### `POST /telemetry`
批量上送，支持按通道分组处理。每条记录字段同契约：

```json
[
  {"message_id":"msg-81","channel_id":"cell-3","signal":"current","value":0.42,
   "unit":"A","observed_at":"2026-09-15T21:00:02+08:00","received_at":"2026-09-15T21:00:03+08:00"}
]
```

处理规则：
- **去重**：`message_id` 已消费过 → `accepted:false, reason:"duplicate"`；
- **乱序**：`observed_at` 早于通道高水位 → `reason:"stale"`，阶段**不回退**；
- **校准**：先按通道/信号/时间查生效校准记录换算工程值，再进入阈值评估；
- 接受的读数更新高水位、最近联系时间与读窗口，随后触发阈值与阶段评估。

响应：`202 {"results":[{"message_id":"msg-81","accepted":true,"value":0.413612,"calibration_id":"..."}]}`

## 命令

`POST /channels/{channel_id}/commands`，统一信封：

```json
{"command": "pause", "role": "operator", "idempotency_key": "uuid-或-业务键", "badge": null}
```

- **必须带 idempotency_key**（契约要求）；同键重放带 `"replayed": true`，不重复执行。
- 状态门控失败返回 HTTP 200 + `{"accepted": false, "reason": "..."}`（语义化拒绝，
  便于操作员终端展示理由），不会改变任何状态。
- `role` 默认 `operator`；`supervisor_unlock` 要求 `role:"supervisor"` + `badge`。

| 命令 | 合法状态 | 作用 |
|---|---|---|
| `precheck` | prepared | 验证预检窗口五类读数齐全且在窗内，登记预检通过时间 |
| `start` | prepared（预检宽限期内） | ENABLE_OUTPUT → ramping |
| `pause` | ramping, steady | SUSPEND_OUTPUT → paused，记住返回阶段 |
| `resume` | paused（无活动联锁、非维护模式、读数窗口新鲜且健康） | 回 ramping/steady |
| `acknowledge` | advisory / operator_reset | 清除联锁；operator_reset 要求读数已恢复正常 |
| `supervisor_unlock` | locked + supervisor 角色 | 清除 supervisor_reset，round+1，gateway 闸门复位 → prepared |
| `enter_maintenance` | paused, prepared | 进入维护模式：不发带电帧、失联不联锁但仍记记录 |
| `exit_maintenance` | 维护中且无活动联锁 | 重新预检通过后才能退出 |
| `emergency_stop` | 非 completed | FORCE_RELAYS_OPEN 每回合物理上只发一次 → locked/emergency_lock |
| `begin_rinse` | steady | 校验完成条件（稳态满 24h、完成窗口氯离子 ≤50mg/L）→ rinsing |
| `finish` | completed（no-op）或 locked+hard_tripped | 异常结束 |

## 错误码

| HTTP | 含义 |
|---|---|
| 200/201/202 | 正常（命令是否被接受以体内 `accepted` 为准） |
| 400 | 请求体或参数错误 |
| 404 | 通道不存在（遥测中未知通道在 results 里标记，不影响其他通道） |
| 409 | 通道已存在 |
| 503 | 正在恢复，安全状态尚未重建 |
