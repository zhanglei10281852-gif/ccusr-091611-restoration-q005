# 青铜器电化学处理安全联锁

该项目定义电化学处理阶段、遥测信号与设备控制命令。安全事件与普通工艺事件使用不同等级，任何命令都带有幂等键，便于在网络重试和进程恢复时识别同一操作。

领域定义保存在 `domain/contract.json`，`examples/telemetry.json` 是包含乱序采集时间的读数资料。运行 `python tools/validate_contract.py` 可确认格式及时间字段。

