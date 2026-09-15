# 静态模块流式示例

继承根目录与 examples 约定。此目录保留原静态 ComputationModule、typed capture 与 Function 的完整五进程示例；主要默认入口已迁移到 ../streaming_database 的 SDK 上传版。

- definition.cpp/main.cpp 定义并注入静态业务模块；verify.cpp 只生产消息、控制部署和查询。
- 消息服务源码共用 ../streaming_database/message_server.cpp。
- 保留 51.5/128/53、固定输入、Clock 和正常 checkpoint 断言；不能宣称本静态脚本验证了进程重建。
- 变更记录与验证见 specs/streaming-udf-default.md。
