# 数据库主动消费示例

继承 [examples 约定](../AGENTS.md)，先读 [README](README.md) 和[当前规格](../../specs/streaming-database-example.md)。

- `definition.cpp` 独立定义成交表、两级 MV、显式 session、阈值 once Trigger 和 200 ms Clock。
- `main.cpp` 将应用静态模块交给通用数据库入口；`message_server.cpp` 是独立消息进程。
- `verify.cpp` 只生产 HTTP 消息、控制部署和查询 SQL。数据库消费、ACK、时钟和函数执行由服务托管。
- 保留两个不同 shard 的 key；同步 README 的输入时间线、242/262/303 评分与固定输入断言。
- 正常验收包含部署前未 ACK、自动消费、无新消息的周期触发、暂停恢复、完整 checkpoint；不增加故障注入。
- `run.sh` 构建三个本例目标并检查所有进程退出码；日志和 RocksDB 数据保留在打印的临时目录。
