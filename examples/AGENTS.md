# 示例开发约定

本目录继承[根约定](../AGENTS.md)。当前唯一完整示例是 [streaming_database](streaming_database/README.md)；修改前读取其 [AGENTS.md](streaming_database/AGENTS.md)和[当前规格](../specs/streaming-database-example.md)。公开 API 以当前头文件为准。

## 示例职责

- `definition.cpp` 独立定义业务表、级联 MV、显式 session、声明式 Trigger 和静态 Function，返回 `ComputationModule`；不能依赖测试 fixture 或在生产通用 runtime 中硬编码示例图。
- `main.cpp` 将模块注入 `database_main`。KV、Coordinator 和至少两个 Compute 为真实独立进程，所有计算角色使用同一静态定义。
- `message_server.cpp` 提供独立 HTTP 消息设施。producer 只 publish 原消息，Coordinator 托管 SourcePoller 主动消费；`verify.cpp` 只生产、部署控制和 SQL/状态验证，不调用 ingest/pump/drain 代替数据库运行。
- 明确区分消息 partition、业务 key 和计算 shard；验证覆盖不同 shard 的输入、级联 MV、阈值 once、固定输入和无新消息的 Clock 请求。
- HTTP 与 Kafka 都通过 `IMessageSource` 接入同一 SourcePoller；HTTP 服务保留有界内存日志，不能写成真实 Kafka 或跨进程持久消息系统。真实 Kafka Compose 后置；本轮不新增专门故障注入测试。

## 修改与验证

- README 使用中文，定义与断言有清楚的英文注释；所有代码、日志、代码围栏及命令使用英文 ASCII，aggregate 使用指定字段。
- `run.sh` 从仓库根目录直接调用，支持构建目录参数或 `EVENTFLOW_BUILD_DIR`；新目录使用 Clang、Debug、Unix Makefiles，已有目录保留构建类型。只构建本例所需目标，不下载依赖。
- `EVENTFLOW_SKIP_BUILD=1` 仅复用已构建程序；运行包无 CMakeLists 时使用 `bin/`，保留三个二进制覆盖变量。
- 启动脚本检查监听就绪、客户端及全部子进程退出状态；保留 sanitizer 失败诊断并打印配置、日志、数据目录。
- 修改行为时同步时间线、依赖图、精确 SQL 断言与能力边界。一次正常 checkpoint 创建不等于全进程恢复验收；旧核心/旧示例结果不验证本轮代码。
- 文档改动检查路径、anchor 和围栏；代码改动运行相关合同与真实多进程示例，证据由[当前规格](../specs/streaming-database-example.md)记录，最终按根约定完成独立审查。
