# EventFlow 文档

EventFlow 用表和级联物化视图维护持续到来的数据，用声明式 Trigger 决定何时受理计算请求，并把特征、请求和成功结果保存在数据库中供 SQL 查询。Catalog 统一记录这些关系与计算定义的依赖。

第一次使用从[仓库 README](../README.md)和[流式数据库示例](../examples/streaming_database/README.md)开始。在仓库根目录运行：

```sh
./examples/streaming_database/run.sh
```

完整示例包含独立 HTTP 消息服务、一个 KV、一个 Coordinator 和两个 Compute。服务启动后，用户通过 SDK 上传 `.so`，提交表、级联 MV 和触发规则，再调用 deploy；客户端向消息服务生产数据，数据库内的 SourcePoller 持续拉取并驱动计算，客户端通过 SQL 检查结果。追加输入后仍由数据库自动推进，无需客户端调用 ingest、pump 或 drain。

## 按阅读目的选择入口

| 你想了解什么 | 阅读入口 |
| --- | --- |
| 从定义业务逻辑到消息生产、数据库自动计算和 SQL 断言的完整流程 | [流式数据库示例](../examples/streaming_database/README.md) |
| 环境要求、依赖、构建命令和整体使用方式 | [仓库 README](../README.md) |
| 在 Docker 中准备源码与工具链，再手动编译和运行 | [Docker 使用说明](../docker/README.md) |
| 委托 Coding Agent 实现自己的计算逻辑 | [EventFlow 使用 skill](../.skills/use-eventflow/SKILL.md) |
| 进程部署、运行控制和一致 checkpoint | [运行手册](core_delivery.md) |
| 沿源码理解 SQL、视图更新、Trigger、请求、存储与恢复 | [详细架构、接口语义与实现导读](implementation_guide.md) |
| 统一 Catalog、计算与存储分离、分片 owner 和输入一致性 | [产品形态与系统架构](feature_computation_architecture.md) |
| 流处理概念、开源框架对照与设计依据 | [流式计算与数据库调研](research.md) |

## 阅读示例时关注什么

**计算定义**说明字段、特征依赖、触发条件和函数输入；**运行配置**说明节点、路由、预算与消息源地址。示例通过 SDK 上传函数和提交计算规则，Compute 从共享 KV 读取 `.so` 并加载。目前只支持首次部署前提交，还不能修改运行中的计算图。

**持续消费**由 Coordinator 托管的 SourcePoller 完成。HttpMessageSource 从独立 HTTP 服务读取消息，输入持久提交得到确认后才 ACK。HTTP 服务是本地消息设施替身；真实 Kafka broker 的集成与验证另有范围，不能从这个示例推导。

**查询结果**与**触发发生**是不同的状态。MV 保存持续维护的关系结果，Trigger 创建请求并固定输入，后台函数完成后才有对应的成功结果；trigger_audit 可以查询触发历史，审计行本身不代表函数成功。查询不会替数据库推进计算。

**恢复时**需要恢复数据处理进度、计算定义、运行状态和 UDF 文件。示例会从 checkpoint 重启数据库，再输入新数据验证计算结果；客户端不需要再次上传 `.so`。

开发约束、接口契约、当前任务、后续范围和独立审查位于 [specs/](../specs/INDEX.md)。历史构建与测试结果只覆盖对应快照；当前实现与示例的验证应查看本轮记录。已支持原生 C++ UDF 上传和首次部署；完整 UDTF/UDAF、Process Function、运行中改图和 HA 暂未实现。
