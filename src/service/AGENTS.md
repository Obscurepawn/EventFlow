# KV、Compute 与 Coordinator 服务

本目录继承[根规则](../../AGENTS.md)，维护真实进程之间的 HTTP 边界与一致性协调。

## 入口与职责

- [kv_server.cpp](kv_server.cpp) 暴露 `IStateStore` RPC；[compute_service.cpp](compute_service.cpp) 管理静态 shard owner、执行器和数据面屏障。
- [coordinator_service.cpp](coordinator_service.cpp) 持久管理 Catalog/部署、路由、源前缀、水位、原投递与 checkpoint，并托管 SourcePoller 持续消费；SDK 通过其 `/v1/meta/` 接入。
- 配置接口在 [kv_server.hpp](../../include/eventflow/kv_server.hpp)、[compute_service.hpp](../../include/eventflow/compute_service.hpp)、[coordinator_service.hpp](../../include/eventflow/coordinator_service.hpp)；CLI 实现是 [database_main.cpp](database_main.cpp)，公开入口为 [database_main.hpp](../../include/eventflow/database_main.hpp)。应用主函数传入相同 `ComputationModule`，其 bind 工厂同时用于 Coordinator 与 Compute。

## 必须保持的契约

- 当前部署是 1 Coordinator、1 共享 KV、N Compute；生产 Compute 使用 Remote KV。业务 shard 静态分配，同 shard 单 owner 串行处理，节点级 gate 建立控制屏障。
- 业务 shard 数为 S 时，control shard 为 S，KV 物理 shard 总数为 S+1；路由 hash 仍使用 S，不能混入控制分区。
- Coordinator 是元数据权威，Compute 校验安装版本、Catalog 指纹、全局 shard 数与 assignment；全部确认前不得开放 RUNNING。
- SourcePoller 只在 RUNNING 消费，保持原投递内容/身份/token/处理时间，确认输入后 ACK。暂停与 checkpoint 关闭新输入；恢复按保存源位置 seek，不采用更后的 broker commit 跳过重放。永久失败通过 source_reader/background_status 暴露，不能静默跳过。
- 后台驱动同时负责函数 completion 与 Clock；无输入也需进展。`tick_interval_ms=0` 不会禁用已配置 source 的消费循环，手动时钟测试须明确是否配置 source。
- 当前为启动前 DDL 和静态部署；动态 Job、在线 Schema/图迁移、自动接管属于后续合同，不能从 spec 的概念接口推断已经实现。
- 原投递、身份和分配处理时间在 pending 时保留；未确认新输入阻挡时钟越过该轮，重试使用原内容。Meta/Compute 的 UNKNOWN 都必须先确认原提交。
- 源前缀仅在计算已确认后推进；区分 `input_committed` 与提交后的 `operation_status`，不能把 HTTP 成功等同于输入可 ACK。
- 水位与处理时钟不可回退；新增 partition 和迟到事件不能绕过该规则。它们不能被表述成全局 event-time 排序保证。
- checkpoint 要求 quiesce，冻结源位置、Catalog/runtime、worker revision/receipt 和 control revision，再创建 KV snapshot；冻结中只允许完成原 checkpoint。
- Rocks 恢复要停止全部进程、从 snapshot 创建新活动目录并核验冻结切面；保留 WAL，不能仅给原 live 数据库添加 restore 标志。
- cache 预算按节点分配到 shard，并保持 owner/epoch/confirmed-state 失效和 UNKNOWN 阻挡；执行器完成不能越过 owner 的提交边界。
- Compute/Meta 的 64 位 version/revision/offset/time 使用规范十进制字符串，shard/partition/count 为整数，业务 Row 使用 Value codec。KV RPC 维持独立的整数 revision 编码。
- 请求/响应大小、字段与结构化错误必须验证；`start` 只在监听就绪后成功，`stop` 关闭并 join，避免生命周期竞态。

## 验证入口

- [compute_service_test.cpp](../../tests/compute_service_test.cpp) 覆盖四种 KV 接入和安装/暂停/恢复；[coordinator_service_test.cpp](../../tests/coordinator_service_test.cpp) 覆盖 Meta 状态、源 ACK、水位与 checkpoint。
- [core_cluster_test.cpp](../../tests/core_cluster_test.cpp) 部署真实 Coordinator/KV/多个 Compute，含 Rocks 全进程恢复；[remote_store_test.cpp](../../tests/remote_store_test.cpp) 检查 KV RPC 和启停。
- [sql_module_service_test.cpp](../../tests/sql_module_service_test.cpp) 验证默认 SQL 模块的自动消费、级联 MV 和 checkpoint，以及无函数请求的普通 Trigger。
- 从仓库根目录对已刷新构建的 Debug 测试目录执行：

```sh
ctest --test-dir build --output-on-failure -R '^(compute_service|coordinator_service|core_cluster|remote_store|sql_module_service)_test$'
```

规格：[数据库主动消费](../../specs/streaming-database-example.md)；[核心交付](../../specs/core-delivery.md)；[控制面与集群](../../specs/control-plane.md)；[运行时契约](../../specs/runtime-contracts.md)。
