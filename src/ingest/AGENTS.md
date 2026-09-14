# 上游消息适配

本目录继承[根规则](../../AGENTS.md)，负责 broker-neutral 消息拉取、手动确认、暂停和重放位置。

## 入口与职责

- [message_source.cpp](message_source.cpp) 实现 `MemMockMessageSource`、`HttpMessageSource` 与 `KafkaMessageSource`，公共契约在 [message_source.hpp](../../include/eventflow/message_source.hpp)。
- [http_message_server.cpp](http_message_server.cpp) 提供独立 HTTP 消息设施，有界日志只在该进程存活期间保留。
- [source_poller.cpp](source_poller.cpp) 实现接口无关的持续消费步骤，保留单条投递与固定时间，只有 SourceApplyResult.input_committed 才 ACK；由 Coordinator 串行托管，不由示例客户端驱动。
- `IMessageSource` 仅传递原始 key/payload、来源与位置；Schema 解析、业务去重、计算提交和 checkpoint 协调属于调用方。
- 业务 Schema 解码由 Coordinator 的 SourceSubscription 接入完成；HTTP JSON payload 不等于数据库 ingest envelope。自动接入的 null payload 不自动解释为删除。
- 同一实例的方法由调用方串行使用；Kafka 当前为静态手动 assignment，不代表动态 rebalance 或自动 owner 接管。

## 必须保持的契约

- 适配器的持久投递身份是 `(source_id, topic, partition, offset)`；`instance_id` 和 `delivery_id` 只授权当前实例的 ACK，不能进入持久业务去重身份。
- 只有计算持久提交确认后才能 `ack`。按已交付顺序推进完成前缀，不能越过未完成记录；合法 offset 间隔不等于漏交付。
- Kafka 禁用自动提交与自动 offset store。ACK 返回 `UnknownCommit` 后，先重试同一个 token，再允许该 partition 的其他 ACK/seek。
- `committed_position` 是已确认前缀后的 next offset，`-1` 表示没有已提交位置；重复确认不能倒退它。
- pause 按 partition 生效，仍允许 ACK；seek 撤销旧 inflight token、保持 pause 状态、不修改 broker group commit，回放旧 checkpoint 不得降低已确认位置。
- close 幂等且不确认未完成记录。跨实例、伪造、撤销后的 token 必须按契约拒绝。
- 保留二进制内容以及 null key/tombstone 与空字符串的区别；poll、inflight、单消息和 MemMock 保留日志均有独立有界预算。
- MemMock 日志满时明确拒绝追加，不能驱逐可重放输入；Kafka 超大记录不能静默跳过或确认，必须经显式 seek/close 处理。
- 源 replay 不替代一致 checkpoint 协议。源位置必须与 Catalog/runtime/水位的冻结边界协调。

## 验证入口

- [message_source_test.cpp](../../tests/message_source_test.cpp) 将同一 `MessageSourceContract` 跑在 MemMock 与 librdkafka protocol mock 上，覆盖前缀、ACK 不确定、pause、seek、预算及 token 生命周期。
- [http_message_source_test.cpp](../../tests/http_message_source_test.cpp) 检查独立 HTTP 设施和适配器；[source_poller_test.cpp](../../tests/source_poller_test.cpp) 检查数据库驱动的确认/暂停/保留语义。适用投递契约在替身与真实适配边界共享，不能只测类存在。
- 该 mock 不是实际 Kafka broker 证据。真实 Kafka Compose 集成后置，恢复实施时遵守根规则的 `apache/kafka:4.3.1`、KRaft 和独立集成路径。
- 从仓库根目录对已刷新构建的 Debug 测试目录执行；默认测试不要求启动外部 broker：

```sh
ctest --test-dir build --output-on-failure -R '^(message_source|http_message_source|source_poller)_test$'
```

规格：[数据库主动消费](../../specs/streaming-database-example.md)；[运行时契约](../../specs/runtime-contracts.md)的输入身份与确认章节；[产品范围](../../specs/product-scope.md)；[核心交付](../../specs/core-delivery.md)。
