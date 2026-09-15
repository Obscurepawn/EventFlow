# 公共头文件协作约定

本目录继承[根约定](../../AGENTS.md)，面向库使用者和所有实现模块。先读 [specs 索引](../../specs/INDEX.md)，新增或改变公开行为时同步对应实现、调用方、示例与规格。

## 接口分组

| 入口 | 对应职责 |
| --- | --- |
| `status.hpp`、`types.hpp`、`key_codec.hpp` | 错误、业务值、稳定身份与编码 |
| `state_store.hpp`、`cached_store.hpp`、`kv_protocol.hpp` | 状态存储、缓存、远程协议 |
| `sql_ast.hpp`、`catalog.hpp`、`catalog_codec.hpp` | SQL、绑定结果与单一 Catalog |
| `engine.hpp`、`event_processor.hpp`、`fsm.hpp`、`cep.hpp` | 原子增量轮与有界触发状态机 |
| `trigger*.hpp`、`typed_schema.hpp`、`typed_trigger.hpp`、`computation.hpp` | 声明、类型适配与启动绑定 |
| `static_function.hpp`、`function_executor.hpp`、`request*.hpp` | 静态函数、固定输入请求与执行器 |
| `message_source.hpp`、`source_poller.hpp`、`http_message_server.hpp`、`source_identity.hpp`、`time.hpp` | MemMock/HTTP/Kafka 投递、服务托管消费、消息设施、身份和时间 |
| `computation_runtime.hpp`、`database_main.hpp` | 静态 ComputationModule 工厂、通用运行时与应用服务入口 |
| `observation*.hpp` | 声明读集、同轮观察和预算基础 |
| `*_service.hpp`、`kv_server.hpp`、`cluster_client.hpp` | 服务边界与 SDK |

## 契约

- 生产原生函数入口为 `udf.hpp`；`submitted_computation.hpp` 提供可通过 SDK 提交的有限定义。最新授权见 [生产 UDF 规格](../../specs/native-udf-delivery.md)：服务启动后的首次部署，受信任 `.so` 使用受控 C++ ABI，Compute 原生线程池执行；不代表运行中图迁移或独立 Worker 已完成。

- 项目函数和模板/lambda 遵循根目录 `noexcept`、命名与具名 aggregate 初始化规则。保持可失败操作返回 `Status`/`StatusOr<T>`，先检查状态再读取值；`StatusOr::value()` 不是业务错误恢复入口。
- 保持接口自包含；公开头文件不能包含 `src/` 私有头、GoogleTest 或暴露第三方实现细节。第三方类型若已是公开契约，改动必须同步所有调用方，不能只改声明。
- 外部依赖保留接口与 deterministic simple adapter。接口修改同时检查 Memory/Rocks/Remote、MemMock/HTTP/Kafka、Simple/ThreadPool 等适用实现和共享合同测试。
- 关系、请求、投递、attempt、owner/epoch 与 revision 的身份域不同；不要复用整数或字符串字段来暗中改变身份语义。
- `ComputationModule.bind` 提供完整静态定义；同一应用的所有计算服务使用稳定模块身份和相同定义。通用服务/运行时不依赖示例或测试业务图，SDK deploy 不上传回调。
- `SourceSubscription` 将 IMessageSource 注入 Coordinator；SourcePoller 的投递确认与模型完成分开，UNKNOWN 不得更换投递/token 或越过确认前缀。
- `TriggerSet::bind` 返回 `BoundComputation`，Catalog/Registry/definition 必须完整校验后一起发布。强类型 Schema 校验不能替代实际 Row 的运行时校验。
- [Trigger API 规格](../../specs/trigger-api-ergonomics.md)文末最新授权覆盖 typed Pattern、静态 `Function`、显式 session 及两类 capture。静态编译回调不代表动态 UDF/UDTF/UDAF 产品已交付；通用观察 host 也不能因接口类型存在就宣称接通。
- 公共声明变更需说明版本、fingerprint、持久化及恢复兼容影响；不能从函数地址推断稳定 implementation identity。

## 验证

从对应 `src/<module>/AGENTS.md` 选择合同测试；模板与强类型 API 额外检查 `typed_schema_test`、`static_function_test`、`typed_trigger_test`、`typed_input_domain_test` 及调用它们的示例。构建流程见[测试约定](../../tests/AGENTS.md)。检查真实调用方的编译和行为，不用仅有头文件语法检查代替接口兼容验证。
