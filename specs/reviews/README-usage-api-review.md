# README 使用示例的独立 API 与语义审查

Reviewer: readme_api_review（独立 fresh-context reviewer）
Date: 2026-09-15
Status: 本轮审查完成；一项 P3 文字问题已修复并定向核对，无未处置的阻断项

## 范围与结论

按根 AGENTS、specs/AGENTS、[本次规格](../readme-usage-examples.md)及原始用户需求，独立审查 README 的物化视图定义、声明式 Trigger、触发审计 SQL/SDK 片段和注释，并对追加的架构图标题删除要求作目视对照。只写本报告，没有修改 README、图片或代码，也没有启动其他 reviewer。

已读取 [Trigger API 最新实施记录](../trigger-api-ergonomics.md)、[审计合同](../trigger-audit-table.md)、[流式数据库示例合同](../streaming-database-example.md)、当前 diff、公开头文件、示例定义/验证器和对应运行时实现。本轮文档满足三项使用说明要求；图中大标题已删除。原发现如下，已由实施者修正。本结论仅针对本次读者文档及图片，不重新宣布产品里程碑或全量系统验收通过。

## 原发现与处置

### R1 / P3：把 StatusOr 与错误码混称

- 原 README 第 1.1 节称 `EF_ASSIGN_OR_RETURN` 失败时“返回错误码(StatusOr<T>)”，容易使读者混淆错误状态、错误码和结果容器。
- 源码证据：[status.hpp](../../include/eventflow/status.hpp) 第 147–151 行：宏失败分支返回表达式的 `.status()`；工厂的 `StatusOr<BoundComputation>` 承载该错误状态。
- 建议：明确失败时提前返回 `Status`，成功时取出结果。
- 处置：实施者已改为“失败时提前返回错误状态 `Status`（由工厂的 `StatusOr<T>` 承载）”。本 reviewer 重新读取当前 README 第 88 行及上述宏实现，确认整改完成。无代码行为修改，无需重跑系统测试。

## 原始要求与接口核对

| 核对项 | 结论及源码证据 |
| --- | --- |
| 物化视图 DDL 与按 key 增量含义 | README 第 1.1 节展示输入表、聚合 MV 和级联投影 MV；三条 DDL 与 [definition.cpp](../../examples/streaming_database/definition.cpp) 第 75–97 行去除空白差异后完全一致。`(ticker, trade_id)` 区分成交，`ticker` 是分区键；现文明确两个不同 trade_id 对应两笔成交，`COUNT(*)` 的笔数解释正确。 |
| 声明式 Trigger 与必要上下文 | README 第 1.2 节使用现有 `EventRef::source`、`Trigger::on_event/clock`、`ViewRef`、`Function`、`capture_current` 和 `request`，与 [trigger.hpp](../../include/eventflow/trigger.hpp) 第 19–38、74–107、125–133 行对应。三个工厂内片段明确复用同一上下文，省略的 SchemaTraits、投影函数、评分函数和 candidate 初始化都有真实 definition.cpp 入口，没有伪装为可单独编译的完整程序。 |
| session 不隐式创建、续期 | [definition.cpp](../../examples/streaming_database/definition.cpp) 第 102–127 行定义 candidate 关系并在首条成交激活；[event_processor.cpp](../../src/runtime/event_processor.cpp) 第 919–927 行在发出请求 occurrence 前检查活动会话、deadline 和 once 状态。README 明示 `.in_session(...)` 只引用已配置会话，与实现一致。示例会话使用最大 Int64 deadline，实际业务换代仍由完整 candidate policy 决定。 |
| guard、capture 与 lifetime 时点 | README 没有把 `>= 100` 误写成 crossing；`capture_current` 明确读受理时 MV。[request_runtime.cpp](../../src/runtime/request_runtime.cpp) 第 821–856 行读确认版本并记录 capture/admission revision，deadline 为 `now_us + lifetime`。固定输入和 242/303 的数字解释对应 [verify.cpp](../../examples/streaming_database/verify.cpp) 第 203–218、252–277、300–305 行的断言。 |
| once、Wait、TryOnce 与 Clock | [event_processor.cpp](../../src/runtime/event_processor.cpp) 第 928–983 行保留 pending intent，第 3577–3598 行区分延期和当次 TryOnce，第 3687–3714 行在受理提交伴随状态中消费 once。第 2393–2408 行为观察到的 key 建立周期，第 2548–2568 行合并延迟时隙。README 的表格区分周期安排、会话允许请求及异步完成，没有承诺 200 ms 完成 SLA；受理失败不消费 once、受理后计算失败不归还的表述正确。 |
| 统一 bind 与审计自动安装 | [computation.cpp](../../src/runtime/computation.cpp) 第 456–505 行生成 request binding，自动安装审计关系，公布 scope 映射，验证整套定义后返回 BoundComputation。README 第 1.3 节说明启动时校验而非 C++ 编译期执行，没有引入另一份计算图或声称动态 UDF 已交付。 |
| SQL 字段、scope 和空值 | 第 5.1 节覆盖当前特征、全部触发、阈值触发和周期触发；所有投影列与 [trigger_audit.cpp](../../src/runtime/trigger_audit.cpp) 第 10–44 行的内置 schema 一致。`scope_key_i` 的 explain 映射对应 computation.cpp 第 483–495 行。source/Clock 和无 request 的 NULL 字段对应 event_processor.cpp 第 2148–2185 行。 |
| 审计不是计算成功凭证 | README 清楚区分 emitted、pending、admitted、success，并说明 `read_request` 可为 NotFound。[event_processor.cpp](../../src/runtime/event_processor.cpp) 第 2221–2223 行先决定是否保留 occurrence；第 2137–2192 行写出审计身份与关联 intent。[trigger_audit_request_test.cpp](../../tests/trigger_audit_request_test.cpp) 第 229–278、281–318 行分别覆盖 Wait 延迟受理和 TryOnce 跳过后保留审计。 |
| SDK helper 可调用性与行读取 | `ClusterClient::open/query/read_request` 签名与 [cluster_client.hpp](../../include/eventflow/cluster_client.hpp) 第 40–62 行一致。[cluster_client.cpp](../../src/client/cluster_client.cpp) 第 252–286 行保证 fields/rows 宽度一致；第 512–530 行提交显式 scope 和预算；[engine.cpp](../../src/runtime/engine.cpp) 第 1787–1826 行限制同一分区并拒绝冲突 scope。helper 不拼接业务 key；当前单 string ticker 也满足 `read_request` 的限制。 |
| 有界历史与排序 | 默认 128 行、256 KiB 来自 [trigger_audit.hpp](../../include/eventflow/trigger_audit.hpp) 第 10–14 行；字节含 KV key 和编码行，淘汰按评估时间及 occurrence ID 由 trigger_audit.cpp 第 103–114、137–192 行实现。README 使用“超过限制”并说明无时间排序保证、outbox ACK 不删历史，未声称无限保存。 |

## 实际验证证据

- 本 reviewer 重新比较了当前 README 的三条 DDL，均与示例完全一致；检查新增章节本地链接无缺失，新增导航所用 `5.1 SELECT 当前特征和触发记录` 标题存在。README 四个 C++ 片段全部为 ASCII，`git diff --check` 通过。
- 独立读取实施者本轮 `/tmp/eventflow-readme-usage-build.log`、`/tmp/eventflow-readme-usage-snippets.log`、`/tmp/eventflow-readme-usage-ctest.log` 的完整输出。日志显示相关 Clang Debug 目标构建完成，片段 OBJECT 编译完成，`trigger_audit_test`、`trigger_audit_request_test`、`streaming_database_example` 共 3/3 通过，CTest 总耗时 23.99 秒。
- 检查片段临时 CMakeLists，确实使用 Clang、C++20、`-Wall -Wextra -Werror`、Address/Undefined/Leak sanitizer 和禁止 sanitizer 恢复的选项。去除空白后，当前 README 四个 C++ 块都完整存在于 `/tmp/eventflow-readme-usage-snippets-0ksivn16/snippets.cpp`，因此后续中文解释修订没有使编译证据对应另一套 C++ 片段。
- 以上构建/CTest 是对实施者当前日志与对象的独立审阅，本 reviewer 没有重复执行构建或 CTest，也没有把计划命令当作运行结果。检查现有测试内容确认审计测试覆盖查询、保留、重复源、UNKNOWN 及请求状态语义；五进程示例主要验证 MV 和评分，不直接执行新增 `print_trigger_audit` helper。

## 图片与最终对象

已用图像查看工具分别读取 `/tmp/eventflow-readme-architecture-before.png` 与最终 `docs/image/eventflow_streaming_architecture.png`。原图顶部“数据库主动Poll消息系统中的消息”大标题在新图中消失；Producer、Message Delivery、计算定义、SQL Client、Coordinator/Meta、SourcePoller/IMessageSource、两个 Compute、共享 KV、图例及对应箭头均保留，没有观察到业务内容删除。README 仍使用居中的 HTML 段落和 `width="720"`，点击仍指向原资源路径。

原图为 1448 × 1086，新图为 1447 × 1087；本报告确认视觉内容保持，不声称逐像素相同。最终图片 SHA-256：`3616a0d78af18700d39582b9a20b59fd6597bd6a10efebcc93d464c3ab4e4ed5`。本次复核 README SHA-256：`6c183da45acff026e8a31392e4863e0220718029d414591b8adba5b3be41ccfc`。

用户同步删减的旧 README 章节和无窗口/TTL 句保持其当前状态；本 reviewer 未恢复它们，也未将这类删除列为本次缺陷。

## 未验证边界

没有新增代码或 API，没有运行完整回归、Release、Docker、真实 Kafka、HTTP 故障注入或新恢复探针。新增 helper 的证据是编译及现有查询实现/合同，未直接联网运行；独立 SQL 文本的证据是当前 DDL/schema、binder/query 实现和已有同类审计用例。`read_request` 目前仅接受一个 string 业务 key，本例满足；审计 SQL 支持复合 scope 不意味着该 SDK 方法也支持任意复合 key。无全局跨 key 查询、长期审计归档或动态用户扩展的新增交付声明。
