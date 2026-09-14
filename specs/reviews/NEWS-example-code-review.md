# 新闻示例迁移：独立代码与语义审查

Reviewer: news_code_review（fresh context）
Date: 2026-09-15
Scope: `news-streaming-example.md` 的本次新闻迁移；本轮双 reviewer 中的代码/语义 challenger。
Base commit: `6a0b00fd88ece3bd6668c6e119118f8ecd49704e`，检查对象为其上的当前工作树。

## 结论与范围

未发现需要修改新闻业务实现的 P0/P1/P2 问题。六字段输入、两层 MV、声明式条件/Clock、静态影响指标和现有五进程部署符合用户要求。唯一 P3 证据绑定发现已通过刷新构建及实跑关闭，原发现和处置保留如下；本轮没有未处置的审查问题。

已读取根、examples、streaming_database、specs 的 AGENTS，`specs/INDEX.md`、[新闻合同](../news-streaming-example.md)和[原集群示例合同](../streaming-database-example.md)。审查四个代码/脚本改动、示例 README 和根 README 新闻部分，并沿现有源码核对 SQL 聚合、typed capture、消息解析和 ACK 边界。未修改实现；本 reviewer 只新增本报告。并行 README 系统结构/archify 图任务不属于此结论。

## 原发现与处置

### R1 / P3：最终格式化文件与原运行二进制之间需要补足证据绑定

初查时 `message_server.cpp` 修改时间为 02:52:46，Debug/Release 消息二进制分别为 02:50:37、02:50:41，原集成及 Release 运行也早于该修改。当前 diff 显示 topic 正确，实际运行日志也明确处理 `news_updates`；没有发现旧 topic 缺陷仍存在。但只凭时间顺序，不能把原运行结果绑定为当前文件字节的构建结果。

实施者确认最后变化仅为 clang-format 排版，并重新构建 Debug/Release 三个示例目标、各运行一次五进程流程，不依赖未保存的格式化前 token 对比。本 reviewer 已读取 `/tmp/eventflow-news-final-{debug,release}-{build,example}.log` 和最终 CTest 详细输出：构建成功，Debug 目标 1/1 通过，Release 成功标记、完整 checkpoint 和五服务 exit=0 齐全。刷新后的两个消息二进制修改时间均晚于最终源码，SHA-256 与刷新前相同；四个被审代码/脚本指纹未变。R1 已关闭，无需新增业务用例或重新发起 fresh-review 轮次。

## 要求、实现与证据核对

| 要求/边界 | 当前实现与审查结果 |
| --- | --- |
| 六个新闻字段 | `definition.cpp:82-92` 定义 ticker、timestamp_us、headline、text_body、sentiment_score、market_price，六列全部 NOT NULL；`verify.cpp:69-88` 发布同名 JSON 字段，`verify.cpp:220-237` 无序比较完整六字段 SQL 行。运行输出保留正文引号/换行和微秒尾数。 |
| 新闻身份与源身份 | 业务主键为 `(ticker,timestamp_us)`，ticker 分片；README 明示同 ticker 同微秒代表同条新闻的假设。源身份仍为 source/topic/partition/offset，未用 broker offset 替代业务主键。现有 Engine 的 `put_row` 和聚合变更链使用旧/新行回撤与加入，因此重复业务键是 upsert，不是无条件累计一条新闻。 |
| 时间单位 | payload 保留完整 Unix 微秒；producer 的 envelope 将其除以 1000 作为毫秒，Coordinator 再乘 1000 作为源 event time。Clock/session 使用数据库 processing time。日志中微秒尾数保存在 SQL，而 checkpoint 的 max_event_us 是整毫秒，正好对应此区别；不提供新的 event-time 精度或窗口保证。 |
| 两级 MV 与数值 | `news_stats` 计算 COUNT、SUM(sentiment) 和 SUM(sentiment * price)；`news_features` 显式把 COUNT 转 FLOAT64 后求平均。第一组乘积合计 103、第二组 256、追加后 159；影响指标分别为 51.5、128、53。选定输入及预期输出可被二进制浮点精确表示，当前等值断言合理。 |
| nullable 与失败语义 | SQL binder 把 COUNT 标成非空、SUM 标成可空；`CurrentNewsFeatures` 的两个 optional 对应该合同。`capture_news_features` 检查正 count 和两列非空，再生成非空快照；`calculate_news_impact` 检查正分母及有限结果。typed binding/解码检查字段类型、可空性和非有限值，SQL 算术也拒绝除零及非有限结果。 |
| 阈值与 once | `definition.cpp:141-159` 明确 `news_count >= 2 && average_sentiment >= 0.5`、活动 session、Wait、once。第二 ticker 首条 sentiment=0.5 时仍不能提前受理；最终 capture 必须为 count=2。追加后 `verify_threshold` 仍断言仅一条结果，完整捕获行及 request_id 与追加前一致。 |
| Clock 与固定输入 | `definition.cpp:161-180` 设置每 ticker 200 ms、TryOnceAtFiring、不设置 once；两类规则都 capture_current。`verify.cpp:345-363` 在没有新输入时等待新的成功请求身份，并检查源位置未动；之后周期指标为 53，而旧阈值保持 51.5。代码及 README 明示过期时隙可合并、200 ms 不是完成 SLA。 |
| 三端源配置 | producer 的 news_feed/news_updates、`message_server.cpp:27-33` 的两个 news_updates partition、`run.sh:126-128` 的 source_id/topic/relation 完全一致。原旧 topic 接线失败已留档，修订后的 Debug/Release 输出均成功。 |
| 真实服务负责推进 | `run.sh:110-134` 启动独立消息服务、KV、两个 Compute 和 Coordinator；producer/SQL verifier 只发布、查询及部署控制。SourcePoller 在 input_committed 后才 ACK。验证器未调用 ingest/pump/drain；返回 shard 与预选的两个业务键对应。 |
| 暂停、checkpoint、范围 | 追加期间暂停的源确认位置和 MV 不变；恢复后自动消费。实际日志包含 complete=true、两个 Compute 及源位置 3/2 的 checkpoint。验证器只显式断言 complete，不把打印的 checkpoint 摘要说成逐字段断言，更不把创建成功当成全进程恢复通过。 |
| 工程约束 | 当前四个代码/脚本文件 ASCII、无 throw/catch 业务控制流；新增函数为 noexcept，业务聚合结构使用具名字段。共享静态模块仍经同一 Catalog/TriggerSet 绑定；没有修改通用 runtime、公有 API、第三方库或新增业务扩展。 |

实现证据另见 `src/sql/binder.cpp:186-217`、`src/sql/expression.cpp:138-154`、`src/runtime/typed_schema.cpp:27-105`、`src/runtime/engine.cpp:145-195`、`src/runtime/engine.cpp:529-561`、`src/runtime/engine.cpp:1204-1256`、`src/service/coordinator_service.cpp:170-208`、`src/ingest/source_poller.cpp:46-96`。

## 实际检查与执行证据

本 reviewer 独立检查源码、工作树、配置、构建 flags、日志全文/关键断言和修改时间，并执行四文件 ASCII/throw-catch 静态检查、`bash -n examples/streaming_database/run.sh` 及相关文件 `git diff --check`，均通过。没有自行重跑已有业务测试，以下执行结果来自实施者提供且本 reviewer 实际读取的本次日志：

- `/tmp/eventflow-news-red-test.log`：新验证器对旧定义失败于 `Relation not found: news_updates`，属于预期行为 RED；不是编译失败。
- `/tmp/eventflow-news-debug-tests.log`：四个相邻目标 trigger_expression_test、typed_schema_test、static_function_test、computation_test 通过；当次示例因消息服务旧 topic 失败，失败记录保留。
- `/tmp/eventflow-news-debug-integration.log` 与 `build/Testing/Temporary/LastTest.log`：接线修订后 http_message_source_test 和 streaming_database_example 两目标通过；前者含 MemMock/HTTP 的 8 个共享合同用例，后者包含五个独立服务、实际 SQL、Clock、pause/resume 和 checkpoint 输出。示例工作目录 `/tmp/eventflow-streaming-5xWr8V/`，五服务 exit=0。
- `/tmp/eventflow-news-release-example.log`：工作目录 `/tmp/eventflow-streaming-rBtDV0/`，两 ticker、全部原始字段、两层 MV、冻结请求、周期结果、源位置、checkpoint 及五服务 exit=0 均可核对。
- Debug/Release 的 `CMakeFiles/ef_streaming_definition.dir/flags.make` 和数据库目标 flags 使用 `/usr/bin/clang++`、C++20；Debug 含 address/undefined/leak 和不可恢复诊断，Release 含 -O3、NDEBUG、thin LTO，无 sanitizer/fast-math。
- 最终 Debug：`/tmp/eventflow-news-final-debug-build.log` 三目标刷新成功；`/tmp/eventflow-news-final-debug-example.log` 的 streaming_database_example 1/1 通过，28.20 秒。`build/Testing/Temporary/LastTest.log` 记录工作目录 `/tmp/eventflow-streaming-VgnCmq/`、51.5/128/53、源位置 3/2、complete=true checkpoint 和五服务 exit=0。
- 最终 Release：`/tmp/eventflow-news-final-release-build.log` 三目标刷新成功；`/tmp/eventflow-news-final-release-example.log` 记录工作目录 `/tmp/eventflow-streaming-x3aVYw/`，对应业务成功标记、complete=true checkpoint 和五服务 exit=0。最终构建/运行日志及 Release 五服务日志中未发现 warning/error/sanitizer 失败诊断。

六个相关目标通过是本次范围的回归证据，不代表重新执行全产品测试；旧 61 目标结果不用于本次结论。

## 审查对象指纹

| 文件 | SHA-256 |
| --- | --- |
| examples/streaming_database/definition.cpp | `d984ecedf6805f02c511cab76fb34daa07da5044917761fda1b39711b5a5af96` |
| examples/streaming_database/verify.cpp | `e7007ec851671d9800f515d4716f2aa905c55f3a2b3fa4a9a971ba1e411f9066` |
| examples/streaming_database/message_server.cpp | `10d750ceede3e44ccfebcadc604d3b82c7f6cf98aba0a3fa73d7d03f091a9848` |
| examples/streaming_database/run.sh | `905d7a7c02c17391b460867f01bfb368f10c20cc7a1b5d2304fd5ff4cf4d1626` |

最终执行二进制指纹如下；与上述最终构建/日志及源码指纹共同绑定，不从旧测试数量推断当前验证状态。

| 二进制 | SHA-256 |
| --- | --- |
| build/bin/eventflow_streaming_database | `87fdee86370ebecd3259f5f55a2cef18c246a4342f96172fedf5bda1eae63cc6` |
| build/bin/eventflow_streaming_verify | `876c2beede776acbfb4e6a5a74df6f8ac935db5de49b6c0edacfaf8e875ba99a` |
| build/bin/eventflow_message_server | `de9a82721715ba24b8fd460cd70904a10710a209d337d014e7c141cca2c77ccf` |
| build-release/bin/eventflow_streaming_database | `4d0594110a840c820bba52480823514f078cc270c3c8f47eee3d66c4dc8ca04f` |
| build-release/bin/eventflow_streaming_verify | `b7e024b6513733a728e79e55b04f1018cf92412ec68fd9318d0c771a28a72ac4` |
| build-release/bin/eventflow_message_server | `dbf02754323c227ea5cbf29e3934249b8428f6b7f268b550c83912085275048a` |

## 未验证边界

- 本新闻端到端流程是五条正常有效输入，没有独立的新闻主键更正、NULL/非法字段、负情绪或“至少两条但平均情绪低于 0.5”的业务分支执行证据。源码和相邻合同支持当前声明，但不能把五条正常输入说成这些分支已经实跑。
- 无窗口/TTL，当前 count 是表内新闻行数；没有生产级 session 换代、长时间容量或精度压力验证。输入 identity 假设已明确，不声称它适合任意上游新闻供应商。
- 无新增故障注入、HTTP 消息进程持久化、真实 Kafka、全进程重建恢复、HA、NLP 或动态 UDF 家族验收；这些不成为本次用户“小改 example”的完成门槛。
- 图形布局及 README 读者体验由同轮另一 reviewer 检查；并行 archify 系统图不包含在本报告的审查对象中。
