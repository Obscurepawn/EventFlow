# 新闻示例的独立读者文档与 API 审查

Reviewer: news_docs_review（独立 fresh-context reviewer）

Date: 2026-09-15

Status: 本轮审查完成；一项措辞发现已定向修复并核对关闭，无本范围未解决阻断项。

## 范围与依据

本报告属于[六字段新闻流示例](../news-streaming-example.md)要求的唯一一轮并行独立审查。核对根、examples、示例目录和 specs 的 AGENTS，以及最新 Trigger 接口合同。实施者负责产品改动；本 reviewer 仅写本报告，没有修改生产、示例或读者文档，也没有另开审查轮次。

审查对象为[根 README](../../README.md)的新闻 Schema、业务 case、合并 DAG、Graph-Based 说明和使用代码，[示例 README](../../examples/streaming_database/README.md)、[运行手册](../../docs/core_delivery.md)、[实现导读](../../docs/implementation_guide.md)中的当前 demo 段落，以及 [.skills recipes](../../.skills/use-eventflow/references/recipes.md) 和 [cluster-and-operations](../../.skills/use-eventflow/references/cluster-and-operations.md)的当前示例引用。

逐项对照 [definition.cpp](../../examples/streaming_database/definition.cpp)、[verify.cpp](../../examples/streaming_database/verify.cpp)、[message_server.cpp](../../examples/streaming_database/message_server.cpp)、[run.sh](../../examples/streaming_database/run.sh)，并检查 `trigger.hpp`、`cluster_client.hpp` 和 `trigger_audit.cpp` 的当前接口/字段。历史成交 spec/review，以及实现导读中的独立 SQL 教学例，允许保留原业务。

并行的 `readme-archify-diagrams.md` 所负责的 README 系统结构段、系统架构图、消息时序图和 `docs/image/` 产物不在本报告范围；不把它们的编辑或验证归入本轮新闻示例结果。

## 原发现、处置与定向复核

| ID | 原发现 | 处置与复核 |
| --- | --- | --- |
| NEWS-DOC-1 / P3 | 示例 README 原文“检查返回 shard/node_id”以及实现导读轨迹中的“核对实际 node/shard”容易被读为两者都有自动断言。实际 `verify_features` 仅断言 `features.shard == expected_shard`（verify.cpp:214-215）；`display` 打印 `node_id`（:149）。 | 已向实施者报告。实施者将示例 README 改为“断言返回的 shard 并打印 node_id”，实现导读改为“断言 shard、打印 node_id，并核对请求身份”。本 reviewer 重新读取两处文本并执行字面断言，确认与源码一致，关闭。没有为措辞问题增加代码断言。 |

该发现涉及验证范围的精确表达，不改变六字段迁移、数值或运行行为。原发现保留，修复后不启动第二轮 fresh review。

## 需求、实现与读者解释的核对

| 要求 | 当前证据与结论 |
| --- | --- |
| 六字段完整 | `news_updates` 恰有六个非空可见字段：ticker/String、timestamp_us/Int64、headline/String、text_body/String、sentiment_score/Float64、market_price/Float64。DDL、JSON producer、SQL 六字段比对与根/示例 README 对应一致。 |
| 新闻身份与时间 | 文档明确 `(ticker, timestamp_us)` 为 demo 的新闻身份，并声明同 ticker 同微秒对应同一新闻、同键输入按 upsert 更新；ticker 是分区键。payload 保留 Unix 微秒，消息 envelope 使用其毫秒部分，Clock/session 用 processing time。没有将新闻时间字段写成 event-time 触发保证。 |
| 文本与指标边界 | producer 包含带引号、换行的完整正文；验证器与运行输出保存并读回全部字段，微秒尾数仍在。情绪来自上游预赋值，价格是新闻时点采样，未声称 NLP 推断、连续行情或实际交易策略。 |
| 两级 MV 与类型 | `news_updates -> news_stats -> news_features` 与三份完整 DDL 一致。逐条累加 `sentiment_score * market_price` 后求平均；`COUNT(*)` 为 Int64，第二层显式 `CAST(news_count AS FLOAT64)`，可空聚合列映射为 optional，capture 检查正条数及可用值。没有把影响指标误写为平均情绪乘以某个最新价格。 |
| 两条独立 Trigger | `sentiment_threshold` 使用新闻事件和 `news_count >= 2 && average_sentiment >= 0.5`，活动 `news_session` 中 accepted-once；`periodic_impact` 每 200 ms 使用 `TryOnceAtFiring`，不依赖阈值分支。两者调用同一个 `calculate_news_impact`，各自拥有请求、固定输入和结果。 |
| 受理与 once | README 区分触发、受理和成功，说明 `capture_current` 固定受理时当前 MV，等待后受理可能看到后续输入；once 在成功受理时消耗，执行失败不会归还。示例先等第二条后的请求受理/完成才追加第三条，时间线假设与验证器一致。 |
| 精确数值 | 独立从实际 producer 输出重算：第一 ticker 前两条 `(25 + 78) / 2 = 51.5`；另一 ticker `(100 + 156) / 2 = 128`；第三条后 `(25 + 78 + 56) / 3 = 53`。源码断言和所有当前示例说明一致。 |
| 接入身份与 SQL | `streaming_news` 模块、`news_feed` source、`news_updates` relation/topic，以及 partition 0/1 在定义、producer、消息服务、运行脚本、实际生成配置和手册中一致。SQL 关系/列、Trigger rule 名及 SDK 方法均存在；生成结果名从 explain/Catalog 获取。 |
| 审计与 API 片段 | 四段根 README C++ 与带真实定义上下文的已编译源码完全相等。`scope_key_0` 及示例选择的审计字段存在于实际内置 Schema。文本区分审计 occurrence、已受理请求和成功函数结果，并注明单 key 查询、行/字节预算和排序边界。 |
| 阅读顺序与图 | 根 README 保持 Schema -> case -> 同图特征 DAG 与 Trigger 目标 -> Graph-Based 理由 -> code。图中两条 Trigger 分支均连接到各自 capture 和函数结果，标明独立、受理读取和固定输入；正文明确当前 demo 使用条件/Clock，CEP 序列只用于解释后续建模能力。 |
| 交付边界 | 文档没有将本轮例子等同于真实 Kafka、全进程恢复、动态 UDF/UDTF/UDAF、Process Function 或全产品验收；明确 HTTP 有界内存消息日志、有限运行、会话/保留策略及正常 checkpoint 创建的边界。 |

## 实际验证与证据

- 本 reviewer 重跑 `python3 /tmp/eventflow-news-docs-check.py`：初轮 9 个 Markdown、217 个本地目标、3 个 anchor、53 个 ASCII 代码围栏通过；阅读顺序、JSON、三条 DDL 与源码/两份指南相等、四段 C++ 与已编译对象源码相等、图源相等及旧当前例标识清理均通过。两份 NEWS review 落盘后再次重跑，11 个 Markdown、230 个本地目标、3 个 anchor、53 个 ASCII 围栏通过，exit 0。
- `bash -n examples/streaming_database/run.sh` 和 `git diff --check` 返回 0。README C++ 原编译日志 `/tmp/eventflow-news-readme-snippets.log` 显示 Clang 18.1.3 编译成功；临时 CMake 配置确为 C++20、三 sanitizer、`-Werror`，本 reviewer 再次 `cmake --build /tmp/eventflow-news-readme-snippets-anz3tonj/build --target news_readme --parallel 2` 返回 0（当前对象无需重新编译）。这证明片段在所述完整上下文中编译，不代表独立运行了 README SDK helper。
- 使用 `view_image` 目视检查 `/tmp/eventflow-case-dag-news.png`，完整六字段、两个分支、目标公式和边标签可读，无可见节点遮挡。独立解析实际 SVG 的 DOM 得到 10 个节点和 13 条边，与 `/tmp/eventflow-case-dag-news.json` 的 `checked_nodes=10`、`checked_edges=13` 相等；路径采样报告的碰撞列表为空。图源与当前 README 围栏逐字相等，SHA-256 为 `58afbb21d19310f301c25fb5507fdee1add082b2dcc18d3657284f2e6b4f03c6`。
- 首次 reviewer SVG 数量探针使用严格 XML 解析，遇到 Mermaid 内嵌 HTML 标签而失败；改为 HTML DOM 解析后得到上述实际节点/边计数。该探针错误不是运行时或图像渲染失败，也未据此修改图。
- 检查初轮 `/tmp/eventflow-news-debug-integration.log`，`streaming_database_example` 与 `http_message_source_test` 2/2 通过；另读 Release 五进程完整输出，并独立解析其 producer/SQL 行、重算结果和核对源配置。
- 在实施者刷新格式化后的最终源码构建/运行后，检查 `/tmp/eventflow-news-final-debug-build.log` 和 `/tmp/eventflow-news-final-release-build.log`：三个示例目标均完成，未见 warning/error 行。最终 `/tmp/eventflow-news-final-debug-example.log` 显示 `streaming_database_example` 1/1 通过，28.20 秒。
- 对最终 `/tmp/eventflow-news-final-release-example.log` 再次运行独立解析断言：五条六字段新闻均出现在 SQL 回读中；引号、换行和微秒尾数保留；重算结果为 51.5/128/53；有无新增消息的周期请求及暂停期间保持数据/ACK 的记录；checkpoint `complete=true`、两个 Compute、源位置 3/2；五个服务均 `exit=0`。实际目录 `/tmp/eventflow-streaming-x3aVYw/` 的 source 配置为 `news_feed/news_updates` 两分区，服务日志未见 sanitizer 诊断。构建/系统运行由实施者发起，本 reviewer 检查当前完整结果和执行独立证据断言，没有将日志检查称为自己重跑完整集群。

本次核对时的新闻实现 SHA-256：

| 文件 | SHA-256 |
| --- | --- |
| `examples/streaming_database/definition.cpp` | `d984ecedf6805f02c511cab76fb34daa07da5044917761fda1b39711b5a5af96` |
| `examples/streaming_database/verify.cpp` | `e7007ec851671d9800f515d4716f2aa905c55f3a2b3fa4a9a971ba1e411f9066` |
| `examples/streaming_database/message_server.cpp` | `10d750ceede3e44ccfebcadc604d3b82c7f6cf98aba0a3fa73d7d03f091a9848` |
| `examples/streaming_database/run.sh` | `905d7a7c02c17391b460867f01bfb368f10c20cc7a1b5d2304fd5ff4cf4d1626` |

## 未验证项与结论边界

本 reviewer 未重跑全量 61 个 CTest，不引用旧成交快照为当前新闻实现的回归证据；未执行真实 Kafka、故障注入、进程重建恢复、重新打包或 Docker 镜像更新。相同新闻主键的更新语义来自当前数据模型及既有引擎合同，本例五条独立新闻的运行没有新增重复业务键探针；文档将该主键选择写为 demo 假设，没有宣称本轮专门测试了所有 upsert 边界。

未联网复核沿用的 RisingWave/Flink 参考页，也未审查并行系统/消息图任务。审计 SQL 与 SDK helper 的字段/API经过源码和编译核对，本 reviewer 没有另起集群执行这些新增查询；五进程 verifier 的 SQL 行为以本轮运行日志和断言为证。

在上述边界内，六字段新闻迁移、当前读者说明、计算图和实际示例结果一致；唯一文档发现已关闭。是否完成主规格的其余验收，由主任务合并同轮语义/工程 reviewer 与最终验证记录决定。
