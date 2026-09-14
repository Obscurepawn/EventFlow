# 六字段新闻流示例与 README 场景迁移

Status: done
Owner: root
Updated: 2026-09-15

## 用户授权与范围

用户要求将现有 example 的 Schema 改为 Ticker、Timestamp、Headline、Text Body、Sentiment Score、Market Price，并小幅调整为对应的特征计算场景，同步根 README。保留现有独立 HTTP 消息服务、KV、Coordinator、两个 Compute、自动消费、两级 MV、条件/Clock Trigger、固定输入及 SQL 验证。

不新增业务扩展、CEP 序列、NLP 模型、专门故障注入或 Kafka 集成。历史成交示例的验证记录保留原样，不作为新 Schema 通过的证据。当前压缩包是先前快照，本任务不自动重打包。

## 数据与计算约定

- 输入 `news_updates(ticker STRING, timestamp_us INT64, headline STRING, text_body STRING, sentiment_score FLOAT64, market_price FLOAT64)`，全部非空；timestamp_us 为新闻业务时间的 Unix 微秒。
- 分区键 ticker；demo 以 `(ticker, timestamp_us)` 标识新闻，假设同一 ticker 同一微秒对应同一条新闻更新，重复业务键按现有 upsert 语义处理。broker 投递身份仍为 source/topic/partition/offset，与新闻身份区分。
- `news_stats` 维护 total_sentiment、news_count 和 sentiment_price_sum = SUM(sentiment_score * market_price)。`news_features` 维护 news_count、average_sentiment = total_sentiment / CAST(news_count AS FLOAT64)、sentiment_price_sum。
- 静态函数 `calculate_news_impact` 计算 `sentiment_price_sum / news_count`，返回 `impact_score`。它是新闻时点价格与给定情绪的演示指标；不做情绪推断，不把采样价格声称为完整行情。
- `sentiment_threshold` 在新闻事件后检查 news_count >= 2 且 average_sentiment >= 0.5，活动 news_session 中最多受理一次。`periodic_impact` 继续每 200 ms 独立 TryOnceAtFiring。两者都在受理时捕获当前特征。
- 第一 ticker 新闻 `(sentiment, price)` 为 `(0.25,100)`、`(0.75,104)`、`(0.5,112)`，首次阈值 impact=51.5，第三条后周期 impact=53。第二 ticker 为 `(0.5,200)`、`(0.75,208)`，impact=128。使用精确可表示数值，避免时间线验证依赖近似小数。
- 标题/正文原样存储并通过 SQL 断言，Timestamp 是业务数据；Clock 和会话仍按数据库 processing time 推进，不新增 event-time 触发保证。

## 验收与工作状态

- [x] 验证器先对新 Schema/业务结果出现预期 RED，再完成实现和 GREEN。
- [x] 六字段发布、路由、SQL 原样读取，两级 MV、两类请求、固定输入、暂停恢复和 checkpoint 创建均实跑通过。
- [x] Clang Debug 三 sanitizer 与 Release 示例构建/运行；必要的相邻合同及格式/ASCII/noexcept/具名初始化检查通过。
- [x] 根 README 保留 Schema -> case -> 合并 DAG -> Graph-Based 理由 -> code 顺序，示例 README 和当前接入说明同步。
- [x] 单轮并行 fresh-context 语义/工程与文档 reviewer 检查本次完整范围，处置发现并定向复验。

| 工作 | Owner | 状态 |
| --- | --- | --- |
| 当前接口、示例和约束核对 | root | 完成 |
| 验证器 RED / 实现 GREEN | root | 完成预期 RED、接线整改及 Debug/Release GREEN |
| README、DAG 与当前接入资料同步 | root | 完成，当前图、四段 C++ 和全部新例说明已核验 |
| 质量检查及独立审查 | root / reviewers | 完成，同轮双 reviewer 的两项 P3 已定向关闭 |

## Changed files 与下一步

实际修改 examples/streaming_database/{definition.cpp,verify.cpp,message_server.cpp,run.sh,README.md,AGENTS.md}、README.md、docs/{core_delivery.md,implementation_guide.md}、.skills/use-eventflow/references/{recipes.md,cluster-and-operations.md}，以及本规格、INDEX 和两份 NEWS 审查记录。通用 runtime、公共 API 和第三方依赖保持当前接口。

预期 RED：更新验证器后编译通过，现有 streaming_database_example 因 `Relation not found: news_updates` 失败，五个服务正常退出；日志 `/tmp/eventflow-news-red-build.log`、`/tmp/eventflow-news-red-test.log`。随后修改定义和 producer/subscription；首次集成 GREEN 尝试发现消息服务仍注册旧 trades topic，Coordinator 报 HTTP message partition does not exist，已定位并同步 message_server.cpp 的两处分区名。该失败原样保留，后续完整新示例与相邻合同的 GREEN 见下方记录。

## 当前验证与并行工作边界

- Clang Debug 的三个示例目标编译通过，`/tmp/eventflow-news-debug-build.log`；消息服务接线修订后刷新 `/tmp/eventflow-news-debug-message-build.log`。
- `trigger_expression_test`、`typed_schema_test`、`static_function_test`、`computation_test` 四个目标通过（首次五目标运行中的集群目标因旧 topic 失败，失败保留）；日志 `/tmp/eventflow-news-debug-tests.log`。
- 定向修复后 `streaming_database_example` 与 `http_message_source_test` 2/2 通过，30.12 秒，exit 0；日志 `/tmp/eventflow-news-debug-integration.log`。合计六个相关目标完成验证，不声称全量 61 目标回归。
- Release 三个目标及消息服务刷新构建通过，`/tmp/eventflow-news-release-build.log`、`/tmp/eventflow-news-release-message-build.log`；直接复用 Release 二进制运行五进程示例通过，`/tmp/eventflow-news-release-example.log`，工作目录 `/tmp/eventflow-streaming-rBtDV0/`。成功标记为 `news_counts=3,2 threshold_impacts=51.5,128 periodic_impact=53 source_next_offsets=3,2`；五个服务退出码均为 0。
- 当前 flags 为 Debug C++20 + address/undefined/leak + fno-sanitize-recover=all；Release 为 -O3 -DNDEBUG -flto=thin，无 sanitizer/fast-math。
- 全项目 clang-format 21.1.2 检查通过，`/tmp/eventflow-news-format.log`；run.sh 的 bash -n、改动代码 ASCII/无 throw-catch、noexcept/具名初始化核对通过，验证器没有 ingest/pump/drain 调用。
- README 四段 C++ 配上真实 schema/session 上下文后由临时 CMake OBJECT 目标使用 Clang 18.1.3/C++20/三 sanitizer/-Werror 编译通过；日志 `/tmp/eventflow-news-readme-snippets.log`，临时目录 `/tmp/eventflow-news-readme-snippets-anz3tonj/`。不将编译当作 SDK helper 的联网执行。
- 新闻 DAG 使用本地 Mermaid 11.17.2 与 Chromium 渲染，`/tmp/eventflow-case-dag-news.{mmd,svg,png,json}`；实际检查 13 条边、10 个节点且无穿过非端点节点的路径碰撞，root 目视通过。图源 SHA-256 `58afbb21d19310f301c25fb5507fdee1add082b2dcc18d3657284f2e6b4f03c6`。
- `python3 /tmp/eventflow-news-docs-check.py` 检查阅读顺序、JSON、三条 DDL 与源码/两份指南一致、四段 C++ 等于已编译对象、DAG 匹配、旧当前例标识清理、链接/anchor/围栏/ASCII；初轮 9 个 Markdown、217 个本地目标、3 个 anchor、53 个代码围栏通过。日志 `/tmp/eventflow-news-docs-check.log`；`git diff --check` 通过。

`readme-archify-diagrams.md` 是并行任务，负责 README 系统结构段及系统/消息时序图。保留其 INDEX、README 和 docs/image 修改，不将它们纳入本次代码行为交付或覆盖其状态。历史成交规格和审查保持原快照，当前新闻 Schema 以本规格和 live source 为准。

## 审查处置与最终执行证据

- 代码 reviewer 提出最终 message_server.cpp 格式化时间晚于原二进制/运行证据。已刷新 Debug/Release 三个示例目标并各重新运行完整五进程流程；不依赖未保存的格式化前 token 对比。
- 最终 Debug：`/tmp/eventflow-news-final-debug-build.log` 构建 exit 0，`/tmp/eventflow-news-final-debug-example.log` 示例 1/1 通过，28.20 秒；工作目录 `/tmp/eventflow-streaming-VgnCmq/`。
- 最终 Release：`/tmp/eventflow-news-final-release-build.log` 构建 exit 0，`/tmp/eventflow-news-final-release-example.log` 成功，工作目录 `/tmp/eventflow-streaming-x3aVYw/`；两次最终运行都保留六字段 SQL、51.5/128/53、source 3/2、complete checkpoint 和五个 exit 0。
- 文档 reviewer 提出 node_id 只有输出没有断言。已将示例 README 与实现导读改为“断言 shard、打印 node_id”，不新增与需求无关的代码行为。
- 最终消息服务二进制 SHA-256：Debug `de9a82721715ba24b8fd460cd70904a10710a209d337d014e7c141cca2c77ccf`；Release `dbf02754323c227ea5cbf29e3934249b8428f6b7f268b550c83912085275048a`。代码/脚本和全部三程序指纹由独立代码报告记录。

目前五条新闻覆盖所述正常场景；没有独立执行主键更正、非法/NULL 字段、负情绪或“条数已达标但平均情绪未达标”的新闻业务分支。源码及相邻合同只支持相应边界说明，不冒充这些分支的本轮执行记录。

唯一一轮并行 [代码/语义审查](reviews/NEWS-example-code-review.md)与[文档/API 审查](reviews/NEWS-example-docs-review.md)已完成：两项 P3 分别通过最终构建/实跑和文档精确措辞关闭，未发现需要修改业务实现的阻断项。11 个本次 Markdown 的静态检查通过，最终日志见 `/tmp/eventflow-news-docs-check.log`。本次新闻迁移无剩余任务，不再开启新审查轮；并行系统图任务保持自身状态。
