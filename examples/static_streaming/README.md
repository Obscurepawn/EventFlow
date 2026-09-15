# 静态模块：从消息到 SQL：数据库主动消费的完整示例

这个例子从头定义新闻业务，启动独立 HTTP 消息服务、共享 RocksDB KV、一个 Coordinator/Meta 和两个 Compute 进程，再由 producer 写消息、用 SQL 等待和断言计算结果。数据库自己读取消息、维护级联 MV、推进 Clock 并执行静态函数。

入口是 [run.sh](run.sh)，业务定义集中在 [definition.cpp](definition.cpp)，生产和验证流程集中在 [verify.cpp](verify.cpp)。数据库入口与验证器来自本目录；消息设施共用 [message_server.cpp](../streaming_database/message_server.cpp)。

## 运行

在仓库根目录执行：

```bash
bash examples/static_streaming/run.sh
```

新构建目录使用 Clang、Debug 和 Unix Makefiles；已有目录保留原构建类型。脚本只构建 `eventflow_static_streaming_database`、`eventflow_message_server`、`eventflow_static_streaming_verify`，不下载第三方依赖。也可以指定构建目录或复用现有二进制：

```bash
bash examples/static_streaming/run.sh build-debug
EVENTFLOW_BUILD_DIR="$PWD/build-debug" EVENTFLOW_SKIP_BUILD=1 \
  bash examples/static_streaming/run.sh
```

运行包中没有 `CMakeLists.txt` 时，脚本直接使用包内 `bin/`。`EVENTFLOW_DATABASE_BINARY`、`EVENTFLOW_MESSAGE_BINARY` 和 `EVENTFLOW_DEMO_BINARY` 可以分别覆盖数据库、消息服务和验证客户端路径。

所有监听端口动态分配。脚本打印服务地址和临时工作目录，保留 JSON 配置、每个进程的日志、`verify.log` 和 `kv-data/`。正常验收结束后回收全部子进程；任一客户端、子进程或 sanitizer 返回失败，脚本都返回非零。最终成功标记形如：

```text
STREAMING_DATABASE_OK shards=2 news_counts=3,2 threshold_impacts=51.5,128 periodic_impact=53 source_next_offsets=3,2
```

## 先看进程和数据路径

```text
verify.cpp producer -- HTTP publish --> message_server.cpp
                                             |
                                   HTTP poll / position / ack
                                             |
                                  Coordinator / SourcePoller
                                      /              \
                                 Compute A         Compute B
                                  shard 0           shard 1
                                      \              /
                                       shared Rocks KV

verify.cpp SQL client -- ClusterClient --> Coordinator --> owning Compute
```

消息服务保留两个 `news_updates` partition 的有界内存日志。`publish` 只追加原始消息；数据库根据部署配置把消息 JSON 解码为 `news_updates` 行，经 Coordinator 路由到相应 Compute。业务 key 的分片通过 `shard_for` 计算，验证程序选取两个分别落在 shard 0、1 的 ticker；消息 partition 和数据库 shard 是两个独立概念，本例为了便于阅读将它们一一配对。

Compute 在共享 KV 上确认业务输入后，数据库才向消息服务 ACK。源位置指向下一条待处理 offset，所以两个 partition 各处理两条后位置都是 `2`。时钟触发不增加源 offset。

`verify.cpp` 没有调用数据库的 `ingest`、`pump` 或 `drain`。它的等待循环只查询 SQL 或消息源确认位置；`tick_interval_ms=20`、`source.poll_interval_ms=10` 配置的是数据库内部驱动周期。

## 独立定义表和级联 MV

`definition.cpp` 的工厂新建 Catalog，定义以下依赖链：

```sql
CREATE TABLE news_updates (
      ticker STRING NOT NULL,
      timestamp_us INT64 NOT NULL,
      headline STRING NOT NULL,
      text_body STRING NOT NULL,
      sentiment_score FLOAT64 NOT NULL,
      market_price FLOAT64 NOT NULL,
      PRIMARY KEY (ticker, timestamp_us)
    ) PARTITION BY (ticker);

CREATE MATERIALIZED VIEW news_stats AS
      SELECT ticker, COUNT(*) AS news_count, SUM(sentiment_score) AS total_sentiment,
             SUM(sentiment_score * market_price) AS sentiment_price_sum
      FROM news_updates GROUP BY ticker;

CREATE MATERIALIZED VIEW news_features AS
      SELECT ticker, news_count,
             total_sentiment / CAST(news_count AS FLOAT64) AS average_sentiment,
             sentiment_price_sum
      FROM news_stats;
```

输入包含 Ticker、Timestamp、Headline、Text Body、Sentiment Score 和 Market Price 六个字段；对应上述 `ticker`、`timestamp_us`、`headline`、`text_body`、`sentiment_score`、`market_price`。`timestamp_us` 是 Unix 微秒，标题和完整正文原样保存，情绪分数由上游给定，价格是该条新闻发生时的市场价格。

本例假设 `(ticker, timestamp_us)` 标识一条新闻，重复业务键会更新该行；ticker 决定业务分区。第一层 MV 汇总新闻条数、情绪总和及逐条情绪与价格乘积的合计，第二层 MV 计算平均情绪。没有窗口或 TTL，聚合覆盖表内保留的新闻。源投递身份独立于此业务主键；时间戳字段也不代替数据库的 processing-time Clock。

工厂最后调用 `TriggerSet::bind`，将 SQL 图、请求捕获、静态函数关系、请求状态和 Trigger 一起绑定。`computation_module()` 返回稳定模块身份 `streaming_news`、版本 `1` 和绑定函数。数据库入口 [main.cpp](main.cpp) 将这个模块传给 `database_main`；所有数据库进程链接同一份定义。

这是一种随应用编译的计算定义。修改 C++ Trigger 或评分函数需要重新编译和重新部署；这里没有上传 C++ 闭包或安装动态 UDF/UDTF/UDAF。

## 两种 Trigger 和固定输入

首条新闻为该 ticker 创建显式 `news_session`。为了专注正常有限流程，本例把截止时间设为最大 Int64 微秒；实际应用应定义新闻会话的结束与换代规则。

| 声明 | 条件与受理规则 | 捕获和输出 |
| --- | --- | --- |
| `sentiment_threshold` | 新闻条数至少为 2 且平均情绪至少为 0.5；每个 session 最多受理一次 | 固定新闻条数、平均情绪和情绪价格乘积合计，计算影响指标 |
| `periodic_impact` | 每个已有 ticker 每 200 ms 有一个 Clock 时隙，当前输入可用时尝试受理，不设置 once | 每次重新捕获当前 MV，生成独立请求和影响指标 |

声明式 API 的核心形态如下；完整 session、Function 和请求选项在 `definition.cpp`：

```cpp
Trigger::on_event("sentiment_threshold", news_event)
    .when((ViewRef(features).column<std::int64_t>("news_count") >= std::int64_t{2}) &&
          (ViewRef(features).column<double>("average_sentiment") >= 0.5))
    .in_session(SessionRef::candidate(session))
    .capture_current<&capture_news_features>(features, "streaming.capture_news_features.v1")
    .request(impact_function, threshold_request);
```

两种 Trigger 都使用 `capture_current`：**受理时读取当前 MV，之后输入固定**。`calculate_news_impact` 返回 `StatusOr<NewsImpact>`，计算 `impact_score = sentiment_price_sum / news_count`，即每条新闻的“预赋值情绪 × 新闻时点价格”的平均值。它是一个演示自定义计算的指标，不执行文本情绪推断。SQL 聚合列可空，捕获函数检查可用值与正的新闻条数；指标函数检查分母及非有限结果。

Clock 的 200 ms 是时隙间隔，不是完成 SLA。过期时隙可能合并；没有新新闻时，数据库仍会推进周期请求。每个请求执行期限为 10 秒；阈值请求最多等待受理 1 秒。

## 输入时间线和精确断言

验证器先在 STAGING 状态写入前四条消息，确认两个 partition 都没有 ACK 后再 `deploy()`。每条消息是包含六个字段的 JSON 对象，例如：

```json
{"ticker":"TICKER_0","timestamp_us":1700000000000001,"headline":"Product outlook improves","text_body":"Management said \"demand is improving\".\nThe report covers the next quarter.","sentiment_score":0.25,"market_price":100.0}
```

实际 ticker 由 `shard_for` 选取并打印。HTTP 的 `key_hex` 和 `payload_hex` 是业务 key/JSON 的十六进制编码，源身份是 `news_feed`，topic 为 `news_updates`。消息 envelope 的 `timestamp_ms` 来自新闻时间的毫秒部分；payload 保留完整微秒数。

| 阶段 | shard 0 的新闻 `(情绪,价格)` | shard 1 的新闻 `(情绪,价格)` | SQL 应看到的结果 |
| --- | --- | --- | --- |
| STAGING 预写 | `(0.25,100)`、`(0.75,104)` | `(0.5,200)`、`(0.75,208)` | 两个 partition 的确认位置均为 `-1` |
| 部署并自动消费 | 2 条，平均情绪 0.5，乘积合计 103 | 2 条，平均情绪 0.625，乘积合计 256 | 阈值影响指标 51.5/128，确认位置 2/2 |
| 无新输入等待 | 不变 | 不变 | 出现新的周期请求；源位置仍为 2/2 |
| 暂停后追加 | 第三条 `(0.5,112)` 保留在消息服务 | 不变 | MV 与源确认位置保持原值 |
| 恢复自动消费 | 3 条，平均情绪 0.5，乘积合计 159 | 仍为 2 条 | 周期影响指标更新为 53；确认位置 3/2 |
| 检查 once 和固定输入 | 当前特征已变化 | 不变 | 第一条阈值请求仍只有一次，输入仍为 2/0.5/103，结果仍为 51.5 |

验证器通过 SQL 对比所有六个原始字段，包含引号、换行的正文和微秒时间戳；同时读取两层 MV、断言返回的 shard 并打印 node_id，比较整个固定输入行（包括 request_id）在追加新闻前后完全一致。阈值首个请求的新闻条数必须为 2，不能因第一条正面新闻就提前受理。

生成的捕获表、函数结果表、Latest 名称由 `bound.explain()` 和 Catalog 查得，客户端不猜内部后缀。典型查询为：

```sql
SELECT timestamp_us, headline, text_body, sentiment_score, market_price
  FROM news_updates WHERE ticker='TICKER_0';
SELECT news_count, total_sentiment, sentiment_price_sum
  FROM news_stats WHERE ticker='TICKER_0';
SELECT news_count, average_sentiment, sentiment_price_sum
  FROM news_features WHERE ticker='TICKER_0';
```

触发记录保存在自动安装的 `trigger_audit`，业务 key 列为 `scope_key_0`。完整 SQL 和 SDK 代码见[根 README 的触发记录查询](../../README.md#51-select-当前特征和触发记录)。审计说明规则已发出触发，函数成功要结合请求状态和结果判断。

## 正常 checkpoint 与边界

最后验证器暂停数据库，读取固定下来的 `status.now_us`，创建完整 `streaming-cut` checkpoint，再恢复 RUNNING。checkpoint 必须返回 `complete=true`，并包含源位置、Catalog 指纹和各 Compute 的运行状态切面；它不是单独调用 RocksDB flush。共享 KV 保留 RocksDB WAL。

此流程验证正常部署、自动消费、暂停恢复和 checkpoint 创建，不包含故障注入或进程重建后的恢复验收。HTTP 消息服务是本地有界消息设施，日志随服务进程退出而消失，不是 Kafka broker；真实 Kafka 集成属于另行部署验证。默认脚本有限运行并自动退出，长期服务还需要符合业务的 session 生命周期、容量和保留策略。
