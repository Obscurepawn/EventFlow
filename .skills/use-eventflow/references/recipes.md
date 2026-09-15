# 完整数据库应用：新闻、级联 MV 与触发评分

默认完整示例是 [streaming_database](../../../examples/streaming_database/README.md)：先启动通用 Server，再用 SDK 上传 `.so`、提交计算规则并部署；恢复时从 KV 读取函数文件，不需要重新上传。业务定义和真实调用分别见 [definition.cpp](../../../examples/streaming_database/definition.cpp) 和 [verify.cpp](../../../examples/streaming_database/verify.cpp)。

```sh
bash examples/streaming_database/run.sh
```

## 静态模块参考

以下保留按 ComputationModule 编写应用的参考，只在需要这套 API 时使用。

参考 [static_streaming](../../../examples/static_streaming/README.md)。它独立定义业务图，启动真实共享 Rocks KV、Coordinator 和两个 Compute，另有独立 HTTP 消息服务。producer 只发布消息，数据库自动消费，客户端用 SQL 验证。

```sh
bash examples/static_streaming/run.sh
```

脚本支持首个构建目录参数、`EVENTFLOW_BUILD_DIR` 和已有二进制的 `EVENTFLOW_SKIP_BUILD=1`。新构建目录使用 Clang、Debug 和 Unix Makefiles。完整源码需要 `cpp3rdlib/`；二进制运行包仅能运行已有程序。执行后的实际输出才是通过证据，本文数值是代码中的预期断言。

## 从业务定义开始

[definition.cpp](../../../examples/static_streaming/definition.cpp)包含完整 Catalog、SchemaTraits、capture、Function、session 与 Trigger 绑定；[definition.hpp](../../../examples/static_streaming/definition.hpp)只暴露模块工厂。

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

六字段新闻输入保存 ticker、Unix 微秒 timestamp_us、headline、text_body、给定 sentiment_score 及新闻时点 market_price。本例以 `(ticker, timestamp_us)` 为新闻行身份，同键输入更新该行；ticker 决定分区。SQL 聚合列的可空类型由 `CurrentNewsFeatures` 对应，捕获后得到 `NewsFeatureSnapshot`。

静态 `calculate_news_impact` 计算 `impact_score = sentiment_price_sum / news_count`，检查正分母和有限结果。它使用新闻给定的情绪及价格，标题/正文保持可查询；不是动态文本推断接口。worker 收到固定输入后执行，函数不直接写 KV 或 owner 状态。

## 声明阈值与周期

以下是 API 链条节选；它依赖完整源码中已定义的关系、session、函数和请求参数，不能单独作为完整程序编译。

```cpp
Trigger::on_event("sentiment_threshold", news_event)
    .when((ViewRef(features).column<std::int64_t>("news_count") >= std::int64_t{2}) &&
          (ViewRef(features).column<double>("average_sentiment") >= 0.5))
    .in_session(SessionRef::candidate(session))
    .capture_current<&capture_news_features>(features, "streaming.capture_news_features.v1")
    .request(impact_function, threshold_request);
```

| Trigger | 输入与语义 | 请求策略 |
| --- | --- | --- |
| `sentiment_threshold` | 每条新闻后读本轮稳定的 news_features，检查新闻条数至少为 2 且平均情绪至少为 0.5 | Wait，当前 session 最多受理一次 |
| `periodic_impact` | 每个已经观察到的 ticker 每 200 ms 尝试一次 | TryOnceAtFiring，不设置 once，需活动 session 与可用输入 |

第一条新闻创建 session；本例使用最大 Int64 截止时间以聚焦有限流程，业务应用应自行定义 session 结束、换代或续期策略。这不代表永久 session 或无 candidate 请求已经实现。

两条规则都 `capture_current`，在实际受理时固定 MV；之后的新闻不能改写旧输入。Pattern 所需的 `capture_match` 则保存所匹配事件的投影，相关接口与区别见 [Trigger APIs](trigger-api.md)。不要为展示 Pattern 把这里的新闻逻辑无故改成另一套业务需求。

Clock 声明没有定时线程。数据库的 `tick_interval_ms=20` 驱动时间，source 的 `poll_interval_ms=10` 驱动消费；200 ms 是业务时隙，不是墙钟完成 SLA。无消息时函数 completion 与 Clock 仍由数据库推进，错过时隙可以合并。

## 将定义交给数据库

工厂将 SQL、请求关系、函数与 Trigger 一起绑定，返回完整 BoundComputation。`ComputationModule.bind` 接收 staged DDL，模块名与版本提供稳定部署身份：

```cpp
ComputationModule computation_module() noexcept {
  return {.name = "streaming_news", .version = 1, .bind = &bind_definition};
}
```

[main.cpp](../../../examples/static_streaming/main.cpp)调用 `database_main`。Coordinator 与 Compute 使用相同模块，通用 [ComputationRuntime](../../../src/runtime/computation_runtime.cpp)管理 EventProcessor 和执行器。客户端的 `deploy()` 安装这一已编译定义，不上传函数或临时改写 Trigger。

完整 source 配置、二进制和进程启动顺序在 [run.sh](../../../examples/static_streaming/run.sh)。服务配置的 `source.type=http` 接入 HttpMessageSource；其 poll/position/ack 边界与 Kafka/MemMock 复用 IMessageSource，HTTP 服务仍只是本地消息设施。

## 生产、部署、SQL 与精确断言

[verify.cpp](../../../examples/static_streaming/verify.cpp)查找两个分别落在 shard 0、1 的 ticker。它没有数据库 ingest、pump、drain 调用；等待循环只读 SQL 或源确认位置。

| 阶段 | 预期行为 |
| --- | --- |
| STAGING publish | 两个 partition 各有两条消息，确认位置都仍为 -1 |
| deploy 后自动消费 | 新闻条数 2/2、平均情绪 0.5/0.625，阈值影响指标 51.5/128，next offset 2/2 |
| 无新消息 | 出现新的周期请求，源位置不变 |
| pause 后追加第三条新闻 | 第一 ticker 的特征保持 2/0.5/103，消息保持未确认 |
| resume | 第一 ticker 新闻条数 3、平均情绪 0.5、乘积合计 159，周期影响指标 53，next offset 3/2 |
| 固定输入与 once | 阈值请求仍只有一次，捕获行连同 request_id 不变，指标仍为 51.5 |

生成的 capture/function/Latest 关系名称通过 `bound.explain()` 与 Catalog 获得，不能猜名称后缀。SQL 查询返回实际 node_id、shard、revision，并要求一个完整业务 scope。

```sql
SELECT news_count, average_sentiment, sentiment_price_sum
FROM news_features WHERE ticker='TICKER_0';

SELECT rule_id, evaluated_at_us, request_id
FROM trigger_audit WHERE scope_key_0='TICKER_0';
```

ticker 使用本次运行输出。`trigger_audit` 由 TriggerSet 自动安装，scope 列名是 `scope_key_0`，不是业务表的 ticker；该审计记录 occurrence 发出，不等于请求已成功，详见 [Trigger audit](trigger-audit.md)。

## 暂停与 checkpoint

验证器最后 pause，读取固定 now_us，创建 `streaming-cut` checkpoint，再 resume。checkpoint 绑定源位置、Catalog/运行状态及各 shard 回执与 KV snapshot，RocksDB WAL 保留。此例验证正常创建，不包含进程重建恢复或专门故障注入。

HTTP 消息服务保存有界内存日志，退出即丢失；它不证明真实 Kafka broker 集成，也不是持久消息系统。恢复需要源设施保留 checkpoint 之后的消息，数据库从保存位置 seek，而不是盲从更后的 broker commit。恢复流程和边界见 [Cluster operations](cluster-and-operations.md)。
