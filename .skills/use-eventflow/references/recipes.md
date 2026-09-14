# 完整数据库应用：成交、级联 MV 与触发评分

优先复用 [streaming_database](../../../examples/streaming_database/README.md)。它独立定义业务图，启动真实共享 Rocks KV、Coordinator 和两个 Compute，另有独立 HTTP 消息服务。producer 只发布消息，数据库自动消费，客户端用 SQL 验证。

```sh
bash examples/streaming_database/run.sh
```

脚本支持首个构建目录参数、`EVENTFLOW_BUILD_DIR` 和已有二进制的 `EVENTFLOW_SKIP_BUILD=1`。新构建目录使用 Clang、Debug 和 Unix Makefiles。完整源码需要 `cpp3rdlib/`；二进制运行包仅能运行已有程序。执行后的实际输出才是通过证据，本文数值是代码中的预期断言。

## 从业务定义开始

[definition.cpp](../../../examples/streaming_database/definition.cpp)包含完整 Catalog、SchemaTraits、capture、Function、session 与 Trigger 绑定；[definition.hpp](../../../examples/streaming_database/definition.hpp)只暴露模块工厂。

```sql
CREATE TABLE trades (
  ticker STRING NOT NULL,
  trade_id INT64 NOT NULL,
  volume INT64 NOT NULL,
  PRIMARY KEY (ticker, trade_id)
) PARTITION BY (ticker);

CREATE MATERIALIZED VIEW trade_totals AS
  SELECT ticker, SUM(volume) AS total_volume, COUNT(*) AS trade_count
  FROM trades GROUP BY ticker;

CREATE MATERIALIZED VIEW market_features AS
  SELECT ticker, total_volume, trade_count, total_volume * 2 AS doubled_volume
  FROM trade_totals;
```

`trade_id` 保存不同成交的行身份；ticker 决定业务分区。输入确认时，两层 MV 沿同一 Catalog 的依赖稳定并更新，不需要额外调用 refresh。SQL SUM 的可空 Schema 由 `CurrentFeatures` 的 optional 字段对应，capture 显式检查可用值后返回非空 `FeatureSnapshot`。

评分函数计算 `doubled_volume + trade_count`，用溢出检查返回 Status。业务函数随应用编译，worker 收到已固定的输入后执行；函数不能直接写 KV 或改变 owner 状态。

## 声明阈值与周期

以下是 API 链条节选；它依赖完整源码中已定义的关系、session、函数和请求参数，不能单独作为完整程序编译。

```cpp
Trigger::on_event("volume_threshold", trade_event)
    .when(ViewRef(features).column<std::int64_t>("total_volume") >= std::int64_t{100})
    .in_session(SessionRef::candidate(session))
    .capture_current<&capture_features>(features, "streaming.capture_features.v1")
    .request(score_function, threshold_request);
```

| Trigger | 输入与语义 | 请求策略 |
| --- | --- | --- |
| `volume_threshold` | 每条成交后读本轮稳定的 market_features，检查总量至少为 100 | Wait，当前 session 最多受理一次 |
| `periodic_score` | 每个已经观察到的 ticker 每 200 ms 尝试一次 | TryOnceAtFiring，不设置 once，需活动 session 与可用输入 |

第一条成交创建 session；本例使用最大 Int64 截止时间以聚焦有限流程，业务应用应自行定义 session 结束、换代或续期策略。这不代表永久 session 或无 candidate 请求已经实现。

两条规则都 `capture_current`，在实际受理时固定 MV；之后的成交不能改写旧输入。Pattern 所需的 `capture_match` 则保存所匹配事件的投影，相关接口与区别见 [Trigger APIs](trigger-api.md)。不要为展示 Pattern 把这里的成交逻辑无故改成另一套业务需求。

Clock 声明没有定时线程。数据库的 `tick_interval_ms=20` 驱动时间，source 的 `poll_interval_ms=10` 驱动消费；200 ms 是业务时隙，不是墙钟完成 SLA。无消息时函数 completion 与 Clock 仍由数据库推进，错过时隙可以合并。

## 将定义交给数据库

工厂将 SQL、请求关系、函数与 Trigger 一起绑定，返回完整 BoundComputation。`ComputationModule.bind` 接收 staged DDL，模块名与版本提供稳定部署身份：

```cpp
ComputationModule computation_module() noexcept {
  return {.name = "streaming_market", .version = 1, .bind = &bind_definition};
}
```

[main.cpp](../../../examples/streaming_database/main.cpp)调用 `database_main`。Coordinator 与 Compute 使用相同模块，通用 [ComputationRuntime](../../../src/runtime/computation_runtime.cpp)管理 EventProcessor 和执行器。客户端的 `deploy()` 安装这一已编译定义，不上传函数或临时改写 Trigger。

完整 source 配置、二进制和进程启动顺序在 [run.sh](../../../examples/streaming_database/run.sh)。服务配置的 `source.type=http` 接入 HttpMessageSource；其 poll/position/ack 边界与 Kafka/MemMock 复用 IMessageSource，HTTP 服务仍只是本地消息设施。

## 生产、部署、SQL 与精确断言

[verify.cpp](../../../examples/streaming_database/verify.cpp)查找两个分别落在 shard 0、1 的 ticker。它没有数据库 ingest、pump、drain 调用；等待循环只读 SQL 或源确认位置。

| 阶段 | 预期行为 |
| --- | --- |
| STAGING publish | 两个 partition 各有两条消息，确认位置都仍为 -1 |
| deploy 后自动消费 | 总量 120/130、笔数 2/2，阈值评分 242/262，next offset 2/2 |
| 无新消息 | 出现新的周期请求，源位置不变 |
| pause 后追加 30 | 第一 ticker 的 MV 保持 120/2，消息保持未确认 |
| resume | 第一 ticker 总量 150、笔数 3，周期评分 303，next offset 3/2 |
| 固定输入与 once | 阈值请求仍只有一次，捕获行连同 request_id 不变，评分仍为 242 |

生成的 capture/function/Latest 关系名称通过 `bound.explain()` 与 Catalog 获得，不能猜名称后缀。SQL 查询返回实际 node_id、shard、revision，并要求一个完整业务 scope。

```sql
SELECT total_volume, trade_count, doubled_volume
FROM market_features WHERE ticker='TICKER_0';

SELECT rule_id, evaluated_at_us, request_id
FROM trigger_audit WHERE scope_key_0='TICKER_0';
```

ticker 使用本次运行输出。`trigger_audit` 由 TriggerSet 自动安装，scope 列名是 `scope_key_0`，不是业务表的 ticker；该审计记录 occurrence 发出，不等于请求已成功，详见 [Trigger audit](trigger-audit.md)。

## 暂停与 checkpoint

验证器最后 pause，读取固定 now_us，创建 `streaming-cut` checkpoint，再 resume。checkpoint 绑定源位置、Catalog/运行状态及各 shard 回执与 KV snapshot，RocksDB WAL 保留。此例验证正常创建，不包含进程重建恢复或专门故障注入。

HTTP 消息服务保存有界内存日志，退出即丢失；它不证明真实 Kafka broker 集成，也不是持久消息系统。恢复需要源设施保留 checkpoint 之后的消息，数据库从保存位置 seek，而不是盲从更后的 broker commit。恢复流程和边界见 [Cluster operations](cluster-and-operations.md)。
