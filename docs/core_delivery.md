# EventFlow 核心集群运行手册

当前默认完整用法是 [SDK 上传 UDF 的流式示例](../examples/streaming_database/README.md)；本文中保留的静态模块代码参考已移到 [static_streaming](../examples/static_streaming/README.md)。原生上传与分发的最新范围见 [生产 UDF 规格](../specs/native-udf-delivery.md)。

完整入口是[数据库主动消费示例](../examples/static_streaming/README.md)：独立声明六字段新闻表、两层物化视图和两个 Trigger，启动集群，向 HTTP 消息服务写入数据，再用 SQL 观察结果。消息消费、Clock 推进和函数完成均由数据库服务负责。此手册解释配置与运行边界；本轮验收状态以[当前规格](../specs/news-streaming-example.md)为准，历史测试结果不代替本轮验证。

## 一条命令运行完整流程

在仓库根目录执行：

```sh
bash examples/static_streaming/run.sh
```

脚本构建并使用三个程序：`eventflow_static_streaming_database` 提供数据库的三个角色，`eventflow_message_server` 提供独立消息设施，`eventflow_static_streaming_verify` 生产数据并验证 SQL。需要复用构建目录时：

```sh
bash examples/static_streaming/run.sh build-debug
EVENTFLOW_BUILD_DIR="$PWD/build-debug" EVENTFLOW_SKIP_BUILD=1 \
  bash examples/static_streaming/run.sh
```

新构建目录使用 Clang、Debug、Unix Makefiles；项目依赖来自 `cpp3rdlib/`，构建不下载第三方实现。Debug 启用 ASan/UBSan/LSan 的失败诊断；Release 使用 `-O3`、`NDEBUG` 和可用 LTO，不启用 sanitizer 或 fast-math。`EVENTFLOW_SKIP_BUILD=1` 只适用于已构建的对应二进制。

脚本自动分配端口，打印服务地址和工作目录，保留配置、日志和 RocksDB 活动数据目录，有限流程结束后回收子进程。运行包没有 `CMakeLists.txt` 时直接使用包内 `bin/`。完整构建方法见[根 README](../README.md)，业务定义和逐条断言见[示例 README](../examples/static_streaming/README.md)。

## 进程与所有权

部署包含 **1 个共享 KV、1 个 Coordinator/Meta、2 个 Compute**，另外启动 **1 个独立 HTTP 消息服务**。消息服务不承担数据库计算，客户端不代替数据库推进输入。

```text
Producer -> HTTP Message Server -> Coordinator / SourcePoller
                                            |
                                  +---------+---------+
                                  |                   |
                              Compute A           Compute B
                               shard 0             shard 1
                                  |                   |
                                  +---- Shared KV ----+

SQL client -> Coordinator -> owning Compute -> confirmed relations
```

| 层 | 职责 |
| --- | --- |
| HTTP Message Server | 保存有界内存消息日志，提供 publish、poll、position、ack；进程退出后日志消失 |
| Coordinator/Meta | 保存单一部署定义和路由，托管 SourcePoller，解码消息并转发，确认源进度，推进时钟，协调暂停和 checkpoint |
| Compute | 每个 shard 安装一个 ComputationRuntime，串行推进 Engine/EventProcessor 和请求；线程池执行静态函数，owner 保存结果 |
| KV | Memory/RocksDB 后端，按 shard 原子提交状态和回执，提供不可变 snapshot；RocksDB 保留 WAL |
| C++ 客户端 | 生产消息、部署控制与 SQL 查询；查询返回实际 node_id、shard 和已确认 revision |

业务 key 例如 ticker，通过 `shard_for` 分配到固定逻辑 shard，再映射到 Compute。消息 partition 与数据库 shard 属于不同身份域，通常不要求一一对应。本例选择两个落在不同 shard 的 ticker，便于观察实际路由。

业务 shard 数为 `S` 时，KV 使用 `S+1` 个物理 shard：索引 `S` 保存 Meta 控制记录，业务哈希只使用前 `S` 个。本例配置两个业务 shard、三个 KV shard。共享 KV 不提供跨 shard 事务、跨 key join、shuffle 或自动重分片。

## 自定义静态计算定义

[definition.cpp](../examples/static_streaming/definition.cpp)在自己的 Catalog 中定义：

```text
news_updates -> news_stats -> news_features
                              |
                  +-----------+------------+
                  |                        |
            sentiment_threshold          periodic_impact
             once per session          every 200 ms
                  |                        |
                  +-> capture_current -> static news_impact -> result / Latest
```

`news_stats` 按 ticker 维护新闻条数、情绪总和及 `SUM(sentiment_score * market_price)`；`news_features` 再计算平均情绪。`sentiment_threshold` 在新闻后检查条数至少为 2 且平均情绪至少为 0.5，每个 session 最多受理一次；`periodic_impact` 在已有 ticker 上每 200 ms 尝试一次请求。它们捕获受理时的新闻条数、平均情绪和情绪价格乘积合计，静态函数计算 `impact_score = sentiment_price_sum / news_count`。后续新闻不会修改旧请求的固定输入。

`ComputationModule` 由稳定的 `name`、`version` 和 `bind` 工厂组成。工厂接收部署前 DDL，返回包含 Catalog、ProcessorConfig 和 FunctionRegistry 的完整 `BoundComputation`；`TriggerSet::bind` 校验并原子绑定定义。[main.cpp](../examples/static_streaming/main.cpp)把该模块传给 `database_main`，Coordinator 与所有 Compute 链接同一份模块并校验部署身份。

这是随应用编译的自定义计算入口。修改 C++ Trigger、Schema 映射或静态函数后需要重新编译、部署相容定义；SDK `deploy()` 不上传 C++ 闭包。通用服务与 `ComputationRuntime` 不依赖示例业务；旧业务场景只作为测试支持材料保留。

## 消息订阅与服务配置

完整配置由 [run.sh](../examples/static_streaming/run.sh)生成。Coordinator 的 `source` 配置形如：

```json
{
  "tick_interval_ms": 20,
  "source": {
    "type": "http",
    "endpoint": "http://127.0.0.1:8090",
    "source_id": "news_feed",
    "relation": "news_updates",
    "assignments": [
      {"topic": "news_updates", "partition": 0, "next_offset": "0"},
      {"topic": "news_updates", "partition": 1, "next_offset": "0"}
    ],
    "poll_interval_ms": 10,
    "max_message_bytes": 262144
  }
}
```

这是配置节选；节点身份、KV 地址和 Compute 路由仍需提供。完整字段解析见 [database_main.cpp](../src/service/database_main.cpp)。`type=http` 使用 `endpoint`，也可设置 `operation_timeout_ms` 和 `max_response_bytes`；`type=kafka` 使用 `bootstrap_servers` 和 `group_id`，复用相同 source/assignment/轮询合同。Kafka 为静态手动 assignment，真实 broker/Compose 集成需单独验证；HTTP 示例不是 Kafka 证据。

消息 payload 是目标表 Schema 对应的 JSON 对象，或按 Schema 排列的 JSON 数组。本例以 `(ticker, timestamp_us)` 标识新闻，完整存储标题、正文、预赋值情绪与新闻时点价格。生产 payload 形如：

```json
{"ticker":"TICKER_0","timestamp_us":1700000000000001,"headline":"Product outlook improves","text_body":"Demand is improving for the next quarter.","sentiment_score":0.25,"market_price":100.0}
```

HTTP publish 用 `key_hex`/`payload_hex` 传递原始字节，具体请求见 [verify.cpp](../examples/static_streaming/verify.cpp)。当前自动接入要求非空 payload，tombstone 不自动映射为删除。可选 `timestamp_ms` 转换为事件时间微秒，并检查溢出；没有时间戳时使用本轮处理时间。业务 Row 决定数据库路由，原始消息 key 不替代 Schema 校验。

## 部署、自动消费和 SQL

服务开始监听时打印 READY；这时不代表计算已运行。Coordinator 处于 STAGING，消息可以预先写入消息服务，但 SourcePoller 不消费、不 ACK。调用 `deploy()` 后，各 Compute 安装同一 Catalog/模块版本与分片配置；全部确认后才发布 RUNNING 并开始消费。

验证器依次检查：

| 阶段 | 输入与期望结果 |
| --- | --- |
| 部署前 | 两个 partition 各写两条，确认位置保持 `-1` |
| 自动消费 | 两个 ticker 各有 2 条新闻，平均情绪为 0.5/0.625，阈值影响指标为 51.5/128，next offset 为 2/2 |
| 无新输入 | 周期请求继续出现，源位置保持不变 |
| 暂停追加 | 第一组追加情绪 0.5、价格 112 的第三条新闻，暂停时 MV 和 ACK 位置保持不变 |
| 恢复消费 | 新闻条数变为 3、平均情绪为 0.5、周期影响指标变为 53，next offset 为 3/2 |
| 固定输入 | 阈值请求仍只受理一次，捕获仍为 2/0.5/103，旧指标仍为 51.5 |

这些数值是示例代码的断言目标；实际通过情况须以本轮执行日志为准。客户端等待只读取 SQL 或源确认位置，没有调用 `ingest`、`pump` 或 `drain`。

```sql
SELECT news_count, total_sentiment, sentiment_price_sum FROM news_stats WHERE ticker='TICKER_0';
SELECT news_count, average_sentiment, sentiment_price_sum
  FROM news_features WHERE ticker='TICKER_0';
SELECT rule_id, evaluated_at_us, request_id
  FROM trigger_audit WHERE scope_key_0='TICKER_0';
```

实际 ticker 由脚本输出；生成的请求捕获表、函数结果表和 Latest 名称通过 `bound.explain()` 与 Catalog 查找，不能猜测内部后缀。SDK 查询需要业务 scope，示例同时提供 SQL 的 ticker 条件和对应 scope。

`ClusterClient` 仍提供 DDL、直接 ingest 和显式时间推进等底层控制入口。启动前 DDL 可以补充受支持的表/MV，部署后不支持在线 DDL。直接 ingest 供需要自行交付输入的接入方式和测试使用；本例采用配置化消息订阅，生产者只调用消息服务。

## 确认、时间与容量

`SourcePoller` 每步最多保留一条投递，固定原 payload、source/topic/partition/offset、token 与处理时间。只有 `input_committed=true` 才 ACK；后续函数尚未完成不等于输入未确认。UNKNOWN 必须继续处理原投递和原提交，不能换身份或跳过确认前缀。永久错误保留原位置并通过 `status/discover` 的 `source_reader` 与 `background_status` 暴露。

`tick_interval_ms` 控制数据库时钟推进，`source.poll_interval_ms` 控制消息轮询。本例分别为 20 ms 和 10 ms；Clock 的 200 ms 是业务时隙间隔，不是完成 SLA。无新消息时，后台循环仍推进函数 completion 和到期工作。未确认输入会阻挡时间越过其固定处理时刻。

C++ Coordinator 配置的 `tick_interval_ms` 默认是 0，CLI 省略时默认 100 ms。设为 0 会关闭空闲时的自动时钟增长；配置了 source 仍会后台消费，并在当前时间推进函数结果。完全手动的测试应同时不配置 source。`pump/drain` 是显式控制入口，SQL 查询不推进计算。

SourcePoller、adapter 的 poll/inflight、单消息、HTTP 请求响应和每轮工作分别受预算限制。Compute 的节点状态缓存预算按 shard 分配，受 owner/epoch/confirmed revision 约束；UNKNOWN 时不能利用缓存绕过确认。状态缓存预算不代表进程总 RSS 上限。

## 一致 checkpoint 与恢复边界

示例最后调用 `pause()`，读取固定的 `status.now_us`，再创建 `streaming-cut` checkpoint，并显式 `resume()`。Coordinator 停止新输入，协调各 Compute 静止，绑定源确认位置、水位、Catalog/模块身份、运行状态与 shard revision/receipt，然后创建 KV snapshot。成功后仍为 QUIESCED；它不是单独一次 RocksDB flush。

恢复使用相同模块、owner、逻辑 node/shard 分配与 source 身份。停止数据库进程后，以 `kv --backend rocks --restore-from OLD_DIR --snapshot ID --data-dir NEW_DIR` 创建不存在的新活动目录，再用 `restore_checkpoint` 配置启动 Coordinator，并重新 deploy。不能在旧 live 数据目录上仅增加 restore 标志冒充回滚。源端必须继续保留恢复点以后的消息；数据库依据 checkpoint 保存的位置 seek，不能采用更后的 broker commit 跳过待重放数据。

本例覆盖正常 checkpoint 创建，未执行进程重建恢复或故障注入；HTTP 消息日志随其服务退出而消失。Memory snapshot 也只在保留原 MemoryStateStore 对象时有效。真实 Rocks 恢复回归入口是 [core_cluster_test.cpp](../tests/core_cluster_test.cpp)，其当前验证结论由主任务记录。更详细的冻结协议见[实现导读](implementation_guide.md#13-一致-checkpoint-为什么不只是一次-kv-flush)。

当前保留 RocksDB WAL。动态 UDF/UDTF/UDAF、Process Function、在线图迁移、通用观察 host、HA、MVCC、跨 key 计算和真实 Kafka Compose 均不能由该示例推导为已交付能力。
