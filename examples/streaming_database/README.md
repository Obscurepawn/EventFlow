# 从消息到 SQL：数据库主动消费的完整示例

这个例子从头定义成交业务，启动独立 HTTP 消息服务、共享 RocksDB KV、一个 Coordinator/Meta 和两个 Compute 进程，再由 producer 写消息、用 SQL 等待和断言计算结果。数据库自己读取消息、维护级联 MV、推进 Clock 并执行静态函数。

入口是 [run.sh](run.sh)，业务定义集中在 [definition.cpp](definition.cpp)，生产和验证流程集中在 [verify.cpp](verify.cpp)。三个可执行文件都来自本目录，不依赖旧示例的业务实现。

## 运行

在仓库根目录执行：

```bash
bash examples/streaming_database/run.sh
```

新构建目录使用 Clang、Debug 和 Unix Makefiles；已有目录保留原构建类型。脚本只构建 `eventflow_streaming_database`、`eventflow_message_server`、`eventflow_streaming_verify`，不下载第三方依赖。也可以指定构建目录或复用现有二进制：

```bash
bash examples/streaming_database/run.sh build-debug
EVENTFLOW_BUILD_DIR="$PWD/build-debug" EVENTFLOW_SKIP_BUILD=1 \
  bash examples/streaming_database/run.sh
```

运行包中没有 `CMakeLists.txt` 时，脚本直接使用包内 `bin/`。`EVENTFLOW_DATABASE_BINARY`、`EVENTFLOW_MESSAGE_BINARY` 和 `EVENTFLOW_DEMO_BINARY` 可以分别覆盖数据库、消息服务和验证客户端路径。

所有监听端口动态分配。脚本打印服务地址和临时工作目录，保留 JSON 配置、每个进程的日志、`verify.log` 和 `kv-data/`。正常验收结束后回收全部子进程；任一客户端、子进程或 sanitizer 返回失败，脚本都返回非零。最终成功标记形如：

```text
STREAMING_DATABASE_OK shards=2 totals=150,130 threshold_scores=242,262 periodic_score=303 source_next_offsets=3,2
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

消息服务保留两个 `trades` partition 的有界内存日志。`publish` 只追加原始消息；数据库根据部署配置把消息 JSON 解码为 `trades` 行，经 Coordinator 路由到相应 Compute。业务 key 的分片通过 `shard_for` 计算，验证程序选取两个分别落在 shard 0、1 的 ticker；消息 partition 和数据库 shard 是两个独立概念，本例为了便于阅读将它们一一配对。

Compute 在共享 KV 上确认业务输入后，数据库才向消息服务 ACK。源位置指向下一条待处理 offset，所以两个 partition 各处理两条后位置都是 `2`。时钟触发不增加源 offset。

`verify.cpp` 没有调用数据库的 `ingest`、`pump` 或 `drain`。它的等待循环只查询 SQL 或消息源确认位置；`tick_interval_ms=20`、`source.poll_interval_ms=10` 配置的是数据库内部驱动周期。

## 独立定义表和级联 MV

`definition.cpp` 的工厂新建 Catalog，定义以下依赖链：

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

一条成交以 `(ticker, trade_id)` 为身份；不同成交累积为同一 ticker 的总量和笔数。第二个 MV 依赖第一个 MV，在同一输入处理链中更新。这里没有窗口或 TTL，例子中的成交保持在表内。

工厂最后调用 `TriggerSet::bind`，将 SQL 图、请求捕获、静态函数关系、请求状态和 Trigger 一起绑定。`computation_module()` 返回稳定模块身份 `streaming_market`、版本 `1` 和绑定函数。数据库入口 [main.cpp](main.cpp) 将这个模块传给 `database_main`；所有数据库进程链接同一份定义。

这是一种随应用编译的计算定义。修改 C++ Trigger 或评分函数需要重新编译和重新部署；这里没有上传 C++ 闭包或安装动态 UDF/UDTF/UDAF。

## 两种 Trigger 和固定输入

首次成交为该 ticker 创建显式 `trading_session`。为了专注正常有限流程，本例把 session 截止时间设为最大 Int64 微秒；这不是新增的永久 session API。真实业务应定义开盘、换日或业务会话结束规则。

| 声明 | 条件与受理规则 | 捕获和输出 |
| --- | --- | --- |
| `volume_threshold` | 每条成交后检查 `market_features.total_volume >= 100`，每个 session 最多受理一次 | 捕获受理时的总量、笔数和两倍总量，生成一次评分 |
| `periodic_score` | 每个已有 ticker 每 200 ms 有一个 Clock 时隙，当前输入可用时尝试受理，不设置 once | 每次重新捕获当前 MV，生成独立请求和评分 |

声明式 API 的核心形态直接保留在代码中：

```cpp
Trigger::on_event("volume_threshold", trade_event)
    .when(ViewRef(features).column<std::int64_t>("total_volume") >= std::int64_t{100})
    .in_session(SessionRef::candidate(session))
    .capture_current<&capture_features>(features, "streaming.capture_features.v1")
    .request(score_function, threshold_request);
```

这是突出 API 链条的节选；完整 EventRef、Function、期限和 once 参数见 `definition.cpp`。两种 Trigger 都使用 `capture_current`：**受理时读取当前 MV，之后输入固定**。评分函数是普通 `noexcept` C++ 函数，返回 `StatusOr<Score>`，计算 `doubled_volume + trade_count`。SQL 的 `SUM` 在 Schema 上可空，因此捕获函数显式检查可空字段，再构造非空评分输入。

Clock 的 200 ms 表示时隙间隔，不是实时完成 SLA。过期时隙可能合并，验收只检查没有新成交时仍出现新的成功请求，不依赖机器在严格 200 ms 内完成。每个请求的执行期限为 10 秒。

## 输入时间线和精确断言

验证器先在 STAGING 状态写入前四条消息，等待后检查两个 partition 都没有 ACK，再调用 `deploy()`。消息中的 payload 是业务 JSON 对象，例如：

```json
{"ticker":"TICKER_0","trade_id":1,"volume":70}
```

实际 ticker 由 `shard_for` 选取并打印。HTTP 的 `key_hex` 和 `payload_hex` 是上述业务 key/JSON 的十六进制编码，源身份是 `market_feed`；它们不是数据库 ingest 请求包装。

| 阶段 | shard 0 的成交量 | shard 1 的成交量 | SQL 应看到的结果 |
| --- | --- | --- | --- |
| STAGING 预写 | 70、50 | 40、90 | 消息服务确认位置均为 `-1`，尚未消费 |
| 部署并自动消费 | 合计 120，2 笔 | 合计 130，2 笔 | 两倍总量 240/260；阈值评分 242/262；确认位置 2/2 |
| 无新输入等待 | 不变 | 不变 | 新的周期评分请求出现；源确认位置仍是 2/2 |
| 暂停后追加 | 新成交 30 保留在消息服务 | 不变 | 数据库仍是 120、2 笔，确认位置仍为 2 |
| 恢复自动消费 | 合计 150，3 笔 | 仍为 130，2 笔 | 两倍总量 300；周期评分更新为 303；确认位置 3/2 |
| 检查 once 和捕获 | 当前特征已变化 | 不变 | 第一个阈值请求仍只有一次，固定输入仍为 120、2、240，结果仍为 242 |

SQL 验证同时读取两个 MV，检查路由返回的 shard，并打印 `node_id`。Trigger 生成的捕获表、结果表、Latest 表名称由 `bound.explain()` 和 Catalog 查得，验证器不猜内部命名后缀。

典型查询如下，其中 ticker 和生成关系名由运行时输出给出：

```sql
SELECT total_volume, trade_count FROM trade_totals WHERE ticker='TICKER_0';
SELECT total_volume, trade_count, doubled_volume
  FROM market_features WHERE ticker='TICKER_0';
```

`verify.cpp` 还查询阈值结果和固定输入，比较追加成交前后的整个捕获行（包括 request_id）完全一致；周期 Latest 则必须出现 `303`。

逐条查看规则发出的触发，可查询自动安装的 `trigger_audit`，其业务 key 列为 `scope_key_0`。完整 SQL、C++ SDK 读取代码以及触发与计算成功的区别见[根 README 的触发记录查询](../../README.md#51-select-当前特征和触发记录)；此处验证程序仍以特征和评分断言为主。

## 正常 checkpoint 与边界

最后验证器暂停数据库，读取固定下来的 `status.now_us`，创建完整 `streaming-cut` checkpoint，再恢复 RUNNING。checkpoint 必须返回 `complete=true`，并包含源位置、Catalog 指纹和各 Compute 的运行状态切面；它不是单独调用 RocksDB flush。共享 KV 保留 RocksDB WAL。

此流程验证正常部署、自动消费、暂停恢复和 checkpoint 创建，不包含故障注入或进程重建后的恢复验收。HTTP 消息服务是本地有界消息设施，日志随服务进程退出而消失，不是 Kafka broker；真实 Kafka 集成属于另行部署验证。默认脚本有限运行并自动退出，长期服务还需要符合业务的 session 生命周期、容量和保留策略。
