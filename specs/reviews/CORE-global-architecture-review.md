# CORE 全局独立架构审查

审查时间：2026-09-14 06:00–06:20 UTC。审查者：`core_global_architecture_review`。
这是本轮最终并行全局审查中的一个独立上下文；只读取生产代码、运行独立进程探针，
不修改生产代码，也不启动新的 reviewer。本文记录审查时的发现，整改与定向验证由主 owner 补充。

## 结论

核心已具备真实的三角色部署和同一 Catalog 驱动的计算路径，并非仅把几个 HTTP 服务
或演示计算拼在一起。正常路径的跨进程测试、Rocks 整体恢复、Clock demo 有对应代码和日志。
但发现 **2 个已独立复现的 P1 核心问题**：未确认输入被时钟越过，以及 worker 分片配置
不一致时仍发布 RUNNING。两项修复并定向验证前，本 reviewer 不建议标记核心完成。

另有实际执行边界、审计信息与交付文档的 P2 项，须明确处置；不要求重新开展第二轮独立审查。

## 依据与范围

- 原始题面仅在本机通过 `pdftotext` 阅读，没有上传、发布或逐字复制到报告。
- 以 [五小时核心规格](../core-delivery.md) 为最新已授权范围；同时读取
  `AGENTS.md`、`specs/INDEX.md`、实现契约 C1/C7/C8/C9、M4/M5 相关条款、README、
  [核心运行手册](../../docs/core_delivery.md) 和 [架构文档](../../docs/feature_computation_architecture.md)。
- 用户允许后置的 UDF/UDTF/UDAF、在线迁移、复杂多 Job、Kafka Compose、完整通用观察 host、
  性能优化均不列为本轮核心阻塞项。Process Function 已移出当前交付。
- 没有 HA、MVCC、跨 key Join 或通用 CDC 队列，属于明确边界，不据此判失败。

## P1-A：未确认输入之后仍推进 Clock，使原样重试失效

**位置：** `src/service/coordinator_service.cpp:249–259,306–327,694–698`；
`src/runtime/event_processor.cpp:3370–3392`。

Meta 先保存 `pending_input` 和 `pending_dispatch_us`，再调用 Compute。若请求在 Compute
接纳前遭遇临时网络错误，这份未确认输入仍保留。此时公开 `pump` 和自动 ticker 都不检查
`pending_input`，可以把 worker 时间推进到更晚。重试原输入仍使用已冻结的旧 dispatch time，
EventProcessor 将它判为时间倒退；Meta 又将这次 `InvalidArgument` 当作验证错误，清除 pending。

独立探针启动真实 KV、Compute、Meta，并在 Meta→Compute 之间加入只拒绝第一次 ingest 的
HTTP 代理。代理返回 `Unavailable`，没有把该输入发给 Compute。使用审查时已有 Debug 二进制，
开启 ASan/UBSan；所有子进程正常退出 0。

```text
deploy                         -> 200 RUNNING
ingest(offset=0, now=100)       -> 503 Unavailable（Compute 尚未接纳）
pump(now=200)                  -> 200；worker processing_time_us=200
原样重试 offset=0, now=100     -> 400 InvalidArgument: processing time cannot move backwards
ingest(offset=1, now=201)       -> 200 input_committed=true
checkpoint(now=202)            -> complete=true, source next_offset=2
```

最后两步说明该原合法输入被分类为可跳过的错误后，系统允许继续推进；这不等于系统直接 ACK
了 offset 0，但违背核心要求的原身份重试，以及未决交付必须先处理的因果边界。若调用方再次重发
offset 0，也不能把更改 dispatch time 后产生的新状态轨迹称为原冻结投递的重试。

**建议：** 未确认输入存在时，不得把其目标 owner 推进到 `pending_dispatch_us` 之后。
可在原 dispatch time 继续运行完成/释放额度；先重试并确认该投递，再允许之后的 Clock。
公开 pump、自动 ticker、暂停恢复后的时钟路径须使用同一规则。

**证据：** `/tmp/eventflow-core-architecture-pending-probe.py`、
`/tmp/eventflow-core-architecture-pending-probe.log`；运行目录
`/tmp/eventflow-architecture-pending-zpdybad6`。

**处置：** 已在审查中即时通知 root；待生产整改及回归测试记录。

## P1-B：部署未验证 worker 的全局 shard_count

**位置：** `src/service/coordinator_service.cpp:167–196,233–246`；
`src/service/compute_service.cpp:222–227,285–294`。

Meta 核对 node_id、版本、Catalog fingerprint、实际 shard 列表及 owner，却未核对 worker
报告中的 `shard_count`。Compute 的业务 hash 校验使用其自身配置。因而相同 shard ID 和
相同 Catalog 不代表相同路由算法：Meta 与 Compute 可以对合法 key 得出不同归属。

独立四进程探针配置 KV 3 个物理 shards、Meta 2 个 data shards、Compute 0 使用 count=2、
Compute 1 使用 count=3 且持有 shard 1。没有修改生产二进制。

```text
deploy                -> HTTP 200, state=RUNNING
worker_shard_counts   -> [2, 3]
trade(KEY0)           -> HTTP 400 Conflict: business scope does not belong to requested shard
```

部署宣称已确认所有 worker 后，第一条合法输入便无法处理，违反启动前完整安装及路由一致性。

**建议：** 安装请求或安装回执固定并验证全局 `shard_count`；缺字段或不一致应在 RUNNING
发布前拒绝。其余影响执行语义的部署字段也应采取明确的固定/验证方式，不能只依赖 Catalog。

**证据：** `/tmp/eventflow-core-architecture-shard-probe.py`、
`/tmp/eventflow-core-architecture-shard-probe.log`；运行目录
`/tmp/eventflow-architecture-pending-jh81bzoh`。四个 EventFlow 子进程退出码均为 0。

**处置：** 已即时通知 root；待生产整改及不一致配置回归。

## P2-C：核心的异步请求与函数线程执行须分别说明

**位置：** `examples/feature_computation_demo/feature_computation_demo.cpp:458–467,663–699`；
同目录头文件 `:89`；`src/runtime/function_executor.cpp:598–624`；
`src/service/compute_service.cpp:488–494`；`src/service/coordinator_service.cpp:434–436`。

真实 Compute 创建的是 `SimpleFunctionExecutor`；`run_ready` 同线程执行 callback。
Compute 调用时持有 shard mutex，Meta 又持有全局 gate 等待 pump fanout。于是现有集群具备
“先持久受理、之后显式/后台 pump 完成”的异步请求语义，但没有在核心路径接入独立函数线程池；
一个耗时 callback 会占住该 shard，Meta 等待期间 SDK 数据入口也被阻塞。

不同 Compute 的 fanout 确实并行，不能因此声称运行中的函数已与 owner 状态推进解耦。
`ThreadPoolFunctionExecutor` 组件存在，也不等于集群使用了它。

**处置建议：** 在核心运行手册明确当前 deterministic executor 和阻塞边界；若本轮“异步”要求
包含耗时函数执行期间 owner 继续接纳输入，则应在交付前接入已有线程池并验证该行为。
不要求跨 Compute 任意函数调度、在线扩容或性能基准来扩大本次范围。

## P2-D：请求审计尚不能还原完整条件 witness

**位置：** `src/runtime/event_processor.cpp:765–766,2885–2890`；
`examples/feature_computation_demo/feature_computation_demo.cpp:426–433`；
`include/eventflow/request_runtime.hpp:6–14`；`src/service/compute_service.cpp:443–469`。

Pending C 的 `TriggerOccurrence` 保存条件读取和致因事件，但受理后会删除 intent/outbox；C8 的
admission factory 返回空参数，只保留 trigger_id/request_id。当前请求查询返回 plan、候选 epoch、
capture/admission revision、时间和 owner，能够证明捕获版本及 Latest 指向哪个请求，却没有持久并
公开当时触发的 volume/quote 条件值。后续触发与捕获之间市场变化时，不能仅凭 request audit 还原
“为何触发”的完整状态。架构文档 `§7.4` 对长期保留 `TriggerWitness` 的描述因此是产品目标。

**处置建议：** 明确已有审计字段和剩余边界；不把完整 explain_request/witness 宣称为核心完成。
是否持久扩展 witness 可随通用观察 host 后置，不阻挡已有条件判断与固定输入请求链路的成立。

## P2-E：交付状态索引与架构现状段须同步

`specs/INDEX.md:5,18` 仍写当前交付先完成扩展，且 Meta/SDK/一致 checkpoint 待实施，未链接最新
五小时核心规格。架构文档 `:311–323` 也把已出现的核心元数据 API 整体写成尚未提供。
这些是有明确“当前”含义的旧状态，不能只靠文首“包含目标”消除冲突。

**处置建议：** 最终交付时将核心规格纳入 Active 索引，区分核心 Meta 已实现与完整 M5 后置项，
更新任务 owner/状态、实际三个 demo 和最终定向验证证据。尚未完成的完整产品规格继续保持开放。

## 已核实成立的架构链路

| 要求 | 源码及证据 | 判定与边界 |
| --- | --- | --- |
| 1 KV + 1 Meta + 多 Compute | `tests/core_cluster_test.cpp:334–341` 检查 4 个不同 PID；`src/service/compute_service.cpp:339–366` 按 shard 创建 owner | 真实进程和共享 Remote KV，非同进程对象代替 E2E；存在 P1-B 配置验收缺口 |
| SQL 与级联 MV | C8 定义 `feature_computation_demo.cpp:193–203`；附加 DDL `:437–447`；Meta DDL/install `coordinator_service.cpp:460–478,233–246` | 统一 Catalog，启动前可增加公共表与级联 MV；固定单字符串业务 key |
| Feature DAG/共享祖先 | C8 features→prediction、features+prediction→risk，`feature_computation_demo.cpp:239–260` | 复用统一函数视图图和 RequestMachine，不存在另一份独立业务 DAG |
| 条件/Clock/事件依赖 | C8 guard `:403–436`，读取级联成交量条件；C/D 要求 B 已受理，`event_processor.cpp:2865–2874` | 与 C8 明确契约一致：B 已受理即可，不要求 B 已成功；通用用户规则集群注册后置 |
| 捕获与 Latest | `request_runtime.cpp:698–750` 同轮冻结输入、受理序和 revision；`engine.cpp:1097–1133` 按候选 epoch/成功状态/受理序选择 | Latest 不按完成先后覆盖；市场变化不改已捕获输入；无 MVCC 依然可成立 |
| Meta bootstrap SDK | `cluster_client.cpp:258–305,310–328` 只向 Meta 调用；Meta 负责按 scope 路由 | 当前是 Meta 代理；不宣称 SDK 已有完整直连路由缓存 |
| 输入确认 | Compute 返回独立 input_committed/duplicate/operation_status，`compute_service.cpp:386–398`；Meta 确认后更新前缀 `:329–369` | 合法 offset 跳号按实际投递序列处理；不要求数值连续。P1-A 尚需修复 |
| 一致 checkpoint | `coordinator_service.cpp:396–432` 关闭准入、全 worker quiesce、确认源切面、持久冻结意图；`:372–394` 核对真实 snapshot revision/receipt | 包含控制 shard 及运行状态，不把 KV flush 冒充分布式 checkpoint |
| Rocks 整体恢复 | `coordinator_service.cpp:608–632` 核对冻结控制 revision 和全部 data shard；`core_cluster_test.cpp:387–455` 重启全部进程、查询前值、原身份重试及后续输入 | 显式停机、新活动目录、固定图与 owner 的恢复；不声称 HA/脏实例自动续跑 |

## 测试与证据边界

- 阅读 `/tmp/eventflow-core-integrated-tests.log`，三个目标均通过，144.61s；该日志由主 owner
  提供，本 reviewer 没有重跑全套构建或全部测试。
- 阅读 `/tmp/eventflow-clock-trigger-demo.log` 的 `CLOCK_DEMO_OK` 及 capture/admission/Latest 输出；
  阅读 `/tmp/eventflow-checkpoint-demo-run.log` 的冻结 revision、全进程恢复及原输入去重、后续值变化。
- 审查截止时 `/tmp/eventflow-core-all-tests.log` 只出现 43/44 目标完成，不能声称整套通过；
  condition demo 运行及 Release/格式门禁由其他 owner 提供最终证据。
- 本 reviewer 自行运行的是上述两个独立故障/配置探针；现有正常路径通过不覆盖这两个故障条件。
- 原始题面中的新闻 headline/sentiment 等字段并非当前 C8 输入列的完整复制；本次明确采用 C8
  作为已批准核心 recipe。文档应称它为题目场景的确定性示范，不宣称原始新闻格式已有完整适配器。

审查时关键文件 SHA-256：

```text
coordinator_service.cpp  a9b7b83fe55a4899d44c8027dafed1200ad08adf7fd7ad123587871b11b04f75
compute_service.cpp      bd533142f7cc4600c394eb1824694aaf29e2aa1214ad1d7f4fd11fbd91c7dcb4
feature_computation_demo.cpp e540cdd8e72a6ca5ac869f242e12c37349164b5f229cda56c1585a5fbecac687
build/bin/eventflow      5192d95225b29411c140f683ae6fffd0ffba3a8be8c65cc618355c718e6cfddb
```

## 整改登记

| ID | 严重度 | 截止本报告状态 | 关闭条件 |
| --- | --- | --- | --- |
| P1-A | P1 | 已复现，已通知 root | 原投递失败后 pump/ticker 不越过冻结时间，原样重试成功且前缀正确；针对性回归 |
| P1-B | P1 | 已复现，已通知 root | 不一致 shard_count 在 RUNNING 前拒绝；正常双 worker 部署与恢复回归 |
| P2-C | P2 | 已通知 root | 明确 executor 边界，或接入线程池并提供相应验证；不混淆请求异步与执行非阻塞 |
| P2-D | P2 | 记录实际边界 | 文档只承诺当前可查询的请求 provenance；完整 witness 保留明确后置 |
| P2-E | P2 | 待最终 handoff 同步 | specs 索引、架构现状、demo/门禁与完成状态一致 |

以上属于同一轮审查。主 owner 修复合理问题并提供定向验证即可，不要求再开启新的全局审查轮次。


## Owner整改记录（2026-09-14，本轮不新增review）

三个P1已完成生产整改及定向验证：未确认的新投递把pump/ticker限制在原dispatch时间，旧已确认重放另行区分；冻结checkpoint未完成时只允许只读操作及完成原checkpoint，pause/deploy/pump/resume不能改写边界；Meta发送并验证逻辑shard_count，Compute在任何KV初始化前拒绝不一致。

修复前3个新增C++故障回归均RED：`/tmp/eventflow-coordinator-review-regressions/before.log`。修复后CoordinatorService全部10项通过（36.8s），并与类型/SQL/表达式/Remote回归一起通过：`/tmp/eventflow-final-fixes-tests.log`、保存的逐项结果`/tmp/eventflow-core-review-fix-ctest.log`。覆盖新输入投递前失败、快照创建前失败且插入控制命令、错误worker分片数不初始化KV；同时保留丢失已创建快照响应、旧输入重放与恢复用例。

本记录关闭上述P1复现，不能代签随后ThreadPool接入或最终完整产物验证。运行时专用函数池接入与最终Release/格式/全量测试仍在完成。完整TriggerWitness的长期保存按核心边界后置，实际RequestManifest捕获/受理版本和固定输入已保留；当前能力与完整产品设计分别说明。


## Owner 最终交付闭环（2026-09-14）

本节是实施者对同一轮独立审查的最终处置，不改变报告原始发现、原快照或 reviewer 自行运行的范围，不新增审查轮。

三个去重严重问题已关闭：未确认原投递不能被 Clock 越过；不完整 checkpoint 冻结期间禁止其它控制写；错误全局 shard_count 在 Compute 初始化前拒绝。原故障回归保留，修复进入最终全量测试。Compute 已使用真实线程函数池，owner 只做有界推进/提交；SDK drain 固定逻辑时间处理 work_remaining，不重试不确定控制失败。

最新生产具名初始化调整之后，Clang Debug/Bear（81 个条目）、Release O3/ThinLTO（无 sanitizer/GTest）、45/45 CTest（414.61s）及六个 Release run.sh 示例均通过。134 文件清单和服务二进制哈希见[最终验证记录](../verification/final-validation.md)，其中保存构建、逐项测试、示例和 AST/工具检查；旧 44 项日志不作为当前 45 项替代。

原题六字段现由独立嵌入式 news_feature_demo 明确展示；它不表示远程固定 profile 已变成任意用户规则上传平台。完整 TriggerWitness、动态 UDF/UDTF/UDAF、复杂 Job/在线迁移、Kafka Compose 和通用 observer host 按用户核心优先范围后置，Process Function 不在当前验收。文档已据此同步。

合理发现的实现与源码质量验证已经闭环；运行包最后重打包/解压运行证据由 root 在统一验证记录封口，不用该待办否定已完成的限定整改，也不提前声称未验的制品已通过。
