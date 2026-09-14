# 核心交付全局工程与恢复审查

审查时间：2026-09-14；独立 fresh-context reviewer：`core_global_engineering_review`。
范围依据：[五小时核心规格](../core-delivery.md)、[核心运行手册](../../docs/core_delivery.md)、根目录 `AGENTS.md`。
这是最终两个全局 reviewer 并行的一轮中的工程/恢复审查，没有另起组件审查或第二轮 fresh review。

**初始结论：发现两个已经用真实进程复现的核心恢复阻塞项，当前不能据现有正常路径测试宣布完整通过。**
生产代码保持只读；本 reviewer 只新增本报告。Owner 修复后应在本文末尾追加处置和定向验证证据，不需要自动开展新一轮 fresh review。

## 已确认发现

### CORE-E01：未确认输入期间推进时钟，使合法原样重试收到永久参数错误

严重性：高；状态：待 owner 修复与定向验证。

实际路径：

1. SDK/HTTP 向 Meta 提交一条 `trade`，SourceIdentity 为 `probe/input/0/0`，`now_us="10"`。
2. Meta 先持久化 `pending_input` 和 `pending_dispatch_us="10"`。在代理中仅丢弃这一次 Compute ingest，返回 `Unavailable`，Compute 尚未收到输入。
3. 调用 Meta `pump(now_us="20")`，当前实现返回成功，Compute 的处理时间推进到 20。
4. 再提交**完全相同**的原输入，Meta 复用已保存的 dispatch time 10。Compute 尚无该 SourceIdentity 的已提交回执，`EventProcessor::process` 因 10 < 20 返回 `InvalidArgument: processing time cannot move backwards`。
5. Meta 将这一响应作为参数错误清掉 pending。再调用第三次原请求才会重新分配到 20 并提交。这不满足原样重试应保持有效的契约，SDK 也不会自动重试 InvalidArgument。

源码依据（本轮审查时行号）：

- [coordinator_service.cpp](../../src/service/coordinator_service.cpp)：249–259 的 pump 未处理 pending input；306–315 保存并复用原 dispatch time；322–325 对 InvalidArgument 清除 pending；694–698 的自动 ticker 也不检查 `pending_input`。
- [event_processor.cpp](../../src/runtime/event_processor.cpp)：3373–3392，仅已持久化的重复输入在时间校验前返回；未提交输入使用正常单调时间校验。
- [cluster_client.cpp](../../src/client/cluster_client.cpp)：388–401，只对 Unavailable/UnknownCommit 作有限重试。

建议处置：未确认交付必须与后续时间推进建立顺序约束；可以在原冻结时间完成必要 owner 工作，不能先把该 owner 的处理时间推进到原请求之后再用原时间重新提交。修复应同时覆盖显式 pump 和自动 ticker，并保持提交未知时不错误 ACK。

定向验收：pre-forward Unavailable → pump/tick → 相同 SourceIdentity、关系、行、提交时间重试；最后恰好提交一次，不返回错误的 InvalidArgument，不要求调用者改 payload/time。另保留“已经确认输入在时钟前进后的重复请求不重放计算”覆盖。

### CORE-E02：未完成 checkpoint 期间 pause 修改 control revision，破坏被冻结的同 ID 重试

严重性：高；状态：待 owner 修复与定向验证。

实际路径：

1. 完成正常输入后，调用 `checkpoint("frozen-probe", now_us="30")`。
2. 所有 Compute 成功进入 QUIESCED，Meta 已持久化 `checkpoint.complete=false` 和原 `control_revision`。
3. 在 KV 代理中仅让这一次 `create_snapshot` 在真正创建快照之前返回 `Unavailable`。
4. 调用 Meta `pause()`，当前实现成功并两次保存 Meta 状态，推进 control shard revision。
5. 重试同一 checkpoint。实现重用原冻结 intent，却使用当前 KV 创建快照，随后返回 `Conflict: snapshot control revision differs from the frozen manifest`。此时错误版本的不可变同名 snapshot 已存在；继续同 ID 重试仍不匹配，换 ID 和 resume 也被未完成 checkpoint 状态挡住。

源码依据（本轮审查时行号）：

- [coordinator_service.cpp](../../src/service/coordinator_service.cpp)：372–380 从现有 snapshot 或新建 snapshot 验证冻结 control revision；400–405 对未完成同 ID checkpoint 直接调用 `finish_checkpoint`；507–518 只阻止 resume，pause 仍会执行 `save`。
- [coordinator_service_test.cpp](../../tests/coordinator_service_test.cpp)：147–150 既有故障注入发生在 snapshot **已经创建之后**，且现有测试没有插入 pause，因此没有覆盖上述差异。

建议处置：冻结 checkpoint intent 后，除完成该 intent 的原批确认/发布外，不允许改变其快照边界的控制写入。再次 pause 可以明确拒绝，或作为不修改任何持久状态的幂等成功；不能推进冻结 control revision。

定向验收：snapshot pre-create failure → pause → 同 ID retry；pause 应安全拒绝或保持冻结状态，原 checkpoint 最终成功且 control/data revisions 与 manifest 相符。保留 snapshot 已创建但回包丢失的既有覆盖。

## 本 reviewer 的独立复现证据

运行：`python3 /tmp/eventflow-core-engineering-probe.py`。

- [完整日志](/tmp/eventflow-core-engineering-probe.log)；[隔离脚本](/tmp/eventflow-core-engineering-probe.py)。
- [进程日志与配置目录](/tmp/eventflow-core-engineering-gpwo__95)。
- 真实进程：一个 Memory KV、一个 Coordinator、两个 Compute；Python localhost 代理只在指定入口注入一次 pre-forward / pre-create Unavailable。
- 使用当前 Debug `build/bin/eventflow`；复现时 SHA-256：`5192d95225b29411c140f683ae6fffd0ffba3a8be8c65cc618355c718e6cfddb`。
- E01 得到 `ingest 400 {"error":"processing time cannot move backwards","error_code":1}`；E02 得到 `checkpoint 400 {"error":"snapshot control revision differs from the frozen manifest","error_code":3}`。
- 脚本对两条错误路径均有断言；退出码 0 表示**成功复现错误**，不表示产品通过。四个自有服务进程均退出 0，未出现 sanitizer 诊断。
- 未更改 root build、生产代码或现有测试；未把协议代理当作 Kafka 或 RocksDB 验收替代品。

## 有边界的正向检查

| 检查项 | 当前证据与结论 |
| --- | --- |
| 真实角色与查询路径 | `core_cluster_test.cpp` 启动不同 PID 的 KV、Meta、两个 Compute，并通过只配置 Meta 的 SDK 路由查询；包含 Memory/Rocks 两种存储。测试会复制当前服务二进制，退出时回收子进程。 |
| 条件/Clock、异步 Request、Latest | Core 测试检查 C8 多 key 的请求成功、结果 Latest、级联 MV，显式推进 Clock；服务调用沿用 `FeatureComputationDemo`、`EventProcessor` 和 RequestRuntime。不是重新实现的临时演示计算器。 |
| ACK 与 UNKNOWN | Compute 响应保留 `input_committed`、duplicate 及后续 operation status；Meta 在确认后持久化源进度，Meta UNKNOWN 用原 `CommitBatch` 重试；SDK 不把 HTTP 200/operation Status OK 单独视为 ACK。E01 是这一边界中的遗漏。 |
| 全局源前缀 | Meta 的 pending input 将交付串行化，确认前不允许不同 body 越过。原规格明确 offset 可以合法跳号，前缀按实际投递顺序定义，因此没有将 offset 数值不连续误报。 |
| 一致 checkpoint | 正常路径在所有 owner 静止后核对 source cut、Catalog、数据 revision/receipt 与 control revision，再保存 snapshot；Rocks 恢复测试停止并重新启动全部进程，从新活动目录恢复结果和源进度。E02 是冻结 intent 的异常路径遗漏。 |
| 恢复 fencing | Meta 显式恢复验证冻结 control revision、每 shard revision/receipt、logical node ID、shard assignment、owner、fingerprint；后于 snapshot 的 live KV 被拒绝。静态停机恢复不等于 HA/自动 dirty 重启，手册已明确边界。 |
| 缓存与并发 | Compute 每 shard 一个 `CachedStateStore`，16 MiB 可淘汰预算按本节点 shards 分配；所有 shard 操作持 shard mutex 或独占 admission gate。共享 `RemoteStateStore` client 有自身 mutex。缓存 pending identity 阻止 UNKNOWN 后读/异批提交；confirmed revision 用于失效。 |
| 时间 | `SystemClock` 用墙钟取 epoch anchor，以 steady clock 计算运行期 elapsed；手动时间和自动时间模式有文档边界。E01 表明单个未确认 delivery 仍需与时钟推进协调。 |
| 工程约束 | 检查的核心项目函数使用 noexcept/Status；项目源中未发现 throw/catch 业务控制流。CMake 拒绝非 Clang，GoogleTest 只链接测试 targets，第三方依赖来自 cpp3rdlib。RocksDB `sync=true`、`disableWAL=false` 保留。 |

## 验证门槛与排除范围

本 reviewer 核对了当前 Debug flags：Clang、C++20、`-fsanitize=address,undefined,leak`、`-fno-sanitize-recover=all`。Release 配置 flags 为 `-O3 -DNDEBUG -flto=thin`，没有 sanitizer 或 fast-math；但审查时 `build-release/bin/eventflow` 时间戳早于当前核心 Debug 二进制，因此该旧产物不能作为最新交付的 Release 构建通过证据。最终 Release 和格式检查由 owner 在收敛修复后运行。

Owner 的最新三核心目标日志 `/tmp/eventflow-core-integrated-tests.log` 报告 46 + 7 + 3 个 GoogleTest 用例通过，本 reviewer 没有把它冒称独立重跑。全量 `/tmp/eventflow-core-all-tests.log` 在审查时仍运行；最终结论应记录最终退出状态。三个 demo 的源码和启动脚本存在，实际完成状态以 owner 的运行日志为准；condition launcher 正在修复，本 reviewer 不覆盖其作者工作。

本轮不把 UDF/UDTF/UDAF、Process Function、在线 DDL/图迁移、复杂多 Job、Kafka Compose、完整通用 observer host、自动重分片与 HA 加回核心 completion gate。未发现需要扩大用户批准交付范围才能解决的阻塞项。

## Owner 处置记录

待补：E01/E02 修复位置、定向回归结果、最终全量/Release/格式及三个 demo 证据。修复前保持本报告初始结论，不因已有正常路径测试全部通过而关闭这两个发现。


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
