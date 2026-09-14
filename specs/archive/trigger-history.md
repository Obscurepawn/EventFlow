# 触发器与事件生命周期：决策和验证历史

本归档合并原 CEP 提案、CEP 组件/宿主及 EventProcessor 的四份实现交接，删除重复的
接口清单、临时分工与已过时待办。它不是新的独立审查，也不描述当前交付状态。
现行语义以[运行时契约](../runtime-contracts.md)和[CEP 规格](../cep-enfa.md)为准；
最终产物证据见[验证报告](../verification/final-validation.md)。下面的测试数量、日志与指纹均属于当时版本。

<a id="cep-design"></a>
## 有界 E-NFA 的设计选择

用户要求多起点、重叠匹配、历史条件与状态读取，因此保留简单 FSM 处理条件/Clock，
另外实现真正的多路径 E-NFA；两者共用 Catalog、唯一 shard owner 和提交协议。
没有把 EFSM 的单状态改名为 NFA，没有另建可独立编辑或提交的计算 DAG。

原调研固定 Apache Flink commit `189b6ca8313ada196596cb53315866e7a531d441`：
[NFA](https://github.com/apache/flink/blob/189b6ca8313ada196596cb53315866e7a531d441/flink-libraries/flink-cep/src/main/java/org/apache/flink/cep/nfa/NFA.java)、
[编译器](https://github.com/apache/flink/blob/189b6ca8313ada196596cb53315866e7a531d441/flink-libraries/flink-cep/src/main/java/org/apache/flink/cep/nfa/compiler/NFACompiler.java)、
[SharedBuffer](https://github.com/apache/flink/blob/189b6ca8313ada196596cb53315866e7a531d441/flink-libraries/flink-cep/src/main/java/org/apache/flink/cep/nfa/sharedbuffer/SharedBuffer.java)、
[条件上下文](https://github.com/apache/flink/blob/189b6ca8313ada196596cb53315866e7a531d441/flink-libraries/flink-cep/src/main/java/org/apache/flink/cep/pattern/conditions/IterativeCondition.java)、
[after-match 策略](https://github.com/apache/flink/blob/189b6ca8313ada196596cb53315866e7a531d441/flink-libraries/flink-cep/src/main/java/org/apache/flink/cep/nfa/aftermatch/AfterMatchSkipStrategy.java)。
只借鉴模式语义与结构；源码当时放在 `/tmp/eventflow-cep-enfa-research/`，没有复制到项目。
下面的 KV 原子协议是 EventFlow 自身的设计，不声称与完整 Flink API 兼容。

| 选择 | 原因及边界 |
| --- | --- |
| TAKE / IGNORE / PROCEED | TAKE 消费并记历史；IGNORE 跳过本事件；PROCEED 不消费。一次输入在一路径最多 TAKE 一次。 |
| Strict / FollowedBy / FollowedByAny / NextOfKind | 原始流邻接、跳过不匹配、保留重叠分支、仅观察第一条同 kind 事件是四种不同语义。负面 News 必须消费 NextOfKind，不能继续等待下一条正面 News。 |
| event/then/or_else/optional/有限 repeat | 编译有限图；拒绝无限重复、negative、greedy、可空重复体及整体零事件匹配。外层 Strict 约束 optional 分组首个实际 TAKE。 |
| 路径历史与当前 MV 分开 | history/registers 属于各自路径；全部 guard 读取同轮冻结的声明 MV。旧时刻 MV 值必须显式复制，不能以后读当前值冒充历史；不需要 MVCC。 |
| 精确共享前缀 DAG | 事件不可变共享，history 节点记录精确前驱；分支释放不破坏仍被引用的历史。没有引入 Flink Dewey 多前驱边压缩。 |
| 三种 after-match | NoSkip 立即输出；SkipToNext/SkipPastLastEvent 按持久接纳序号排序，必要时等待更早起点。待裁决匹配冻结历史、registers、reads 与 matched_at，发布另记 emitted_at。 |
| processing-time within | 区间为 `[first_take, deadline)`，等号先到期。event-time 重排、跨源总序与无限模式另行设计。 |

原提案的可选 `captured_views` 不是通用历史查询；Process Function 不属于本次交付。
CEP 历史表示已接纳事件，基表更新或 TTL 撤回不会自动抹除过去匹配；候选失效需显式 epoch/Reset。
通用集群 observer host 的后置范围以[核心交付](../core-delivery.md)为准。

<a id="cep-component"></a>
## CEP 组件的边界与证据

`CepProgram` 是纯状态转换组件，`CepState` 为不可变候选；step/advance 失败不会部分
修改原状态。路径和 shared history、pending matches、时间/序号、完整模式/预算指纹进入
严格 codec；恢复校验图连接、邻接、deadline、typed 值、引用完整性及不可达垃圾。
callback 改动必须升级版本，不能用函数地址生成语义指纹。组件不负责源 ACK 或 KV 提交。

预算覆盖图节点、paths、事件/历史、pending matches、编码/输出字节及 step work。
接纳/编码前用有界 `expire(INT64_MAX)` dry run 验证未来 timeout 和待裁决输出可释放；
容量失败返回 ResourceExhausted，不截断合法匹配。预算是逻辑保留量，非进程 RSS 保证。

历史 TDD 先复现重叠起点丢失、峰值 shared buffer、清理容量、NextOfKind、恢复时间、
转义读别名及 nullable group 的 Strict 边界。独立组合枚举 oracle 覆盖 `{A,B,X}` 长度
0..5 的 364 条流、4,368 个流/配置组合、19,692 次前缀推进/codec 恢复。

最终组件历史证据：23/23、0 skip、84.817 秒，Clang 18/C++20/Werror/ASan/UBSan/LSan；
`/tmp/eventflow-cep-component/final-test.log`。关键 RED 在同目录
`boundary-red.log`、`final-boundary-red.log`、`grouped-red.log`。当时冻结 SHA-256：

```text
cep.hpp      617b33d447f5ec27ff2ae3f53c27f4a6213c22fd01729f00e45eced37942bfd4
cep.cpp      b43e4cc55dff120d2f988ac64b081dcd23d08762d7481ec6770496b3e7bd5927
cep_test.cpp 9d9dd1103ebfddb06cd55c94c9255a1ad227da4b87d6194816d49c62af62a578
```

<a id="cep-owner"></a>
## 宿主原子性与控制容量整改

EventProcessor 先按原 SourceIdentity 去重，稳定输入/MV，冻结全部 FSM/CEP reads，再
推进规则或 sink；输入、生命周期状态、source positions、outbox 与 typed sink 同批提交。
UNKNOWN 保留最终原批及候选状态，resolver 不再次调用 guard 或分配匹配身份。
typed sink 仍不能任意写 RequestCapture/FunctionView/RuntimeStatus。

CEP 宿主使用私有可丢弃 prepare：prepublish ResourceExhausted 丢弃完整 overlay，
不推进 source/FSM/CEP/clock。永久 callback/schema 错误仍 fail-stop。已成功的到期
子轮次通过 committed_rounds 保留，后续输入未确认不能 ACK。timeout 清理本身不额外
生成业务 occurrence；排序等待的完成匹配释放时输出原冻结 witness。

独立审查发现两个重要反例：简单 FSM timeout 与 pending CEP 同时输出会挤满 outbox；
当前 snapshot 装得下但释放后完整 snapshot 变大，会使已接纳状态永久无法结束。
整改 `reserve_control` 同时校验当前状态和 ACK 既有可消费 outbox 后的未来控制 envelope，
覆盖全 key 的 FSM/TTL/periodic、最大时间宽度、二次 JSON 转义、CEP 周期增长及 SIZE_MAX
算术；不运行用户 callback。恢复也重复该检查。混合 periodic CEP 的声明预算不兼容时
在 Engine 初始化前拒绝，不能依赖默认配置碰巧可用。

历史证据保留在 `/tmp/eventflow-cep-owner/`：`red.log`、`reservation-red.log`、
`review-red.log`（8 个 RED 与 4 个容量 1 正向约束）；整改后
`review-final-cep.log` 为 100/100、9.202 秒，`review-final-baseline.log` 为 97/97、
9.366 秒，构建见 `review-fix-build.log`。四条存储路径均为真实 Memory/Rocks/HTTP。
原 76 项通过仅是整改前基线，不能用来关闭上述缺陷。

[独立 CEP 宿主审查](../reviews/CEP-owner-fresh-review.md)保留精确冻结指纹及关闭证据：
组件 23、宿主 100、旧生命周期 97、Engine 3、独立探针 6，共 229 项。
完整 witness 在 pending/outbox 中冻结，不表示消费后已经实现长期历史致因归档。

<a id="event-processor"></a>
## EventProcessor 的早期生命周期决策

一个 owner 独占 Engine；每次 process/advance 固定 now。到期子轮先 TTL delete 与
MV 撤回，再冻结 reads、推进 timeout、expiry 与合并 Clock，之后才处理同一 now 的源输入。
错过的 Clock slot 只作 scheduled 审计，不能重新捕获其过去状态。首次 scope 不补发其
出现前时隙；TTL generation 防止旧 deadline 删除新行。

精确 dedup receipt 有条数/字节窗口，source high-water 单独保留；过旧重放明确拒绝，
本 shard processed_positions 不冒充全局 broker 前缀或 watermark。整份有界 lifecycle
snapshot 简化原子性，但复制成本随活跃状态增长；没有声称增量 Timer spill 或最优吞吐。
durable outbox 不依赖一次性返回 vector；后续子轮失败不会丢已提交 occurrence。

早期 93 项四后端证据在 `/tmp/eventflow-processor-build/Testing/Temporary/LastTest.log`。
随后独立生命周期审查修正旧 deadline 被当作 callback 当前时间的问题：实际 event.time_us
使用 now，原 deadline 另存 scheduled_time_us，新相对 timer 从 now 起算。最终整改和
97 项基线见[生命周期审查](../reviews/M3-lifecycle-fresh-review.md)与
[里程碑记录](../reviews/M3-review.md)，不能沿用早期 93 项替代新语义验收。
