# README 图示独立语义审阅

Date: 2026-09-15
Reviewer: diagram_semantics_review
Status: needs targeted fixes

## 范围与结论

本报告是本次同轮 fresh-context 独立全局审阅中的语义审阅。检查用户授权的根 README 系统架构图重画及新增消息处理时序图，并对照“保留原图细节、仅本地无界面 Chromium/图片查看、README 不提供 HTML 链接、最终图示只保留两张 PNG”的追加要求。

实际读取了根/文档/规格约定、[本轮规格](../readme-archify-diagrams.md)、[README 系统结构](../../README.md#系统结构)、两份暂存 JSON 和当前关键源码；使用本地 `view_image` 逐张查看两张目标 PNG，并对照旧图 `docs/image/eventflow_streaming_architecture.png`。未使用 MCP 浏览器或用户 Edge，未运行产品构建、测试或真实 Kafka。

发现两项 P2 文档准确性问题，需要修正后定向核对。主要原子提交、固定输入、输入 ACK、UNKNOWN、Clock、checkpoint 和后置范围说明与当前实现相符；本结论不宣布产品或其他并行任务验收通过。

## 原发现

### S1 / P2：时序图将正常就绪路径的 submit 放到了源 ACK 之后

- 位置：消息时序 PNG 的“受理与确认 / 后台计算与结果”两段；原 JSON 消息 `input-response`、`ack-source`、`dispatch-function`，分别位于 y=418、446、474。
- 当前图按纵向顺序表达“受理确认 → 返回 input_committed → ACK → submit”。README 同时将该图定义为“输入就绪且计算成功的路径”。当前正常路径在输入调用内部完成受理、领取和提交给执行器，因此图中的这个调用顺序与源码不符。
- 源码证据：[ComputationRuntime::ingest_source](../../src/runtime/computation_runtime.cpp) 第 228–235 行先执行 `process`，随后在操作成功且非重复时调用 `pump`，然后才返回；同文件第 238–247 行由 `pump` 调 `claim_ready`，第 186–204 行先持久领取并确认，再执行 `executor_->submit`。之后 [ComputeService](../../src/service/compute_service.cpp) 第 489–503 行才能返回输入响应，[CoordinatorService](../../src/service/coordinator_service.cpp) 第 645–706 行保存控制进度，[SourcePoller](../../src/ingest/source_poller.cpp) 第 56–75 行才执行 ACK。
- 影响：只阅读 PNG 会误认为派发总要等待源确认，或者误判 ACK 与函数执行之间的先后关系。README 第 437 行的“submit 也可能先于 ACK”保留了协议层面的独立性，但没有纠正图中所选正常路径的顺序。
- 建议：将 `dispatch-function` 移到受理/领取确认后、`input-response` 前；保留“ACK 无需等待函数完成”。如要表达执行器背压等延迟派发路径，应显式标注该条件，而不是用它代替当前正常路径。
- 原始处置：待实施者定向修正与重新导出；本报告不修改图示。

### S2 / P2：缓存节点把 CachedStateStore 的职责归给了 RemoteStateStore

- 位置：架构 PNG 的“有界缓存 / RemoteStateStore”节点及 README 第 421 行对应职责表。
- 当前图将节点预算、owner/epoch 校验、已确认状态缓存和 UNKNOWN 屏障直接映射为 `RemoteStateStore`。真实部署使用独立的 `CachedStateStore` 包装 `RemoteStateStore`；后者负责 HTTP KV 传输和回执解码。
- 源码证据：[ComputeService](../../src/service/compute_service.cpp) 第 453–466 行按所属 shard 划分节点缓存预算，创建 `CachedStateStore` 后传给 `ComputationRuntime`；[cached_store.hpp](../../include/eventflow/cached_store.hpp) 第 11–18、36–41、60–63 行给出预算、串行 owner、UNKNOWN 与 epoch 合同；[cached_store.cpp](../../src/storage/cached_store.cpp) 第 220–230、307–340、378–385 行实现确认检查、原提交屏障与 epoch 失效。[database_main.cpp](../../src/service/database_main.cpp) 第 166–168 行创建底层 `RemoteStateStore`；[remote_store.cpp](../../src/storage/remote_store.cpp) 第 129–145、312–343 行是 HTTP 调用和返回错误转换，没有图中所述缓存预算/epoch 管理。
- 影响：这是面向源码的架构图，当前组件名会把读者带到错误实现，也掩盖缓存可包裹其他 `IStateStore` 适配器的分层。
- 建议：节点改为“有界缓存 / CachedStateStore”，将 `RemoteStateStore` 写入通往共享 KV 的 HTTP 边或邻近说明；同步 README 组件表，保留原有预算与 UNKNOWN 语义。
- 原始处置：待实施者定向修正与重新导出；本报告不修改图示。

## 已核实的语义

| 检查项 | 本轮当前源码证据与判断 |
| --- | --- |
| 主动消费、固定投递身份、重放位置 | `source_poller.cpp:40–57` 固定消息与处理时间；`coordinator_service.cpp:231–245` 从数据库位置 seek，`619–625` 先保存原投递及分派时间。图保留生产、拉取、返回身份、确认与重放细节。 |
| 同一 Catalog、多个 Compute、单 owner | `compute_service.cpp:411–414` 校验绑定结果 fingerprint，`453–466` 每 shard 装配运行时，`480–490` 使用 shard mutex；图展开 Compute A、注明其他实例同结构，未引入第二张可独立维护的函数图。 |
| 输入轮原子边界 | `event_processor.cpp:2472–2493、2588–2649、2745–2800` 在关系稳定后读 guard，将规则状态、发出记录/审计与生命周期编码纳入同一个 Engine 提交；成功后才设置 `input_committed`。 |
| capture 与 once | `event_processor.cpp:936–983` 在发出轮固定 match 参数到 intent；`request_runtime.cpp:821–856` 在受理准备阶段读取当前确认版本并记录 capture/admission revision；`event_processor.cpp:3688–3714` 在受理 companion 中消费 occurrence/once 并与 Request 同批提交。图文没有将一次受理等同于函数成功。 |
| UNKNOWN | `event_processor.cpp:2780–2788、4377–4458` 保留待确认状态和原操作；`request_runtime.cpp:564–570` 在未确认时保留任务；缓存的阻断证据见 S2。图文没有把 HTTP 成功、输入确认和后台完成混为一谈。 |
| 函数完成与后继 | `computation_runtime.cpp:279–297` 将带身份的 completion 交回 processor；`request_runtime.cpp:937–966` 同轮写结果、刷新依赖并保存请求状态。图中的结果轮与输入轮分离正确。 |
| TTL、Clock 与水位 | `event_processor.cpp:3830–3940` 在数据输入轮前处理 expiry fence、请求/Timer settlement；`coordinator_service.cpp:1247–1263` 即使无新消息也推进运行循环，并避免超越未确认输入的固定时间。水位由 Coordinator 第 688–704 行维护；图文没有用 event-time 水位替代 processing-time Clock。 |
| 完整 checkpoint | `coordinator_service.cpp:759–799` 检查输入确认、暂停源、quiesce Compute、核对源切面并保存 Catalog/水位/运行时报告；`709–734` 再核对存储快照。README 第 403 行明确 KV snapshot 仅是存储边界。 |
| Kafka 与后置范围 | README 第 311、424、541 行区分 HTTP 示例、已有消息适配器与真实 Kafka 集成后置，保留动态扩展函数、Process Function、在线迁移和 HA 的边界；图未声称这些范围已通过运行验证。 |

## 审阅对象绑定与未验证项

以下 SHA-256 绑定本次原发现对应的 PNG；整改后应记录新值，不能把原值用作最终产物证据。

| 原审阅文件 | SHA-256 |
| --- | --- |
| `docs/image/eventflow-system-architecture.png` | `56eb9f6707d4946a4256255c618e5e7d0ea7fd7e05ddda2f04af8bce6578653c` |
| `docs/image/eventflow-message-sequence.png` | `eb0fef6c9dff46fe330b59557901472e4ff9494fb97137aef469c42bbe154603` |

本审阅只声明源码对照和实际 PNG 目视。未独立重跑 archify showcase、四种桌面浏览器尺寸或 PNG canonical export；相关回执由实施者在本轮规格中绑定最终对象。本地 PNG 导出未包含 JSON `cards`，所以本报告只将 README 中实际可见的补充说明作为已交付语义证据，没有把暂存 HTML/JSON 的卡片误算成 PNG 内容。

作者已明确将在交付前移出 HTML、SVG、JSON、浏览器截图与回执等中间材料；当前尚未清理不列为缺陷。最终交接前仍须检查本轮图示仅留下两张目标 PNG、README 没有 HTML 链接，以及整改后的图文和回执 hash 一致。旧图对照件及其他文档资产是否保留按本轮已确认范围处理，不由 reviewer 擅自删除。

本轮不重复启动 fresh-context 审阅；实施者修正 S1/S2 后定向核对箭头顺序、组件名、README 与最终 PNG 即可。

## 实施者同轮处置（2026-09-15）

- S2 已修正：PNG 节点为 `CachedStateStore`，HTTP 边为 `RemoteStateStore`，README 同步说明分层；重新运行架构图 validate/deliver/visual-check，并目视最终 PNG。
- S1 曾按建议调整自绘时序图；用户随后明确选择其提供的 `trigger.svg`，要求使用对应 PNG。按该最新授权原样转换，保留其阶段分组，在 README 图片下明确“当前就绪成功路径先 submit，再返回输入确认并 ACK”。不将用户选图宣称为严格源码调用 trace，不篡改源 SVG。
- 最终架构 PNG SHA-256：`b334abf8dc3a910a52a71b7338806a70355a6798de8db27acc93a49d00911406`。最终用户指定时序 PNG SHA-256：`7cfac776ed14204935ace0cf17a44af55c4147324045601f134ea2cf5b630652`。
- 仅对实际变更定向复验；未新增独立审阅轮或产品构建测试。完整最终证据见[验证记录](README-diagrams-validation.md)。
- 本次 20 个未引用图示中间文件已移出仓库，保留两张 PNG；原发现与原快照结论保留。
