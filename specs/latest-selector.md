# Latest selector 接口与语义

Status: active（核心已实现，最终源码门禁待确认）
Owner: root
Updated: 2026-09-14

## 目标与当前证据

Latest 是唯一 Catalog 图内的受控关系：当前候选内，只让**整个 Request 已成功**且有结果行的请求参与比较，以受理时的 `admission_seq` 选最大者。候选切换或失效立即撤回当前投影；旧请求仍可正常完成并保留历史。Latest 可被 SQL 继续派生，A 的完成轮产生 LatestEmbedding 变化后可以触发 B readiness。

依据 [请求规格](request-functions.md)、[运行时契约](runtime-contracts.md) 和[函数视图提案](archive/request-history.md#function-design)。已实现的接口事实是：

- `include/eventflow/catalog.hpp` / `src/sql/catalog.cpp`已有LatestView和typed Candidate/RequestStatus annotation，统一dependencies、snapshot与fingerprint；RequestStatus的请求域放在annotation，关系自身不赋予请求lineage。
- `include/eventflow/request_machine.hpp`：Manifest 已持有 `candidate_epoch`、`admission_seq`、完整 identity；Request phase 与节点成功分开。
- `include/eventflow/request_runtime.hpp` / `src/runtime/request_runtime.cpp`已有request_status_relation绑定；capture签名继续接收RequestAdmission，Manifest在capture ready后创建。finalize按整Request phase产生受控status，与record/index和companion同批。
- `src/runtime/engine_internal.hpp` / `src/runtime/engine.cpp`已有typed host授权、两阶段Latest/SQL稳定、初始化marker与winner/provenance点读校验；公开/null-Registry open仍拒绝受控关系，绑定Registry的private owner路径已启用。Function worker只返回payload，由host补完整identity。
- EventProcessor v2与examples/feature_computation_demo已经消费该能力，A完成后持久level dirty驱动后续B受理；同轮observer基础已实现，完整通用host绑定按五小时范围后置，不建立持久witness队列。

以下定义和核心路径已实现；初始化/GC、全图资源与通用事件订阅的限制仍按各节保留。实际有效 StoreLimits 接入和恢复的限定审查见文末，不能把 selector 组件证据扩大为任意配置保证。

## 方案选择

| 方案 | 代价与结论 |
| --- | --- |
| 给每个 FunctionView payload 增加 epoch、seq、success | Worker 无可信 Manifest；中间函数成功不能代表 Request 成功；SQL 投影还可能丢字段。拒绝。 |
| Catalog 受控 RequestStatus 关系 + Latest 三输入 selector | 复用关系点读、RowChange、现有 finalize 和 SQL；仅增加两个基表 annotation 与一种 view kind。采用。 |
| Latest 从私有 RequestRecord 按需读取，或把 RequestRecord 暴露成虚拟表 | Engine 需要认识 RequestRuntime 私有编码/索引与恢复逻辑，Catalog 无显式状态输入；做通用虚拟表又超出本切片。暂不采用。 |

不增添可编辑的执行图，不为每个 request 生成 SQL/query 节点，不在 completion 上扫描历史 arg-max。

## Catalog 定义与精确 schema

增加 `RelationKind::LatestView`、`LatestViewPlan`；基表继续使用 `BaseWriteRole::RuntimeStatus`，增加互斥的 `candidate` / `request_status` annotation。annotation 与 Latest plan 都进入 canonical definition bytes、fingerprint、deep clone、snapshot 重建校验和 explain。普通 SQL 旧编码在没有新 metadata 时保持兼容。

以下Catalog接口已在[公开头文件](../include/eventflow/catalog.hpp)实现；所有项目实现继续`noexcept`，正常失败返回Status。

```cpp
struct CandidateDefinition {
  std::string name;
  std::vector<Field> scope_fields;
  std::vector<Field> payload_fields;
};
struct RequestStatusDefinition {
  std::string name;
  std::string request_domain;
  std::vector<Field> identity_fields;
  RelationId candidate_relation = 0;
};
struct LatestViewDefinition {
  std::string name;
  RelationId result_relation = 0;
  RelationId request_status_relation = 0;
  RelationId candidate_relation = 0;
};
StatusOr<RelationId> Catalog::define_candidate(const CandidateDefinition&) noexcept;
StatusOr<RelationId> Catalog::define_request_status(const RequestStatusDefinition&) noexcept;
StatusOr<RelationId> Catalog::define_latest(const LatestViewDefinition&) noexcept;
```

首版按 SDK 在初始 Catalog 声明，不要求实现新的 SQL DDL 语法。`define_*` 仅做 metadata；运行时启用与初始化必须由唯一 Engine owner 完成。

**Candidate schema v1**：可见字段依次为规范 `scope_fields...`、`candidate_epoch INT64 NOT NULL`、`active BOOL NOT NULL`、声明的 `payload_fields...`。主键与 partition 都是 scope；`unique_per_partition=true`；`identity_domain` / `identity_fields` 均空。payload 包含 news_id、text_version、参考价等业务字段，由示例声明，不写死进 Engine。保留字段名冲突、重复 field ID、空 scope、nullable scope 均拒绝；Catalog 重排 field ID 为连续规范序号。

**RequestStatus schema v1**：可见字段依次为规范 `identity_fields...`、`candidate_epoch INT64 NOT NULL`、`admission_seq INT64 NOT NULL`、`phase STRING NOT NULL`。完整物理主键为 `(scope..., request_id)`；partition 仅为 scope；最后一个 identity 字段必须是非空 String 请求身份，与现有 RequestCapture 规范一致。phase 固定为 `ACCEPTED` / `RUNNING` / `SUCCEEDED` / `FAILED` / `CANCELLED`。每个完整请求身份只有一行。

RequestStatus 的 **`RelationDescriptor::identity_domain` 仍为空**，这是生命周期状态关系，不能被 RequestMachine 当作待计算节点。其 annotation 单独保存 `request_domain` 与规范 `identity_fields`，用于校验其对应的请求身份；不赋予 SQL 后继 request lineage。一个 annotation 对应一个 candidate relation，候选与状态 scope 字段的名称、类型、顺序必须一致。RequestStatus 可以被普通 SELECT 查询，但不能成为 FunctionView 请求输入。

**Latest schema v1**：完整复制 result 的可见字段、顺序和 nullability，之后增加保留 provenance 字段 `__ef_latest_candidate_epoch INT64 NOT NULL`、`__ef_latest_admission_seq INT64 NOT NULL`。SQL binder 可以读取生成的保留列，用户定义仍禁止占用 `__ef_`。物理主键与 partition 都收缩为 scope；`unique_per_partition=true`；`identity_domain` / `identity_fields` 均空。结果完整 request identity 只用于源关系点读，不能成为 Latest 物理键。source 若通过 SQL 隐去了 request_id，内部 selector 状态仍保存获胜者完整 identity；公开查询 provenance seq/epoch 仍可判断版本。

Latest 绑定必须验证：

1. 三个输入已经定义且 ID 更小；result 为带非空 request domain 的 FunctionView 或保持完整身份的 SqlView，不允许任意 PublicInput 冒充成功结果。
2. result 的 domain、完整 identity 字段/类型/顺序，与 RequestStatus annotation 完全一致；不能只比较 DataType 列表。
3. 三者业务 scope 一致；status 的 `candidate_relation` 必须等于 selector 的 candidate 输入。
4. Latest 输出不再携带 request domain；后继普通 SQL 可 projection/filter/按 scope lookup join。若要成为新 Request 的输入，必须在该新 Request admission 中复制到它自己的 RequestCapture。
5. kind 与 descriptor pointer 必须一一对应；未知 metadata format、非法 annotation 组合、state_id 碰撞都拒绝。

`dependencies(Latest)` 恰为 result、request_status、candidate（去重）。`dependencies(RequestStatus)` 包含它声明的 candidate；这个受控基表只接受 owner 的生命周期写，不因为候选 RowChange 自动生成请求状态行。其他 ordinary base table 仍无依赖。这个关联与 Latest 的三个实际数据输入都在同一 Catalog 中可解释，不在 runtime 保存计算边。RequestMachine 目标与祖先仍只允许原请求身份子图，不能把 Latest/RequestStatus 拉入自身目标集合。

## Host 写权限与兼容迁移

保留 `CaptureRequest(RoundContext&, const RequestAdmission&)`、`RequestCommitHook` 和 `RuntimeFinalize` 的现有签名。给 `RequestPlanDefinition` 尾部追加 `std::optional<RelationId> request_status_relation`，缺省为空保持原测试/调用兼容。绑定时校验 plan targets 的域与 annotation 一致；属于 Latest 输入域的 plan 必须绑定对应状态关系，否则在 open 阶段拒绝启用。一个域只允许一个 RequestStatus producer binding，多个同域 plan 可共用它。

Manifest 已有的 `uint64_t candidate_epoch/admission_seq` 不改 ABI。新关系 v1 可见类型是 Int64，所以关联该状态关系的 admission 必须满足 `1 <= epoch, seq <= INT64_MAX`，转换前检查；到达上限返回 `ResourceExhausted` 且不消费 seq、不捕获/提交部分状态。未使用此新关系的原 RequestRuntime 行为不变。这个溢出边界不是历史结果数上限，不增加“每 key 最多几个请求”的限制。

在 private `engine_internal.hpp` 提供 typed host factories，返回现有 TableChange：

```cpp
struct RequestManifest;
enum class RequestPhase;
enum class CandidateOperation { Activate, Deactivate };

static StatusOr<TableChange> make_request_status_write(
    RoundContext&, RelationId, const RequestManifest&, RequestPhase) noexcept;
static StatusOr<TableChange> make_candidate_write(
    RoundContext&, RelationId, const Row& scope, CandidateOperation,
    std::int64_t expected_epoch, const Row& payload) noexcept;
```

这是私有 owner capability，不进入公共 SDK。Engine 可以读取 Manifest/phase 的声明字段，不能调用 RequestMachine 方法或链接 RequestMachine 实现库；可在 engine 内部用等价小 typed data struct 消除 include 环。worker、普通 hook、PublicInput apply 均拿不到这种入口。

factory 做规范编码与状态转移校验，并在 RoundState 的私有授权写集中登记 `(relation, identity, exact row)`。`apply_owned` 对带 annotation 的基表必须精确匹配并消费一次授权；普通 `TableChange`、companion、普通 emit、raw T/S/I 写不能制造或覆写这些行。授权仅属于当前 disposable round，UNKNOWN 重试使用原已构造 batch，不重建授权/元数据。保护 selector 的 state_id 命名空间，不能仅沿用“有 SQL plan 才禁止 raw stage”的检查。

RequestRuntime finalize 在 refresh 判定整 Request phase 后：stage record/active-index → 调用 typed status factory → 调用既有 companion → 返回合并写列表。companion 不可覆盖新状态关系；重复身份写冲突整轮拒绝。request status 行从已受验证的 Manifest 与 phase 生成，禁止从用户 arguments、callback row、worker payload、单个 NodeRecord 成功推导 `SUCCEEDED`。admission callback 无须知道 admission_seq。

第一次写状态时，factory 必须在该 admission round 的稳定 Candidate 上确认 `active=true` 且 epoch 匹配，再固定 scope/request_id/epoch/seq。之后更新这些 immutable 字段必须相等；旧请求在候选切换或过期后仍可变为成功/失败，但 selector 会拒绝其成为当前结果。非终态可保持原 phase、ACCEPTED→RUNNING 或直接进入终态；RUNNING 不回 ACCEPTED；终态只接受完全一致重放。普通生命周期不得删除状态行。

Candidate factory 的规则：

- 首次 Activate 只接受 `expected_epoch=0`，生成 epoch=1；之后仅在 `expected_epoch` 等于持久行 epoch 时生成 epoch+1，检查溢出。caller 不能指定任意较小 epoch 或复用已失效 epoch。
- 新 payload 属于新 epoch。相同候选重复消息应由生命周期 owner 在调用 factory 前识别为 duplicate；不能以重复 Activate 再消耗一次 A/候选生命周期。
- Deactivate 需要匹配 current epoch，把 `active` 变为 false并保留 epoch/payload；相同 Deactivate 可幂等。墓碑仍是一行；不允许 false→true 在同 epoch 重新激活。
- 引擎不认识 news_id/text_version 的旧/重复判定。feature_computation 的业务 owner 必须验证它们才申请新 epoch；typed factory 防止 epoch 倒退，不能替代新闻版本去重。
- 已失效/替换的 epoch 永不重新激活；尝试回退、复活或拿旧 expected_epoch 更新返回 Conflict/FailedPrecondition，整个 round 无变化。

这些 typed factories 是 owner 内部写校验，不是新增的存储 writer；其结果仍进入既有同 shard、同 revision 的提交。

## 同一 Catalog 中的增量选择

每个 `(Latest RelationId, scope)` 保存至多一个 selector 状态，复用该 relation 的受保护 `S/state_id` 命名空间，编码格式 `format=1`：

```text
candidate_epoch: int64
winner: absent | { admission_seq: int64, request_identity: typed Row }
```

获胜 payload 存普通 Latest T 行，state 不再复制一份大 payload。state 与 T 行必须同批写入。candidate 不存在时不创建 state；inactive candidate 可保留无 winner 状态，也可删除 selector state，因为 Candidate 墓碑保留了 epoch fence。恢复校验 winner 与 Latest 行 provenance 一致，不能以“不一致就重新扫全历史”掩盖损坏。

stabilize 对 Latest 的每次执行收集两类有限工作集：candidate delta 的 scopes，以及 result/status delta 的完整请求 identities。它由本轮 RowChange 和 Catalog 依赖产生，不持久化另一张调度图。处理次序：

1. 对 affected scope 点读 Staged Candidate 和 selector state。epoch 变化、candidate 不存在或 inactive：撤回旧 Latest T 行并清 winner。同 epoch 的候选变更只允许合法 Deactivate；发现倒退/同 epoch 非法复活是状态错误，不选择历史补值。
2. 对受影响请求按完整 identity 点读 Staged result 与 RequestStatus。两行俱在、phase=SUCCEEDED、candidate active 且 epoch 相同才成为 eligible。
3. 与本 candidate 的 winner 比 seq。更大者替换；更小者只留历史；相同 seq 必须是同一请求身份和相同结果，否则报不一致。runtime 的持久 seq 本来就是唯一单调分配，不加 request_id 字符序 tie-break。
4. 把最终 winner 行作为普通 RowChange 继续同一 Catalog 后继 SQL。没有 eligible 新行时保留原 winner；较新 Request FAILED/CANCELLED 不清掉较早成功值。candidate clear 才撤回当前候选的全部 Latest。

结果在 phase 1 先产生，而 Request 仍 RUNNING 时不会进入 Latest。finalize 判定 SUCCEEDED 并写 status 后，第二次 stabilize 会因该 status identity 点读已有结果，从而选择它。Request 成功但 source SQL 无输出也不产生 winner，不等待、不填 null 伪结果。单个函数成功但另一个必需目标失败时，该 request 永不进入 Latest。

执行多轮 stabilize 时应处理当前阶段新增/净变化并去重，或确保上述比较幂等；不能仅保留第一次 roots 然后漏掉 final status delta。Latest 输出与 SQL 后继都无 request identity domain，所以保留现有 final status descendant 禁止回写请求子图的检查，不放开这个全图保护。

CurrentCandidate、RequestStatus 只接受上述受控写，result 继续 immutable new-only。因此首版不支持“撤销已成功 winner 后回落到次大历史请求”。非法普通 delete/update 拒绝；受控 GC 也不能删除仍被当前 winner 引用的状态或结果。这个限制与失败新请求保留旧成功结果不同。

## 初始化、重启、查询、状态大小与删除

**初始化只允许可证明的空基线。** 最小交付在初始 Catalog 内同时安装 Candidate、RequestStatus、Latest 和后继 SQL，再开放 admission；Engine 初始化事务持久化 Catalog fingerprint 与 `latest_initialized(format=1)` 标记。不得在现有历史上新建空 selector 后宣称它是完整 Latest。

当前切片只支持在初始Catalog声明Latest；尚未实现`ever_admitted`域使用证明或历史backfill，不提供运行后追加Latest的激活路径。后续若开放追加DDL，必须先证明绑定request domain从未受理请求，或实现有完备性证明的有界初始化。可采用每域一个`ever_admitted`单调标记，在第一次受理时与record/status同批保存且不随GC清除；缺少证明按“未知”拒绝。禁止通过max_rows截取前N条历史充当初始化完成；此目标证明不能写成当前已有接口。

正常恢复读取已持久 Latest T 行、selector state、Candidate 与 RequestStatus；不重算所有历史。Catalog/初始化标记不匹配、state 缺损等返回失败并要求显式迁移/恢复。新 owner 仍遵循 C9 的 clean-stop/dirty-run 边界，本提案不增加自动 crash 接管。可选的有限 backfill 是后续显式维护操作，必须先定义完备性证明和激活事务，不属于这里的隐式 fallback。

| 对象 | 大小和访问界限 | 删除条件 |
| --- | --- | --- |
| selector state + Latest T | 每个 scope 每个 Latest 至多一个 winner；O(scope 数 × selector 数)，与已完成请求数无关；completion 做本轮有限 identities 的点读 | 候选失效/替换时撤回 T，清 winner；保持必要 epoch fence 的 Candidate 墓碑 |
| Candidate | 每个已使用 scope 一行，payload 字节受 schema/owner budget 约束；inactive 墓碑也占磁盘 | 普通 expiry 不物理删除；永久业务 key 退役须已停止该 scope 的准入/任务、封住旧源与重放，并把 epoch fence 留在 durable retirement 记录；本切片无此流程则明确保留墓碑 |
| RequestStatus / 函数与 SQL 历史 / RequestRecord | 每个请求分别落 KV，不装入 active book；可继续产生任意多批历史，容量按持久存储与显式保留策略管理 | 非终态、outbox/回放仍引用、当前任何 Latest winner 引用的请求不可 GC；过期终态且无这些引用才可同批清理关联历史。GC 还须保留 request 去重/旧完成 fencing receipt 到重放安全点；本切片未实现该 GC 时保留并说明磁盘会增长 |
| 运行时缓存 | 受 node memory budget/LRU 约束，不为所有历史请求建内存 index | owner/epoch/confirmed-state 失效；UNKNOWN 不可命中绕过原批确认 |

Latest 查询按普通 scope 查询/SQL 返回 0 或 1 行，也支持继续派生；历史表仍可按 scope 在现有查询预算内查询，超预算返回 ResourceExhausted，不伪称只存在前 N 条。若需大历史翻页是独立受限查询能力，不能为 selector 计算引入全历史常驻内存。

每个 admitted request 需为后续 terminal record/status、当前 Latest 行/state 的可能替换、声明后继 SQL、取消/完成 receipt 与控制 outbox 预留可验证预算。RequestRuntime 的 max_record_bytes/max_active_bytes 检查、Engine 每批 rows/bytes/overlay limit 与 EventProcessor 控制轮 reservation 都要计入这些新增写；单独“record 放得下”不足以表示 admission 可成功。值大小用已固定 function output/schema 限额估算，不能假设 Latest payload 很小。预算不足应在 admission 可回滚阶段拒绝；已受理请求的完成/取消不得永久卡在新 trigger 或普通数据队列满。所有预留仍计入 node 总预算，不额外给 completion 无上限内存。

## A 完成唤醒 B 的闭环

```text
CurrentCandidate ───────────────┐
RequestStatus(A) ───────────────┤
CaptureText(A) → Embedding(A) → LatestEmbedding(scope) → SQL readiness
                                                       ↓
                                     completion同批持久level dirty
                                                       ↓
                                      后续控制轮读取稳定readiness
                                                       ↓
                            Pending B → admission/capture of B's inputs
                                                       ↓
CaptureEmbedding(B) + CaptureMarket(B) + CaptureHistory(B)
  → shared SQL/function ancestors → Prediction(B) → LatestSuccess(scope)
```

Latest 的 data edge 在 Catalog；规则读取、触发与 admission 属于 owner 的事件/控制协议。B 捕获 A 的当前 embedding 值到 B 自己的 request identity，不把 A 与 B 的不同 request_id 当函数多输入直接对齐。

EventProcessor在A completion的companion中同批保存候选的level dirty，后续有界控制轮通过B计划capture读取当时稳定的Latest/SQL readiness，不要求再来市场输入。满trigger queue时由独立预留的level槽保留重检查需求；重启仍可继续。B once只在真正受理时与捕获/Manifest同批消耗；UNKNOWN期间不派发B或公开未确认once。level dirty可以合并，它没有冻结每次RowChange的read witness，不能证明任意关系的`10→120→10` crossing订阅；完整通用host绑定是后续产品项；选定同轮同步规则状态/Trigger提交，不使用通用CDC或独立逻辑观察通道。

这一链路必须按实际 round 的读写关系调整 EventProcessor 反馈检查，允许 A 稳定输出驱动后续 B；仍拒绝同轮把规则自身输出送回自身请求输入的非法反馈。不能简单删除原 reachability 检查。

## 持续回归轨迹

| 轨迹 | 必须观察的结果 |
| --- | --- |
| Current epoch=7，B seq=10，D seq=11；D 先成功，B 后成功 | Latest 保持 D；B 的结果/终态历史可查询；后继 SQL 只看到实际 winner 变化 |
| B 较早成功，较新 D 失败/取消 | Latest 保持 B；LatestRequestState（独立业务状态）可以表示 D 失败，但不能混作 LatestSuccess |
| 中间 Embedding/Feature 节点成功，另一个目标仍 running 或失败 | RequestStatus 未 SUCCEEDED 时该 request 不能进入 Latest |
| result 先写，finalize 后 Request 成功 | 同一确认 commit 中 record、status、winner、后继 SQL 一致，不能发布半成品 |
| request 成功但结果 SQL filter 无行 | 无新增 Latest；请求不永久 waiting |
| epoch7→8，旧请求在8激活后完成 | 7 的 Latest 同批 clear；旧结果只留历史；8 仅从8的新成功请求产生 winner |
| candidate expiry，无新 source input | active=false 与 Latest delete/后继 SQL 撤回同批；同 epoch 复活及旧 expected_epoch 更新被拒绝 |
| A 完成且市场之前已 Ready，之后没有市场输入 | completion同批持久level dirty；后续控制轮B恰好受理一次，捕获正确epoch的embedding |
| Capture→Function→SQL source 隐去元数据列 | host RequestStatus 仍提供正确 epoch/seq/success；selector 用完整隐藏 identity 点读对齐 |
| epoch/seq 类型错误、域语义不一致、snapshot 篡改、public/companion/raw 写入 | 定义/打开/写入失败且无部分状态；epoch/seq 溢出不消耗 sequence |
| 第一个提交 UNKNOWN，resolve 后 replay/旧 attempt 回包 | 原完整批确认；不重捕获、不重计算 selector 逻辑发布、不重复 B once |
| clean reopen；持久有 winner 与历史 | 最新值/epoch/seq准确恢复；不扫描全部 request 历史；非法缺初始化标记或追加历史 Latest 激活被拒绝 |
| 小额度、max_active_requests=1、trigger queue 满、最大允许结果 | completion/cancel 可通过保留额度提交并释放占用；metadata/Latest/SQL 新写都计入预算 |
| 单 scope 连续大量 D、交替成功/失败/候选修订 | active 内存与 selector 状态有界，结果历史不因固定小行数上限被静默截断；明确验证持久历史增长/GC边界 |

## 当前实现与证据

[Engine](../src/runtime/engine.cpp)、[RequestRuntime](../src/runtime/request_runtime.cpp)和[EventProcessor](../src/runtime/event_processor.cpp)已接通 typed Candidate/status、Latest 增量选择及文本编码完成后的 level 唤醒。[应用定义](../examples/feature_computation_demo/feature_computation_demo.cpp)和[测试](../examples/feature_computation_demo/feature_computation_demo_test.cpp)均在主树；旧临时工作目录不再是待合并来源。

[首轮审查](reviews/M4-latest-runtime-fresh-review.md)发现 scoped scan 缺失 T 行未拒绝，以及 Latest/后继 SQL/GROUP BY-only 状态的 mutation 预留遗漏；[限定整改复验](reviews/M4-latest-remediation-review.md)以七个独立强化 probe 关闭具体反例。完整 byte/read/store/companion 预算随后由[有效预算独立审查](reviews/M4-effective-budget-review.md)验证，不能将两个阶段的证据范围混为一谈。

[owner 审查](reviews/M4-owner-fresh-review.md)与[应用整改审查](reviews/M4-app-remediation-review.md)分别保留单 owner、partial rounds、TryOnce/cap、恢复和 driver UNKNOWN/批量 completion 问题的闭环。报告里的快照及测试数只证明限定阶段；最新核心全量门禁统一见[核心交付](core-delivery.md#验证证据与剩余门槛)。

SDK `read_request` 返回实际 capture revision R、admission revision R+1、受理时间/序号和 candidate epoch；SQL 查询 revision 为当前读取版本。occurrence 消费后尚无独立完整 TriggerWitness 历史，不能从 Manifest/Latest 推断每项旧 guard 读值已经保留。

## 下一步与边界

最终全局审查已经并行完成一轮，合理问题已修复；当前等待最终源码门禁和制品核对，不重开审查。通用观察 host、GC/backfill、任意 Schema 迁移、复杂 Job 和 Kafka Compose 后置。四种 KV adapter 的组件/app 契约与真实多进程/Rocks 恢复分别记录，均不代表真实 broker 验证。
