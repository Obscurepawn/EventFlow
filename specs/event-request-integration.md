# EventProcessor 与 RequestRuntime 接入

Status: active（核心已实现，最终源码门禁待确认）
Owner: root
Updated: 2026-09-14

## 目标与证据边界

保留一个 shard 的唯一 Engine，把已经实现的 RequestRuntime 接到 EventProcessor。源输入的关系变化、FSM/CEP、Timer、trigger outbox 继续同批；后续请求受理时，固定输入、RequestRecord/active-index、occurrence 消费和 accepted-once 同批；函数完成从独立控制入口进入，同批发布结果、请求状态、Latest 及可恢复的 B readiness 重检查。

依据 [请求规格](request-functions.md)、[运行时契约](runtime-contracts.md)、[Latest selector](latest-selector.md)。当前实现已在主树；[owner 审查](reviews/M4-owner-fresh-review.md)与[应用整改复验](reviews/M4-app-remediation-review.md)保留限定阶段证据，最终代码门禁见核心规格。

当前真实边界：

- EventProcessor独占`unique_ptr<Engine>`；无请求配置时保留原生命周期路径，请求模式走expiry fence、释放/受理、普通Timer的v2控制顺序。`ProcessResult.committed_rounds`保留已提交子轮，后续失败不丢失这些效果。
- RequestRuntime 已借用同一个 `Engine&`，拥有自己的 active book 与 UNKNOWN candidate；`RequestCommitHook` 在请求状态刷新后、final RuntimeStatus/Latest 的第二阶段稳定前执行。
- `apply_lifecycle`已增加private `owned_prepare`：PublicInput→SQL→typed Candidate→SQL/Latest→普通hook，同批且保持窄权限；metadata_frame不把source/fence UNKNOWN改成Request归属。普通hook仍不能写RequestCapture、FunctionView或带annotation的RuntimeStatus。
- 未启用请求时仍保留format=1及原fingerprint；请求模式format=2已保存timer_frontier、ordered expiry notices、intent/once/level dirty和审计进度，允许声明内的Timer debt恢复。TryOnceAtFiring、绝对deadline cap、read_candidate及有界active_requests已实现。
- Engine已查询StoreLimits并产生有效EngineConfig，EventProcessor使用该配置做预算预留；实际canonical/wire限制及恢复预算已有独立限定验证；最终核心组合仍由root另记，不凭旧owner报告关闭新改动。

本文固定M4 owner契约；其上已有真实Meta/KV/多Compute、SDK、启动前DDL与全局checkpoint/Rocks恢复。完整Actor、多Job、用户扩展和通用host绑定按[五小时核心范围](core-delivery.md)后置。同轮observer基础已有实现，采用产生变化的owner轮内同步评估并保存规则状态/Trigger，不建设通用CDC、持久witness队列或独立逻辑观察通道。feature_computation的B readiness仍是可合并level重检查，不据此声称任意关系crossing已接入集群host。

## 选定的执行结构

采用 **guard-free expiry fence + 请求控制轮 + 有界普通生命周期轮**。每次公开 dispatch 固定一个 `now_us`，不在内部重读时钟。三个阶段共享它；发生 UNKNOWN 的原批确认不重采时间。

1. **Expiry fence**：删除所有 `deadline <= now_us` 的 Table TTL 行，取消相应 TTL 项；对已过期的当前候选调用 typed Deactivate；稳定 SQL/Latest；持久化 `$expire` 待执行通知以及尚未处理 Timer 的事实。不执行 FSM/CEP guard、update、sink 或 periodic event，不产生新 occurrence，不受 trigger outbox 空槽限制。
2. **释放与既存受理**：completion/cancel/request timeout 使用 `now_us` 调 RequestRuntime。已有 occurrence 的受理先验证 trigger/candidate 期限，再 capture，成功时原子消费 occurrence 与 once。此阶段不先执行会产生更多 occurrence 的一般 Timer。
3. **普通生命周期工作**：有额度时执行 FSM/CEP 到期、TTL `$expire` 通知、合并 periodic slot，以及可合并的 B/C level 重检查。所有新 source 在其应先处理的到期工作完成后才能提交；输入上限仍不能挡住第一、二阶段。

满 active=1、outbox=1 时，顺序必须能够是：`fence → complete A/release active → admit existing trigger/consume outbox → drain emitting timer`。不能把顺序写成 `advance all timers → complete A`；也不能用旧的 `state.time_us` 处理 A 来绕过 outbox。

这里的“同批”不是把整个公开调用改成一个大事务。source 第一次产生 durable PendingTrigger 是一个已确认子轮；后续真实 admission 读取该轮形成的当前 revision R，另一个批次提交 R+1。后一批内 request 与 occurrence/once 不可拆开。否则把同轮尚未提交的源写入称作 capture revision R 会伪造证据。

## 时间与到期通知的精确定义

请求模式记录两个不同事实：`time_us` 是已确认 owner dispatch 的最大 processing time；`timer_frontier_us` 是普通 Timer/FSM/CEP 已处理完成的进度。若 frontier 落后，持久状态必须显示 debt；不把所有 FSM 时间强行改为 owner time。

- fence 在读取 capture 之前必须完成全部到期 Table TTL 和 candidate 失效。不能只清此次 request scope 的 TTL 后就声称取得整个 shard 的稳定 R；跨关系 capture 必须看到同一个已提交 R。
- 不允许 Candidate 仅靠一般 FSM `$timeout` 才失效。请求配置须声明候选的持久 deadline；fence 直接执行 typed Deactivate。发生新闻替换时，旧 epoch 永不重新激活。
- completion 在 `now_us >= attempt_deadline` 时沿用 RequestMachine 的 timeout-before-completion fence；在 `now_us >= request_deadline` 时请求终结。旧 candidate 的合法历史完成仍可保存，已失效 Candidate 使其无法进入 Latest。
- fence 删除 TTL 后，原 `$expire` 不能消失。每次 sweep、每个确实删除行的 scope 保留一条 `ExpiryNotice{id, scope, deleted_at_us}`，同一 sweep 内多行仍与原实现一样只有一次该 scope 的 `$expire`。不同已提交 sweep 的通知不合并，按其 durable 顺序消费。
- notice 表示 **guard 尚未执行**，不是已有不可变 view-change 观察。稍后 `pump(u)` 执行它时，guard 的 processing time 为该次固定的 `u`，reads 来自该次稳定视图，`deleted_at_us` 仅作 scheduled 审计。timeout 的 reads 也在实际执行轮捕获；相对 Timer 从 `u` 起算。不能拿 `deleted_at_us` 回放旧市场，不能把新相对 Timer 放入过去。
- 每个 scope 先 advance 其旧 FSM/CEP deadline，再依序消费该 scope 的 expiry notices，最后处理本次 coalesced periodic；各已提交输出、transition sequence 和 clock position 都要正常返回。source 同 key 的 step 只在这些工作清空后执行。
- 新 source 不得跨过未完成 debt。completion/取消/超时属于能释放额度的控制操作，不是伪造的 source data。新 source 未提交时不登记 processed position，也不确认 broker offset。

feature_computation保留source/Clock驱动的FSM/CEP与level重检查。通用观察的选定方向是在final stabilize后的同一owner轮同步评估，并将规则状态/Trigger一并提交；现有observer基础支持该边界，完整host绑定后置。dirty bit与尚未执行的Timer notice不冒充通用逐变化观察；也不为此建立持久witness队列。

## 最小公开接口

保留现有`open/process/advance/acknowledge_occurrences/query_sql`；配置末尾追加可选请求配置，旧调用保持语义。以下是已实现接口的核心轮廓，完整默认值和追加字段以[公开头文件](../include/eventflow/event_processor.hpp)为准，辅助结构的持久表示见后文。

```cpp
struct RequestKey {
  Row scope;
  std::string request_id;
};
enum class ProcessorOperationKind {
  Source, Advance, Acknowledge, AdmitOccurrence,
  CompleteRequest, CancelRequest, ClaimTasks, Pump
};
enum class PendingCommitOwner { Lifecycle, Request };
struct PendingOperationInfo {
  ProcessorOperationKind kind;
  PendingCommitOwner commit_owner;
  std::int64_t fixed_now_us = 0;
  bool awaiting_commit = false;
};
enum class RequestDisposition {
  Accepted, Updated, Deferred, Duplicate, Ignored, Expired, Skipped
};
struct RequestOperationResult {
  RequestKey key;
  RequestDisposition disposition;
  std::optional<RequestRecord> record;
  Status reason;
};

// Append to CommittedRound. These are newly confirmed effects only.
std::vector<FunctionTask> tasks;
std::vector<std::string> cancel_task_ids;
std::vector<RequestOperationResult> requests;

// Append to ProcessResult.
std::optional<RequestOperationResult> request_result;
std::optional<PendingOperationInfo> pending;
bool work_remaining = false;

// Add to EventProcessor.
StatusOr<ProcessResult> admit_occurrence(std::string_view occurrence_id,
                                        std::int64_t now_us) noexcept;
StatusOr<ProcessResult> complete_request(const RequestKey& key,
                                        const FunctionCompletion& completion,
                                        std::int64_t now_us) noexcept;
StatusOr<ProcessResult> cancel_request(const RequestKey& key,
                                      std::int64_t now_us) noexcept;
StatusOr<ProcessResult> claim_tasks(const RequestKey& key, std::int64_t now_us,
                                   std::size_t limit = 16) noexcept;
StatusOr<ProcessResult> pump(std::int64_t now_us) noexcept;
StatusOr<std::optional<PendingOperationInfo>> pending_operation() const noexcept;
StatusOr<RequestRecord> read_request(const RequestKey& key) noexcept;
StatusOr<CandidateProgress> read_candidate(const Row& scope) const noexcept;
StatusOr<std::vector<RequestRecord>> active_requests() const noexcept;
StatusOr<std::vector<FunctionTask>> pending_tasks(std::size_t limit) const noexcept;
```

`complete_request` 的形状刻意没有 SourceIdentity，函数 worker 也没有 Engine/store/companion 能力。可信 owner 以持久 task 的 scope/request_id 路由回包，RequestMachine 继续验证 owner/epoch/request/node/attempt/task_id。不能接受调用方用不存在的 source identity 驱动 FSM。

`pump` 是有界控制入口：先处理到期请求和已存在的待受理 intent，再利用剩余额度执行一般 Timer 与 level checks；启动后主动调用，平时由最早 deadline 或完成通知唤醒，不等新 source。`max_rounds_per_call` 同时限制所有子轮，返回 `work_remaining=true` 让 owner 再调度，不在 dispatch 内等待额度。

`request_result` 描述本次指定请求操作；`committed_rounds[*].requests/tasks/cancel_task_ids` 是逐 revision 的持久新效果。duplicate/deferred 可以没有新 round；deferred 不暴露未提交 record。请求完成已经提交而之后 timer 满时，返回 envelope 的 `operation_status=ResourceExhausted`，但完成 round 和终态结果都保留。调用者不能只检查最后一个 `commit` 就丢掉此前输出。

外层 `StatusOr` 只用于进入任何子轮前的参数/owner 错误。一旦本调用已经提交子轮，后续错误必须装入 `ProcessResult.operation_status`；不能通过 `EF_ASSIGN_OR_RETURN` 把整个 envelope 提前丢弃。`input_committed` 只属于真正 source；其他操作永不置 true。

`pending_operation()` 在 UNKNOWN 时也可读，只返回 owner 的不可变描述，不读取 staging 数据。这里的 pending 是待确认的原操作；普通 ResourceExhausted 没有未决 KV 批时，仅保留 durable debt/intent，调用者在下一次 dispatch 使用新的固定 now 重试。

## 请求 binding、occurrence 与 accepted-once

请求配置持有 registry、plans、RequestRuntimeConfig、版本固定的 trigger bindings 和 level bindings。全部算子依赖仍来自 Catalog；bindings 只表达 trigger/plan/lifetime，不复制 FunctionView DAG。

```cpp
struct RequestTriggerBinding {
  std::string id;
  std::uint64_t version = 1;
  std::string rule_id;
  std::string emission_name;
  std::string plan;
  bool once_per_candidate = false;
  std::int64_t pending_lifetime_us = 0;
  std::function<StatusOr<RequestAdmission>(
      RoundContext&, const TriggerOccurrence&)> make_admission;
  std::string required_accepted_binding;
  RequestTriggerMode mode = RequestTriggerMode::Wait;
  std::string deadline_column;
};
struct LevelRequestBinding {
  std::string id;
  std::uint64_t version = 1;
  std::string plan;
  std::int64_t pending_lifetime_us = 0;
  // Readiness is evaluated by this plan's capture callback at admission revision R.
};
```

binding factory 在 source/Timer occurrence 形成的同一个稳定轮里构造有界 `PendingRequestIntent`：occurrence ID、binding/version、RequestAdmission、截止时间、可选 once identity。这里只冻结 trigger 身份/参数；市场行仍由计划在真正 admission 的 R 捕获。`request_id` 使用规范、确定性的绑定/occurrence 身份编码，不能在重试时再生成随机 ID。binding factory 不写 KV、不派任务、不调用 Engine。

once identity 是 `(binding_id, scope, candidate_epoch)`，B/C 各自独立。pending 时 once 为 false；A 函数失败或 B/C 缺 readiness 均不能提前消费。成功受理后即使请求失败，once 仍保持 consumed，重试属于原请求。

`admit_occurrence` 从持久 outbox/intent 读取 admission，调用方不能另塞一个 request_id 绕过 once：

1. 执行 expiry fence；过期或 candidate 不匹配的 intent 通过一个 lifecycle 批次标记原因并移除，释放 outbox，但不伪造 accepted。原因记录使用该候选已有的有界状态位；不能为过期取消新增无界终态历史。
2. 对 eligible intent 调 `RequestRuntime::admit(..., now, companion)`。capture `ready=false` 时丢弃整个 admission overlay，保持 outbox/once；前面已提交 fence 子轮仍返回。
3. companion 在同一请求批次中移除对应 occurrence/intent、设置 accepted-once、更新所属 scope 的候选状态、置 level dirty，并 stage 完整 lifecycle snapshot。函数的 TaskOutbox 和请求 active-index 仍由 RequestRuntime stage。
4. RequestRuntime 返回 Committed 才同时发布 lifecycle candidate 与 request book；UNKNOWN 时两个候选都留在对应 owner，不能重捕获。

现有 `acknowledge_occurrences` 保留给普通 downstream sink。配置绑定为 request 的 occurrence 不允许由这个通用 ACK 擦除；必须经过成功 admission，或经过上述显式过期/取消 disposition。普通未绑定 occurrence 继续使用旧接口。`pending_occurrences` 仍返回原类型；在类型末尾追加可选 request intent summary，或提供单独只读查询，不能改变原 ID/read/source 字段的含义。

## 最小 Engine / RequestRuntime 私有变更

RequestRuntime 既有 public API 与 companion 签名足够完成受理、完成、claim、超时、取消；不把 Engine 控制权搬到 worker，也不另造 RequestRuntime 的 store writer。

仅需给 `EngineRuntimeAccess::apply_lifecycle` 追加一个默认空的 owned 阶段：

```cpp
static StatusOr<DispatchResult> apply_lifecycle(
    Engine& engine, const std::vector<TableChange>& inputs,
    const DispatchContext& context, const RoundHook& after_stable,
    const RuntimePrepare& owned_prepare = {}) noexcept;
```

确切顺序为 `validate/apply PublicInput → stabilize → owned_prepare → apply authorized RuntimeStatus → stabilize → after_stable → stabilize → commit`。owned_prepare 只允许 owner 的 typed Candidate lifecycle 写；不开放新的 RequestCapture/FunctionView 入口，不绕过 Latest 的 exact-write authorization。source 的 candidate 切换在 FSM 读之前已稳定；expiry fence 的 TTL 删除和 candidate Deactivate 是同一批。原默认空调用维持现有顺序。

请求模式的生命周期轮统一使用可丢弃的 private overlay。容量不足只回滚当前未提交候选；永久算子错误仍 fail-stop，不能扩成忽略所有错误。原普通 Engine public hook 权限不变。

**feature_computation level路径不依赖观察排队。** companion只持久化该scope的level dirty；finalize/status/Latest/后继SQL确认后，下一控制轮读取稳定readiness。Engine新增的observer基础则在最终稳定阶段同轮同步调用，允许授权观察状态写入与声明 sink 的受控传播；它不把新Latest读为phase-1旧值，也不持久排队待执行的witness。完整通用host绑定按五小时范围后置，旧seal/witness排队提案不再作为当前实施步骤。

## durable level recheck 与容量 1

每个已存在候选只保存固定大小的 `level_dirty` 和 B/C readiness/once 状态；不是把每次 completion 放入独立无界 mailbox。source/candidate 改变、request companion、TTL fence 都可置 dirty。完成/取消/超时只修改已预留槽位，因此 active 满、trigger 满时仍可提交终态。

`LevelRequestBinding` 只允许表达“当前条件满足就尝试受理一次”的 BReady/C prerequisite；不接受计数、crossing 或每次变化回调。它有独立类型与命名，不能调用同名 `$completion`/`$view_change` 通用 FSM 事件冒充订阅。owner 从已有候选的 scope/epoch、binding/plan 构造确定性的 RequestAdmission；readiness 完全放在该 plan 的既有 capture callback 中，它在实际 R 点读最终已确认的 Latest、市场输入与前置状态并一次冻结输入，`ready=false` 则丢弃本轮。无需额外只读 Engine frame，也不先执行未来 guard。每次 pump 对一个 dirty binding 最多试一次；not-ready 保留 intent，等待后续完成、源变化或 deadline 唤醒，不在当前调用自旋。

C 的市场确认继续由真实 source 的 immutable occurrence/候选 pending-C 状态记录；level check 只处理其 B/Embedding 前置是否就绪，不重建已经错过的 C 市场事件。D 继续按持久 periodic slot 工作，miss 记录 skipped，不借 dirty 补发。

为了避免 outbox=1 恰被等待 B 的 Pending C 占据、B 又必须先占一个 outbox 槽，B level admission **直接使用已有候选中的有界 level intent**。其逻辑 trigger ID 为 `(binding, scope, epoch)`；成功 admission 的 companion 设置 once、清该 level intent，不再先 emit 一个必须占通用 outbox 的临时 occurrence。额度或 readiness 不足时保留 dirty/intent。这个 intent 是明示的独立有界预算，计入配置的 PendingTrigger 总预算与 state 字节，不是隐藏无上限的第二队列。

completion 若成功提交后进程立刻退出，重启加载 dirty 后仍执行 B check；因此 A 完成能在没有新 source 的情况下唤醒 B。若 A 的旧 epoch 完成，dirty 可重查当前候选，但 admission 仍按当前 epoch 与一次性身份检查，旧结果不能令新 B ready。

## 容量与恢复 codec

请求模式使用 `format=2` lifecycle 编码；完全未启用请求配置时仍输出原 format=1 和原 fingerprint bytes。不得为了可选功能给所有旧配置的 fingerprint 无条件追加空字段。

v2 在原字段外包含：请求绑定/候选配置 fingerprint、`timer_frontier_us`、有界有序 `deferred_events`、有界 request intents、每个候选的 epoch/deadline/once/disposition/level_dirty。`deferred_events` 的每项有 kind、durable sequence、scope 和 scheduled time，本切片仅允许 `TtlExpiryNotice` kind；未知kind严格拒绝；该字段不作为未来持久witness队列扩展入口。`level_dirty` 只属于单独的 level intent，不能替代这一有序事件编码。RequestRuntime 的 record/index 继续沿用自身编码，不复制 NodeRecord 或 TaskOutbox 到 lifecycle JSON。

- v2 可以保存 deadline 已小于 owner `time_us` 的 FSM/CEP/clock；校验它确实属于尚未 drain 的有效 Timer，frontier 没有越过该 deadline。保留 CEP 自身状态编码校验；不能把所有过期 state 都放行。
- 同一个 source 在 debt 未清空时不执行。fence 删除 TTL 时把其控制预算转移给 expiry notice；一条 notice 至少消耗一条当时存在的 TTL，控制消息不会新增 TTL，因此通知数量最多为准入时已经预算的 Timer 数。notice 只保存 identity/scope/time，不保存任意大 payload。
- 每个 active 候选预留固定的 once/dirty/终态原因/最大数值宽度空间；不在完成时临时申请新的 trigger 条目。request book 的未来状态预算继续沿用已验证的 RequestRuntime 预留。
- 准入时必须一起检查完整生命周期 snapshot、outbox、TTL→notice 转移后的最大 envelope，以及 Engine 单批 mutations/bytes。存在一个合法 TTL 集合但其无输出 fence 本身无法提交的配置，不可接受。首版保留现有“一轮清全体到期 TTL”的策略；不要悄悄只清一部分后执行 capture。实现若改为分片 fence，须显式保存 sweep cursor，返回所有已提交子轮，并在 sweep 完成前禁止 capture。
- 不运行未来用户 guard/update/sink 探测预算。数量依据 FSM transitions、CEP 已承诺上限、候选/Timer 数和 codec 最坏宽度；已有纯 CEP drain 的特例不能扩成运行未来用户逻辑。
- v1 只在请求配置关闭时按原路径读写。不要把非空旧生命周期自动升级为带请求的 v2：首次启用请求要求初始完整 Catalog/空基线，或后续明确的受控迁移。普通旧单源示例与其既有 snapshots 不需要迁移。

## UNKNOWN 与已提交子轮

外层只保存一个 `PendingOperation` tagged union，包含原操作参数、固定 now、剩余步骤位置、`PendingCommitOwner` 和尚未发布 lifecycle candidate。Engine 仍只保存一个原 CommitBatch。

| 阶段 | 已确认状态 | 本地 pending | 确认入口 |
| --- | --- | --- | --- |
| fence/source/Timer UNKNOWN | 原 lifecycle + 原 request book | 新 lifecycle、输出、原 Operation；Engine 原批 | 原 Engine/lifecycle resolver |
| request admission/control UNKNOWN | fence 等较早子轮已经发布 | RequestRuntime 的 Book/result + 外层 companion lifecycle candidate；Engine 同一原批 | 只调用 RequestRuntime::resolve_pending_commit |
| request 批确认，随后 Timer 满 | request 与 lifecycle 已发布 | 没有 KV pending；只有 durable debt | 返回已提交 request round，后续 pump 新 dispatch |

具体调用序列：

```text
dispatch(now=t)
  fence R -> R+1 COMMITTED: append round(R+1)
  request operation at t -> R+2 UNKNOWN:
    retain request pending and lifecycle companion candidate
    return round(R+1), UnknownCommit, pending=Request
resolve_pending_commit()
  RequestRuntime::resolve_pending_commit(): retry/query original R+2 batch
  COMMITTED:
    publish lifecycle companion candidate exactly once
    append only round(R+2), including tasks/cancels/request result
    continue original bounded operation at fixed t if required
  UNKNOWN: retain everything; expose no staging record/task/latest
```

resolver 绝不再次返回调用方已经拿到的 R+1；也不因为内部 request `StatusOr` 错误而忘掉 R+1。RequestRuntime 非状态结果但 `has_pending_commit()==true` 时仍保存外层 tag/candidate。被拒绝的提交不当成确认，不发布本地候选；按当前 owner conflict/fail-stop 规则处理，不能改 batch 后沿用旧 ID。

跨进程重启不要求持久化函数回包本身：已提交的 request record/outbox、lifecycle dirty/debt 是恢复依据；未提交完成由任务重派/重复回包恢复，逻辑 fence 只允许一次结果。未知提交必须先走存储 receipt 协议确认，不从 cache 或新 capture 开始。

## 初始化与后续多 Job 边界

请求模式由 EventProcessor 使用 private Engine open 绑定 Registry；先初始化/恢复 lifecycle，再 open RequestRuntime，避免其 index 初始化先推进 revision 后触发现有 lifecycle 的 `revision != 1` 禁止挂载。初始化任何 UNKNOWN 都返回明确错误，下一次 open 以 durable state/receipt 恢复，不能假装空库成功。

成员销毁顺序确保 RequestRuntime 先销毁、Engine 后销毁。lifecycle/request namespace 互斥，并检查所有 Catalog relation state_id，不能只检查 `relation->plan`；输入、sink、reachability 统一用 `RelationKind/BaseWriteRole/dependencies()`，不得把 Latest/FunctionView 当无 plan 的普通表。

当前对外继续由 EventProcessor 拥有 Engine。内部将逻辑收敛到 `Engine&` 和同 adapter 的非拥有引用，便于后续抽出一个物理 shard owner，把不同 Job 的 lifecycle contexts 挂到同一个 Engine/统一 Catalog。多 Job 实现不能为每个 Job 新建相同物理 shard 的独立 Engine，也不能立即创建多个互相争抢 `attach_request_owner` 的 RequestRuntime；届时应让一个 runtime 容纳 namespaced plans，统一 owner 仲裁。本次不实现该框架。

## 八条持续回归轨迹

1. **source 原子性与旧接口**：source 同轮改 base/MV/FSM/CEP/TTL/outbox；拒绝则无 source position。旧 format=1 样例读写与普通 occurrence ACK 不变。source 重投先 dedup，不能重复激活候选或消费 once。
2. **真实捕获 R 与 not-ready**：多个 scope 的 TTL 均在 t 到期，先删除全部再 admit；Manifest `capture_revision=R`、`admission_revision=R+1`。not-ready 不消费 trigger/seq/once、不提交 capture；先前 fence round 仍返回。
3. **同批受理与 UNKNOWN**：admission 在存储实际提交后回复 UNKNOWN，重试前查询/派发都看不到 staging；恢复仅原批，trigger 消费/once/manifest/active-index/capture 全有或全无，capture callback 只运行一次。
4. **双满仍释放与期限**：active=1、outbox=1，同时存在会 emit 的 FSM timeout/CEP/periodic、TTL 到期。分别送 completion/cancel/request timeout；不靠旧 now 或新 outbox 槽即可终结并释放，随后 pending admission 能释放 outbox。恰好 attempt/request deadline 的成功包不被接受。
5. **Latest 失效同边界**：candidate 在 t 过期或被 source 更高版本替换；旧任务在 t 或之后完成只留历史，Latest 无短暂旧值发布。不同 admission_seq 乱序成功仍选正确 winner；整 Request 失败时单节点成功不进入 Latest。
6. **durable B 唤醒与 Pending C 容量**：A 完成提交后、下一次 level check 前重启，没有新 source 仍受理 B 一次。outbox=1 被 Pending C 占据时，B 使用预留 level intent 受理，B accepted 再唤醒有效 C；D 多次 miss 保留 skipped，不补旧 slot。
7. **partial rounds 不丢不重复**：fence 已提交，request UNKNOWN，再确认后 Timer 满或轮数到限。三次返回按 revision 各只出现一次新 round；确认不重放旧 RowChange/tasks；只有真实 source 最终提交才置 `input_committed`。对无 pending 的重试使用新 dispatch now。
8. **债务恢复、预算与观察边界**：存下逾期 FSM/CEP/clock + 多条 `$expire` notice 重启，按顺序执行，guard/update 不在预算检查时运行；late guard 使用执行轮 fixed now/read，relative timer 从执行时起算。readiness dirty 可合并且不广播 generic view-change；大状态/终态/candidate最大数值宽度与 TTL→notice 转移不堵释放通道。

## 当前实现与证据

实现入口为 [EventProcessor 头文件](../include/eventflow/event_processor.hpp)、[实现](../src/runtime/event_processor.cpp)、[Engine 私有桥接](../src/runtime/engine_internal.hpp)、[事件请求测试](../tests/event_request_test.cpp)和[特征计算 driver](../examples/feature_computation_demo/feature_computation_demo.cpp)。生产代码、应用和测试均已并入主树，不再从旧临时目录继续合并。

[owner 整体审查](reviews/M4-owner-fresh-review.md)记录指定 v2 快照的 503 项配套回归与八个独立 probe，覆盖顺序、UNKNOWN、TryOnce/cap、codec 和有界恢复。[预算整改](reviews/M4-event-request-budget-remediation.md)关闭 expiry SQL 字节与 periodic 多输出低估反例；[应用整改](reviews/M4-app-remediation-review.md)关闭 driver UNKNOWN/offset 同步与批量 completion 丢失。实际 StoreLimits/有效 Engine 配置与恢复预算另见[有效预算审查](reviews/M4-effective-budget-review.md)。

这些报告只覆盖各自快照。真实 Meta/KV/多 Compute、线程函数池、SDK drain、启动前 DDL 与 checkpoint/Rocks 恢复已接通；最终头文件对应的全量测试状态统一保留在[核心交付](core-delivery.md#验证证据与剩余门槛)。

## 当前边界与下一步

PendingTrigger 的完整 occurrence 在受理前保留，被 admission/ACK 消费后尚无独立历史 TriggerWitness。RequestManifest 和捕获行保留实际受理/输入信息，不承诺全部历史 source cause 或 guard 读值。

继续完成最终源码门禁与制品核对，记录本轮已完成全局审查的整改证据，不再开新轮。完整观察 host、多 Job 与在线迁移后置；本规格中的单 owner、实际时间、释放通道和原批 UNKNOWN 合同继续作为其前置约束。
