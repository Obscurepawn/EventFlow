# M4 EventProcessor v2 owner 与接口独立审查

日期：2026-09-14。审查者：独立 fresh-context 子 Agent `m4_owner_fresh_review`。

结论：本轮针对 **EventProcessor v2 `470599df`**、Engine 私有 `owned_prepare`、RequestRuntime/Latest 组合的源码审查与新构建验证，**未发现新的必须修正的内核问题**。指定新请求回归 **88/88**、旧 EventProcessor/CEP 回归 **197/197**，加 4 组下层组合回归后共 **503/503**；自行编写的 **8/8** 对抗探针通过。该结论限于本文追踪的 owner 切片，**不将 M4、完整资源契约或系统最终交付标为完成**。

审查期间没有修改被测生产源码、仓库测试或 CMake；仅创建本报告和 `/tmp/eventflow-m4-owner-review/` 下独立构建/探针。主树的 spec 状态及后续整合由 root 维护。

## 依据与版本边界

先读取 root `AGENTS.md`、`specs/INDEX.md`、`specs/request-functions.md`、`specs/event-request-integration.md`、`specs/latest-selector.md`，按 `specs/runtime-contracts.md` C2/C3/C6/C7 建立检查矩阵，再检查实际源码。既往 RequestRuntime owner、FunctionExecutor、Latest 整改及 EventProcessor 预算报告用来识别已知风险及未完成边界，未将其通过标签当作当前版本的运行证据。

被测源码目录为 `/tmp/eventflow-event-request-work`。本轮没有使用旧 `bcc620a1` 预算快照的二进制或对象文件。所需项目库全部在新的 `/tmp/eventflow-m4-owner-review/build/` 编译；第三方头文件和预编译库来自该源码目录的 `cpp3rdlib/`，没有构建下载。

本轮入口 header/cpp/test hash 与 root 指定值一致；这三项以及 Engine/RequestRuntime 生产 cpp 在开始与最终运行后均一致。最终完整标识如下：

```text
6972c9dbe78d0dc050a08dd2daba4302ca396bb5658c0005cd0af89ec3d89bdd  include/eventflow/event_processor.hpp
470599df9255fa100f14bd392ad422092382a48543b4ecb712df8d64a18fc8ef  src/runtime/event_processor.cpp
1695bb17b6ee4e1a33351488832a520702b976c0211a70147a9b0334e31c584e  src/runtime/engine.cpp
41d20b00aab7198273065332f1c23992032e0ad4b2186c6ff4b8c8c3075c38d7  src/runtime/engine_internal.hpp
ba03b8f3f4194e7e5eb3f09efa00045dd7ee0820003553174c3e6883f55a5937  src/runtime/request_runtime.cpp
52d3226268c1e8a9b441a1babcc3aa37200b03fd528a73182fdffed84804b91e  include/eventflow/request_runtime.hpp
1d3b71a60cc68c7b44b58aeecae65eb52c8ed6d9dfd7101b9afa59f7aad14da7  tests/event_request_test.cpp
```

完整 14 文件清单见 [source.sha256](/tmp/eventflow-m4-owner-review/source.sha256)。注意：此 tmp 树 `request_latest_test.cpp` 为 `7c9519b1...`，实际 **53 项**；它不是主树较新的 57 项版本。已通知 root，整合时必须保留主树新增回归，不能用此 tmp 测试覆盖。生产 RequestRuntime/Engine 的指定实现已在本轮编译验证。

## 契约核对

| 契约 | 当前代码与实测证据 |
| --- | --- |
| 一个 shard 的唯一 Engine owner | EventProcessor 持有 Engine，再将相同 Engine/store 交给 RequestRuntime；worker 入口只有固定 task/completion。`request_round` 的 companion 在同一请求批次写 lifecycle、移除 occurrence、消费 once；不出现第二份业务 DAG。 |
| C2：全 shard 过期在 capture/completion 之前 | `expiry_fence`（EventProcessor 2309）一次遍历全部 TTL 与 Candidate，typed Deactivate 与 TTL 删除同批，不运行 guard。`run_requests`（3047）phase 0 后才进入请求控制轮。独立跨 scope 探针同时删除两个 scope 的 TTL、失效当前 A Candidate，再在后续 revision 完成 A；其合法历史结果存在，但 Latest 为空。 |
| C3/C7：控制先释放，既存受理先于一般 Timer | `run_requests` phase 1/2 是显式控制与超时，phase 3 是既存 intent，phase 4 为 level，phase 6 才是 Timer。容量 1 的独立探针观察 `fence → complete/release → admit B → timeout` 连续 revisions；四后端回归另覆盖 cancel、timeout、完成恰在 attempt deadline、Pending C 占满 outbox。 |
| source revision 与真实 admission 分离 | source 先形成 durable intent；RequestRuntime 在稳定 revision R capture，受理记录 R+1；成功 companion 才原子消费 intent/once。88 项测试中的 source/admit/claim/complete 轨迹在四 KV adapter 运行，检查固定 capture/admission revision。 |
| Engine `owned_prepare` 的窄权限和顺序 | `Engine::apply_input_round`（1154 起）先稳定 PublicInput，再暂时开启 `metadata_frame` 调 owned prepare，仅接受 typed Candidate 且消耗 exact authorization，然后稳定 Candidate/Latest，最后执行正常 hook。`metadata_frame` 不将 source/fence 变为 `runtime_frame`；`pending_for_request` 仍只由真正 RequestRuntime frame 设置。 |
| UNKNOWN 归属、固定 now、partial rounds | EventProcessor `pending_info`、`resolve_pending_commit` 分别调用 Lifecycle/Request resolver，保留原 operation.phase 与 fixed now；返回只包含新确认的 rounds。独立验证 fence UNKNOWN 的待确认操作仍是 CompleteRequest/Lifecycle，确认后不伪造 completion；另验证 Request UNKNOWN 的 cancel 仅发布一次 cancel_task_id。已提交 source 的 TryOnce 后续 UNKNOWN 同样保留 source 成果。 |
| `max_rounds_per_call=1` 无时间空轮饥饿 | 没有实际 TTL/Candidate 失效时 `expiry_fence` 不单独更新时间；每次新调用可以直接完成实质控制工作。独立 trace 在 now=7/8/9/10、每次最多一轮下依次完成 fence、请求完成、B 受理、Timer。TryOnce 配置则明确要求至少 2 轮，并为同次 firing 后的 admission/skip 预留额外轮数。 |
| Timer/notice 与 level 的语义边界 | `update_frontier` 从剩余 FSM/CEP/clock/notice debt 保守计算 frontier；`timer_round` 先旧 FSM/CEP deadline，再有序 expiry notices，再 periodic。notice 的原时间仅作 scheduled 审计，guard 使用当前 now。level dirty 等待整个请求 finalize/status/Latest 提交后，下一独立轮读取稳定值；不冒充通用 completion 事件 witness。 |
| 新 TryOnce 审计 | `freeze_intent` 保存原 occurrence/evaluated/slot，`request_round` 在原 firing 时尝试，not-ready/no-capacity/prerequisite miss 写有界 skip 并移除 intent；不同 now 的残留 TryOnce 不重捕获旧时隙。独立测试 source 本身 UNKNOWN 后 fixed-now 确认/skip，以及随后重启/市场变 Ready 都不补发。 |
| candidate 绝对 cap | binding 的声明字段必须为非空 Int64；intent deadline 取 candidate deadline、绝对 cap、pending lifetime 的最小值。启动时将 persisted cap 与 typed Candidate 行交叉验证。独立测试 cap 恰在 source now 已关闭时无 intent/capture，同时 Candidate 本身仍 active；四后端回归覆盖 Pending 在 exact cap 到界过期。 |
| 新状态预算和 codec | lifecycle 同时计 occurrence intent 和全部 per-candidate level slots；`reserve_control` 为 audit counter/time/reason 的最大数字宽度预留。独立验证 2 个 level 槽 + 1 个 occurrence 在总上限 2 时拒绝 source、无 position/Candidate/revision；16 KiB 状态预算下将合法 skip counter 推到 UINT64_MAX 后仍可重启，下一次输入在提交前明确拒绝而不 overflow。 |
| 有界 active 恢复 | `active_requests()` 来自 RequestRuntime active book；open 按 bounded index 逐项点读 request，Candidate 只扫描已预算的当前 scope 行。独立代理禁止扫描 RequestStatus/Capture/Function 历史，在 20 个终态归档 + 1 个 active 后仍可 reopen/claim，history scan 次数为 0。 |
| Latest 的确认与身份 | RequestStatus 与 Candidate 走受控 typed 写；RequestRuntime complete 后最终 phase/status 决定 Latest，可合法保存旧 candidate 的结果但不能成为当前 Latest。当前 EngineLatest 38、RequestLatest 53 及上层新旧 epoch/历史完成回归通过。 |

## 独立对抗探针

[probe.cpp](/tmp/eventflow-m4-owner-review/probe.cpp) 只包含已有 fixture 以复用 Catalog 和 KV setup，自行编写下列断言。运行过滤器仅选择 `M4OwnerProbe.*`，不将被包含但未运行的原测试算作独立探针。

| 探针 | 独立观察 |
| --- | --- |
| `CrossScopeFenceUnknownAndSingleRoundReleaseRemainOrdered` | 两个 scope 的 TTL 到期 + 一个 Candidate 失效；fence reply UNKNOWN；确认只发布 fence；递增 now 且一轮上限下 complete、B admission、旧 deadline Timer 按 revision 发生。outbox/active 各 1；输入进度仍精确为已有两条 source。 |
| `RequestUnknownPublishesCancellationAndTaskReleaseOnlyOnce` | 正在运行的任务 cancel UNKNOWN；未确认前 active API 拒绝；resolve 只发布一次取消 task ID；duplicate cancel 无新 round，reopen 保持 Cancelled。 |
| `TryOnceSourceUnknownUsesOriginalNowAndPersistentSkipAcrossReopen` | 源 batch UNKNOWN 时 capture=0；resolve 固定原 now 后只有一次 capture 和一次 skip；重启后 Ready/pump 不补旧请求。 |
| `CombinedLevelSlotsRejectBeforeCandidateOrSourcePosition` | level/outbox 的联合预算拒绝发生在 source 提交前，无 Candidate、无源进度、无 revision 增长；可 reopen。 |
| `CodecRejectsNewAuditCapAndIntentMismatches` | 逐个直接修改持久 JSON：未知 audit reason、Wait 模式的非零 skip、未来 audit time、与 typed Candidate 不符的 cap、缺 audit 槽、与 cap 不符的 intent deadline、frontier 超过 owner time；7 个变体全部拒绝 reopen。此直接 KV 写仅用于损坏检测测试。 |
| `AbsoluteCapAlreadyClosedSuppressesCaptureAndSurvivesReopen` | cap=source now，Candidate deadline 尚远；本轮不生成 intent、不 capture，持久 Expired 原因，Candidate 仍 active；重启保持。 |
| `RecoveryAndTaskClaimNeverScanTwentyArchivedRequestRows` | 20 terminal + 1 active，禁止三种请求历史关系扫描后 reopen、active、claim、pending_tasks 均成功，扫描次数为 0。 |
| `SkipAuditReservesUint64WidthAndRejectsCounterExhaustionBeforeInput` | 用合法宽度状态构造 UINT64_MAX-1，再经真实 TryOnce miss 递增到 UINT64_MAX；无截断/溢出，可重启。继续触发返回 ResourceExhausted，source position/revision 不动，owner 未 fail-stop。 |

这 8 项独立探针使用 Memory adapter。四种 KV adapter 的正常与故障契约覆盖来自本轮重新编译和运行的参数化回归，不能说每条独立探针都跑过四后端。

## 新构建和测试证据

工具链：Ubuntu Clang 18.1.3，CMake 4.4，Unix Makefiles。项目与最终探针均为 C++20，编译参数含 `-Werror -Wall -Wextra -Wpedantic -fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`，链接启用相同三 sanitizer。各 CTest 环境含 `detect_leaks=1`、`halt_on_error=1`、`LSAN_OPTIONS=exitcode=23`。

```sh
cmake -S /tmp/eventflow-m4-owner-review -B /tmp/eventflow-m4-owner-review/build \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS=-Werror
cmake --build /tmp/eventflow-m4-owner-review/build --target owner_probe \
  event_request_test event_processor_test event_processor_cep_test \
  request_runtime_test request_latest_test engine_runtime_test engine_latest_test -j 4

env LD_LIBRARY_PATH=/tmp/eventflow-event-request-work/cpp3rdlib/lib \
  ctest --test-dir /tmp/eventflow-m4-owner-review/build/eventflow \
  -R '^(event_request_test|event_processor_test|event_processor_cep_test|request_runtime_test|request_latest_test|engine_runtime_test|engine_latest_test)$' \
  --output-on-failure

env LD_LIBRARY_PATH=/tmp/eventflow-event-request-work/cpp3rdlib/lib \
  ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
  /tmp/eventflow-m4-owner-review/build/owner_probe '--gtest_filter=M4OwnerProbe.*'
```

| 当前运行 | 结果 |
| --- | --- |
| `event_request_test` | 88/88；四 KV adapter，含新 TryOnce/cap/active 恢复 |
| `event_processor_test` | 97/97 |
| `event_processor_cep_test` | 100/100 |
| `request_runtime_test` | 58/58 |
| `request_latest_test` | 53/53；明确是 tmp 当前版本 |
| `engine_runtime_test` | 69/69 |
| `engine_latest_test` | 38/38 |
| 合计 | 503/503，CTest 7/7，112.35 秒，exit 0 |
| 最终独立探针 | 8/8，3,415 ms，exit 0 |

日志：[configure-final.log](/tmp/eventflow-m4-owner-review/configure-final.log)、[build.log](/tmp/eventflow-m4-owner-review/build.log)、[probe-build-final.log](/tmp/eventflow-m4-owner-review/probe-build-final.log)、[regressions.log](/tmp/eventflow-m4-owner-review/regressions.log)、[LastTest.log](/tmp/eventflow-m4-owner-review/build/eventflow/Testing/Temporary/LastTest.log)、[probe-final.log](/tmp/eventflow-m4-owner-review/probe-final.log)。最终成功编译无 warning/error，测试无 sanitizer diagnostic。

探针搭建阶段有两次仅属审查 harness 的纠正：父 CMake 作用域不可见子项目 imported GTest target，改为链接相同 `cpp3rdlib` 预编译库；首版探针使用无 scope 的 SQL query，被正常分区查询约束拒绝，改为分别指定 a/b scope。二者没有修改被测代码，也不作为产品缺陷。

## 发现处置与剩余 gap

1. **本轮没有确认新增内核缺陷。** 新 v2 owner/TryOnce/cap/progress 切片可以按本文范围整合；root 需保留主树额外 4 项 RequestLatest 测试并重新完成合并后的门禁，不能只沿用 tmp 的 503 项结果。
2. **完整 C3 资源契约仍需显式部署约束与证据。** 当前实现对 lifecycle 最大状态、Catalog 关系/隐藏状态闭包、变量宽度及 audit 预留有直接源码和边界测试；本轮不证明任意 Engine 与 IStateStore 各自独立的 `max_read_bytes`/canonical batch/count 配置都能在受理前统一验证。IStateStore 接口本身没有暴露后端限额协商；必须由后续部署配置形成兼容约束并验证，不能因本次默认四后端通过而关闭原报告中的全部 byte/read/store/companion 要求。
3. **codec 不是恶意直接 KV 写入的完整恢复审计。** 本轮验证了上述 7 种新字段损坏；沿用既往明确边界，完全删除合法 active-index 身份的外部损坏不会靠无界历史扫描自动检测。该完整性审核属于 M5 人工恢复协议，不据此加入无界扫描。
4. **不覆盖 C8 应用 driver 的最终正确性。** 本次只检验 owner 核心与通用接口；A/B/C/D 应用、线程执行器如何持续推进工作、超时后的 task tracking 等由独立 app reviewer 检查。当前结果不证明完整业务演示交付。
5. **不覆盖 M5 与最终系统 gate。** 真实 Coordinator/KV/多个 Compute 进程、Kafka Compose 集成、watermark/consistent checkpoint、clean-stop/人工 crash recovery、SDK/DDL/Job 控制面、后续 UDF/UDTF/UDAF/Process Function、generic completion/view-change 有序 witness/seal/reservation、多个 fresh-context 最终全局 review 均不在本文通过范围。M4 与最终 goal 不能因此完成。
