# M4 RequestRuntime 独立 fresh-context 审查

日期：2026-09-14。审查者：独立子 Agent `request_owner_fresh_review`。范围仅为 RequestRuntime 及其必需的 RequestMachine / Engine 私有桥接；本报告不证明 M4 完成。

状态：RequestRuntime 组件在下述边界内通过本轮审查。初审 4 项问题由实现负责人修正，独立原探针复验通过；B1 为明确保留的直接 KV 写入 / 人工恢复完整性边界。M4 整体仍未完成。审查者只写本文件和 `/tmp/eventflow-request-owner-fresh-probe/`，未修改生产代码或仓库测试。

## 原始要求与审查范围

本轮从 `AGENTS.md`、`specs/INDEX.md`、`specs/request-functions.md`、`specs/runtime-contracts.md` 的 C3 / C7 / C9 建立验收边界，再读取当前源码和测试；未依赖此前 RequestRuntime 的实现者解释或审查结论。

主要审查文件：

- `include/eventflow/request_runtime.hpp`
- `src/runtime/request_runtime.cpp`
- `tests/request_runtime_test.cpp`
- 必要追踪：`src/runtime/request_machine.cpp`、`src/runtime/engine.cpp`、`src/runtime/engine_internal.hpp`

需证明的核心不变量是：固定 capture revision R、admission revision R+1；同一 Catalog 图中的共享祖先；Request / active-index / terminal record / outbox 与关系结果同批提交；UNKNOWN 原批确认前不发布候选内存状态或任务；scope / request / attempt fencing；完成、取消和超时能够释放额度；唯一 owner 与固定版本不被公开 API 绕过。

## 初审发现与处置

### R1 — 公开 Engine 确认入口可使 Runtime 永久滞留 UNKNOWN（高，必须修正）

初审位置：`src/runtime/engine.cpp:965`、`src/runtime/request_runtime.cpp:607`。

复现：`RequestOwnerFresh.PublicEngineResolveMustNotStrandRuntime`。

1. 已建立 RequestRuntime 后让一次 `admit` 获得 UNKNOWN，实际 KV 已提交。
2. 调用公开 `engine.resolve_pending_commit()`，它返回成功并清空 Engine pending。
3. 再调用 `runtime.resolve_pending_commit()`，返回 `request owner has no pending commit`；Runtime 的候选 Book 仍在 pending，`active_requests()` 也永久拒绝。

这违反 Runtime / Engine 共同发布的 owner 协议。正确修复应对 pending 记录提交归属，通过私有入口确认 RequestRuntime 批次，同时保留来源事件批次的公开确认路径。初始化 UNKNOWN 尚未返回 Runtime 对象时，也必须有可恢复的确认路径。

处置：已修正并独立复验通过。Engine 保存 `pending_for_request`，公开 resolver 拒绝抢占已 attached RequestRuntime 的提交；Runtime 通过私有 `resolve_request_commit` 确认。额外测试确认普通输入 UNKNOWN 在 attached 期间仍可用公开 resolver，初始化 factory 返回 UNKNOWN 以及 Runtime 在 UNKNOWN 中被销毁后也保留恢复入口。

### R2 — companion 回调持有覆盖 Request namespace 的权限（高，必须修正）

初审位置：`src/runtime/request_runtime.cpp:509`、`:578`；`src/runtime/engine.cpp:437`。

复现：`RequestOwnerFresh.CompanionCannotCorruptRuntimeIndex`。

Runtime 先 stage 正确 record / index，再调用公开 `RequestCommitHook`。回调执行 `context.stage({request_index_key, "corrupt"})` 时，`runtime_frame` 使 namespace 检查整体豁免。`admit` 返回成功，revision 从 3 变为 4；重建 Engine / Runtime 后报 `Invalid JSON`。内存中已受理记录与 durable index 因此不一致。

回调用于同批写 lifecycle / outbox / accepted-once 状态，不能因此获得 Runtime 内部 record / index 的覆盖权限。capture 也接收相同 RoundContext，需同步限制。RequestRuntime 的内部 stage 应走独立私有能力。

处置：已修正并独立复验通过。`runtime_frame` 不再整体豁免 namespace 检查；仅内部 `stage_request_state` 在自身调用期间授予 stage 权限。原 companion 复现与新增 capture 覆盖探针均被明确拒绝，revision 不变且 Runtime 可重建。

### R3 — 销毁 Runtime 后 Catalog 激活会使已受理请求无法恢复（高，必须修正）

初审位置：`src/runtime/request_runtime.cpp:137`、`:383`；`src/runtime/engine.cpp:991`、`:1151`。

复现：`RequestOwnerFresh.DetachMustNotPermitCatalogToOrphanAcceptedRequests`。

`admit → claim → runtime.reset() → engine.install_sql(无关新表)` 全部成功。但随后的 `RequestRuntime::open` 报 `request index layout or configuration mismatch`；此前已提交的 Running request / outbox 失去正常恢复入口。

对象析构只释放内存 owner，不代表 durable 请求、任务或归档的版本约束已经解除。若当前实现没有版本迁移 / rebind 协议，应明确拒绝可使旧请求无法恢复的 Catalog 激活，且保护应在 Engine 重建后仍成立。

处置：已修正并独立复验通过。普通 `Engine::install` 对已有 request identity domain 的 Catalog 拒绝激活，保护不依赖 Runtime 对象 lease；仓库补充测试验证 Engine 重建后也保持拒绝且旧 outbox 可恢复。后续显式受控 rebind 属于独立范围，不能通过普通 install 默许。

### R4 — active 字节预算只计当前编码，可能受理后无法启动（中，必须修正）

初审位置：`src/runtime/request_runtime.cpp:223`、`:448`、`:578`。

复现：`RequestOwnerFresh.ActiveByteLimitReservesProgressCapacity`。

本例 Ready record 编码大小为 6144 bytes。设置 `max_active_bytes=6144` 后，唯一请求能够成功受理，但第一次 `claim` 因新增 task identity / Running 元数据而返回 `active request byte limit reached`。没有其他请求可完成释放空间，重新 claim 不会改善；必须取消或等待请求期限到达。独立探针确认 cancel 可提交，因此问题是已受理工作无法推进，并非终态写入必然死锁。

应在受理时按请求可能增长的有界状态预留预算。可接受的行为是受理阶段明确 `ResourceExhausted`，或成功受理后允许其在已有预算内推进，不能要求任意过小配置仍被受理。

处置：已修正并独立复验通过。每个 active record 按 `machine.max_state_bytes + admission envelope + 64` 与当前编码大小的较大者计费，提前预留状态增长。原 6144-byte 紧预算现于受理阶段返回 `ResourceExhausted`，无 record / active 残留。record 上限扫描中的成功受理路径仍可 claim、cancel 并重建。

### B1 — 删除有效 active-index 条目不会被恢复扫描发现（明确完整性边界）

复现：`RequestOwnerFresh.RemovedActiveEntryMustNotSilentlyHideRunningOutbox`。

测试绕过 Engine，直接以合法 commit owner / revision 改写 KV index，把 `active` 置为空数组，保留 Running record。重建成功，但 `active_requests()` 和 `pending_tasks(16)` 均为空。

当前恢复只加载 index 所列条目，并未全扫 terminal archive 验证反向完整性。因此它能拒绝重复身份、错误 plan、条目指向终态或缺失记录等局部不一致，却不能识别 index 中被完全删掉的 active 身份。本轮不把可任意写 KV 的主体视为普通公开 Runtime 调用者；4 项 owner 修复后，合法单 owner 原子写路径不应产生该状态。本项保留为 M5 人工恢复 / 完整性审核边界，不用无界 archive 扫描扩大本次范围。

## 初审验证证据

探针目录：`/tmp/eventflow-request-owner-fresh-probe/`。使用独立 CMake / Unix Makefiles 构建，Clang 18.1.3、C++20、`-Wall -Wextra -Wpedantic -Werror`，ASan / UBSan / LSan 和 failing diagnostics。RequestRuntime、RequestMachine、Engine 三份生产源码在该目录重新编译；其他库链接仓库当前 Debug 库，未声称所有依赖均重新构建。

初审命令：

```sh
cmake -S /tmp/eventflow-request-owner-fresh-probe \
  -B /tmp/eventflow-request-owner-fresh-probe/build \
  -G 'Unix Makefiles' -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
cmake --build /tmp/eventflow-request-owner-fresh-probe/build --target probe -j3
env ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
  /tmp/eventflow-request-owner-fresh-probe/build/probe --gtest_filter='RequestOwnerFresh.*'
```

| 验证 | 结果 | 日志 |
| --- | --- | --- |
| 当前仓库 RequestRuntime 原有测试 | 52 / 52 通过，四种 KV adapter，11.342s | `baseline.log` |
| R1–R4 与 B1 原复现 | 5 / 5 挑战断言失败，分别复现上文问题或边界；exit 1，无 sanitizer 报告 | `probe-initial.log` |
| 记录预算 12000 / 16000 / 20000 / 24000 / 32000 / 48000：未受理不留 active，成功受理后 claim / cancel / reopen | 通过 | `followup.log` |
| active-index 错 plan、重复条目 | 恢复明确拒绝 | `followup.log` |
| detach 后公开 hook 写同 namespace 的 I key | 拒绝且 revision 不变 | `followup.log` |
| 同 store 上第二 Engine 对旧 revision 尝试受理 | Conflict，未覆盖首个 owner 的新状态 | `followup.log` |

初审源指纹：

```text
include/eventflow/request_runtime.hpp f9fe30750a89ddba5d549105277d74e77ad07d2fdbe75596b4a773994fb8abd9
src/runtime/request_runtime.cpp 9590fced98c16f9164909b99e62d0e7fda5ef19ec6bb1e756eca9e9d6889babf
src/runtime/request_machine.cpp 482986f12596a913db20808ae29948b90a93b2cb9242a2adfab7924fa11c2672
src/runtime/engine.cpp e761ca9bf1938ed322066273c0c993ed4c2c4c6669174a60a93eac2d9d21d136
tests/request_runtime_test.cpp 384640890bb63b6596720e4465e0325a1ee4af08a5c86d24b406e18cec47d8ac
```

## 修复后的独立复验

重新读取并编译修正后的 RequestRuntime / RequestMachine / Engine。探针 suite 改名为 `RequestOwnerIndependent`，避免与实现负责人纳入仓库的回归测试同名。R4 的正确断言为“可在受理阶段明确拒绝；一旦受理应可推进”，其余原复现断言保留。

```sh
cmake --build /tmp/eventflow-request-owner-fresh-probe/build --target probe -j3
env ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
  /tmp/eventflow-request-owner-fresh-probe/build/probe \
  --gtest_filter='-RequestOwnerIndependent.RemovedActiveEntryMustNotSilentlyHideRunningOutbox'
```

结果：69 / 69 通过，exit 0，13.814s；包含当前仓库 58 项和额外 11 项独立探针。编译日志 `build-corrected.log`，运行日志 `corrected.log`；均位于上述 `/tmp` 目录。未出现 compiler warning 或 sanitizer 诊断。主动排除的唯一断言是 B1 对直接 KV 删项的检测要求，该边界没有被修复或宣称通过。

交接前再次执行同一 build，结果无需重新编译（`build-match.log`），并用 `sha256sum -c corrected-source.sha256` 核对六份审查源文件全部匹配本次通过的版本。最终主要源指纹：

```text
src/runtime/request_runtime.cpp b1752f3a9db9960e8121673adf68aedce6de00a272e9818676c4bf0560b12601
src/runtime/engine.cpp c9fc39e1d64d822db7601202305bdfeec1e6aad0ca506300e994eab112eefdff
src/runtime/engine_internal.hpp 83e27423c4e6cebd6d84a16a64d6cadcefa0a4ee96fa9640f5c2ecd0d24efa24
tests/request_runtime_test.cpp cd1f5ba284dd6e2f120639be2904f6e8c7cce465d8663ef12b0bee53da1b378d
```

额外成功路径覆盖：

- RequestRuntime 初始化 UNKNOWN 时工厂不返回对象，Engine 仍可确认原批，然后重新 open / admit。
- RequestRuntime 在 UNKNOWN 中被销毁后，Engine 可确认，重新 open 能恢复已提交请求并 claim。
- capture 与 companion 都不能 raw stage 已注册的 Request namespace。
- 紧 record 预算不会使已经成功受理的样例无法写 cancel 终态或重建。
- 同 backend 上第二 Engine 在首个 owner 推进 revision 后无法覆盖其状态。
- detach 后普通 hook 的 S / I namespace 保护，以及 Engine 重建后的固定 Catalog 保护。

源码追踪与测试共同支持的结论：固定输入和同图多层共享祖先、record / index / SQL 后继同批提交、UNKNOWN 原批确认、attempt / scope / 终态 fencing、容量释放和 active 字节预留、受控 owner 权限在当前组件范围内成立。私有 Engine bridge、CaptureRequest 和 RequestCommitHook 仍属于受信任 owner 代码，不能把内存内 C++ capability 解释为面向恶意插件的安全隔离。

## 尚未完成的范围

RequestRuntime 与 EventProcessor 的 source / Timer / trigger outbox 尚未接通；Latest selector、A/B/C/D 完整业务轨迹、真实执行池与 owner 的队列满闭环、全局一致 checkpoint、受控停机和人工恢复仍需后续集成验证。当前测试包含 Runtime 对四 KV adapter 的重建，不等于真实多进程部署或 dirty-run 自动恢复的承诺。M4 仍为 active。
