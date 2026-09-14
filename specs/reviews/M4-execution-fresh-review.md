# M4 FunctionExecutor 与 Engine 私有执行桥接独立审查

日期：2026-09-14。审查者：`m4_fresh_execution_review`，独立 fresh-context 子 Agent。

**结论：发现并复现 1 项 P1 内存预算缺陷；root 已修复，独立原始 probe 及相关回归复验通过。本次已覆盖的组件表面未发现其他必须修复的问题。此结论不是 M4 整体完成或最终全局审查结论。**

## 范围与方法

先读取 `AGENTS.md`、`specs/INDEX.md`、`specs/request-functions.md`，再核对 `specs/runtime-contracts.md` C7/C8 及 `specs/archive/request-history.md#function-design`。审查以下文件及相关测试：

- `include/eventflow/function_executor.hpp`、`src/runtime/function_executor.cpp`。
- `include/eventflow/engine.hpp`、`src/runtime/engine.cpp`、`src/runtime/engine_internal.hpp` 中既有的 `EngineRuntimeAccess::open/apply/attach_request_owner/detach_request_owner`。
- `tests/function_executor_test.cpp`、`tests/engine_runtime_test.cpp`，以及受共享 SQL 执行路径影响的 Engine 基线测试。

审查者没有修改生产代码或仓库测试。root 在审查期间修复了下面的 P1，并新增了仓库回归；审查者在 `/tmp/eventflow-m4-execution-fresh/` 建立独立 CMake 构建及自行编写的 probe，重新编译当前源码并运行验证。未使用实现者口述的测试结论代替本次执行结果。

并行开发的 `RequestRuntime`、新增 `apply_lifecycle` 和 `state_store` 私有 accessor 不属于本次语义审查范围；它们可能出现在同一编译单元中，成功编译不代表已经独立审查其行为。

## 发现、处置与复验

| 编号 | 严重度 | 位置 | 问题与影响 | 处置 |
| --- | --- | --- | --- | --- |
| EXE-01 | P1 | `src/runtime/function_executor.cpp`：`validate_row`、`FunctionExecutorState::execute`、`finish_locked` | 原实现用 Row/string/vector 的 `size()` 计费后直接 move callback 结果。callback 的超大 spare capacity 可进入 completed 队列，突破已预留 completion 字节预算；取消分支 `clear()` 同样会保留 Row backing allocation。 | root 改为先校验逻辑值、深拷贝成 host 持有的值，再按保留的 capacity 校验；取消及拒绝路径 swap 释放。独立原始 probe 复验通过，问题关闭。 |

EXE-01 的原始独立复现：`max_output_bytes_per_task = 1024`，函数输出签名为一个 `INT64`；callback 构造 `Row`，执行 `reserve(1000000)`，仅 `push_back` 一个整数后返回。Simple 和 Threaded 均返回成功 completion，而 `completion.output.capacity() * sizeof(Value)` 都为 **40,000,000 字节**。GoogleTest 明确报告两个分支断言失败，进程退出码 1；不是 sanitizer 报错推断。

修复后，同一份独立 probe、同一配置、同一 callback 的上述数值在两种执行器中均为 **40 字节**，测试退出码 0。源码复查确认：

- `validate_row(..., retained=true)` 检查 Row 和嵌套 vector 的 capacity，并保守计算 string capacity 及终止符。
- `execute` 在逻辑值通过后复制 callback 输出，随后再次校验实际保留容量；超限则返回 `ResourceExhausted` 并释放 host 行。
- `finish_locked` 的取消分支使用 `Row{}.swap(output)`。
- 仓库新增 `FunctionExecutor.CallbackSpareCapacityDoesNotEscapeIntoRetainedCompletion`，覆盖 Simple/Threaded 及 Row/string/vector 三层超额预留；本次重新运行该测试所在的完整测试程序。

这修复的是执行器保留的输出内存。callback 执行期间自行申请的临时内存仍属于可信内部函数的实现责任，不具备进程隔离或强制终止能力。

## 独立行为挑战

独立 probe 源文件：`/tmp/eventflow-m4-execution-fresh/execution_probe.cpp`。该目录是临时验证产物；EXE-01 的持久回归位于仓库 `tests/function_executor_test.cpp`。

| 自行编写的检查 | 判定依据与本次结果 |
| --- | --- |
| `FreshExecutor.RetainedOutputCapacityFitsConfiguredBudget` | 上述原始容量复现保留不变；修复后 Simple/Threaded 均通过。 |
| `FreshEngine.TwoPhaseAggregateJoinFilterMatchesReferenceAcross600Frames` | 固定种子的 600 个两阶段 frame，两个 business key、每 key 多行，随机 upsert/delete/重复写及两阶段回撤。图为 `status A → SUM → JOIN(status B) → WHERE → SUM`。自行维护 map 作为参考模型；在 finalize 内验证第一阶段 Staged 行，提交后逐关系验证最终查询结果。全部通过。 |
| `FreshEngine.SqlOverflowDiscardsRawAndTypedStagingAndAllowsNextFrame` | SQL 整数乘法溢出前已 stage raw extension 状态及 typed 行。失败后原 revision、raw KV、基表均保持原像，Engine 未 poison；下一次合法 frame 成功。通过。 |
| `FreshEngine.StaleEngineCannotInvokePrepareEvenForUnchangedFrames` | 两个 Engine 指向同一 store/revision；第一个提交后，第二个尝试空 frame，在 prepare 调用前返回 Conflict。另验证同一 Engine 的重复 attach 被拒绝。通过。 |
| `FreshEngine.UnavailableAfterCommitRetriesExactBatchWithoutRepeatingEitherPhase` | store 已提交后丢弃响应，返回外层 `Unavailable`。两阶段 typed 变化与 raw outbox 一起处于 pending；查询及新 frame 被阻断，Engine 时间/revision 未提前推进。resolve 的完整 `canonical_batch` 字节与原提交完全一致，prepare/finalize 各仅一次，确认后派发两个净变化。通过。 |

## 契约核对

| 关注点 | 当前源码和实际测试支持的结论 |
| --- | --- |
| 任务容量、取消与 completion 排空 | active 包含 queued/running/completed-but-not-polled；submit 不等待空间，poll 转移 completion 后释放 reservation。queued cancel 不调用函数；running cancel 保留额度至 callback 返回。既有 Simple/Threaded 共享契约及真实两 worker/latch 测试重新通过。 |
| receipt、结果 cache 的身份 | active/receipt 按 task ID 检查完整 frozen payload；ready time 属于 receipt 内容。pure 结果 cache 使用函数 ID、version、implementation identity 和 typed inputs；missing optional 与 present-null、类型、浮点 signed zero 有区分。固定 registry version 不可替换；返回 Row 不修改缓存/receipt。entry/byte limits 及 impure bypass 测试通过。 |
| frozen 输入及函数权限 | submit 复制固定输入；worker 只接收 typed `FunctionInputs`，公开接口没有 KV/Engine/RoundContext 参数。普通 apply/hook emit 不允许 RequestCapture、RuntimeStatus 或 FunctionView；raw stage 不能直接写 T 命名空间。此为可信内部 callback 的接口约束，不是对任意 C++ closure 的安全沙箱。 |
| Catalog/registry 精确匹配 | 私有 open 核对函数 ID/version/implementation、输入 alias/required/精确字段、输出字段。公开 Engine::open 对 FunctionView 拒绝。snapshot 经过 Catalog 重建验证，测试中的签名篡改在 store 初始化前被拒绝。 |
| capture/function 不可变 | 私有 prepare 仅可返回受控角色；capture/function 仅新增，不允许 delete、覆盖或同 frame 重复 identity；visible/physical identity 不一致拒绝。finalize 仅允许 RuntimeStatus，且其后代不能进入 request identity domain。 |
| 多阶段 SQL 传播 | touched set 保留净变化为零的行用于重置后代，公开 flatten 才去除净零；聚合从 Before 状态加整个 frame 的原像/终像差额，避免第二阶段重复累计。既有四后端契约与独立 600 帧参考模型均通过。 |
| 发布前失败与 UNKNOWN | 私有 apply 的准备、受控写、SQL 或 finalize 错误丢弃该 frame，未发布 typed/raw 变化。UNKNOWN/Unavailable 保留原批次、变化、时间，并阻断新操作；resolve 不再执行 callback。上面的 SQL 溢出和完整批次字节复现给出额外证据。 |
| owner lease | attach/detach 是同一 Engine 实例的独占标志；private apply 在调用 prepare 前检查 store 当前 revision。KV commit 仍校验静态 owner assignment。它不等于跨进程租约、在线选举或线程同步锁。 |

## 新鲜构建与测试证据

CMake 4.4.3，Clang 18.1.3，独立 Unix Makefiles Debug 构建。实际编译命令包含 `-Werror -Wall -Wextra -Wpedantic -fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`。项目生产源采用 `-std=c++20`。执行环境统一为：

```sh
LD_LIBRARY_PATH=/home/obscure/EventFlow/cpp3rdlib/lib
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
LSAN_OPTIONS=exitcode=23
```

核心复验命令：

```sh
cmake --build /tmp/eventflow-m4-execution-fresh/build --target execution_probe function_executor_test engine_runtime_test engine_test engine_review_test engine_contract_test -j4
ctest --test-dir /tmp/eventflow-m4-execution-fresh/build/eventflow -R '^(function_executor_test|engine_runtime_test|engine_test|engine_review_test|engine_contract_test)$' --output-on-failure
/tmp/eventflow-m4-execution-fresh/build/execution_probe
```

| 程序 | 本次实际结果 |
| --- | --- |
| `function_executor_test` | 39/39 通过 |
| `engine_runtime_test` | 69/69 通过；含 Memory/RocksDB/Remote-Memory/Remote-RocksDB 参数化契约 |
| `engine_test` | 3/3 通过 |
| `engine_contract_test` | 48/48 通过 |
| `engine_review_test` | 54/54 通过 |
| 独立 `execution_probe` | 5/5 通过；其中一个测试执行 600 个 frame |

合计仓库测试 213 项、独立测试 5 项。构建及最终两个运行命令退出码均为 0；无 ASan/UBSan/LSan 诊断。最终 CTest 用时 17.83 秒，独立 probe 用时 9.31 秒。日志：

- `/tmp/eventflow-m4-execution-fresh/regression-build.log`
- `/tmp/eventflow-m4-execution-fresh/regression-test.log`
- `/tmp/eventflow-m4-execution-fresh/probe-build-final.log`
- `/tmp/eventflow-m4-execution-fresh/probe-final.log`
- `/tmp/eventflow-m4-execution-fresh/build/eventflow/Testing/Temporary/LastTest.log`

编译信息从 `build/compile_commands.json` 及独立目标 `flags.make` 读取；未把 CMake 成功配置当成测试通过。

## 未覆盖边界与后续验收

1. 本审查没有验证 RequestRuntime 的 Manifest/NodeRecord/attempt、共享祖先一次派发、目标终态、TaskOutbox 恢复、Latest selector、A/B/C/D 行为，不能代替它们的独立集成审查。
2. 本次已测快照中的 per-Engine attach 标志不限制持有该 Engine 引用的其他代码调用 public apply/install/resolve，也没有 Request 专属 raw S/I namespace 的注册保护。审查交接时 root 已告知开始追加 attached-owner 的 install gate 与保留 S/I 状态保护；这些后续改动不在本文件的已验证快照内，交由 RequestRuntime 独立 reviewer 继续验证。唯一 owner 的入口封装仍须在整体接入时验收。
3. UNKNOWN 与函数 cache 的业务 fencing 由 owner 完成。执行器只回传 opaque task identity；它没有授权 cache 命中直接落 KV，也没有承担重启后的 durable receipt。epoch/attempt/终态相关整体验收仍必需。
4. 四后端测试中的 Remote 使用本机服务适配器；本次没有部署多 Compute 进程、Kafka/Docker 集成、崩溃恢复或一致 checkpoint。不构成这些要求的证据。
5. 本次运行 ASan/UBSan/LSan，没有运行 TSan；有限的真实线程场景不等于所有交错的证明。callback 的有限运行时间、分配和纯函数声明仍需由内部函数实现遵守。
6. 原需求要求 M4 里程碑及最终多轮 fresh-context 全局审查；本文件仅关闭本次明确范围的组件审查及 EXE-01，M4 spec 应继续保持 active，直到完整请求闭环及其他验收都有证据。

## 本次复验源码快照

并行工作区没有可用的已提交差异基线；以下 SHA-256 标记最终验证时实际文件内容。

| 文件 | SHA-256 |
| --- | --- |
| `include/eventflow/function_executor.hpp` | `b6198fa67ae86836eefac25cc8087ae20d033335e95e7e1e0b48ab1957d9ab40` |
| `src/runtime/function_executor.cpp` | `ba9e036bb750d89db2672978362a283db961adf205124df1d752d04c599e7bd1` |
| `include/eventflow/engine.hpp` | `fa36588ecd71290323d4f5df09ccbaae94547d80b092be807a6798dc5fc53888` |
| `src/runtime/engine.cpp` | `02315ccc44245e7b7a747d905ebdc7d6176f75eb9800615852e6bfc73705e364` |
| `src/runtime/engine_internal.hpp` | `b890045f47e8c76f611af93992a10279b7f517def9c02a10d5b731042e31fe31` |
| `tests/function_executor_test.cpp` | `56a9623afdf89dc4f4f6186c7b1d473c44042f4f03b86808f9501e497ae0ffe6` |
| `tests/engine_runtime_test.cpp` | `bc6df9d64ec4740c48029f858bab0fc1acc8e3b9c5bce4f517c0f7cea392c551` |
| 临时独立 `execution_probe.cpp` | `87dc9be34289e8132b5b4b513ffb35f979e871836a4a9c035c4d421a17d09892` |
