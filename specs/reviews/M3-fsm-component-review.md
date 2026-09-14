# M3 FSM 独立组件审查

日期：2026-09-14。审查者：fresh-context agent `m3_io_time_review`，未参与 FSM 实现。

依据为 `AGENTS.md`、`specs/INDEX.md`、`specs/event-lifecycle.md` 和 `specs/runtime-contracts.md` 的 C6。审查范围包括 `include/eventflow/fsm.hpp`、`src/runtime/fsm.cpp`、`tests/fsm_test.cpp`；只读生产源码和仓库测试，补充探针留在 `/tmp`。本次没有审查 Engine/event processor 集成，不作为 M3 里程碑或最终全局审查。

## 结论与处置

首次审查发现 1 项 P2：部分成功的 `advance`/`step` 返回路径没有验证最终状态的编码长度，可能返回超过 `max_state_bytes`、无法再次编码保存的状态。该问题已由 root 修复，并以未修改的原审查探针独立复验关闭；当前 19 项测试全部通过，没有遗留的已确认组件缺陷。其余检查没有发现可复现的优先级、strict-next、timeout 边界、序号溢出或 codec 身份校验缺陷。

## P2：成功返回的 FSM 状态可能无法编码（已修复并复验）

**位置：** 首次审查版本 `src/runtime/fsm.cpp:207` 在 `advance` 最后更新 `time_us` 后直接返回；`:223` 的 reset 先通过 `initial_state(event.time_us)` 检查序号为 0 的状态，再将 `transition_sequence` 改为真实递增序号，随后直接返回。没有匹配 transition 的 `step` 也继承 `advance` 返回的未检查结果。

**实际触发：** 从一个真实可编码的初态得到长度 N，将 definition 的 `max_state_bytes` 配置为 N，重新创建程序。这个定义和初态均合法，随后：

1. 对 untimed 状态调用 `advance(state, INT64_MAX)`，时间字段由 1 位变成 19 位；函数返回成功，但 `encode_state(result.state)` 返回 `ResourceExhausted`。
2. 对同一状态输入不匹配的 `quote` 事件，时间为 `INT64_MAX`；没有执行任何 transition，`step` 同样成功返回无法编码的结果。
3. 使用可重复触发的 `on_event`，在时间 0 实际接纳 9 条 `news`，得到合法的 `transition_sequence=9`。再输入 reset，结果序号为 10；reset 返回成功，而序号多出的 1 个编码字节使后续 `encode_state` 失败。

**影响：** 上层依据成功状态继续 staging 时，直到持久化编码才发现状态不满足组件自身预算；`step/advance` 成功结果不能保证作为下一次调用或恢复状态使用。该问题不需要溢出或损坏输入，只需有效配置接近状态预算边界。

**建议：** 在所有成功返回之前，对最终 `output.state` 完成与 `encode_state` 一致的验证。应覆盖纯 advance、未匹配事件以及 reset 覆写序号后；失败仍只发生在局部副本，不能发布部分 emissions 或修改调用者 state。

独立探针为 `/tmp/eventflow-m3-independent-review/fsm_review_test.cpp`：`FsmReview.TimeAdvanceCannotReturnAnUnencodableState` 和 `FsmReview.ResetCannotReturnAnUnencodableTransitionSequence` 均按正确行为断言失败。实际输出确认三种调用的 `.ok()` 都为 true，而各返回 state 的 `encode_state(...).ok()` 均为 false。3 项补充测试中 2 失败、1 通过，进程退出码 1，没有 sanitizer 诊断。

**2026-09-14 整改复验：** root 在 `advance` 更新时间之后和 `step` 最终返回之前均调用 `encode_state(output.state)`；因此 reset 改写序号、未匹配事件和纯时间推进都受到最终状态预算约束。新增仓库回归 `FinalTimeAndSequenceGrowthMustFitPersistedStateBudget` 覆盖三条路径。审查者不修改原探针，只重新编译/链接当前库和仓库测试，独立运行 19 项（当前仓库 16 + 原 reviewer 3）：19 / 19 通过，退出码 0，无 sanitizer 诊断。P2 关闭。

## 已检查的契约

| 项目 | 代码与测试证据 | 结果 |
| --- | --- | --- |
| Definition 边界 | `FsmProgram::create`；`InvalidDefinitionsAreRejectedBeforeAnyDispatch` | 限制 states/transitions/register fields/read aliases/steps/state bytes；校验状态名、字段类型和唯一性；timeout 必须为正且有无条件 fallback。 |
| 优先级 | `transition` 按定义顺序遇到首个匹配 guard 即返回 | 独立 `FirstMatchingPrioritySkipsLaterGuardErrors` 通过，后续匹配 guard 中的错误没有被执行。 |
| strict-next 与事件类型 | `followed_by` 的匹配 transition 与可选无条件 rejected fallback | 无关 Quote/Trade 不消费；首个指定 News 不满足条件也结束 strict-next；非 strict 模式可继续等待下一条匹配 News。 |
| timeout 与同刻数据 | `step` 先 `advance`，timeout 内部事件使用原 deadline；外部 `$timeout` 被拒绝 | equality 先 timeout；周期状态循环由共享 `max_steps` 终止；外部事件不能伪造内部 timeout。 |
| Once/Reset/Crossing | `on_event`、`crossing`、reset 分支 | Once 只在 waiting 触发；reset 恢复寄存器和初态、重建/取消 deadline 并递增序号；Crossing 仅 false-to-true emit。reset 的原编码预算 P2 已修复并复验。 |
| typed registers/guard 错误 | `validate_registers`，StatusOr guard/update，all/any/not | 拒绝寄存器形状/类型变化；组合器短路并传播普通错误；update 失败不改变调用者 state。 |
| 声明读集 | `validate_reads` | alias 集合必须完全匹配；缺失和额外读被拒绝，nullopt 是显式缺失行；非空读按预算计费。稳定性由 owning round 提供。 |
| 恢复与定义版本 | `encode_state`/`decode_state` | 绑定 program ID/version，验证状态名、寄存器、非负时间、entered_at 和确切 deadline；过期/伪造 deadline 和错误版本被拒绝。 |
| overflow/原子错误 | `enter`、`transition`、reset | deadline 加法和 transition sequence 有显式检查；timeout 与同次 data/reset 共用转换预算；错误丢弃局部状态/emissions。 |

## 运行证据

- 独立运行主任务已经构建的 `build/bin/fsm_test`：15 / 15 通过，退出码 0。
- 独立 CMake 目标链接当前 `build/libef_fsm.a`、`build/libef_core.a` 与预打包 GoogleTest；未修改或重建主 build。`FsmReview.*`：3 项，其中上述预算回归 2 失败、优先级回归 1 通过，退出码 1。
- 两次运行均使用 Clang ASan/UBSan/LSan 构建产物，并开启 leak 检查与失败退出，无 sanitizer 诊断。测试成功不能代替已复现的语义问题整改。
- 修复后独立运行完整 `fsm_review_test --gtest_brief=1`：当前仓库 16 项与未修改的原 reviewer 3 项全部通过，共 19 / 19，退出码 0。前述失败结果保留为整改前证据。

```sh
cmake -S /tmp/eventflow-m3-independent-review -B /tmp/eventflow-m3-independent-review/build -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
cmake --build /tmp/eventflow-m3-independent-review/build --target fsm_review_test -j 2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 /tmp/eventflow-m3-independent-review/build/fsm_review_test --gtest_filter='FsmReview.*'
```

首次审查 SHA256：

```text
e0e11309623df4c11a628a8b4550bd1f9bb9fce33898ae72534a42ddd3680b7d  include/eventflow/fsm.hpp
9329d84c649834af9c83ddb31fdea809d2c2e35fba01697cdfdea11318e7f6ce  src/runtime/fsm.cpp
b5118ff4ed10ecc536db47c2b3f5231336edfcfec8f76b93bd0243f347d24e9d  tests/fsm_test.cpp
0953f33f2136b69a7929ae79659ed6c345c4879d330c26840b798de010afa49e  /tmp/eventflow-m3-independent-review/fsm_review_test.cpp
```

修复复验版本 SHA256：

```text
57858cdc4cae20ae55a3931c11fc123278b9abb09abcaff1baf5b9f1a6b8da49  src/runtime/fsm.cpp
5ae4469db9c4580db8516b78c625b73b38083331666f6b6ad4db47b86a930154  tests/fsm_test.cpp
```

## 剩余范围

- P2 已修复并独立复验关闭；本报告不据此将 M3 里程碑标记完成。
- C6 的完整构造器/组合能力仍需最终逐项映射。当前公开便利构造器为 `on_event`、`crossing`、`followed_by`，Once/strict-next/within/reset 可通过参数或 definition 表达；LevelPredicate、Periodic、Ready AND/OR 的完整产品接入不能仅凭这些组件测试视为完成。
- callback 的纯函数约束、MV read 的 round 稳定性、业务/运输去重发生在 step 前、emit 到 Capture 的 staging 传播、Timer/FSM/progress 同 shard 原子持久化，以及提交后才派发，都需 Engine/event processor 与多进程测试证明。
- 程序版本由定义作者在 callback 或声明语义变化时显式递增；当前 codec 校验 ID/version，不对任意 C++ callback 自动计算语义指纹。
