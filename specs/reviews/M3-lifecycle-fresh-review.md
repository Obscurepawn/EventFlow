# M3 生命周期独立 fresh-context 审查

日期：2026-09-14。审查者：`m3_lifecycle_gate`，未参与本次 EventProcessor/FSM 实现。本文是 M3 里程碑审查，不替代最终多位 fresh-context 全局审查。

审查依据：`AGENTS.md`、`specs/INDEX.md`、`specs/event-lifecycle.md`、`specs/runtime-contracts.md` 的 C1/C2/C6 及总规格当前优先级决定。实际读取当前 EventProcessor、Engine、FSM、source、clock/watermark、state/cache 接口和实现，以及对应测试与已有独立组件 review。没有修改生产源码、仓库测试或主构建；独立探针和构建保存在 `/tmp/eventflow-m3-lifecycle-gate/`。

## 结论与处置

首次审查确认 **1 项 P2（R1）**，包含同一时间语义缺陷的两个可复现表现：延迟 timeout 回调缺少当前 dispatch 时间；其转换新建的相对 Timer 被倒推到过去并在同次 advance 中再次过期。Root 已修复，原独立反例未修改并已全部转绿，R1 当前为 **CLOSED / 已独立复验**。当前生命周期 gate 在本文列明的 M3 范围内 **PASS**，没有遗留已确认缺陷；M4/M5 和最终全局验收边界仍保留。

修复后独立重编当前 EventProcessor/FSM，并链接匹配当前 Catalog 头文件的新主构建库，**129 / 129 通过、0 skip、退出码 0，无 ASan/UBSan/LSan 诊断**（仓库 97 + reviewer 32）。原时间反例、UNKNOWN、部分完成和存储联合恢复全部通过，详见文末复验记录。

其余本次核查路径未发现已确认缺陷。首次独立运行包含当前仓库 93 个实例和 reviewer 首批 20 个实例：113 项中 109 通过、4 失败；扩展后单独运行 32 个 reviewer 实例，24 通过、8 失败，失败均属于 R1。运行均无 ASan/UBSan/LSan 诊断。下面保留原始证据；后续修复应追加复验，不覆盖原始发现。

## R1 / P2：延迟 timeout 回调与新相对 Timer 使用过去的 deadline（已关闭，以下保留首次发现）

**契约：** C2 要求每次 dispatch 固定一个 processing time，本轮所有算子共享；过期先于同刻输入。原 deadline 可以作为审计或 Timer 身份，但不能替代当前执行时间。读取当前稳定 MV 的 guard/update 应能一致地使用当前执行时间。

**当前调用链：** `EventProcessor::Impl::run` 在整个到期子轮次使用 `operation.target_us`；Engine 在该时间完成所有过期 TTL 的 typed delete、稳定 MV 后进入 hook。hook 调用 `FsmProgram::advance(state, at, reads)`。首次审查版本的 `FsmProgram::advance` 却构造 `FsmEvent{"$timeout", cause, deadline, {}}`，`transition` 再以 `event.time_us` 调用 `enter`。`FsmContext` 只有该 event 和旧 state，没有可供回调使用的独立当前执行时间。

**复现 1：** 在 0 接纳 arm，进入 deadline=5 的 armed 状态；直到 `advance(20)` 才执行。插入 timeout guard 记录 `context.event.time_us`，实际记录为 **5**；同次 `CommittedRound.processing_time_us` 为 **20**。这会让使用时间差判断 readiness/freshness 的 timeout guard/update 对当前 MV 采用过期的时间基准。

**复现 2：** armed 的 timeout 转到 settling，settling 的相对 timeout=7，再由 timeout 转回 idle。相同 `advance(20)` 实际得到：

```text
state = idle
entered_at_us = 12
deadline_us = null
occurrences = 2
```

固定当前时间的语义应得到 `settling / entered_at_us=20 / deadline_us=27 / occurrences=1`。原实现先把 settling 进入时间设为 5，然后立即把刚创建的 deadline=12 再次过期。这不是按显式历史时刻驱动的 replay。

**独立证据：** `lifecycle_probe.cpp` 中 `ReviewTimeoutGuardCanObserveFixedDispatchTime` 与 `ReviewLateTimeoutStartsFreshRelativeTimerAtCurrentTime`，分别在 Memory、Rocks、HTTP→Memory、HTTP→Rocks 四个参数上失败。日志：`initial-run.log`、`expanded-run.log`。两项共 8 个失败，无 sanitizer 诊断；使用当前公开 API，没有篡改持久化状态。

**Root 已接受的整改方向：** `FsmEvent.time_us` 始终为实际 processing/evaluation 时间，另加可选 `scheduled_time_us` 保存 timeout 原 deadline；`FsmEmission` 同样区分实际时间与调度审计时间，EventProcessor 编解码传递该字段。timeout 转到新的 timed state 时，以当前 now 建立相对 deadline，不能补造并连续过期过去的新状态。历史 replay 由调用方显式逐个 deadline advance。对应调整旧 timeout-cycle 测试和 EventProcessor 的 timeout outbox 容量预留。

**首次处置：** Root 接受整改；完成实际修改后，已按文末记录通过 reviewer 原探针复验并关闭。此次报告没有替 Root 修改规格或标记整个里程碑完成。

## 已核查的生命周期不变量

| 项目 | 实际源码/测试证据 | 判断与边界 |
| --- | --- | --- |
| 输入身份与精确内容 | `event_payload`、`process` 在运行 Timer/Engine 前检查 source tuple、scope、typed payload；processing time 不进入 broker 内容 | 持久身份独立于当前 delivery token；重复不推进 MV/FSM/时钟，同身份不同内容 Conflict。独立多分区相同 offset 探针通过。 |
| 有界去重历史 | receipts 按 count/bytes 淘汰，positions 单独保留 | 保留内精确比较；早于 high-water、但 receipt 已淘汰的输入明确 FailedPrecondition，不能重算或静默 ACK。不是无限历史的任意重放能力。 |
| 输入与分区确认 | processor progress 与数据同批；source 内部 `acknowledge` 按已交付完成前缀推进 | 本 shard `next_offset=max accepted+1` 可以有数值 gap，不能当全局 broker frontier。source 的 token/replay/UNKNOWN 契约和调用方 compute-confirmed 后 ack 测试已存在；多 Compute 路由整合属 M5。 |
| MV 稳定读与 Capture | Engine 先稳定输入 graph；hook 为所有规则冻结读，再 emit capture；Engine 再稳定 capture graph | guard 不见同轮一半更新的 capture 子图；可以读前轮 capture 派生 MV。sink 与其 MV、FSM、进度、outbox 原子提交；反馈 graph 和跨 scope 输出拒绝。 |
| strict-next / reset / once / crossing | FsmProgram 的有序 transition、`followed_by` 参数、reset 分支；EventProcessor 运输去重 | 独立集成探针验证 Quote 不消费、第一条负面 News 消费，重复不再次消费，后续正面 News 不重新触发。FSM 组件 review 已覆盖 deadline equality、优先级和状态编码预算。 |
| C6 组合能力映射 | `on_event`；`on_event(...,"*",predicate)` 表达 level；`crossing`；`followed_by(...within,strict_next)`；once 参数；reset_event；all/any/not_guards；PeriodicPolicy | 这些是当前可调用的构造/参数入口，不声称已有完整自定义插件体系；实际 A/B/C/D、Ready 前置及 Request 接入属 M4。 |
| TTL/窗口撤回与 Latest | `data_candidate` 覆写当前 generation/deadline；到期轮次生成 typed Delete；每 scope 单行 base relation 表达 Latest | 替换更新 TTL，旧 deadline 不删新版本；到期撤回级联 MV，Crossing 可回落；当前 Latest 表示当前接纳行，不声称任意历史 LATEST BY 或候选业务版本策略。 |
| 周期 fixed-now | 周期合并最新 slot，实际 FsmEvent 使用 at；ClockFiring/PeriodicPosition 保存计划 slot/skipped | 仓库 slot100/TTL105/advance110 回归通过。独立同刻两 policy 的 occurrence ID 不冲突、读值相同、恢复保留两项。R1 仅涉及 FSM timeout 时间路径。 |
| UNKNOWN 与缓存边界 | Engine 保留 canonical batch；processor 保留候选状态/原 operation/新 occurrence；ready 拒绝 pending | resolve 不重执行 guard/factory，不提前公开候选状态。独立 UNKNOWN-before-write 探针验证相同 canonical batch 重试、callback 只执行一次；仓库已有已写入但丢失响应路径。 |
| 部分提交与 outbox | `ProcessResult.committed_rounds` 只返新确认轮次；outbox 属于同批 runtime state | 先完成 Timer、后数据拒绝或 UNKNOWN 时不丢已完成效果，也不二次通知。独立 max_rounds=1 后原输入重试探针只产生一次 timeout occurrence。 |
| outbox 容量与消费 | 每轮入 Engine 前 reserve；acknowledge_occurrences 独立提交并遵守 UNKNOWN | outbox 满时明确背压；消费可独立释放容量。callback 实际输出/编码超限仍沿用已经开始 dispatch 的 fail-stop 契约。 |
| 恢复状态边界 | `open/decode` 校验 Catalog/规则版本、scope、deadline、generation、source progress、outbox identity/read schema | 独立存储快照恢复探针联合验证 FSM、TTL、dedup、outbox、MV/源位置，覆盖四适配器；Rocks 恢复目标实际关闭后重新 open。不是 M5 dirty-run/自动恢复证明。 |
| Clock/Watermark/TimerQueue 组件 | `time.cpp` 和既有 IO/time review | 单调 clock、generation Timer、idle/late tracker 与 snapshot codec 有组件证据；EventProcessor 当前接收显式时间，不持有 IClock/WatermarkTracker。 |
| 跨 round cache | CachedStateStore 的确认 revision/owner epoch/UNKNOWN 阻断，EngineCache 真实 HTTP 后端集成；独立组件 review | 状态缓存的独立复验已在另份 review；新 query cache 由另位 reviewer审查。本报告不把组件预算当成 Compute 节点总预算已分配。 |

## 首次独立构建与运行

独立 CMake/Unix Makefiles 工程直接重新编译当前 `event_processor.cpp`，包含仓库 lifecycle 测试并增加 reviewer TEST_P；依赖库取自已完成的 `/tmp/eventflow-processor-build/eventflow`，没有触发主 build 的 clean 或重建。Clang 18.1.3、C++20、`-Werror -Wall -Wextra -Wpedantic -fsanitize=address,undefined,leak -fno-sanitize-recover=all`。第三方预编译库的内部没有重新插桩。

```sh
cmake -S /tmp/eventflow-m3-lifecycle-gate -B /tmp/eventflow-m3-lifecycle-gate/build -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
cmake --build /tmp/eventflow-m3-lifecycle-gate/build -j 2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 /tmp/eventflow-m3-lifecycle-gate/build/lifecycle_gate --gtest_filter='*Review*' --gtest_brief=1
```

Reviewer 八组参数化探针共 32 个实例：R1 两组 8 失败；其余六组 24 通过，包括 strict-next 运输重复、多源分区、部分 round budget、同刻周期、UNKNOWN-before-write、联合存储恢复。`initial-run.log` 还保留独立运行当前仓库 93 个实例全部通过的证据。

## 当前范围边界与交接

- M3 组件层提供 processing-time 生命周期、声明式有限 FSM、输入身份去重、显式 advance、durable occurrence outbox 和存储恢复能力。完整里程碑状态仍由 Root 在 R1 闭环、主构建回归和规格同步后判断。
- 全局 watermark 的分区输入协调、late 策略如何影响 event-time 窗口、idle 控制消息持久化仍须 M5 Owner/Coordinator 链路落实；当前 tracker 是组件，`ProcessorEvent` 还没有 event-time/late-policy 输入路径，不能宣称已经实现完整 event-time 窗口。
- 启动恢复后读取一次 SystemClock、无输入自动驱动 advance、暂停输入、跨 shard 已完成连续 ACK 前缀、owner epoch 切换和 Compute 节点总 cache budget 是 M5 接线责任；显式 advance 的测试不能代替真实进程调度。
- KV snapshot/flush 只是存储边界。M5 一致 checkpoint 必须绑定 Catalog/graph、source 恢复位置、merge/clock 进度、FSM/timer、Request/完成及控制状态，再发布 manifest；恢复必须依该位置 seek，不能使用当前 broker group commit 替代。RocksDB WAL 保留。
- durable outbox 到冻结 Request 的接纳、函数派发/完成、PendingTrigger 及失败/重试/取消属于 M4；消费先有持久接纳，再以 occurrence ID 幂等确认。只有 sink 的存在不等价于全部 Request 协议完成。
- 三类实际进程和至少两个 Compute 的公开 HTTP E2E 尚未由本审查证明；HTTP 测试为同进程服务对象真实 socket。Kafka wire mock 也不等价于真实 broker 集成。固定 `apache/kafka:4.3.1` 的 Compose 任务按用户决定后续补充，不阻塞本次主功能审查。
- UDF/UDTF/UDAF/Process Function 按最新用户决定待核心完成后开展，不列为此次 M3 缺陷。最终仍须多位新上下文 reviewer逐原始需求、架构/接口、实现和测试证据做全局验收。

## R1 整改源码复核进展（复验前记录）

Root 已修改 `FsmEvent/FsmEmission`，新增可选 `scheduled_time_us`；`advance` 的 timeout event 使用当前 `time_us`，原 deadline 仅保留为 scheduled 审计字段，新的相对等待从当前时间开始。EventProcessor 编解码同步保存并验证 scheduled 时间不晚于实际时间；timeout 容量预留改为一次当前转换。C2、M3 规格和实现说明同步记录新语义；主测试新增四后端直接回归。审查者已只读核对这些实际源码变更。

此时 R1 仍为 OPEN，待独立重新编译运行。另一个并行 Catalog 改动改变了结构体 ABI，因此不使用新头文件配旧 engine/sql 静态库；等待 Root 提供匹配的新构建后再复验。旧库与新头文件混用产生的错误不能当作代码缺陷证据。

## R1 独立复验与最终处置

**结果：CLOSED；本次 M3 生命周期 gate PASS。** 审查者没有修改原两项时间反例或另外六组独立探针。复验工程改为直接编译当前 `src/runtime/event_processor.cpp` 和 `src/runtime/fsm.cpp`，其余库链接 Root 已完成的、与当前 Catalog/FSM 头文件匹配的 `build/libef_*.a`；主构建没有被 reviewer 改写或重建。

1. 独立运行 `--gtest_filter='*Review*' --gtest_brief=1`：**32 / 32 通过**，3.643 秒，退出码 0；日志 `/tmp/eventflow-m3-lifecycle-gate/fixed-probes.log`。原两项反例在四个 backend 上均转绿，timeout guard 观察 20，新相对等待保持 settling/entered_at=20/deadline=27，只有一次 timeout occurrence。
2. 复核 Root 新增回归时发现其 `max_occurrences=1` 却调用 `pending_occurrences(10)`，与公开 limit 契约不符；这是测试参数错误，不是生产实现缺陷。Root 将三处读取改为 1，reviewer 未修改仓库测试。
3. 重新编译该测试翻译单元后独立运行完整套件：**129 / 129 通过、0 skip**，13.919 秒，退出码 0；日志 `/tmp/eventflow-m3-lifecycle-gate/fixed-full-run.log`。包含仓库 97 个实例和 reviewer 32 个实例，覆盖 Memory、Rocks、HTTP→Memory、HTTP→Rocks。
4. 仓库新增的 `DelayedTimeoutCallbackAndFreshWaitUseCurrentProcessingTime` 在四个 backend 上通过：update 回调观察 now=20；容量 1 能接受唯一 timeout；重开后 outbox 同时保存实际时间 20 与 scheduled deadline 5；消费后 advance(26) 不触发，advance(27) 正常触发。

两次复验均启用 Clang ASan/UBSan/LSan 和失败退出，未出现 sanitizer 诊断。修复改变时间语义，故本次通过证据为重新编译后的运行结果，不沿用首次 93 项测试结论。C2 和 M3 规格已经明确记录实际时间与 scheduled 审计时间的区别。本文前面的 OPEN、旧时间行为和失败输出均为带标题标识的历史审查记录。

完整复验命令：

```sh
cmake -S /tmp/eventflow-m3-lifecycle-gate -B /tmp/eventflow-m3-lifecycle-gate/build -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
cmake --build /tmp/eventflow-m3-lifecycle-gate/build -j 2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 /tmp/eventflow-m3-lifecycle-gate/build/lifecycle_gate --gtest_brief=1
```

复验版本 SHA256：

```text
ce6c4e5b471347e7302d9b4e3bb5204e9778139a31341a0ce33b27ae41c352f2  include/eventflow/fsm.hpp
958ea6713758503364381c550e53c66d9004ebcb2ea5ddc15b2409c42d07f3f5  src/runtime/fsm.cpp
d339a32360cb883c2423a5649d88a9061572ab5ebcd4c0807cc9e156517966c6  src/runtime/event_processor.cpp
4c556e716cce98fd3792b3dec281d4bbec4e1821826bf0a9fd58c678762236ec  tests/event_processor_test.cpp
2f8c01081499d0d7feacfb886ced7d11826c181ded67816bc081a1572ef2f79a  /tmp/eventflow-m3-lifecycle-gate/lifecycle_probe.cpp
```

本次没有新增未解决生产缺陷。Root 可据本审查关闭 M3 生命周期部分的 R1；完整 M3 状态还需汇总主构建和其他组件审查证据。M5 的源/clock 驱动、global watermark/late-policy、一致 checkpoint、真实多进程和最终多个全局 review，仍按前述边界执行，不能由此次 PASS 替代。
