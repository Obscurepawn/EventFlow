# M4 EventProcessor 预算限定整改复验

日期：2026-09-14。复验者：`fresh_status_review`。实现依据只使用冻结副本 `/tmp/eventflow-event-request-budget-snapshot`。

结论：**原预算审查的两项 P1 具体反例可在本文记录的冻结版本关闭。** all-candidate expiry SQL 字节预留不足会在新增 source 提交前拒绝，已受理 A 仍可经过 fence 取消；同一 periodic 事件的多个 FSM 输出按总量校验，无解配置提前拒绝，可解配置真实产出两条 occurrence。独立原探针整改版及正向补充 **3/3**、新增边界探针 **5/5**、冻结既有回归 **72+100+97=269 项**全部通过，无 sanitizer 诊断。本次未确认新的预算低估。

**这是复用既有独立 reviewer 线程的限定整改复验，不是新的 fresh-context 全局 review。** 本线程此前的 RequestStatus 和 Latest 复验均不替代此次证据；本报告也不证明 M4 完成，不覆盖 live 中继续开发的 `SourceScopeRuntimeV2` / `TryOnce` 新 API。

## 基线、独立性和修改范围

- 需求依据：冻结副本 `AGENTS.md`、主树 [规格索引](/home/obscure/EventFlow/specs/INDEX.md)、[EventProcessor 接入规格](/home/obscure/EventFlow/specs/event-request-integration.md) 中 all-shard expiry fence、释放先行、不得运行未来 guard/update/sink 探预算的约定。
- 原发现：[原两项 P1 报告](/tmp/eventflow-event-request-budget-review.md)，原独立探针 `/tmp/eventflow-budget-review/review.cpp`。未采信 owner 对修复结果的口述 GREEN，也未复用其二进制或 `.o/.a`。
- 新独立目录 `/tmp/eventflow-event-budget-remediation`，CMake wrapper 直接 `add_subdirectory` 冻结源码，在自己的 `build/eventflow` 中编译全部所需项目对象。依赖仅链接 snapshot 的 `cpp3rdlib`，没有构建下载。
- [review.cpp](/tmp/eventflow-event-budget-remediation/review.cpp) 复制原探针并改为包含 snapshot fixture；针对认可的提前拒绝语义强化断言，并增加可解 periodic 正向测试。
- [bounds.cpp](/tmp/eventflow-event-budget-remediation/bounds.cpp) 为此次新增的独立边界探针；[bounds_fixture.inc](/tmp/eventflow-event-budget-remediation/bounds_fixture.inc) 仅复制冻结测试的 fixture setup 到不同 namespace，并增加一个可设长度的 Candidate String payload，便于在实际公共入口验证完整行边界。被测实现保持冻结原文。
- 未修改 snapshot/live/main 的任何生产源码、测试或规格。仓库内本次仅新增本报告；审查程序、构建与日志均位于自己的 `/tmp` 目录。

## P1-1：all-candidate expiry SQL 字节闭包

当前 [reserve_control](/tmp/eventflow-event-request-budget-snapshot/src/runtime/event_processor.cpp:859) 对所有持久 TTL 和 active Candidate 的 scope 分别计算 `relation_envelopes` 与可达闭包，累计关系 row/key、Latest/聚合 state 和相关 SQL 输入的保守编码；随后将这些数值与最大数值宽度 fence snapshot、生命周期 key、Engine metadata 合并检查。candidate 的 RequestStatus annotation 依赖不被误当作一次 expiry 自动生成所有请求历史状态行。

独立原图保持：Candidate inactive 时产生一个 2048 字节常量 payload 的 SQL 视图；Engine `max_bytes=800 KiB`，lifecycle `max_state_bytes=512 KiB`，RequestRuntime record/index 为 128/64 KiB，machine state 64 KiB。测试先提交 scope=a 的 source 并 admit/claim A，再尝试至多 400 个同时间、不同 scope 的 silent source（它们仍激活 Candidate）。

最终实测：

- source offset **36** 返回 `ResourceExhausted`，`input_committed=false`；revision 不变，processed position 保持 `next_offset=36`。因此成功保留的是原 scope 加 35 个新增 scope，没有把不能保证 future fence 的第 36 个新增 scope 确认为已接收。
- `cancel_request(A,51)` 成功提交 **2 个子轮**，即全局到期 fence 和请求终态；最终 Request 为 **Cancelled**，pending task 为空，再按 key 读取仍为 Cancelled。
- 不是要求原恶意预算配置接受全部 401 个 source，也没有以调大 Engine 字节配置规避原问题。

**探针语义调整如实记录：**原 fixture 在 t=1 claim，attempt timeout 为 20，因而 t=51 的合法控制处理会先将已超时 attempt 判为 Failed。首次强化脚本错误地只接受 Cancelled，出现两条 phase 断言失败；该次调用实际上已成功提交 2 轮并释放任务，不是字节预算再次堵塞。最终独立副本只把 `attempt_timeout_us` 设为 100，保留 Request lifetime 和原 Engine/lifecycle 字节配置，以明确覆盖 t=51 时仍运行的 A 被取消。最终版本 3 项组合实跑通过；初次诊断保留在 [probes.log](/tmp/eventflow-event-budget-remediation/probes.log)，最终证据见 [probes-final.log](/tmp/eventflow-event-budget-remediation/probes-final.log)。

处置：**关闭这条已复现的 expiry 字节遗漏。** 此处没有证明所有可能拓扑、所有独立 store/read 限制已穷尽，边界见下文。

## P1-2：同一 periodic 事件的多 FSM 输出总量

[bind_requests::check_control_event](/tmp/eventflow-event-request-budget-snapshot/src/runtime/event_processor.cpp:673) 累加同事件每条 FSM 可能输出与 CEP 已声明上限，并比较总数量及 `max_outbox_bytes/max_occurrence_bytes`；[reserve_control](/tmp/eventflow-event-request-budget-snapshot/src/runtime/event_processor.cpp:1084) 将 `max_control_emissions` 纳入未来 snapshot 输出容量。

独立测试仍使用 `p1`、`p2` 两条 `on_event(...,"tick")`、interval=10、`max_occurrences=2`，没有 request trigger binding：

| 配置 | 实测结果 |
| --- | --- |
| `max_outbox_bytes=max_occurrence_bytes` | `open` 返回 `ResourceExhausted`，无解 periodic 配置在接收 source 前拒绝。 |
| `max_outbox_bytes=2*max_occurrence_bytes` | source@1 正常提交且 outbox 初始为空；advance@10 产生 p1/p2 两条 tick occurrence；ACK 两条后 advance@20 再次产生两条。 |

处置：**关闭这条已复现的同事件取 max 而未求和问题。** 不把独立 timer 事件分轮处理的 max 逻辑误改成必须预留所有历史事件同时产生；此次验证的是同一个事件实际同时驱动两个 FSM。

## 补充上界核对和独立探针

| 要求 | 源码与实测证据 |
| --- | --- |
| 任意合法 UTF-8 request ID 的 JSON 宽度 | `relation_envelopes` 第 2415–2416 行使用每 plan 的最大合法 raw ID 字节界（上限 1024）；第 2488、2498、2500 行按 `6*N+2` 计 String 编码，Latest state 第 2525 行也保留此类增长。独立调用实际 JSON codec：1024 个 `\x01` 控制字符达到精确 `6*1024+2=6146`；反斜杠、引号及多字节中文/emoji 均落在上界内并 round-trip。结合 UTF-8 校验与 JSON 字符规则，此界不依赖 ID 恰为 ASCII 普通字符。 |
| Candidate 完整行受 `max_event_bytes` 限制 | `prepare_candidate` 第 779–786 行先生成规范完整行再 `encode_row` 检查。独立经 process 输入测试：包含新增 String payload 的完整 Candidate KV value 恰 **1024 字节**时提交；**1025 字节**时拒绝，且 payload 原始长度本身仍小于 1024，证明检查包含身份/epoch/active/schema wrapper。拒绝不推进 revision/processed position。 |
| sink 完整行受相同限制 | FSM `emit` 第 1682–1689 行和 CEP `emit_cep` 第 1729–1736 行均对完整规范行编码检查。独立实跑 FSM sink 的 **1024 接受 / 1025 拒绝**边界；拒绝无 source position、outbox 或 revision 推进。CEP 对应路径做源码核对，并执行冻结 CEP 回归，未另声称独立 CEP sink 精确 1024 边界已测。 |
| GROUP BY-only 的隐藏 S 与 scope 宽度 | 第 2468–2476 行按 group_by 构造聚合投影输入；第 2527–2530 行即使 aggregates 为空也计入 S；key 使用实际 scope 编码，row 的 partition 字段也使用实际 JSON 宽度。独立增加 `SELECT k FROM latest GROUP BY k`，scope 为 **96 个 `\x01`**：`max_mutations=10` 的 admission Deferred/ResourceExhausted、capture=0、revision 不变；11 时完整成功、GROUP BY T 和 S 均持久存在，query 返回原 scope 并可 reopen。 |
| 完整 lifecycle 增长后最大合法函数结果仍可终态 | `reserve_request_publication` 第 2578–2584 行预留 **整个 `max_state_bytes`**，而不是当前 compact snapshot；machine bytes 同 RequestRuntime 使用 record/2 clamp。独立令 Engine=3 MiB、lifecycle=2 MiB、function limit=32 KiB，先 admit/claim，再加入 40 KiB source payload 直到 **offset51** 因 snapshot 容量拒绝，已持久 lifecycle 为 **2,062,880 / 2,097,152 字节**。随后真实执行器返回结果，验证 RequestMachine 校验的完整 stored-row 数组编码恰 **32,768 字节**，complete 仍 Success、任务释放，reopen/read 仍 Success。此 32 KiB 是该接口完整行数组编码界，非只计 worker payload，也非含 KV schema wrapper 的总 value 字节。 |

上述 5 项独立探针合计 **5/5 通过，7,658 ms，exit 0**，见 [bounds.log](/tmp/eventflow-event-budget-remediation/bounds.log)。它们使用 Memory adapter；四后端正常契约证据由下列冻结回归补充，不能把 5 项独立边界探针说成每一项均在四后端运行。

## 构建与实跑记录

工具链 `/usr/bin/clang++`，Clang 18.1.3。核对 CMake 生成的 Engine/EventProcessor 及探针 `flags.make` 和 linker 命令：`-Werror -Wall -Wextra -Wpedantic`、C++20、`-fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`。所有所需项目对象均来自本次独立目录的新编译。

```sh
cmake -S /tmp/eventflow-event-budget-remediation \
  -B /tmp/eventflow-event-budget-remediation/build \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS=-Werror
cmake --build /tmp/eventflow-event-budget-remediation/build \
  --target budget_review event_request_test event_processor_test event_processor_cep_test -j 4
# Add/configure the independent bounds target, then build both final probe binaries.
cmake -S /tmp/eventflow-event-budget-remediation \
  -B /tmp/eventflow-event-budget-remediation/build \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS=-Werror
cmake --build /tmp/eventflow-event-budget-remediation/build \
  --target budget_review budget_bounds_review -j 4

LD_LIBRARY_PATH=/tmp/eventflow-event-request-budget-snapshot/cpp3rdlib/lib \
  ctest --test-dir /tmp/eventflow-event-budget-remediation/build/eventflow \
  -R '^(event_request_test|event_processor_test|event_processor_cep_test)$' --output-on-failure

LD_LIBRARY_PATH=/tmp/eventflow-event-request-budget-snapshot/cpp3rdlib/lib \
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
/tmp/eventflow-event-budget-remediation/build/budget_review '--gtest_filter=BudgetReview.*'

LD_LIBRARY_PATH=/tmp/eventflow-event-request-budget-snapshot/cpp3rdlib/lib \
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
/tmp/eventflow-event-budget-remediation/build/budget_bounds_review '--gtest_filter=BudgetBoundsReview.*'
```

CTest 的 sanitizer 环境来自冻结 CMake 的 test properties。最终配置/构建及全部最终测试命令 exit 0；项目与最终探针构建无 warning/error，测试无 sanitizer diagnostic。增加第二个 probe target 时一次未显式重配的 build 返回 `No rule to make target`；显式 CMake configure 后解决，未修改被测源码。

| 产物 | 直接结果 |
| --- | --- |
| [build.log](/tmp/eventflow-event-budget-remediation/build.log)、[build-bounds-final.log](/tmp/eventflow-event-budget-remediation/build-bounds-final.log)、[build-final.log](/tmp/eventflow-event-budget-remediation/build-final.log) | 独立项目、边界探针编译及最终 targets 构建成功 |
| [probes-final.log](/tmp/eventflow-event-budget-remediation/probes-final.log) | 原问题合理整改探针及 periodic 正向 **3/3**，1,618 ms |
| [bounds.log](/tmp/eventflow-event-budget-remediation/bounds.log) | 补充边界 **5/5**，7,658 ms |
| [regressions.log](/tmp/eventflow-event-budget-remediation/regressions.log)、[LastTest.log](/tmp/eventflow-event-budget-remediation/build/eventflow/Testing/Temporary/LastTest.log) | `event_request_test` **72/72**；`event_processor_cep_test` **100/100**；`event_processor_test` **97/97**；CTest **3/3**，62.07 秒 |

## 实测内容标识

冻结入口 `event_processor.cpp` hash 在开始、构建后和最终实跑后均为 root 指定的 `bcc620a1...`。以下源码全部读取/编译自 snapshot；不代表 live 或主树后来发生的修改。

```text
bcc620a1db515f55adf054a89a780cd32050cdb980aee90e3a72291987c9bcab  src/runtime/event_processor.cpp
ecabcdaf54a983114e8ece58882a74ca92804d1f275721969a843d6b5e5d0151  include/eventflow/event_processor.hpp
1695bb17b6ee4e1a33351488832a520702b976c0211a70147a9b0334e31c584e  src/runtime/engine.cpp
41d20b00aab7198273065332f1c23992032e0ad4b2186c6ff4b8c8c3075c38d7  src/runtime/engine_internal.hpp
ba03b8f3f4194e7e5eb3f09efa00045dd7ee0820003553174c3e6883f55a5937  src/runtime/request_runtime.cpp
52d3226268c1e8a9b441a1babcc3aa37200b03fd528a73182fdffed84804b91e  include/eventflow/request_runtime.hpp
27a795488f822a018c32ec6a1356b0b8ac4526e31c14b2a88e5bea1c230f75d1  tests/event_request_test.cpp
4c556e716cce98fd3792b3dec281d4bbec4e1821826bf0a9fd58c678762236ec  tests/event_processor_test.cpp
ce95beb484e2e3ee3ce958efe68a769bd4dd6103373ff651792729f68872a39b  tests/event_processor_cep_test.cpp
```

独立审查程序 SHA-256：

```text
3b10005a6dc9cca0b3bfac3eecb7efbc216fd2d0071eb61ff52181cc317c51d3  review.cpp
a1a9d8f5507ff5bbdba5f895ac687c1db8f31b9a16bc30fda13dc7c1127fd6a8  bounds.cpp
c9d09ac69ceaa53a498779a22a9902992727caa6473c4aecca139d9684251457  bounds_fixture.inc
f986ad5d4d5d8e3d1e63bf240d91003066a3eb088f01614705b7c4a36a8f6329  CMakeLists.txt
```

## 保留边界与下一步

本次可以关闭原两项 P1 的具体遗漏，不能将有限图和边界实测扩张为任意 Catalog、任意 callback/SQL 表达式、任意独立 Engine 与 IStateStore read/canonical-batch 限制组合下的完整终态进展证明。当前闭包预留增加了相关读入行的保守估算，但本轮没有逐项压低独立 store read/batch 限制进行穷尽验证。运行时拒绝超出 Candidate/sink 声明 envelope 的 callback 输出，也不等于替 callback 提供了输出上限的静态证明。

snapshot 不包含 live 的 `SourceScopeRuntimeV2` / `TryOnce`、后续 candidate audit/deadline-cap/progress 新状态；它们需要针对新 hash 的增量审查，不能继承本报告的通过标签。本次未运行 C8 app targets、真实多进程 Coordinator/KV/多个 Compute、Kafka、全局 checkpoint 或最终全局 review。root 可把本记录用于原两项发现的处置，同时继续保持 M4 active 和相应独立交付 gate。
