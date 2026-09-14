# 核心 CEP 与 EventProcessor 独立审查

日期：2026-09-14。审查者：`cep_fresh_owner_review`，独立 fresh-context 子 Agent，未参与被审生产实现。

## 结论与范围

**最终结论：本轮核心 CEP 与 EventProcessor 集成审查通过。** 当前代码实现了多候选 E-NFA，并接入同批状态、source progress 和 occurrence outbox；并非将单状态 EFSM 改名。初轮独立反例揭示的两处到期阻塞已修复，并在冻结源码上通过原反例复验。最终独立重建并运行 **23 个组件测试、100 个四 KV 合同、97 个原 EventProcessor 测试、3 个 Engine lifecycle 测试及 6 个自写对抗测试，全部退出码 0、无失败、无跳过、无 sanitizer 诊断**。这支持关闭 `specs/cep-enfa.md` 当前单 owner 范围的语义、原子提交和容量恢复验收。

本审查依据 `AGENTS.md`、`specs/INDEX.md`、`specs/cep-enfa.md` 与 `specs/archive/trigger-history.md#cep-design` 阅读原始要求，再审查当前组件、宿主和 Engine prepare 入口。提案中的可选 captured_views、跨源 event-time、水位线与全局 checkpoint 不被当作本轮已实现功能。审查启动时不继承实现者聊天历史，也不以实现者报告作为通过证据；冻结通知用于确定初轮审查时点，发现缺陷后再与实现者讨论修复契约。

本轮仅修改本文件，独立 probe 与构建产物在 `/tmp/eventflow-cep-fresh-review/`；没有修改生产代码、原有测试、私有 PDF、参考仓库或外部应用。此结论只覆盖核心 CEP 的当前单 owner 集成，**不代替 M4 审查或项目最终多次 fresh-context 全局审查**。

## 初轮发现与最终处置

### CEP-F1：延迟匹配与同轮 FSM timeout 争用唯一输出槽，可使到期永久失败

优先级：必须修复。最终状态：**已独立复验关闭**。初轮复现后交回 `cep_owner_integration`，由其修复；reviewer 未修改生产代码。

配置为 `max_occurrences = 1`。CEP 为 `(A then Z) or B`、`SkipToNext`、`within_us = 10`；简单 FSM 为 `idle --A--> waiting(timeout=10) --$timeout / emit alarm--> done`。依次接纳 `A@0`、`B@1` 后，CEP 存在 1 个待裁决匹配，durable outbox 为空。`advance(10)` 与 `advance(20)` 都返回 `ResourceExhausted: lifecycle entry budget exceeded`，shard 虽未 FAILED，却始终没有可 ACK 的 occurrence，无法释放容量或推进到期。

原因为 `EventProcessor::Impl::encode` 只为 CEP pending 预留 count 与 outbox bytes，没有为同一到期轮次中的简单 FSM 输出预留；`reserve_outbox` 也只计算已经持久化的 outbox。到期时 FSM alarm 与 CEP match 必须一起写入，超限后整轮丢弃，因此两者都永久卡住。不能通过丢弃 alarm/match 或增大默认预算关闭本项。

独立初轮证据：`probe.cpp` 中 `DelayedCepAndFutureFsmTimeoutMustRemainDrainable`；`probe-red2.log`，实际测试进程退出码 1。

最终处置：`reserve_control` 汇总 CEP pending 和 FSM timeout/TTL/周期可能输出，且对受控状态图做有限可达分析；忽略 guard 取保守上界，不执行回调。修复后 B 在 source commit 前被拒绝，pending 保持 0；`advance(10)` 成功输出原有 FSM alarm，证明并非只把错误提早返回。新增四 KV 合同和独立同一反例均通过。容量为 1 的两级 timer 链也能先发布、ACK，再推进下一次 alarm，已有 outbox 没有与其后继输出被错误叠加预留。

### CEP-F2：pending 转为完整 occurrence 时总 lifecycle snapshot 膨胀，没有预留

优先级：必须修复。最终状态：**已独立复验关闭**。初轮复现后交回 `cep_owner_integration`，由其修复；reviewer 未修改生产代码。

使用同一 CEP，不配置 FSM。独立 MemoryStore 实测 pending snapshot 为 **2790 bytes**，正常到期后 snapshot 为 **2990 bytes**。从新建 processor 开始固定 `max_state_bytes = 2790`，`A@0`、`B@1` 均被接纳，但 `advance(10)` 返回 `ResourceExhausted: lifecycle snapshot exceeds byte budget`。outbox 为空，输入的 source progress 已经确认，不能靠 ACK 或后续到期释放。将正常 pending snapshot 在相同 2790-byte 配置下恢复也会成功，之后出现相同阻塞。

原实现通过纯 `advance(INT64_MAX)` 检查未来 match 的单 occurrence 包装大小，并预留 outbox count/bytes，但没有把 future outbox 与剩余 FSM/CEP、TTL、receipt、position 等状态一起编码后核对 `ProcessorConfig::max_state_bytes`。CEP 内部 snapshot 缩小并不保证宿主完整 snapshot 缩小。

独立初轮证据：`probe.cpp` 中 `DelayedCepSnapshotMustReserveLifecycleExpansion`；`probe-red2.log` 包含 fresh admission 和 snapshot reopen 两条失败轨迹，实际测试进程退出码 1。

最终处置：实际 snapshot/outbox 继续按真实编码核对，另对 ACK 已有 outbox 后的未来控制轮构建完整编码上界，包括最大宽度时间/序号、FSM 状态/寄存器、CEP 状态、timer 及 future occurrence 的数组开销。`encode` 与恢复的 `decode → encode` 使用同一检查。修复后原 2790-byte 新建配置会在 B 接纳前拒绝，并能清理 A；原 pending snapshot 在不足预算下重开直接拒绝，不能恢复成不可推进状态。独立原例与新增四 KV 合同均通过。

### CEP-F3：默认 CMake 测试入口尚未收录新增 owner 合同

优先级：交付前处理。状态：已关闭；root 加入正式 target 后，本 reviewer 已重新读取确认。

初轮根 `CMakeLists.txt` 已收录 `cep_test`、`engine_lifecycle_test`，但未收录 `event_processor_cep_test`。76 个四 KV 合同是通过独立 `/tmp` CMake fallback target 构建执行的；后续 root 已将该测试加入正式 `BUILD_TESTING` foreach，并 `PRIVATE` 链接 `ef_transport`；独立用 `rg -n event_processor_cep_test CMakeLists.txt` 确认当前第 137/158 行。修复后的独立重编将使用正式 target。

### CEP-F4：规格与提案状态落后于实际实现

优先级：交接前处理。状态：root 已同步整改中状态；本 reviewer 已读取核对当前规格和提案。

初轮 `specs/cep-enfa.md` 的宿主接入、真实 KV 合同和独立审查任务仍为 `todo`；提案仍声明“尚未批准实现”“不启动重写”。随后 root 已同步用户批准范围、实现状态、独立审查发现及剩余任务；未将提案中的可选点一并声明完成。最终修复结论仍需交接时再同步。

## 要求与实现核对

| 原始验收要求 | 当前实现与独立证据 | 判断与处置 |
| --- | --- | --- |
| 真实多起点、同输入多路径推进 | `CepStateData::paths/pending`；`step` 遍历旧路径与新起点，TAKE/IGNORE 可同时保留；`A1,A2,B` 组件与四 KV 合同 | 满足已测范围 |
| PROCEED 保留输入、TAKE 后不重复消费 | `compile`、`closure`；TAKE 后仅做 epsilon closure；optional、nullable group、branch/repeat 测试 | 满足已测范围 |
| 顺序、严格/宽松、分支、optional、有限 repeat、within；拒绝不支持语法 | `validate_pattern` 拒绝 negative/greedy、无上限重复、nullable repeated body 和整体零事件模式；有限图及 match length 有界 | 满足当前正向子集 |
| `NextOfKind` 第一条 News 负面后不能等待下一条正面 | IGNORE 对 NextOfKind 仅允许 kind 不同；guard 拒绝的同 kind 不保留该路径；组件/四 KV 合同 | 满足已测轨迹 |
| 分支 registers/history 隔离、共享前缀安全回收 | `Path` 自有 `Row registers`，history/event 通过 `shared_ptr<const ...>` 共享；引用所有权和codec完整性、分支历史 guard、最后引用释放测试 | 满足已测范围；预算为逻辑收费，不是 RSS 保证 |
| 所有规则使用本轮冻结 MV；历史与当前 MV 不混淆 | 宿主 hook 先捕获全部 `captured/captured_cep`，再执行 FSM/CEP/sink；延迟匹配自有 reads 和 `matched_at_us` | 组件和四 KV 合同通过 |
| 固定 now、到期先于同刻数据、deadline 仅作审计 | `EventProcessor::Impl::run` 固定 target；到期子轮先提交；`CepProgram::expire` 以 `deadline <= now` 删除路径 | 相等边界、延迟 advance、到期后数据背压测试通过 |
| NoSkip / SkipPastLastEvent、持久 admission order | `finish` 按起点 sequence、matched time、canonical id 排序；有更早活跃起点时阻塞晚匹配；SkipToNext 也已实现 | 组件轨迹及 pending 恢复通过 |
| 源去重先于 matcher，同 identity 不同 payload 冲突 | `process` 先检查持久 receipt/position，再调 matcher；身份包含 source/topic/partition/offset，processing time 不作为 broker payload | 四 KV 合同通过；超过 receipt 保留窗口需兼容 checkpoint，未静默重复接纳 |
| CEP、outbox、sink、source progress 同批提交 | hook 在 Engine 暂存关系/状态后，最后 stage 整个 lifecycle；共享同一 Catalog/owner | 四 KV 合同及提前 FSM/sink 写入后 CEP 超限回滚通过 |
| UNKNOWN 原批次，不重跑 guard、不重复分配身份 | pending state、occurrences、operation 与 Engine batch 保留；resolve 只确认原 mutations | UNKNOWN before/after apply 四 KV 合同通过 |
| 超限整轮不推进、shard 可恢复 | 私有 `EngineRuntimeAccess::apply_lifecycle` 对 ResourceExhausted 丢弃 overlay；公共 hook 仍 fail-stop，永久 callback 错误仍 fail-stop | 当前回滚测试通过；CEP-F1/F2 修后独立复验关闭 |
| pending count/byte 预留、防止到期不能输出 | 组件 `reserve_cleanup` 对完整超时 drain 检查 work/output；宿主检查 pending count、outbox bytes 和单 occurrence 包装 | 初轮缺口已由完整 control envelope 修复，CEP-F1/F2 已关闭 |
| codec version、identity、reference完整性与恢复 | 组件校验图可达性、前驱序号/时间、严格衔接、寄存器类型、孤儿引用；宿主校验配置与 scope/规则/匹配见证 | 组件腐坏测试、四 KV snapshot restore/outbox 恢复通过 |
| 保留已有简单规则 | 原 `event_processor_test` 97/97；CEP 使用额外私有 prepare 入口，简单-only 路径保留原 public apply_many | 原 97 基线保持通过；组合资源缺陷已关闭 |

## 独立运行证据

构建使用独立目录 `/tmp/eventflow-cep-fresh-review/build`，通过 `add_subdirectory(/home/obscure/EventFlow eventflow)` 复用项目 CMake 的真实 target 依赖，并单独补上未注册的 owner 合同 target；不是引用实现者已有可执行文件。

```sh
cmake -S /tmp/eventflow-cep-fresh-review \
  -B /tmp/eventflow-cep-fresh-review/build \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS=-Werror
cmake --build /tmp/eventflow-cep-fresh-review/build \
  --target cep_test event_processor_cep_test event_processor_test engine_lifecycle_test -j3
cmake --build /tmp/eventflow-cep-fresh-review/build --target fresh_owner_probe -j3
```

运行每个可执行文件时均设置：

```sh
env LD_LIBRARY_PATH=/home/obscure/EventFlow/cpp3rdlib/lib \
  ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  LSAN_OPTIONS=exitcode=23 <test_binary>
```

独立编译器为 Ubuntu Clang 18.1.3。CMake 目标实际包含 `-Werror -Wall -Wextra -Wpedantic -fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`；生产依赖链接仓库 `cpp3rdlib` 中的预置库，没有下载第三方实现或改用其它构建系统。

| 独立执行 | 结果 | 日志 |
| --- | --- | --- |
| `build/bin/cep_test` | 23/23，通过；含长度至 5 的重复/连续性/within 枚举与逐前缀 codec | `component-test.log` |
| `build/event_processor_cep_test` | 76/76，通过；Memory、Rocks、HTTP+Memory、HTTP+Rocks | `owner-test.log` |
| `build/bin/event_processor_test` | 97/97，通过；原基线完整运行 | `baseline-test.log` |
| `build/bin/engine_lifecycle_test` | 3/3，通过 | `engine-test.log` |
| 自写资源组合 probe | 2/2 失败，构成 CEP-F1/F2；不是 sanitizer 诊断 | `probe-red2.log` |
| 自写 nullable group + alternative 独立 oracle | 1/1，通过；模式 `A then ((B? strict C) or D)`，外部 FollowedByAny、within=3，枚举 ABCDX 全部长度至 4 的 781 条流，每个前缀做 codec roundtrip，与独立组合枚举比较完整标签/事件身份 | `oracle-test.log` |

以上表格保留初轮证据，说明通过已有测试并未掩盖独立反例。最终复验见下节。

## 冻结源码上的最终独立复验

实现者正式冻结后，reviewer 重新配置并构建全部上述目标，使用正式 `build/bin/event_processor_cep_test`，没有误用此前 fallback 的旧可执行文件。最终构建日志为 `final-configure.log`、`final-build.log`；编译退出码 0，未出现 warning/error。以下五个测试进程分别读取了实际结束状态，均退出码 0。

| 最终执行 | 结果 | 独立日志 |
| --- | --- | --- |
| CEP 组件 | 23/23，59.028 秒 | `final-component.log` |
| 正式四 KV CEP owner 合同 | 100/100，12.562 秒 | `final-owner.log` |
| 原 EventProcessor 基线 | 97/97，12.713 秒 | `final-baseline.log` |
| Engine lifecycle prepare | 3/3，45 毫秒 | `final-engine.log` |
| 自写对抗与独立枚举 | 6/6，10.363 秒 | `final-probes.log` |

6 个独立测试覆盖：原 FSM+pending 输出槽反例、原总 snapshot 2790→2990-byte 反例及受限恢复、同一 CEP 的周期 Clock 分支与 pending 争位、`SIZE_MAX` outbox 配置算术、二次 JSON 转义增长、nullable group/alternative 的 781 条流。周期竞争测试使用显式可承载的 component limits，验证 B 拒绝后 Clock 仍可到期输出。

二次转义反例使用 512 个普通字符的 waiting state、250 个引号的目标 state、3000-byte receipt。真实 snapshot 从 9516 增长至 10010 bytes，9644-byte 配置现在会在接纳前拒绝，之后空控制轮仍能前进。修复按最终外层编码大小选择 registers/state 上界；`max_occurrence_bytes + 1` 也在有界可用空间判定后才求值，避免无符号最大值配置造成除零。两项由独立 source review 提醒并在本 reviewer 的 probe 中验证，不依赖实现者单测报告。

`reserve_control` 仅操作状态副本、声明元数据与纯 CEP drain，不调用 FSM/CEP guard/update、sink 或外部 I/O；UNKNOWN 仍只重试原批次。独立测试完成后执行 `sha256sum -c final-sha256.txt`，下列最终文件以及初轮表中其余未修改文件全部匹配：

| 最终变化文件 | SHA-256 |
| --- | --- |
| `src/runtime/event_processor.cpp` | `58f0222376ae26fd01d96cd577374006567ffb6cca86019cdeba8d8b9c11a241` |
| `tests/event_processor_cep_test.cpp` | `ce95beb484e2e3ee3ce958efe68a769bd4dd6103373ff651792729f68872a39b` |
| `CMakeLists.txt` | `38143cdcfee18c2d49ecbb284950287cab51909e62419a2e9e2d7249c8dde107` |

这是一份保守预算契约。带控制 update 的 FSM 按其声明的完整 codec 上界预留；周期 CEP 按声明的路径、输出和状态上界预留，而不是在 admission 时执行回调猜测真实需求。调用方需要协调组件与 Processor 的 count/byte/timer 限额；例如默认 8 MiB CEP state 上界不能放进默认 2 MiB Processor 的周期控制 envelope，当前实现会在打开 Engine 前显式拒绝这类配置。声明上界足够小且一致时，真实 Clock TAKE、冻结读、coalescing、重开恢复已在四 KV 合同中通过。该策略可能早于实际最小需求拒绝输入；没有静默删路径或丢匹配。

## 初轮源码身份

本目录尚无可引用的已跟踪提交，因此用文件 SHA-256 固定审查对象。`reviewed-sha256.txt` 保存初轮列表；修复后的最终 SHA 和重验证据已记录于上节。

| 文件 | SHA-256 |
| --- | --- |
| `include/eventflow/cep.hpp` | `617b33d447f5ec27ff2ae3f53c27f4a6213c22fd01729f00e45eced37942bfd4` |
| `src/runtime/cep.cpp` | `b43e4cc55dff120d2f988ac64b081dcd23d08762d7481ec6770496b3e7bd5927` |
| `include/eventflow/event_processor.hpp` | `befb67c0552b5a7315014aeb566eb5456b45d2fb2e738d834deff9355e20f2e4` |
| `src/runtime/event_processor.cpp` | `42556419abe2e3ba5d21ef3201d43a7998a9c38fb85d85cb7b1493c19a4a36c0` |
| `tests/cep_test.cpp` | `9d9dd1103ebfddb06cd55c94c9255a1ad227da4b87d6194816d49c62af62a578` |
| `tests/event_processor_cep_test.cpp` | `2251c020d15ee68ad48a1882e71876acfc901a9ff20c533e7b37ebf8d79267fa` |
| `src/runtime/engine.cpp` | `c9fc39e1d64d822db7601202305bdfeec1e6aad0ca506300e994eab112eefdff` |
| `tests/engine_lifecycle_test.cpp` | `8b2d1c87ea868c0a813cb1668fd576c617329ffce6a7002284faba457e467d00` |

## 未覆盖与交接边界

- HTTP 合同是在本地同测试进程启动真实 KV HTTP server，证明协议适配与 KV 行为，不等同于真实 Coordinator、KV、多 Compute 多进程部署。
- 没有运行本地 Kafka Docker Compose 集成、源 manual ACK/pause/resume 的整体业务链路或网络进程崩溃矩阵。EventProcessor 仅返回 `input_committed`，实际 broker ACK 由调用方承担。
- 没有证明全局 watermark、一致 checkpoint 对 source positions/Catalog/runtime 的绑定、跨源 event-time 顺序或完整重放协议。四 KV snapshot 测试只证明当前单 shard 存储镜像和本地恢复。
- 没有评审 RequestRuntime 全部控制状态、函数调用链或扩展家族；Engine 私有 lifecycle 入口只在 CEP 需要的原子 prepare/错误隔离范围内审查。
- 没有声明 Release `-O3`/LTO 构建、真实负载 RSS/吞吐或上万路径压力已验收。
- 本轮两项 must-fix 已关闭，root 可据最终 SHA 与本报告更新核心 CEP 规格状态；后续 RequestRuntime/完成唤醒、M4 总体验收、多进程与一致 checkpoint、最终多次全局审查仍需各自独立证据。
