# M4 app 两项 P1 整改独立复验

Reviewer: `m4_app_remediation`（独立 fresh context，无实现职责）  
Date: 2026-09-14  
Disposition: **APPROVE（限定整改）**；关闭原 F1/F2 两项 P1，范围内 C8 回归通过，未发现新的本范围阻碍。此结论不关闭 M4、M5 或整个交付目标。

## 范围与依据

本轮是 [M4 app 首轮独立审查](M4-app-fresh-review.md) F1/F2 的限定整改复验，审查主树 `/home/obscure/EventFlow` 的 `examples/feature_demo.hpp`、`examples/feature_demo.cpp`、`examples/feature_demo_main.cpp`、`tests/feature_demo_test.cpp`，并追踪它们调用的 EventProcessor/RequestRuntime/FunctionExecutor 合同。依据 `AGENTS.md`、`specs/INDEX.md`、`specs/event-request-integration.md`、`specs/request-functions.md`、`specs/runtime-contracts.md` C8 及前次报告，不把前次报告或 root 的 GREEN 当作本次测试证据。

采用 develop-with-specs 与 verification-before-completion 的证据流程。reviewer 只修改本文件和 `/tmp` 下的独立探针、构建产物；没有修改项目源码、既有测试、CMake、spec 或 `.reference-repos/`。没有发布或上传私有资料。

结论仅适用于这一快照的单 owner、确定性 `SimpleFunctionExecutor` app 与范围内 C8。HTTP→Memory/RocksDB 适配器测试在同一测试进程内启动 KvServer。本轮不证明 M5、Coordinator/KV/多 Compute 多进程部署、dirty-run/crash 恢复、consistent checkpoint、通用 completion/view-change 订阅、用户扩展、Release 或整个交付目标。并行新增的 StoreLimits API 尚未被本轮 Engine/EventProcessor 使用；本 reviewer 只为 ABI/依赖匹配重新构建其代码，不评审它的功能合同。

## F1：driver UNKNOWN 确认与 source position 同步 — CLOSED

整改代码：`examples/feature_demo.cpp:512` 的 `synchronize_confirmed()`、`:546` 的 `resolve_pending()`，以及 `ingest/pump/run_functions` 的入口调用。

- driver 在继续 owner 工作前调用原批的 `resolve_pending_commit()`。该 resolver 没有新的时间参数，原未决操作继续使用 owner 保留的 fixed now；后续 `pump(now_us)` 才开启新 dispatch。
- resolver 返回的新 `committed_rounds` 进入 `consume()`；仍有 UNKNOWN 时返回，不调用会依赖未确认状态的 active/position 读取。确认之后，从 `processed_positions()` 同步 `next_offset_`，从有界 `active_requests()` 重建活动集合与可信任务路由。
- 外部调用方直接使用公开 `processor().resolve_pending_commit()` 后，下一次 driver 调用仍会同步持久 source position 和活动集合。恢复不依赖本地审计 deque，也不扫描完整 RequestStatus 历史。

原失败探针 `SourceUnknownMustBeRecoverableThroughDriver` 与 `RawProcessorResolveMustNotDesynchronizeDriverOffset` 在本次匹配构建上均通过。第一条能只用 `pump` 收敛 UNKNOWN；第二条能在底层直接确认后继续接纳高版本新闻，不再以旧 offset 报 payload 冲突。新增 `SourceConfirmedBeforeAdmissionUnknownKeepsOffsetAndOnce` 在第二个 commit 丢 ACK，确认后仍为一个 source position、一次 A 受理；后续输入使 offset 正确推进到 2。UNKNOWN 期间公开 authoritative reader 返回 FailedPrecondition 是预期 fence，探针没有绕过它读取 staging。

## F2：已 poll 完成批的所有权 — CLOSED

整改代码：`examples/feature_demo.hpp` 的 `completions_`，`examples/feature_demo.cpp:469` 的 `consume()`、`:560` 的 `claim_ready()`、`:630` 的 `run_functions()`。

- 只有 `completions_` 为空时才继续 poll，每次最多取得 64 项。结果留在 driver deque，当前项得到 owner 的已确认 request_result 后才移除；UNKNOWN 返回不销毁当前项或后续结果。队列项数有界，单条 output 又受 executor 的输出预算约束。
- `synchronize_confirmed()` 以持久 Running task 集合清理已经确认、取消或过期的队列项。因此底层直接确认第一项也不会导致重送第一项，更不会把后续仍 Running 的项一起删除。
- `consume()` 先消费 envelope 中各个已确认子轮的 request、cancel 和 clock 效果；任务 dispatch 改为之后读取持久 outbox。提交标记只在 executor submit 成功后登记，满队列不能使尚未消费的后续确认效果消失。

原失败探针 `CompletionUnknownMustRetainOtherPolledResults` 在匹配构建上通过：一次 poll 的两个 A 完成中，第一条丢 ACK；直接确认并 pump 后，第二条仍可提交为 Success。指标仍为 callback_calls=1、cache_hits=1、active_tasks=0，没有为恢复重跑 callback。新增 driver-only 变体不调用底层 resolver，也能收敛两个 Request 并排空队列。

## 独立对抗结果

原 `/tmp/M4app-probe/probe.cpp` 原样复制为 `/tmp/M4app-remediation-probe/original.cpp`；SHA-256 为 `59d1635dbcd1ea0197f2fc5bc5e71f729715557e9d78c592ddbdd136a05d5cbb`。原七条测试的断言未修改。五条补充测试在同目录 `extra.cpp`，使用真实 MemoryStateStore commit 成功后替换 ACK 的装饰器，没有伪造持久行。

| 探针 | 本轮结果与直接证据 |
| --- | --- |
| SourceUnknownMustBeRecoverableThroughDriver | PASS；driver pump 确认原 source |
| RawProcessorResolveMustNotDesynchronizeDriverOffset | PASS；外部确认后输入身份同步 |
| CompletionUnknownMustRetainOtherPolledResults | PASS；首条 UNKNOWN 后第二条成功且 callback 不重跑 |
| FixedCaptureAndSharedAncestorAreAuthoritativeRows | PASS；capture/admission 为 R/R+1，受理后改 Quote/history 不改固定捕获，A 与预测链共 4 次 callback |
| ReplacedCompletionRemainsQueryableWithoutWinningLatest | PASS；旧完成保留历史，不能赢得当前 Latest |
| FullActiveQueueRetainsNewCandidateAUntilOldCompletionsReleaseCapacity | PASS；16 active 加第 17 个候选，完成释放后最终 17 条历史、active 归零 |
| CurrentSlotDoesNotBackfillAndHistoryAgeBoundaryIsOpen | PASS；D 不补旧槽，120s history 边界正确 |
| DriverAloneResolvesCompletionUnknownAndDrainsRetainedBatch | PASS；仅用 driver run_functions 恢复，两个 A 都成功，之后再次运行返回 0 |
| CancelledPolledTailCannotBeRepublishedOrReexecuted | PASS；第一项确认后取消已 poll 的第二项，第二项不发布、不重算；新候选仍能完成 |
| DirectQueuedCancellationIsSynchronizedWithoutCallingUserCode | PASS；直接取消 queued A，driver 同步并 drain；Cancelled 终态保留，只有另一个任务执行 callback |
| SourceConfirmedBeforeAdmissionUnknownKeepsOffsetAndOnce | PASS；source 已确认后下一子轮 UNKNOWN，offset/once 不重复 |
| CompletionFaultSweepKeepsConfirmedEffectsAndSharedGraph | PASS；扫描第 1–12 个后续 commit 位置，断言至少 5 次实际 UNKNOWN，并包含本地已消费 Success 后再 UNKNOWN 的路径；恢复后两个 A 成功、B 仅受理一次、prediction/risk Latest 均存在、callback=4/cache_hit=1/active=0 |

完整新构建的探针总计 **12/12 PASS**，退出 0，耗时 68.115s。日志 `/tmp/M4app-remediation-probe/final-results.log`。后述 StoreLimits 一行差异后，当前库另一次完整运行仍为 **12/12 PASS**，退出 0、耗时 66.006s，日志 `/tmp/M4app-remediation-probe/delta-results.log`。两轮均为 reviewer 独立新运行。

## C8 与普通无故障路径

本轮重新阅读 app 的 Catalog 声明、candidate/high-water、capture、trigger/level binding 和四 KV 测试。函数依赖来自单一 Catalog：Embedding；features→prediction；features 与 prediction→risk。预测 Request 的两个 targets 是 prediction/risk，共享祖先由 RequestMachine 去重，app 没有再建一张可编辑执行图。

四 KV 完整回归覆盖：A 后市场 Ready；C 先于 Embedding 且等待 B accepted；D 多次 miss/当前槽；旧版本历史完成与同 ticker 新 news_id 替换；2s readiness、300s candidate、120s history 和 10s trade 到期；Quote age 等号；30s C 绝对期限等号；非法价格/时间与有限数值溢出；high-water 在正文 TTL 后保留；有界 active/outbox 的对象重开路由；timeout 取消排空；以及新增三条故障回归的四适配器展开。

完整四 KV 运行结果：**70/70 PASS**（2 条定义测试 + 17 条合同 × 4 适配器），CTest 退出 0、耗时 258.18s；`/tmp/M4app-remediation-final-tests.log`、`/tmp/M4app-remediation-final-build/Testing/Temporary/LastTest.log`。这是本 reviewer 独立运行，未借用 root 的结果。运行覆盖的 app/owner 源码与下列哈希一致；存储随后一行差异及当前库补验见下节。

独立 `eventflow_feature_demo` smoke 退出 0，日志 `/tmp/M4app-remediation-final-main.log`。输出包含 A/B/C/D、当前槽不补发、旧结果被 Latest fence 拒绝、无新输入 candidate/Latest 失效；本地最终指标 callbacks=17、cache_hits=3。

## 构建、执行和快照匹配

Clang 18.1.3，C++20，`-Werror -Wall -Wextra -Wpedantic`，`-fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`。从新目录重新编译选中目标的全部项目依赖，没有复用首轮 `/tmp/M4app-review` 或 root 的库。测试运行启用 ASan/UBSan 的 halt_on_error 和 LSan exitcode=23；feature_demo_test 的 CTest TIMEOUT=600。

```sh
cmake -S /home/obscure/EventFlow -B /tmp/M4app-remediation-final-build \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS=-Werror -DBUILD_TESTING=ON
cmake --build /tmp/M4app-remediation-final-build \
  --target feature_demo_test eventflow_feature_demo eventflow -j4
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
ctest --test-dir /tmp/M4app-remediation-final-build \
  -R '^feature_demo_test$' --output-on-failure --timeout 600
```

- 配置、完整新构建均退出 0：`/tmp/M4app-remediation-final-config.log`、`/tmp/M4app-remediation-final-build.log`。Probe 另用 `/tmp/M4app-remediation-probe/CMakeLists.txt`，链接这一新构建的库；其 final-config/final-build 日志均成功。
- 初次诊断构建期间 StoreLimits 的虚接口尚在变化，故该构建及其 70 项通过不计最终验收。补充探针第一次错误地在 UNKNOWN 期间调用受封锁的公开 reader，引发断言；修正探针后使用旧库/新头文件又遇 ABI 错配。这两项均记录在 `/tmp/M4app-remediation-probe/results.log`、`extra-results.log`，不是项目整改，也不作为主树逻辑缺陷。
- 最终完整构建前后，61 个生产源码/头文件、app 文件及该测试的支持文件哈希一致：`/tmp/M4app-remediation-final-matching-inputs.sha256`、`/tmp/M4app-remediation-final-build-input-check.json`。较宽的全项目 manifest 记录于 `final-before.sha256`、`final-after-build.sha256`（同 `/tmp/M4app-remediation-` 前缀）。期间仅无关的 StoreLimits 测试注册/CMake 和其测试文件继续变化；已用当前 CMake 再配置、构建本轮三个目标，退出 0 且没有重新编译，日志为 `final-config-refresh.log`、`final-build-refresh.log`。
- 完整运行结束核对发现 `src/storage/remote_store.cpp` 从 `c9f6fa78fb544db6745e96c64f7a08f686b7dc7c0e3ee2c7d72da49cf7a95e5c` 变为 `d146309101620c072da142c00777b4e3fb5819ae2bb1507ffb3ae2a6f92835da`。reviewer 独立移除新增的 `response.get_header_value_count("Content-Encoding") > 1 ||` 一行，重算内容哈希恰等于旧值，证明全部差异只在 `call_limits()` 的响应头拒绝条件；证据 `/tmp/M4app-remediation-remote-delta.txt`。该路径未被 app 调用，get/scan/commit/status 路径、虚接口和对象布局未变化。其他 60 个匹配输入直到运行结束仍相同；`/tmp/M4app-remediation-final-after-tests.sha256` 如实保留这一差异。
- 已重新编译当前 transport 并重新链接 app 测试和独立探针，构建退出 0：`/tmp/M4app-remediation-delta-build.log`、`/tmp/M4app-remediation-probe/delta-build.log`。当前库 **12/12 探针 PASS**（66.006s）与 **8/8 HTTP 合同 PASS**（28.289s），各退出 0。HTTP 子集包括三条新增故障回归与正常 A→B/shared graph 的 HTTP→Memory、HTTP→RocksDB 两种展开；日志 `/tmp/M4app-remediation-delta-http-tests.log`。完整 70 项结果属于前述 remote-store 旧哈希；本轮不把它重命名为新 remote-store 全量验收。
- 当前库补验构建前至测试结束，包含 CMake、include、src、examples、tests 的全部 manifest 完全一致：`/tmp/M4app-remediation-delta-before.sha256`、`/tmp/M4app-remediation-delta-after.sha256`、`/tmp/M4app-remediation-delta-check.json`。所有用于本报告结论的完成运行均无 ASan、UBSan 或 LSan 诊断。

```text
bcec87392862aab1c224dd95873fbc9b84490fcc0b93be83f81ee13473cccfab  examples/feature_demo.hpp
a31af10a51c231dfd3a37b4a470a3b2db2a88a9529c765505327ea022616fa4d  examples/feature_demo.cpp
57da84e2c072ec4da8fc1eb1a0e3d0852b574e820794a27fc8ae6b62101d4379  examples/feature_demo_main.cpp
afa985224dddb975d72b81ea80122c4fbf2b234bfe3a46e8de494728d4bbddb1  tests/feature_demo_test.cpp
6972c9dbe78d0dc050a08dd2daba4302ca396bb5658c0005cd0af89ec3d89bdd  include/eventflow/event_processor.hpp
470599df9255fa100f14bd392ad422092382a48543b4ecb712df8d64a18fc8ef  src/runtime/event_processor.cpp
2194ba1f330c944dfa1287ba64d91fc0e0788dee39ebee2ace116b1238293cae  include/eventflow/state_store.hpp
d146309101620c072da142c00777b4e3fb5819ae2bb1507ffb3ae2a6f92835da  src/storage/remote_store.cpp
```

后续责任边界：root 根据本报告及 owner 独立审查更新持久规格中的准确状态；保留先前失败报告。未来修改 driver、owner 或相关 ABI 后，应使用相应新快照证据。本文不关闭 M4/M5、后续扩展或最终全局审查。


路径说明：示例后来按用户要求移入 `examples/feature_computation_demo/` 并重命名；本报告的日志和SHA对应当时受审版本，源码链接指向当前位置。
