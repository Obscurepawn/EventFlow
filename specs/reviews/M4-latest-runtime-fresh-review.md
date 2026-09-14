# M4 Latest runtime 组合独立 fresh-context 审查

日期：2026-09-14。审查者：`latest_engine_fresh_review`。状态：初审完成，F1/F2 尚未关闭；本报告不证明 Latest runtime 已完整满足原规格，也不证明 M4 完成。

## 源码与原始要求边界

本轮独立读取主树 [AGENTS.md](/home/obscure/EventFlow/AGENTS.md)、[规格索引](/home/obscure/EventFlow/specs/INDEX.md)、[Latest 规格](/home/obscure/EventFlow/specs/latest-selector.md) 和 [C7/C8/C9](/home/obscure/EventFlow/specs/runtime-contracts.md:77)。实现证据全部来自隔离源码树 `/tmp/eventflow-latest-runtime-work`，不能解释为主树已采用这些文件。`/tmp/eventflow-latest-request-handoff.md` 仅用于定位，不作为测试或正确性证明。

主要审查对象为隔离树的 [engine.cpp](/tmp/eventflow-latest-runtime-work/src/runtime/engine.cpp)、[engine_internal.hpp](/tmp/eventflow-latest-runtime-work/src/runtime/engine_internal.hpp)、[request_runtime.cpp](/tmp/eventflow-latest-runtime-work/src/runtime/request_runtime.cpp) 与 [request_runtime.hpp](/tmp/eventflow-latest-runtime-work/include/eventflow/request_runtime.hpp)，以及同树的 Catalog/binder、EngineLatest/RequestLatest 测试。审查者未修改生产代码或仓库测试，只写本报告及 `/tmp/eventflow-latest-runtime-fresh-probes/`。

## 发现与处置

### F1：Latest T 行缺失可被按 scope SQL 静默解释为空结果（高，必须修正）

初审位置：[query_trusted](/tmp/eventflow-latest-runtime-work/src/runtime/engine.cpp:1307)、[scan_rows](/tmp/eventflow-latest-runtime-work/src/runtime/engine.cpp:525)、[validate_latest](/tmp/eventflow-latest-runtime-work/src/runtime/engine.cpp:372)。

独立探针 `FreshRuntimeReview.MissingWinnerRowCannotLookLikeAnEmptyLatest`：

1. 使用真实 RequestRuntime 完成 `admit → claim → complete`，已有成功的 RequestStatus、result、Latest T 和 selector S。
2. 销毁 Runtime/Engine，测试直接删除 Latest T 行，保留 winner S、Candidate、RequestStatus 与 result；该写入仅用于模拟持久状态损坏。
3. 重建 Engine/Runtime，然后执行 `SELECT answer FROM latest`，显式 `QueryContext.scope={"a"}`。
4. 实际返回 `OK`、0 行；应因 winner S 与 Latest T 的存在性不一致返回 `FailedPrecondition`。

原因：普通非 JOIN 查询进入 `scan_rows`，仅为扫描到的 T 行执行 `validate_latest`。没有 T 行时不会检查 selector S。现有 `read_row` 已有缺失行的一致性核验，但该 SQL 路径没有调用它。已有测试 `ReopenedScopedReadsRejectMissingOrInconsistentWinnerState` 实际只篡改存在状态的 sequence，不能证明缺行被拒绝。

修正方向：已知 scope 的 Latest 查询/扫描需要点读并验证完整 row/state/candidate/winner 组合，包括空结果路径；不能依靠历史扫描或自动补值掩盖损坏。

第一轮整改复验：root 将 SQL 的 Latest 单表查询改为 `read_row`，原 SQL 缺行断言 GREEN。但同一损坏状态下 `RoundContext::scan_relation(Latest, scope)` 仍返回 OK/空；该 host/capture 入口继续走旧 `scan_rows`，独立追加断言 RED。日志 `/tmp/eventflow-latest-runtime-fresh-fixed-targeted-probes.log`。F1 仍需统一 scoped scan 的完整性核验，未关闭。

### F2：受理预算未覆盖 Latest/SQL 完成闭包，合法成功回包可以反复无法落地（高，原规格必须修正项）

初审位置：[reserve_status_transition](/tmp/eventflow-latest-runtime-work/src/runtime/request_runtime.cpp:213)、[Engine stage mutation limit](/tmp/eventflow-latest-runtime-work/src/runtime/engine.cpp:350)、[完成两阶段提交](/tmp/eventflow-latest-runtime-work/src/runtime/request_runtime.cpp:706)。

独立探针 `FreshRuntimeReview.AdmittedSingleRequestCanCommitItsSuccessfulCompletion` 使用已有公开配置和真实单函数计划：`max_mutations=7`、`max_active_requests=1`，其余使用默认值，无 companion、无新 trigger、无普通数据拥塞。

`admit` 已确认受理，`claim` 已确认并返回一个任务；执行器返回合法成功后，`complete` 返回 `ResourceExhausted: round mutation count exceeded`。revision 未改变，Request 保持 RUNNING；重交相同回包仍失败，新请求因 active 容量被占用而拒绝。该探针没有证明取消也会卡住，不能将结论扩大为所有终态操作均失败。

必要 metadata 预检仅要求至少 4 个 mutations，未计入 function/result、Latest T/S 和后继 SQL 等实际完成写入。错误在一次不可提交的 overlay 内返回，原子性仍保持；问题是已经承诺受理的合法成功路径没有可验证的提交容量。将 max_mutations 调大只是使这个具体配置通过，不构成原规格要求的 reservation 证明。

RequestRuntime 交接已说明完整预算缺口，本轮新增了独立可执行反例。仍需在受理可回滚阶段计算或声明整个完成/取消闭包的 mutation、key/value 字节、读预算、record/index、Engine metadata、Latest/SQL、receipt/outbox 上界，并与 node/存储批次额度一致； EventProcessor 的保留控制额度需单独接通。状态：未关闭。不得将必要 metadata 门槛称为完整终态进展保证。

第一轮整改复验：root 在 `reserve_status_transition` 中从 capture/status roots 遍历唯一 Catalog 的有限闭包，按关系 T 行、Latest/聚合 S 状态及 record/index/Engine metadata 计数。原 `max_mutations=7` 轨迹现在在调用 capture 前拒绝，revision/status/active book 不变；独立 `max_mutations=10/11` 正向轨迹完成并释放 active capacity，随后 seq=2 的新受理与取消均成功。

但第一版判定隐藏状态使用 `!plan->aggregates.empty()`，与 Engine 实际 `!plan.group_by.empty()` 分支不同。合法 SQL `SELECT k FROM latest_visible GROUP BY k` 没有聚合函数，仍持久化 group row-count S。独立探针 `MutationReservationIncludesGroupByWithoutAggregateFunctions` 追加第二个可见 k 的 Latest 和两个这种 SQL 子节点，`max_mutations=14` 可以受理、claim，但 completion 再次报 `round mutation count exceeded`。日志 `/tmp/eventflow-latest-runtime-fresh-groupby-reservation-probe.log`。该遗漏仍属于 F2 mutation 问题，等待修正；其他 byte/read/companion/store-limit reservation 尚未证明，不能由 mutation 修正一并关闭。

## 已确认的源码契约与测试证据

| 要求 | 本轮直接证据与结论 |
| --- | --- |
| 单 Catalog 图与 domain/identity 对齐 | `Catalog::from_snapshot` 在 Engine open 重建验证；RequestRuntime open 将 plan 的完整 identity 字段与 status annotation 比较，并要求同域 Latest 使用同一个 status binding。无新增持久计算边。 |
| typed exact write 授权 | Engine 的 owner frame 为规范 `(T key, encoded row)` 登记一次性授权，apply 时精确消费；伪造、改写、重复/未消费意图整轮失败。公开 Engine 不启用受控 metadata；Runtime companion 不得通过返回伪状态行覆盖 factory 结果。 |
| 整 Request 成功才 eligible | Runtime finalize 先 `RequestMachine::refresh`，再 stage record/index、按 Manifest/整个 Request phase 构造 status。worker 只有 payload；Engine selector 同 identity 点读 result/status，要求 SUCCEEDED + active 同 epoch。现有四 adapter 共用轨迹通过。 |
| admission_seq / epoch / 旧回包 | 状态写入范围为正 Int64，epoch/seq immutable；终态不回退。逆序完成按 seq 选择，新失败/取消保留旧成功；候选修订后旧请求可终态但不能覆盖新 epoch；超时及旧版本回包有无副作用测试。 |
| Candidate 墓碑 | Activate 检查 expected_epoch 并加一，Deactivate 保留 epoch/payload；metadata delete 无授权，旧 expected_epoch 拒绝，inactive 不以同 epoch 重新激活。业务 news_id/text_version 去重仍属生命周期 owner，helper 不证明该业务去重。 |
| 两阶段稳定化与 SQL 净变化 | `prepare → stabilize → finalize → stabilize` 使用累计 before/final touched rows；聚合从 Before 状态重算本轮净 delta。独立探针在 phase 1 产生 Latest，phase 2 expiry 将其撤回，过滤、聚合、二级 SQL 均为空；下一 epoch 输出 total=11、child=12，无残留累计或虚假 Latest RowChange。 |
| UNKNOWN 原批 | Engine 保留完整 batch 与确认后 changes；Runtime pending Book/任务在确认后发布。独立构建执行的四 adapter 既有测试覆盖 admit/claim/complete/cancel UNKNOWN、原批比较、callback 不重执行以及旧回包。 |
| 初始化边界 | 初始空 store 原子写 Catalog fingerprint + `latest_initialized=1`；非空无 metadata 拒绝。已有 request/metadata/Latest Catalog 禁止普通 install 改写。独立删除 marker 探针明确拒绝 reopen，不从历史隐式生成初始化证明。 |
| 恢复及查询一致性 | 存在 Latest T 时校验 selector、Candidate、胜出 status/result 和 payload；active RequestRecord 与 status 同 revision 点读核验。F1 显示缺 T 的 SQL 路径仍有漏洞。恢复按需核验，不声称启动时已扫描并验证所有历史和 scope。 |
| 状态保护 | raw stage 仅 S/I，0 namespace 保留；Latest state_id 的 S/I 都拒绝普通 hook/companion 覆写，RequestRuntime namespace 的授权与业务 hook 分离。 |
| 有界 selector 和历史查询 | selector 只处理本轮受影响 scopes/identities，completion 不调用历史 scan。独立 max_rows=2/max_active_requests=1 的 100 请求轨迹通过，单 selector S 前缀恰 1 行、Latest seq=100；历史 query 超限显式 ResourceExhausted；重建后仍为 seq100。只证明此规模与配置，不是性能基准。 |
| 已受理终态进展 | F2 未关闭；现有测试和最低 metadata 预算不足以证明原规格的整轮 reservation。 |

## 独立构建与运行

构建目录：`/tmp/eventflow-latest-runtime-fresh-build`；source：`/tmp/eventflow-latest-runtime-work`；第三方来自该 source 的 `cpp3rdlib` 链接，指向主树打包依赖。所有项目对象均由本次独立目录编译，没有复用实现者 build 目录对象。

```sh
cmake -S /tmp/eventflow-latest-runtime-work -B /tmp/eventflow-latest-runtime-fresh-build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_CXX_FLAGS=-Werror -DBUILD_TESTING=ON
cmake --build /tmp/eventflow-latest-runtime-fresh-build --target engine_latest_test request_latest_test -j 4
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 LD_LIBRARY_PATH=/tmp/eventflow-latest-runtime-work/cpp3rdlib/lib ctest --test-dir /tmp/eventflow-latest-runtime-fresh-build -R '^(engine_latest_test|request_latest_test)$' --output-on-failure
```

以上配置、构建、测试退出 0；`EngineLatest` **38/38**，`RequestLatest` **53/53**。编译数据库核验 engine/request_runtime 和两份测试均使用 Clang、`-Werror`、`-fsanitize=address,undefined,leak -fno-sanitize-recover=all`；构建无 warning/error，测试无 sanitizer 诊断。

新增独立探针使用 CMake 构建，链接上述独立 build 的 matching 静态库；复用现有 fixture 的图/adapter 建立代码，新增轨迹与断言全部位于 [probes.cpp](/tmp/eventflow-latest-runtime-fresh-probes/probes.cpp)，没有把重跑既有样例计为新探针。

```sh
cmake -S /tmp/eventflow-latest-runtime-fresh-probes -B /tmp/eventflow-latest-runtime-fresh-probes/build -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
cmake --build /tmp/eventflow-latest-runtime-fresh-probes/build -j 4
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 LD_LIBRARY_PATH=/tmp/eventflow-latest-runtime-work/cpp3rdlib/lib /tmp/eventflow-latest-runtime-fresh-probes/build/latest_fresh_probes '--gtest_filter=FreshRuntimeReview.*'
```

| 日志 | 本轮结果 |
| --- | --- |
| `/tmp/eventflow-latest-runtime-fresh-config.log`、`/tmp/eventflow-latest-runtime-fresh-build.log` | 独立配置/构建退出 0 |
| `/tmp/eventflow-latest-runtime-fresh-test.log`、独立 build 的 `Testing/Temporary/LastTest.log` | 91 项既有测试通过，CTest 2/2 targets |
| `/tmp/eventflow-latest-runtime-fresh-probe-build.log` | 新探针 Clang/三 san/`-Werror` 构建退出 0 |
| `/tmp/eventflow-latest-runtime-fresh-probe-test.log` | 前 4 项独立探针：F1/F2 两项 RED，marker/100 请求两项 GREEN；退出 1 |
| `/tmp/eventflow-latest-runtime-fresh-phase-probe.log` | 第 5 项两阶段 SQL/aggregate 净零探针 GREEN，退出 0 |
| `/tmp/eventflow-latest-runtime-fresh-probe-all-test.log` | 最终组合执行全部 5 项独立探针：3 GREEN、F1/F2 两项 RED，退出 1，无 sanitizer 诊断 |
| `/tmp/eventflow-latest-runtime-fresh-fixed-test.log` | 第一轮整改源码重建后 EngineLatest 38 + RequestLatest 55 共 93 项通过；独立 CTest 2/2 targets |
| `/tmp/eventflow-latest-runtime-fresh-fixed-targeted-probes.log` | SQL 原缺行断言 GREEN、host scoped scan 同状态 RED；原小 mutation 拒绝与 max10/11 正向 GREEN |
| `/tmp/eventflow-latest-runtime-fresh-groupby-reservation-probe.log` | GROUP BY-only S 计数遗漏的真实 runtime 轨迹 RED |

初审源码 SHA256（用于区分 root 后续整改）：

```text
4903f7f8f17baa8a6d630022a90c5ef02899d6c8779d79a85ecfc17b5c235444  /tmp/eventflow-latest-runtime-work/src/runtime/engine.cpp
33af8170522e3c488746e099e53018250371843cf02ee02e28475b758ece37fc  /tmp/eventflow-latest-runtime-work/src/runtime/engine_internal.hpp
1da26e045c25fa03d803f57eeff7a89de7a79476a44de74740579b95d56c1c80  /tmp/eventflow-latest-runtime-work/src/runtime/request_runtime.cpp
52d3226268c1e8a9b441a1babcc3aa37200b03fd528a73182fdffed84804b91e  /tmp/eventflow-latest-runtime-work/include/eventflow/request_runtime.hpp
109ab754b007d99e9e2e797e1035d320f8956508509d70af864b7edbd4bbf026  /tmp/eventflow-latest-runtime-work/tests/engine_latest_test.cpp
7c9519b1aceccea8c43a2c46cffa150ae52f0ec2f0827750f2f2c61bf488c4c2  /tmp/eventflow-latest-runtime-work/tests/request_latest_test.cpp
```

## 剩余范围与下一步

root 修正 F1 后需在同一独立 build 重建并复跑原缺行探针。F2 需补充真实闭包预算契约与受理拒绝/合法完成正向证据；仅记录 gap 不等于关闭。M4 规格应保留 active 状态。

本轮没有验证 EventProcessor completion 稳定变化的 durable 唤醒、trigger queue 满时的保留控制轮、A/B/C/D 完整业务去重/once 轨迹、真实 Coordinator/KV/多个 Compute 进程、Kafka Docker Compose、全局 checkpoint/clean-stop 协议或最终多轮独立全局 review。四 adapter 测试中的 HTTP server 在测试进程内运行，不能代替多进程端到端证明。GC 未实现，终态 RequestStatus/result/history 继续占用磁盘；selector 单行证据不表示持久历史有界。
