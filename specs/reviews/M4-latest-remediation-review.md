# M4 Latest F1/F2 限定整改复验

日期：2026-09-14。复验者：`fresh_status_review`。

结论：**F1 的 SQL/host scoped scan 缺行漏洞可关闭；F2 的两个已复现 mutation 计数遗漏可关闭。** 主树源码在本复验者新建的独立目录编译，原探针复制并强化后 **7/7 通过**，退出码 0，无 ASan/UBSan/LSan 诊断。**F2 涉及的完整 byte/read/store/companion 终态预算要求仍未关闭。** 本报告不宣称 Latest、M4 或系统最终交付完成。

## 复验独立性和范围

本线程先前完成过 RequestStatus 专项审查。本次因原 Latest reviewer 唤醒及新建线程受 `agent thread limit` 限制，由 root 复用本线程执行原 F1/F2 的限定整改复验；**这是已有独立 reviewer 的限定复验，不是新的 fresh-context 全局 review**，也不以此前 RequestStatus 审查替代本次证据。

本次重新读取 [原 Latest runtime review](M4-latest-runtime-fresh-review.md)、[AGENTS.md](/home/obscure/EventFlow/AGENTS.md)、[规格索引](/home/obscure/EventFlow/specs/INDEX.md) 和相关 Latest/Request active specs，直接检查主树修复代码。原失败描述用于确定复现轨迹，没有直接采用实现者的通过结论。未重跑旧版本 RED；旧失败证据保留在原报告，本次提供修复版本的新构建与 GREEN 证据。

- 主树源码：`/home/obscure/EventFlow`。
- 新建项目 build：`/tmp/eventflow-latest-remediation-source-build`，所有所需项目静态库从主树源码新编译，没有复用实现者或原 reviewer 的 `.o/.a`。
- 原始探针：`/tmp/eventflow-latest-runtime-fresh-probes/probes.cpp`。
- 独立副本：[probes.cpp](/tmp/eventflow-latest-remediation-probes/probes.cpp)、[CMakeLists.txt](/tmp/eventflow-latest-remediation-probes/CMakeLists.txt)，只改为包含主树 fixture、链接本次独立库，并强化 `max_mutations=7/14` 的早拒绝断言。原 SQL 与 `RoundContext::scan_relation` 缺行断言、10/11/16 正向轨迹、marker/100 请求/两阶段撤回轨迹均保留。
- 未修改任何生产源码、仓库测试或规格；本次仓库内仅新增本报告。探针虽包含现有两份测试文件以复用 fixture，但运行过滤器仅选择 `FreshRuntimeReview.*` 的 7 项，不把未执行的既有测试计入结果。

## 原发现的处置

### F1：关闭限定的 SQL 与 host scoped scan 缺行漏洞

[Engine::query_trusted](/home/obscure/EventFlow/src/runtime/engine.cpp:1318) 对 Latest 单表查询采用 `read_row`；[scan_rows](/home/obscure/EventFlow/src/runtime/engine.cpp:475) 对有 scope 的 Latest 也采用相同点读，并保留单行 byte budget 校验。`read_row` 在 T 缺失时仍调用 [validate_latest](/home/obscure/EventFlow/src/runtime/engine.cpp:421)，后者检查 selector winner 与 T 存在性一致，错误返回 `FailedPrecondition`。

实测原轨迹：真实 RequestRuntime 完成成功请求，测试直接删除 Latest T、保留 winner S/Candidate/status/result，重建 Engine/Runtime 后，`SELECT answer FROM latest`（显式 scope）及 `RoundContext::scan_relation(Latest, scope)` **均返回 `FailedPrecondition`**。没有把空扫描当作正常无结果，也没有自动补值。探针见 [第 7 行](/tmp/eventflow-latest-remediation-probes/probes.cpp:7)。

此结论只覆盖原报告的有 scope 入口；不声称启动时扫描并核验了所有历史和 scope。

### F2：关闭两个 mutation 反例，保留完整预算要求

[reserve_status_transition](/home/obscure/EventFlow/src/runtime/request_runtime.cpp:213) 从 capture/status roots 沿唯一 Catalog 的依赖闭包计数，包含 record/index/Engine metadata、每个关系的 T，以及 Latest 或 **非空 `group_by`** 关系的 S。判定与 Engine 实际 GROUP BY 隐藏状态一致，不再以 `aggregates` 是否非空代替。预检调用位于 [admit](/home/obscure/EventFlow/src/runtime/request_runtime.cpp:624) 的 capture 之前。

| 独立轨迹 | 实测结果与处置 |
| --- | --- |
| 原单函数、Latest、SQL 后继，`max_mutations=7`、active 容量 1 | admit 返回 `ResourceExhausted`；capture 次数 0、revision 不变、active book/status/capture 表均空；读取 request record 为 `NotFound`。原“已受理成功完成反复卡住”的轨迹在受理前被拒绝。 |
| 同图 `max_mutations=10/11` | 两种配置均真实 admit→claim→执行器成功→complete；commit 已确认、Request 为 Success、Latest=14、SQL 后继=15、active 容量释放，实际完成批 mutation 数不超过配置。随后新请求 seq=2 可受理并取消。 |
| 追加第二个 Latest 和两个 `SELECT k ... GROUP BY k`、无 aggregate function 的 SQL 子节点；`max_mutations=14` | admit 返回 `ResourceExhausted`，且同样 capture=0、无 record/status/capture 写入、revision 不变、active book 为空。原 GROUP BY-only S 漏计反例关闭。 |
| 相同双 Latest/双 GROUP BY-only 图，`max_mutations=16` | admit/claim/complete 成功，Request 为 Success，两个 GROUP BY 视图均返回 `k=a`，active 容量释放。 |

探针分别见 [max7](/tmp/eventflow-latest-remediation-probes/probes.cpp:37)、[max10/11](/tmp/eventflow-latest-remediation-probes/probes.cpp:58)、[max14/16](/tmp/eventflow-latest-remediation-probes/probes.cpp:93)。不足配置在 admission 前明确拒绝是本次认可的行为；没有要求所有预算配置都必须受理，也没有以单纯调大配置作为修复依据。

**处置边界：原 F2 应记录为“已复现的 mutation 问题已整改，完整预算要求仍开放”，不应无条件标成全部关闭。** 本次源码与探针没有证明 Engine marker/key/value 的完整字节增长、Latest/result/任意后继 SQL 的最大输出字节、累计读取预算、IStateStore canonical batch limits、companion/receipt/outbox、EventProcessor 满 trigger queue 的控制额度已形成统一可验证上界。`max_active_requests=1` 的这些探针不证明任意拥塞和最大结果下所有终态路径都可前进。

## 配套回归

同一次 filtered run 执行并通过其余三个原独立探针：

| 探针 | 本次直接观察 |
| --- | --- |
| `InitializationMarkerCannotBeSilentlyRecreatedAfterHistory` | 已产生请求历史后直接移除 `latest_initialized`，reopen 返回 `FailedPrecondition`，不重造初始化证明。 |
| `LargeHistoryHasOneSelectorAndQueryRefusesTruncation` | `max_rows=2`、active 容量 1，连续完成 100 个请求；Latest answer=200、seq=100，selector S 前缀恰 1 行，active 每次释放；超预算历史查询返回 `ResourceExhausted`；重建后仍返回 answer=200。只证明此轨迹和规模，不证明历史磁盘有界或性能指标。 |
| `SecondPhaseCandidateExpiryClearsTransientAggregateAndSqlDescendants` | phase 1 可读到暂存 Latest，phase 2 Deactivate 后 Latest、直接 SQL、过滤、聚合及其子 SQL 均空，最终公开 changes 不含净零 Latest 变化；下一 epoch 的 total=11、child=12，无残留累计。 |

本次 7 个独立探针使用 Memory adapter；没有将此结果扩大为新的四后端全量验证、真实多进程重启或 Kafka 集成。也未验证 EventProcessor v2 的 A 完成持久唤醒 B、B/C once、C8 全轨迹或系统 checkpoint/clean-stop。

## 构建、命令与结果

工具链实际为 `/usr/bin/clang++`，Ubuntu Clang 18.1.3。独立项目编译数据库确认 Engine/RequestRuntime/RequestMachine 使用 C++20、`-Werror -Wall -Wextra -Wpedantic`、`-fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`；探针 compile/link flags 同样启用三 sanitizer 与 `-Werror`。项目与探针均只用 CMake/Make，第三方从主树 `cpp3rdlib` 链接，无构建下载。

```sh
cmake -S /home/obscure/EventFlow \
  -B /tmp/eventflow-latest-remediation-source-build \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_CXX_FLAGS=-Werror -DBUILD_TESTING=ON
cmake --build /tmp/eventflow-latest-remediation-source-build \
  --target ef_request_runtime ef_transport -j 4

cmake -S /tmp/eventflow-latest-remediation-probes \
  -B /tmp/eventflow-latest-remediation-probes/build \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
cmake --build /tmp/eventflow-latest-remediation-probes/build -j 4

ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
LSAN_OPTIONS=exitcode=23 \
LD_LIBRARY_PATH=/home/obscure/EventFlow/cpp3rdlib/lib \
/tmp/eventflow-latest-remediation-probes/build/latest_fresh_probes \
  '--gtest_filter=FreshRuntimeReview.*'
```

四条配置/构建命令及测试命令均退出 0；构建日志无 warning/error，完整测试输出为 **7 tests / 1 suite，7 passed，11,419 ms**，无 sanitizer 诊断。日志：

- [项目配置](/tmp/eventflow-latest-remediation-config.log)、[项目构建](/tmp/eventflow-latest-remediation-source-build.log)。
- [探针配置](/tmp/eventflow-latest-remediation-probe-config.log)、[探针构建](/tmp/eventflow-latest-remediation-probe-build.log)。
- [完整测试输出](/tmp/eventflow-latest-remediation-probe-test.log)。

## 实测版本

以下主树源码 hash 在构建前和测试完成后相同；日志与结果仅对应这些内容，不替随后并行工作的新修改背书。

```text
cf73bfc7519b62dd38f3cdfca5c9d9e0425641d7ce3a3148c947556aecb89790  src/runtime/engine.cpp
33af8170522e3c488746e099e53018250371843cf02ee02e28475b758ece37fc  src/runtime/engine_internal.hpp
ba03b8f3f4194e7e5eb3f09efa00045dd7ee0820003553174c3e6883f55a5937  src/runtime/request_runtime.cpp
52d3226268c1e8a9b441a1babcc3aa37200b03fd528a73182fdffed84804b91e  include/eventflow/request_runtime.hpp
e279ca6428fa59ac54012d509a0c4c43a8767ffb115f1eecd0fc597fa9129672  tests/request_latest_test.cpp
109ab754b007d99e9e2e797e1035d320f8956508509d70af864b7edbd4bbf026  tests/engine_latest_test.cpp
7533212673b962e924b608696aad9367a6d53f099f7fc94fc138e1fba6c76ef3  /tmp/eventflow-latest-remediation-probes/probes.cpp
669fed594c1a170a6391f2f42f1b0f0503951562f83f75f704c6aafad7f775f3  /tmp/eventflow-latest-remediation-probes/CMakeLists.txt
```

root 可引用本记录更新原 F1 与 F2 mutation 子项的处置，保留其余预算缺口及 M4 active 状态。原项目规定的独立里程碑/最终全局审查仍须另行完成。
