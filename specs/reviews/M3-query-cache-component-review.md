# M3 Engine 查询计划缓存独立组件审查

日期：2026-09-14。审查者：fresh-context agent `m3_io_time_review`，未参与该查询缓存实现。

本次依据 `AGENTS.md`、`specs/INDEX.md`、`specs/sql-runtime.md` 的查询/UNKNOWN 契约及内部逐次 Catalog 深克隆待收尾项，检查 `include/eventflow/engine.hpp`、`src/runtime/engine.cpp`、`tests/engine_query_cache_test.cpp`。读取 `M3-query-cache-implementation-notes.md` 了解实现边界，以下结论由当前源码与独立运行支持。

## 结论

本次未发现可复现的信任边界、计划失效、结果/revision 缓存、预算或重入缺陷。独立运行仓库 22 项测试全部通过；补充隔离套件包含同一 22 项及 reviewer 的 6 项参数化探针，共 28 / 28 通过，Clang ASan/UBSan/LSan 无诊断。

`specs/sql-runtime.md` 所述“内部 query 路径避免逐次深克隆 Catalog”偏差，在当前 `query_sql` 路径已被源码与实际调用计数验证消除。公开 `query(BoundPlan)` 仍执行外部计划完整校验；不把公开接口保留的校验工作当作未完成的缓存优化。

这是查询缓存组件审查，不是 M2/M3 最终里程碑或全局完成声明。没有重开此前已关闭的 M2 runtime 问题；没有检查 EventProcessor 生命周期新实现。未修改生产代码、仓库测试或主 build，只新增本文档和 `/tmp` 探针。

## 检查结果

| 契约 | 当前源码位置 | 审查结果 |
| --- | --- | --- |
| 内部 Catalog 有独立受信任来源 | `engine.cpp:724-729`、`:440-445`；`catalog.cpp` 的 `bind_query/bind_query_sql` | open 从 `Catalog::from_snapshot` 得到已验证、独立持有的 Catalog；内部 binder 直接使用它。缓存只接收本 Engine 新绑定的 `shared_ptr<const BoundPlan>`。 |
| 私有路径不削弱公开校验 | `engine.hpp` 的 private `query_trusted`；`engine.cpp:883-892` | 公开 query 先 ready、OperationGuard、`validate_bound_plan`，然后执行。内部 SQL 也持有 guard，但计划来自受信任 binder。外部 BoundPlan 不进入内部 LRU。 |
| 可变外部别名不污染缓存 | `engine.cpp:445-446`、`:475-476`、`:922` | SQL key 保留为两份拥有所有权的字符串；内部 plan 不通过 API 返回。QueryResult 的 fields/rows 是本次结果；外部表达式、调用者 SQL 缓冲和返回值修改不会改变驻留计划。 |
| scope/行/revision 每次重算 | `engine.cpp:893-954` | 每次检查显式/推断 scope 冲突、类型、shard、当前查询/Engine 预算，建立当前 revision 的读取 round，再捕获和求值。缓存不保存 QueryContext、关系行或 QueryResult。 |
| 成功 DDL 清理旧计划 | `engine.cpp:421-426`、`:537-540` | Catalog 激活同时清空 map/LRU/bytes 并释放旧内部 binder；下一次查询重新绑定。普通数据提交不清计划，查询仍读到新行。 |
| UNKNOWN install 只在确认后激活 | `engine.cpp:541-545`、`:847-857` | pending 时保留旧 Catalog/计划但禁止查询；同批确认后统一 activate 清理。UNKNOWN apply 的确认不改变 Catalog，因此可以继续复用原计划。 |
| ready/reentrancy 在缓存前生效 | `engine.cpp:481-488`、`:956-961` | active、failed 或 pending 状态在查缓存/绑定之前被拒绝，热缓存不能绕过，hit/miss 也不因该拒绝而增加。OperationGuard 覆盖执行并在提前返回时释放。 |
| 精确 SQL 文本与 LRU | `engine.cpp:429-438`、`:466-478` | exact text 才命中；空格/大小写不同是不同键。命中移动至 LRU 尾端；entries/bytes 预算共享生效；无法单独容纳的计划仍可执行且不进入缓存。 |
| 逻辑 byte charge | `engine.cpp:450-465` | 两份 SQL、canonical plan 表示、完整 Catalog fingerprint 与固定 metadata 费用先校验，再决定插入。各加法均使用剩余额度检查；entry count 同时约束容器条目数。 |
| 禁用及正常拒绝 | `engine.cpp:430,447`、`:865-875` | 任一 cache 上限为 0 都只关闭条目缓存，不重新开启逐 query Catalog 克隆。非法 SQL/DDL 不替换已有有效计划；查询错误不返回缓存结果。 |

## 独立验证

主任务已固定本轮源码与主构建库；审查者只读使用这些库，没有与主 CTest/Release 构建争用输出目录。

1. `build/bin/engine_query_cache_test --gtest_brief=1`：22 / 22 通过，退出码 0。
2. `/tmp/eventflow-m3-independent-review/query_cache_review.cpp` 包含当前全部仓库查询缓存测试，再新增 3 项 `TEST_P`，分别运行 Memory/Rocks，共 28 项。独立 CMake 目标链接当前 `libef_engine/sql/storage/exact_sum/core.a` 与 `cpp3rdlib` 预打包库；Clang 18.1.3，C++20，`-Wall -Wextra -Wpedantic -fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`。

补充探针及实际结果：

- **`ReviewStableCatalogIsNotRebuiltPerInternalQuery`**：链接器 `--wrap` 统计真实 `Catalog::from_snapshot` 调用，wrapper 增加计数后调用原函数，不模拟 Catalog。分别启用/禁用条目缓存：open 后重复 SQL 与新的不同 SQL 均未增加 Catalog 重建次数；DDL 激活后的第一次查询恰增加 1 次，随后新的不同 SQL 与重复查询都不再增加。Memory/Rocks 均通过。
- **`ReviewCallerTextAndReturnedResultCannotChangeResidentPlan`**：修改第一次结果中的字段名、字段类型和 row 值，再把调用者原 SQL 缓冲的 `'a'` 改为 `'b'`；新 SQL 正确返回 b 的值，重新查询原 SQL 仍返回原 schema/a 的当前值，确实命中内部原计划。Memory/Rocks 均通过。
- **`ReviewWarmPlanRechecksPositiveReadBudgetsAndEmptyResults`**：热计划建立后增添同 scope 数据；`max_rows=1` 和 `max_bytes=1` 都返回 ResourceExhausted。随后删除该 scope 全部数据，热计划返回空结果；证明缓存不保留旧结果，也不跳过正数的小预算。Memory/Rocks 均通过。

最终隔离执行输出：

```text
[==========] 28 tests from 1 test suite ran. (1271 ms total)
[  PASSED  ] 28 tests.
```

退出码 0，无 ASan/UBSan/LSan 诊断。该耗时仅是本轮测试日志，不构成性能基准。

```sh
cmake -S /tmp/eventflow-m3-independent-review -B /tmp/eventflow-m3-independent-review/build -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
cmake --build /tmp/eventflow-m3-independent-review/build --target query_cache_review -j 2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 /tmp/eventflow-m3-independent-review/build/query_cache_review --gtest_brief=1
```

本轮源码/最终探针 SHA256：

```text
4b2ad0887a373dc537e4ed86e1a9e803ff69d81724073cfa9d0fcebc3fdde211  include/eventflow/engine.hpp
889554f22510019d4a7140449c4ba2b71ec49b77fb7f1566be242a2be87ddd2f  src/runtime/engine.cpp
196388492b31d71909d2b881307705baa5d34150007b4c5415abea4150d677d7  tests/engine_query_cache_test.cpp
fa3f35d30c6638e22f8426340d0efe7b91eeccdea655739a7e1074d1e2506fff  /tmp/eventflow-m3-independent-review/query_cache_review.cpp
```

## 边界与交接

- query cache 只缓存绑定计划，性能证据为命中行为及 Catalog 重建调用次数；没有宣称某个查询吞吐量或延迟改善比例。
- max_query_cache_bytes 是已声明的计划表示逻辑预算，不是 RSS 测量；Catalog 元数据、临时 parser/binder 数据、当前行/结果和其他缓存仍需由 Compute 分配节点预算。此处只验证单 Engine 的 cache entries/charge 限制。
- 未来函数视图/UDF/Schema 定义变化必须参与 Catalog provenance/激活失效；不能在缓存计划下静默替换已绑定的执行语义。本次未审查未来扩展接口。
- 原 M2/M3 规格状态、全量测试、Release/Bear 证据和里程碑关闭由 root 统一维护。本报告只为内部逐次 Catalog 深克隆偏差及当前 query-cache 组件提供独立证据。
