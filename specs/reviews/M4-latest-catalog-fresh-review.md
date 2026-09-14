# M4 Latest Catalog 隔离切片独立审查

日期：2026-09-14。Reviewer：`latest_catalog_fresh_review`，独立 fresh-context 子 Agent。

结论：**六文件 Catalog patch 可作为元数据切片采用；未发现必须退回这六个文件整改的阻断缺陷。采用主树前仍须补上 Engine 显式拒绝门禁，并在正式 CMake 注册新测试。** 门禁缺失已由本审查独立复现，尚未在本审查中修复或验证修复。此结论不代表 Latest runtime、M4 里程碑或全系统交付通过。

## 范围与版本

原始要求读取自主树 [AGENTS.md](/home/obscure/EventFlow/AGENTS.md)、[specs/INDEX.md](/home/obscure/EventFlow/specs/INDEX.md)、[Latest spec](/home/obscure/EventFlow/specs/latest-selector.md) 和 [C7/C8 契约](/home/obscure/EventFlow/specs/runtime-contracts.md:77)。实现者交接只用作定位，测试结果在独立 build 中重新产生。

实现位于 `/tmp/eventflow-latest-catalog-work`；主树 `/home/obscure/EventFlow` 在本审查期间尚未采用该 patch。被审查的 [patch](/tmp/eventflow-latest-catalog.patch) SHA256：

```text
899131e2c5917a461c4133d7ea4e6ec34ab56385e1a52bb1d860579c85f19922
```

审查范围严格为以下六个隔离文件：

- [catalog.hpp](/tmp/eventflow-latest-catalog-work/include/eventflow/catalog.hpp)
- [catalog.cpp](/tmp/eventflow-latest-catalog-work/src/sql/catalog.cpp)
- [binder.cpp](/tmp/eventflow-latest-catalog-work/src/sql/binder.cpp)
- [binder_internal.hpp](/tmp/eventflow-latest-catalog-work/src/sql/binder_internal.hpp)
- [expression.cpp](/tmp/eventflow-latest-catalog-work/src/sql/expression.cpp)
- [latest_catalog_test.cpp](/tmp/eventflow-latest-catalog-work/tests/latest_catalog_test.cpp)

另对隔离树中的既有 Engine 做采用边界 probe。审查者没有修改主树生产/测试文件，也没有修改隔离实现文件；新增验证源、CMake harness 与日志全部在 `/tmp/eventflow-latest-catalog-review`。主树只新增本审查记录。

## 发现、处置与采用条件

| ID | 发现及影响 | 处置与当前状态 |
| --- | --- | --- |
| LC-I1，采用前必须处理 | 既有 `Engine::open_runtime` 只对 FunctionView 检查 registry，对新 annotation/Latest 没有拒绝门禁。合法的 Candidate + RequestStatus + RequestCapture → SqlView → Latest 图不含 FunctionView，公共 `Engine::open` 实际返回成功，并把 MemoryStateStore revision 从 0 推到 1；此时没有 Latest selector 或可信生命周期写协议。 | Root 已确认这是集成条件，负责在任何初始化提交前显式拒绝 `candidate`、`request_status`、`latest`。普通 open 不得借无 Function 图绕过；后续只有完整 typed owner/selector 协议实现并验证后才能私有启用。本审查保持失败 probe，未声称修复。 |
| LC-I2，采用时必须处理 | 六文件 patch 有 `latest_catalog_test.cpp`，刻意不含 CMake 改动。该测试直接使用 RequestMachine。单独采用 patch 而遗漏注册将不会执行新增测试。 | 正式 CMake 测试清单增加 `latest_catalog_test\|ef_request_machine`，保持 GoogleTest 仅 BUILD_TESTING 测试目标使用，并执行整合后相关 gate。隔离 CMake 已有注册；本次重新执行了它。 |

LC-I1 的源码依据是 [隔离 Engine 打开检查](/tmp/eventflow-latest-catalog-work/src/runtime/engine.cpp:828) 及 [初始化提交](/tmp/eventflow-latest-catalog-work/src/runtime/engine.cpp:878)。审查构造的是规范允许的保持完整隐藏身份的 SQL result，没有伪造 CatalogSnapshot。

独立 [边界 probe](/tmp/eventflow-latest-catalog-review/latest_engine_boundary_probe.cpp) 与 [失败输出](/tmp/eventflow-latest-catalog-review/engine-boundary.log) 保留了可执行整改入口：1 test，2 个断言失败，进程 exit code 1，分别为 `opened.ok() == true` 和 `revision == 1`（预期拒绝且 revision 仍为 0）。这是采用条件失败，不能与下面通过的 Catalog 测试合并成全绿结论。

## 契约逐项检查

| 要求 | 源码与独立证据 | 判断及边界 |
| --- | --- | --- |
| Candidate 规范字段、scope 键与 RuntimeStatus 所有权元数据 | [canonical_fields / candidate_descriptor](/tmp/eventflow-latest-catalog-work/src/sql/catalog.cpp:147) 重排 field IDs，scope/payload 共用原 ID/name 去重集；Schema 拒绝 nullable/非法 key 类型。新增原测试覆盖空 scope、保留名、未知类型等；独立 probe 使用两个 String scope 与 nullable FloatVector payload。 | 元数据符合 v1。`RuntimeStatus` 标签本身不证明 owner 授权。 |
| RequestStatus 域身份与 Candidate scope 语义一致 | [request_status_descriptor](/tmp/eventflow-latest-catalog-work/src/sql/catalog.cpp:187) 对 scope 比较完整 Field；[validate_domain](/tmp/eventflow-latest-catalog-work/src/sql/catalog.cpp:52) 双向纳入 status annotation。独立 probe 使用物理声明顺序不同的 Capture，并拒绝同类型 scope 名称/次序互换及 request ID 改名。 | 不是只比较 DataType；annotation 留存 domain/identity，descriptor 两字段保持空。 |
| Latest 三输入与物理键缩为 scope | [latest_descriptor](/tmp/eventflow-latest-catalog-work/src/sql/catalog.cpp:265) 校验 earlier IDs、source kind/domain、完整 identity、Candidate 绑定；复制可见 schema 后追加两列，identity_types 收缩为 partition_types。原测试与独立 probe 检查 hidden scope、nullable payload、错误完整身份长度/值、两个 scope 的顺序。 | Result 的完整 request identity 由 source 保存；Latest 的物理键只保留 scope。运行时 winner identity 尚未实现。 |
| SQL 后继无请求 lineage，scope aliases 保留位置 | [partition_lineage](/tmp/eventflow-latest-catalog-work/src/sql/binder_internal.hpp:39) 从 Latest 回溯 result 的相同可见 slot，再沿 SQL Input lineage 追踪。独立 probe 为两个同类型 scope 各造两个 alias，交错投影并再建 SQL view；正确 lookup join/推断 scope 通过，交叉位置 join 与遗漏一维 join 拒绝。 | 未发现把第二个 alias 丢失或按类型混淆 scope 的问题。原 `FunctionAndRequestMachineRejectLifecycleAndLatestInputs` 重新运行通过。 |
| SQL 可读取/继承生成 provenance，用户不能伪造保留列 | [生成投影约束](/tmp/eventflow-latest-catalog-work/src/sql/binder_internal.hpp:23) 要求同名非空 Int64 Input；[binder alias 检查](/tmp/eventflow-latest-catalog-work/src/sql/binder.cpp:501) 对显式 alias 仍执行保留名限制；[plan 校验](/tmp/eventflow-latest-catalog-work/src/sql/catalog.cpp:695) 同样检查。独立 probe 验证 star/chain/filter/join/算术读取，以及常量、payload、epoch→seq、更名后恢复保留名均不能伪造。 | 未加引号标识符沿既有 parser 转小写，`__EF_latest_admission_seq` 因而是合法读取；这不赋予创建保留列的权限。 |
| kind/pointer/annotation 一一对应，格式拒绝 | [validate_kind](/tmp/eventflow-latest-catalog-work/src/sql/catalog.cpp:17) 强制互斥 annotation、RuntimeStatus base role、对应 plan pointer 与 format=1；from_snapshot 再规范重建。独立 15 种突变包括未知 kind、丢失 pointer、混入其他 annotation、format=0。 | 相关突变全部拒绝。 |
| 单 Catalog 图、依赖、拓扑与 state_id | [dependencies](/tmp/eventflow-latest-catalog-work/src/sql/catalog.cpp:826) Latest 返回 result/status/candidate，RequestStatus 返回 candidate，普通 base 无依赖；定义与 snapshot 重建依次要求既有输入。state_id 由 RelationId 确定并规范比较。独立 probe 拒绝未来输入 ID、wrong source、state_id=0，原测试覆盖 state collision。 | 没有增加 runtime 计算边；依赖不意味着 Candidate delta 自动写 RequestStatus。 |
| Snapshot provenance、深拷贝与非法 schema | [from_snapshot](/tmp/eventflow-latest-catalog-work/src/sql/catalog.cpp:878) 从已重建前缀重新生成 descriptor；Latest 校验绑定 version/fingerprint，再比较规范 descriptor bytes。独立 probe 检查 pointer 均为新对象，拒绝 scope slot、key slot、provenance name/nullability、fingerprint、unique flag 等突变。 | 深拷贝由重建获得；原测试另实际修改调用方持有的三个 payload，restored 内容不变。 |
| Rowhelper 规范类型、host identity、epoch/seq/phase | [布局与行检查](/tmp/eventflow-latest-catalog-work/src/sql/expression.cpp:253) 先校验 metadata/schema，再使用 typed access。独立 probe 接受 1 与 INT64_MAX，拒绝 0/负数/Float64 epoch、错误/NULL phase、不同 request ID；nullable result 正常往返。 | 只证明行级 schema/值约束；不证明不可逆终态、uint64→Int64 转换边界、seq 分配或 owner capability。 |
| 旧 canonical 编码保持 | `descriptor_json` 的新增字段只在新 metadata 存在时写入；旧普通 SQL/Capture/Function 分支未改定义编码。独立 build 分别编译原基线与新 SQL 库，再运行同一 ordinary SQL + Capture→Function→SQL fixture，完整 bytes `cmp` 成功。 | 覆盖该组合 fixture，非任意历史 Catalog 的穷尽证明。 |

## 本次新鲜构建和执行证据

所有构建均使用 CMake / Unix Makefiles，Clang 18.1.3、C++20、Debug、`-Werror -Wall -Wextra -Wpedantic`。实际生成的 [ef_sql 编译参数](/tmp/eventflow-latest-catalog-review/build/implementation/CMakeFiles/ef_sql.dir/flags.make) 与 [独立 probe 编译参数](/tmp/eventflow-latest-catalog-review/build/CMakeFiles/latest_review_probe.dir/flags.make) 均包含：

```text
-fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer
```

执行环境为 `ASAN_OPTIONS=detect_leaks=1:halt_on_error=1`、`UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`、`LSAN_OPTIONS=exitcode=23`。第三方使用现有 `cpp3rdlib` headers/prebuilt libraries，没有下载或引入第三方实现源码。

独立 harness 为 [CMakeLists.txt](/tmp/eventflow-latest-catalog-review/CMakeLists.txt)。主要命令：

```sh
cmake -S /tmp/eventflow-latest-catalog-review \
  -B /tmp/eventflow-latest-catalog-review/build \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON -DCMAKE_CXX_FLAGS=-Werror
cmake --build /tmp/eventflow-latest-catalog-review/build \
  --target latest_catalog_test catalog_test function_catalog_test expression_test \
  request_machine_test latest_review_probe canonical_baseline canonical_current \
  latest_engine_boundary_probe -j 3
LD_LIBRARY_PATH=/home/obscure/EventFlow/cpp3rdlib/lib \
  ctest --test-dir /tmp/eventflow-latest-catalog-review/build/implementation \
  -R '^(latest_catalog_test|catalog_test|function_catalog_test|expression_test|request_machine_test)$' -V
```

| 执行项 | 新鲜结果 | 原始证据 |
| --- | --- | --- |
| Catalog / FunctionCatalog / LatestCatalog / RequestMachine / Expression | 5 targets；19 + 18 + 18 + 21 + 24 = **100 tests 通过**；CTest exit 0，4.48 s | [tests.log](/tmp/eventflow-latest-catalog-review/tests.log)、[build.log](/tmp/eventflow-latest-catalog-review/build.log) |
| Reviewer 独立对抗 probe | **5/5 通过**；exit 0，无 sanitizer diagnostics | [probe 源码](/tmp/eventflow-latest-catalog-review/latest_review_probe.cpp)、[probes.log](/tmp/eventflow-latest-catalog-review/probes.log)、[最终 probe 构建](/tmp/eventflow-latest-catalog-review/build-probes-final.log) |
| 采用前 Engine 拒绝条件 | **1/1 失败**，两断言证明静默接受并初始化；exit 1 | [engine-boundary.log](/tmp/eventflow-latest-catalog-review/engine-boundary.log)、[Engine probe 构建](/tmp/eventflow-latest-catalog-review/build-boundary.log) |
| 基线/新 canonical bytes | 独立重新编译并运行，两输出 `cmp` exit 0；SHA256 均为 `4caadf746f62a2cc0c562b5ebe7352486176eec697920574d531f3349c75d9fd` | [baseline](/tmp/eventflow-latest-catalog-review/canonical-baseline.json)、[current](/tmp/eventflow-latest-catalog-review/canonical-current.json) |
| Patch 可应用与主树隔离 | baseline 目录 `git apply --check` exit 0；六个交付文件 SHA256 与 handoff manifest 相同，五个既有主树文件仍与 baseline SHA256 相同 | [hash manifest](/tmp/eventflow-latest-catalog-hashes.json)；本审查实时命令检查 |

审查 harness 初次配置遇到子目录非 GLOBAL GTest imported target 的作用域问题，随后只在独立 harness 使用现有 GoogleTest 预编译库路径修正；不属于产品构建失败。独立 probe 初轮把未加引号的大写 `__EF_` 误设为应拒绝，检查 [既有 parser](/tmp/eventflow-latest-catalog-work/src/sql/sql_parser.cpp:35) 后修正为应接受并重新编译通过，首轮输出保存在 [probes-initial-expectation.log](/tmp/eventflow-latest-catalog-review/probes-initial-expectation.log)。

## 剩余范围与交接

Root 采用时先落实 LC-I1 / LC-I2，再用主树实际整合代码复验；不能把本报告中的隔离路径或测试证据替换成“主树已通过”。门禁整改应使上述失败 probe 变绿，并补充 Candidate-only / RequestStatus 与私有 open 的覆盖。六文件 patch 的 ABI 扩展需要相关使用方重新编译。

Latest selector、typed host factories、授权写集、RequestRuntime status 发布、整 Request SUCCEEDED 判断、同轮 winner 与 SQL 原子传播、candidate epoch 转移、UNKNOWN/replay、初始化/恢复校验、预算/GC、completion 触发 B 等仍属于后续 runtime 工作。本审查没有运行全系统、四 KV adapter 的 Latest 一致性契约、真实多进程、Kafka 或最终多轮全局 gate；没有将任一 M4 runtime 验收项标为完成。
