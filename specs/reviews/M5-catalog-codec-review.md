# M5 Catalog wire codec 独立组件审查

日期：2026-09-14。审查者：`fresh_status_review`。

最终结论：审查发现的合法定义范围/资源配置不一致已由实现者修正，并由本 reviewer 独立复验关闭；**当前限定范围内无未关闭 must-fix。** 主树 codec source `d7f6f312...` 在独立 Clang-Werror/三 sanitizer build 上通过 **17/17**：9 项独立探针及当前 8 项仓库 codec 用例。原失败和第一版整改遗漏的证据保留如下，不以单次 GREEN 隐去整改过程。

本次复用此前执行 RequestStatus/Latest/预算审查的独立 reviewer 线程，重新读取 M5 需求和新组件实现；**这是独立组件审查，不是新的 fresh-context 里程碑或全局 review**。MetaService、部署协议、整个 M5 均未因此完成，M5 最终仍需规格要求的 fresh-context milestone/global reviews。

## 范围和独立证据

- 原始要求：[AGENTS.md](/home/obscure/EventFlow/AGENTS.md)、[specs/INDEX.md](/home/obscure/EventFlow/specs/INDEX.md)、[M5 Catalog codec 切片](/home/obscure/EventFlow/specs/control-plane.md)。
- 全量阅读新 `include/eventflow/catalog_codec.hpp`、`src/sql/catalog_codec.cpp`、`tests/catalog_codec_test.cpp` 和 CMake 的 `ef_catalog_codec` / `catalog_codec_test`；沿调用链检查 `Catalog::from_snapshot`、canonical descriptor/plan JSON、Schema/typed value codec 和表达式语义校验。
- 被测代码来自主树；独立 CMake wrapper/build 位于 `/tmp/eventflow-catalog-codec-review`。所有需要的 `ef_core/ef_sql/ef_catalog_codec` 项目对象从源码在该新目录编译，没有使用实现者对象或采信其测试结果。
- 原版本 header/source/test 保留在该目录 `before/`，内容 SHA 与本审查开始时的三份文件完全相同；专用 `codec_before` / `catalog_codec_review_before` 同时使用旧 codec source 和旧 header，普通 Catalog/SQL 实现保持相同。原三份内容可独立复现初始失败。
- 初始 [probes_before.cpp](/tmp/eventflow-catalog-codec-review/probes_before.cpp) 提供 8 类独立边界；当前 [probes.cpp](/tmp/eventflow-catalog-codec-review/probes.cpp) 又补充字段配置各分支检查。复用现有 full_catalog fixture 建图，断言和边界由本 reviewer 新增，未编辑仓库实现或测试。
- 仓库内仅写本报告；没有重跑 M4 或扩展为网络/攻击清单。

## F1：隐藏的固定数组限制与公开字段预算不一致

级别：必须修正。最终状态：宽表、宽投影及聚合项分支均已独立复验关闭。

初始 codec 的 `visible_count`、field/type/slot 数组固定限制为 1024，但 Catalog 本身并没有把所有合法表限定为 1024 个字段。独立用公开 SDK 构造 **1025 列**表，`Catalog::define` 和 `Catalog::from_snapshot` 均成功，canonical 定义只有 **57,624 字节**，在原默认 byte/relation/expression/provenance 限额内；codec 的 encode/decode 却都返回 `InvalidArgument: Catalog integer is out of range`。初始 8 项探针中 7 项通过、此项失败，见 [probes-before.log](/tmp/eventflow-catalog-codec-review/probes-before.log)。

实现者随后增加 `CatalogCodecConfig::max_fields_per_relation=4096`，field/type/slot/visible/schema.fields 改为配置检查，预算不足返回 ResourceExhausted。对 codec source `7b48e1d3...` 的独立复验：1025 列表、精确字段预算 1025、低一档 1024 的拒绝和 `SELECT *` 宽投影已通过。

但该版 `Decoder::plan` 的 `aggregates` 仍固定 `array_field(...,1024)`，产生两个真实反例：

1. 通过 SDK `SelectAst` 构造 **1025 个 `COUNT(*)`** 的物化视图，`Catalog::define` / `Catalog::from_snapshot` 均成功，字段预算默认 4096；encode/decode 仍被旧 1024 限制拒绝为 `ResourceExhausted: Catalog array limit exceeded`。这里使用 SDK AST，因为同等长的 SQL 文本会先触及 SQL parser 的 token 限额；不把 parser 的既有约束误报为 codec 问题。
2. 公开字段预算设为 **6**，在原 totals 的四个 AggregateSpec 后追加三个未投影的合法 aggregate state 项，保持其余规范字段一致，decode **成功接受 7 个 AggregateSpec**。因此该分支不遵守较小的公开配置，也不只是错误码分类差异。

第一版整改的复现见 [probes-field-fix1b.log](/tmp/eventflow-catalog-codec-review/probes-field-fix1b.log)。最终 source `d7f6f312...` 将 aggregate 数组同样改为 `config.max_fields_per_relation`。独立重建复验：1025 列表、宽 SELECT * 投影、SDK 1025 个 COUNT(*) 都能往返；预算为 6 时第 7 个 AggregateSpec 返回 ResourceExhausted。F1 关闭。Function inputs=32 与 expression depth=128 则是既有 Catalog 约束，本审查没有要求无限扩大它们。

## C1：Schema key 数组的预算检查顺序

对 `max_fields_per_relation=6`、超过 6 个条目的 `storage_schema.primary_key/partition_key` 数组，第一版整改先在 Schema 语义检查返回 InvalidArgument，而不是在 codec 数组预算检查返回 ResourceExhausted。**它没有接受非法图，不等同 F1 的接受超预算状态项。** 最终版在 `schema_from_json` 前对两者应用相同配置；独立测试这两个分支及另外 20 个 metadata 数组、visible_count 均符合超界 ResourceExhausted。C1 关闭。

## 其余八类核对内容

| 类别 | 独立检查及当前证据 |
| --- | --- |
| 完整定义与旧 canonical | 所有关系 kind、Candidate/RequestStatus annotation、SQL/group/join 和 Function 签名一起往返；新增完整类型表及 SDK literal 包含 Int64、Bool、TimestampMicros 最小值、String、Float64、FloatVector、typed NULL。规范 bytes 与原 snapshot.fingerprint 完全相同；再次 encode 仍相同。没有修改旧 canonical serializer。 |
| literal/NULL/nullable/隐藏 scope | NULL 保持 monostate、具体 DataType、nullable=true、untyped_null=false；Float64 和 vector 中的 -0.0 符号保留；省略可见 partition 字段的 scoped SQL 正确恢复 kNoSlot 和 inferred_scope。非法空 projection/group/aggregate argument、untyped_null 元数据拒绝。 |
| 未知字段 | 对 root、relation、schema、schema field、plan、expression、output field、function/definition/input、candidate、request_status、latest/definition、aggregate 15 层逐个加未知字段，均拒绝；未把结构解析时忽略的字段默默丢弃后接受。 |
| 数字类型/溢出 | metadata integer 字段分别替换负数、double、bool、string、null 均拒绝；relation/state/schema field ID 的 UINT32 溢出以及原始 JSON 的 Int64/UInt64 溢出拒绝；nullable 不能用整数代替。 |
| 图与 annotation 语义 | 全图删除 Candidate/Function/RequestStatus/Latest annotation、缺 kind/domain/identity_fields、改 status domain、非法 join 右关系和 GROUP BY slot 均拒绝。合法 group、join、request lineage 从完整 canonical definitions 重建，仍由 Catalog 原验证器判定语义。 |
| 精确资源边界 | 基准图 **13,840 bytes / 11 relations / 27 expression nodes / 最大 expression depth=2 / 累计 provenance 40,748 bytes**；各项恰好上界时 encode/decode 均成功，各自少一单位均 ResourceExhausted，零值及 depth=129 配置 InvalidArgument。新增字段预算同样测试 exact/-1/zero。计数横跨 filter、group_by、projection、aggregate arguments，而不是只验 projection。 |
| prefix provenance | 每个 SQL/Function/Latest descriptor 的 catalog_version 和 catalog_fingerprint 逐项等于其之前完整 Catalog 的规范 prefix，三类都实际覆盖；更改各类绑定版本拒绝；仅基表图无 plan 时不虚构 provenance 消费。 |
| 原始 JSON 深度 | 深度 512 可通过前置 nesting gate 后按结构错误返回 InvalidArgument；513 在 parse 前 ResourceExhausted。合法 literal 内 700 个方括号及转义引号/反斜杠不计结构深度；尾随 token、截断输入拒绝。 |

资源限制的接受行为与有效图重建已按上述边界验证。本轮没有做 allocator 峰值测量，不把 wire 字节/表达式/provenance 计数等同整个 MetaService 的进程内存硬预算；例如 encode 会先调用 Catalog::from_snapshot 验证 typed snapshot，再走 decoder 的同一资源策略。

## 构建与运行命令

```sh
cmake -S /tmp/eventflow-catalog-codec-review \
  -B /tmp/eventflow-catalog-codec-review/build \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS=-Werror
cmake --build /tmp/eventflow-catalog-codec-review/build \
  --target catalog_codec_review_before -j 4
cmake --build /tmp/eventflow-catalog-codec-review/build \
  --target catalog_codec_review catalog_codec_test -j 4

LD_LIBRARY_PATH=/home/obscure/EventFlow/cpp3rdlib/lib \
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
/tmp/eventflow-catalog-codec-review/build/catalog_codec_review_before \
  '--gtest_filter=IndependentCatalogCodec.*'

LD_LIBRARY_PATH=/home/obscure/EventFlow/cpp3rdlib/lib \
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
/tmp/eventflow-catalog-codec-review/build/catalog_codec_review \
  '--gtest_filter=IndependentCatalogCodec.ExistingWideSchemaCanRoundtripWithinDeclaredLimits:IndependentCatalogCodec.FieldBudgetAppliesToEverySchemaAndMetadataArray'

# Final matching implementation and complete component regression.
cmake --build /tmp/eventflow-catalog-codec-review/build \
  --target catalog_codec_review catalog_codec_test -j 4
LD_LIBRARY_PATH=/home/obscure/EventFlow/cpp3rdlib/lib \
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
/tmp/eventflow-catalog-codec-review/build/catalog_codec_review \
  '--gtest_filter=IndependentCatalogCodec.*:CatalogCodec.*'
```

实际使用 Clang 18.1.3，C++20，`-Werror -Wall -Wextra -Wpedantic -fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`，仅链接 cpp3rdlib 头和预编译库。配置和构建均成功；原始失败及第一版聚合项复验失败均为功能断言，没有 sanitizer diagnostic。中间一次宽聚合测试使用过长 SQL 文本而触发 fixture 的 `SQL token budget exceeded`/abort，已改用现有合法 SDK AST 构造再实跑；不以那次 abort 作为 codec 缺陷证据。

最终 [build-final.log](/tmp/eventflow-catalog-codec-review/build-final.log) 构建退出 0、无 warning/error；[probes-final.log](/tmp/eventflow-catalog-codec-review/probes-final.log) 完整 **17 tests / 2 suites，17 passed，15,635 ms，exit 0**，无 ASan/UBSan/LSan 诊断。此 run 直接包含并执行匹配源码的仓库 8 项和独立 9 项，不将未运行的其他 M5/M4 tests 计入。

初始版本：

```text
a35ec4231765c6f0998a20b337328da74c5a817c7832211cd21fd16c1ffae783  include/eventflow/catalog_codec.hpp
2fa840a0eed1163b8ffe8f89044ed07b6b0b9f252e26c5800567f8c3853b0f5b  src/sql/catalog_codec.cpp
94921dae3259aa9341ab145c2514c95d35dc2c62ef0834ca18a36e2273356873  tests/catalog_codec_test.cpp
```

第一版字段整改的实测 source：`7b48e1d3f4fd9177615dcfd46de25c0063c376c2568dba9a3671ec780a9ee525`。其余 canonical serializer 在审查时为：

```text
edbcdc14146ef5f11ad904e42ede38baf9dedc89ea4091f0bdab5edcfb5fab1d  src/sql/catalog.cpp
0b107256ddf48370df8e8cc94db35148c66de4da33dba256a74a96dadbc72663  src/sql/binder.cpp
```

最终实测的三份组件文件 hash 在重建前和测试后相同；上述 catalog.cpp/binder.cpp hash 也保持审查开始时的值：

```text
2a282f85fd63cfe1fa078d8b78af2aa885d48e5aaaa314dc15d8cae4f8f516cf  include/eventflow/catalog_codec.hpp
d7f6f312feb355ef4b083bf4abadc99ffda6bc538340eeb0277589dcca97c308  src/sql/catalog_codec.cpp
fb37c9d5e2f4143bf776335ee1da0a7423ec1926d0055848894cd31668dfb02e  tests/catalog_codec_test.cpp
```

独立审查程序内容标识：

```text
f87662ac95c881573816d259e8a6965bd77e4231ff0183d90e0e6bb0b6c51a13  /tmp/eventflow-catalog-codec-review/probes.cpp
fd12a3cee64d2a4d5a2b72ff05ed7c13f9d6716b7b17f9c05ce9a2ad062b77bf  /tmp/eventflow-catalog-codec-review/probes_before.cpp
b56fdfc9177fa64357b21d9019498520f4e9127c232d7593ff93465600e54209  /tmp/eventflow-catalog-codec-review/CMakeLists.txt
```

此组件审查与两项处置已完成。M5 仍为 active；组件 codec 不提供 MetaStore、RPC、Job/route/deployment、checkpoint 或 callback recipe 的实现证明，也不替代最终 fresh-context milestone/global reviews。
