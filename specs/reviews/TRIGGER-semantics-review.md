# Trigger 语义独立审查

Reviewer: trigger_semantics_review（独立 fresh-context）  
Date: 2026-09-14  
Round: 本里程碑唯一一轮独立审查中的语义审查  
Disposition: 发现 1 个 P1、2 个 P2；修复与定向验证由实施者追加，本报告不表示验收完成。

审查依据为 `AGENTS.md`、`specs/INDEX.md` 及 `trigger-api-ergonomics.md` 文末“最新授权与实施进展”。强类型静态函数、两种 capture、显式 session 和完整 Pattern 到请求链路属于当前范围；没有沿用历史“只做提案”限制，也没有增加动态上传、HA、通用观察 host 等要求。

## 发现

### S1 / P1：无关 Catalog 关系的分区长度可使请求预算计算越界

位置：`src/runtime/event_processor.cpp:2754`、`:2791-2793`，`EventProcessor::Impl::relation_envelopes`。

该函数遍历整个 Catalog，却使用当前 owner 的 `scope[i]` 替换每个关系的分区字段预算。Catalog 中任何关系的 `partition_slots.size()` 大于当前 scope 长度时都会越界；该关系无需属于输入、请求依赖或 Trigger。

最小复现：正常 ModuleFixture 的 input/session 都按 STRING ticker 分区；仅额外定义一个未使用的 `unrelated(ticker, sequence)` 表，按 `(ticker, sequence)` 分区。原来的 `fixture.pair()` 不变。`TriggerSet::bind` 与 `EventProcessor::open` 成功，第一条输入在预算预留路径触发 ASan `heap-use-after-free`，调用链为 `relation_envelopes:2792 -> reserve_control -> encode -> round`。实际根因是 `scope[1]` 越界读取，不是用户回调或匹配参数的生命周期。

探针：`/tmp/eventflow-trigger-semantics-review/probe.cpp` 中 `SemanticsProbe.UnrelatedCatalogPartitionDoesNotBreakAValidModule`。完整诊断：`/tmp/eventflow-trigger-semantics-review/unrelated-catalog.log`。

建议：预算推导只对已证明与当前 owner scope 兼容的关系使用具体 scope 值；无关关系保留保守字段界限或从该 owner 的相关图中排除。对访问增加明确边界检查。单 Catalog 本来允许保存无关关系，不应仅通过要求所有关系分区相同来掩盖问题。验证需保留这个“合法模块 + 无关复合分区表”的测试，避免只修复 S3 后误认为越界已消除。

### S2 / P2：Clock 仅检查 policy ID，未检查事件类型冲突

位置：`src/runtime/computation.cpp:269-290`，特别是 `:283-289`；`src/runtime/event_processor.cpp:2245-2263` 将每个 Clock firing 按 kind 投递到全部 FSM。

现有检查只拒绝重复 policy ID，以及与 `input_relations` 的关系名称相同的 kind，既未拒绝两个 Clock 共用 kind，也未覆盖 `EventRef::source` 自定义别名。规格第 4 节明确要求检查 source/其他 policy 的碰撞。

实际行为：两个独立 Trigger 的 Clock 周期分别为 1 秒和 2 秒，policy ID 不同，但都显式使用 `event_kind = "tick"`。绑定成功。观察 scope 后推进 1 秒，产生两条 occurrence，2 秒规则提前触发。这不是仅缺少诊断信息，而是执行了错误的业务规则。另一个探针证明 `.kind = "feed_alias"` 的 source 和同名 Clock 也可一起绑定；输入表实际名称为 `input`，因此现有比较漏过冲突。

探针：`RejectsDuplicateClockEventKinds`、`RejectsClockKindMatchingSourceAlias`。日志分别为 `clock.log`、`source-clock.log`（均在上述 `/tmp` 目录）。前者断言为实际 occurrence 数量 2、预期 1。

建议：模块绑定时收集完整 source kind 和 Clock kind，校验新增与已有 policy，并使结果与声明顺序无关；保留显式兼容旧 kind 的能力，但拒绝一个 kind 指向不同调度来源。针对 source 别名增加绑定失败测试，并保留两个不同周期的 owner 行为测试。

### S3 / P2：typed source 的分区声明未与实际 owner/session 校验

位置：`src/runtime/computation.cpp:30-57`；`src/runtime/trigger.cpp:475`；`include/eventflow/typed_trigger.hpp:59-65`。

`TypedEventRef::bind` 只证明 `key_by` 对应 payload relation 的 Catalog 分区键。`bind_trigger` 比较当前 Trigger 内部的事件与 read scope，但没有把事件关系传给模块层；`validate_session` 只遍历 guard reads 和 capture-current reads。因此没有 read 依赖的 typed Pattern，或只用 `capture_match` 的 Pattern，可以声明与 owner/session 完全不一致的业务分区。

独立于 S1 的复现：payload schema 按 INT64 `sequence` 分区，typed source 明确 `.key_by(&ModuleEvent::sequence)`；实际 owner 输入与 session 按 STRING `ticker` 分区。绑定与打开均成功。输入 ticker 都为 T、sequence 分别为 0 和 1 的两条事件，得到一个 CEP match，尽管它们属于 typed source 声明的不同分区。这里两个 scope 都只有一列，不触发 S1 的长度越界，直接证明分区合同被静默忽略。

探针：`RejectsSourcePartitionShapeDifferentFromOwnerAndSession`，日志 `source-scope.log`。实际结果为 `bound.ok() == true` 且 `pending_occurrences` 非空。

建议：把绑定后的 EventSource 元数据保留到模块层，校验每个有 payload schema 的 source 与 owner 输入分区及显式 session 的类型/形状兼容。该检查也应覆盖没有 request 的 Trigger。这里仍可保持“key_by 不生成 ProcessorEvent、不做 shuffle”的既定边界；缺少的检查发生在静态定义之间，无需扩展运行时重分区功能。

## 已核对的语义与证据边界

读取了 expression、typed schema、static function、Pattern、typed projection、Trigger 和 computation 的头文件/实现，以及 EventProcessor 的输入域、冻结 intent、请求绑定、UNKNOWN/恢复、预算路径和 FunctionRegistry::clone。还核对了生产 feature_computation 配置、旧 news 的单等待迁移，以及 typed_pattern_demo 的 A/B/C/D 断言。

- typed Pattern 通过 `input_kinds` 在 owner 层排除其他 kind；guard 不通过的本类型事件仍交给 E-NFA。原始 Pattern 未自动变成 typed 子流。非匹配输入仍调用 `advance`，不阻止 processing-time 到期。
- label 的 required/optional/repeated 形状在投影绑定时校验；输出按值编码，`capture_match` 从 occurrence 中提取 A/B 并经 admission arguments 保存，`capture_current` 在受理准备读取声明的当前视图。没有发现这两条路径混用最新源事件的问题。
- session policy、once 和 Wait/TryOnce 继续由原 owner/request 协议处理。Catalog 与 Registry 先私有复制再装配；失败不发布到传入对象。函数复用比较 ID/version/implementation/schema，没有修改原 Registry 的迹象。
- 新 definition identity 进入 FSM/CEP/owner 持久指纹，不占业务 register。空 identity 的旧 codec 路径仍存在。这里未把“旧手写配置与新生成配置恢复兼容”视为默认成立。

本 reviewer 实际执行：使用 Clang 18.1.3、CMake Debug 与 AddressSanitizer/UndefinedBehaviorSanitizer/LeakSanitizer 构建一个 `/tmp` 探针目标，链接从主 build 复制的静态库；未重新配置、构建或修改主树 production/tests。四个新增探针均证明上述缺陷，其中 S1 由 sanitizer 终止；其余探针报告明确的行为/绑定断言失败。另在同一隔离 executable 运行既有 `Computation.*`，6/6 通过（849 ms），包含 UNKNOWN 后 snapshot 恢复且投影只执行一次的端到端测试。日志 `baseline.log`。

复现构建与选择运行：

```sh
cmake -S /tmp/eventflow-trigger-semantics-review \
  -B /tmp/eventflow-trigger-semantics-review/build \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
cmake --build /tmp/eventflow-trigger-semantics-review/build -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1 \
  /tmp/eventflow-trigger-semantics-review/build/probe \
  '--gtest_filter=SemanticsProbe.RejectsDuplicateClockEventKinds'
```

主树全量测试、Release、格式/clangd、全部示例、ThreadPool 共享契约及跨进程集成由 root 的本轮证据负责，本报告不重复声称这些已由 reviewer 运行。typed_pattern_demo 的两种 capture 比较是源码断言核对，不冒充本 reviewer 的独立运行结果。修复后应补目标回归并更新本报告 disposition；按仓库约定不自动开启第二轮 fresh-context 审查。

## S1 修复与定向验证（实施者追加）

S1 已修复。`relation_envelopes` 在读取具体 key 值前校验 owner scope 的长度、类型和 shard；仅为分区类型与当前 owner 一致的关系推导当前 key 的预算。无关复合分区、整数分区关系及其级联 MV 仍可保留在同一个 Catalog。SQL、Latest 的依赖预算和请求/到期发布的预算查找改用可检查的 lookup，缺少兼容依赖返回 Status；同时显式检查分区、可见列和身份列索引边界。

新增 [relation_envelope_scope_test.cpp](../../tests/relation_envelope_scope_test.cpp)：合法 ticker source/session 与两组无关表和两层级联 MV 共存，实际完成 typed Pattern 匹配、输入捕获、函数执行和 Latest 查询，结果为 3.0、回调执行一次；错误长度或类型的直接 scope 返回 InvalidArgument，owner revision 不变。没有通过拒绝所有无关 Catalog 关系规避问题。

证据使用隔离 CMake 目标直接重新编译修复前/后的 [event_processor.cpp](../../src/runtime/event_processor.cpp)，其他组件链接本轮 reviewer 保存的库快照；未用旧 EventProcessor 库替代修改后的源码。新测试以 Clang、`-Werror`、ASan/UBSan/LSan 构建：

- RED：`/tmp/eventflow-relation-envelope-red.log`，复现 `relation_envelopes` 中 `scope[1]` 越界导致的 ASan heap-use-after-free。
- GREEN：`/tmp/eventflow-relation-envelope-green.log`，新增 2/2 测试通过，661 ms。
- 未修改的事件/请求合同：`/tmp/eventflow-relation-envelope-event_request.log`，Memory/Rocks/HTTP 适配器下 96/96 通过，56.361 s。
- 未修改的 CEP owner 合同：`/tmp/eventflow-relation-envelope-event_processor_cep.log`，Memory/Rocks/HTTP 适配器下 100/100 通过，10.296 s。

源码与测试已 clang-format。此记录只关闭 S1 的修复与定向门禁；S2/S3、主树最终构建和全量验收由 root 单独记录，不新增 fresh-review 轮次。


## 实施者整改记录（同一轮审查）

四项去重发现已完成修复并有定向回归，不另开fresh review：

- 空typed guard：运行时指针为空时保留InvalidArgument，NTTP空指针编译拒绝；9项typed测试通过，见/tmp/eventflow-null-guard-green.log与/tmp/eventflow-static-null-guard-after.log。
- 无关Catalog分区越界：预算推导先检查owner scope，只为兼容分区关系生成envelope；依赖查找与索引有显式校验。无关复合/整数分区表及其级联MV与真实请求共存；新2项、原请求96项、CEP owner100项三san回归通过，见/tmp/eventflow-relation-envelope-green.log、-event_request.log、-event_processor_cep.log。
- Clock kind冲突：模块绑定统一收集已有/新Clock kind以及源表名称和声明的source kind，拒绝互相冲突，且与声明顺序无关；合法1秒与10秒规则仍独立调度。
- Source/owner分区差异：BoundTrigger保留事件关系信息，模块对全部typed/非typed源、无reads/无request的情况也与owner分区形状校验；request另与Session校验。

最后两项新增回归先RED后GREEN，computation_test共9项通过930ms，见/tmp/eventflow-binding-review-red.log、/tmp/eventflow-binding-review-green.log。最终代码格式化后正在执行Bear全量Clang Debug和Release构建，随后执行全量回归及七个示例；最终门禁另记，不能用这里的定向结果代签全部交付。


最终闭环：本轮55个测试目标完成验证（53个初跑通过，两个修正后的完整目标复跑通过），七个Release与解压包示例通过；所有质量门禁通过。源码/产物身份与原始失败、定向修复、最终结果的范围见[重构验证](../verification/trigger-refactor-validation.md)。原独立审查发现保留，本记录完成实施者处置，不构成另一轮fresh review。
