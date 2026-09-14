# Trigger API 独立工程审查

审查日期：2026-09-14。Reviewer：`trigger_api_review`，独立 fresh-context reviewer；本报告属于本次 Trigger 里程碑的同一轮并行全局审查。审查期间只读生产代码、测试与文档，仅新增本报告，在 `/tmp/eventflow-trigger-api-review.ofv7jl/` 创建独立探针；没有修改项目构建目录，没有重跑全量构建。

审查依据：[AGENTS.md](../../AGENTS.md)、[当前索引](../INDEX.md)、[Trigger 最新授权](../trigger-api-ergonomics.md)。按文末最新授权审查强类型输入、静态函数、两种 capture、Session 和自动 Request 装配，未把这些能力降为未实现建议。HA、动态插件、通用观察 host、Kafka Compose 不在本次增量验收中。

## 结论与处置状态

发现三个需要修复的 P2 问题，未发现有证据支持的 P1。现有正向测试不能覆盖以下配置边界；应完成对应修复及定向回归后再由实施者汇总本轮门禁。本报告不要求重新开启 fresh-context 审查。

| 编号 | 优先级 | 问题 | 初审状态 |
| --- | --- | --- | --- |
| API-1 | P2 | TypedPattern 的空 guard 函数指针可通过绑定，首条匹配输入触发非法调用 | 已独立复现；待实施者处置 |
| API-2 | P2 | Clock 的 event kind 未排除其他 Clock 和声明的 source kind，导致提前触发 | 已绑定及真实 owner 复现；待实施者处置 |
| API-3 | P2 | event payload relation 的业务分区形状未与 owner 输入形状比较 | 普通和 typed source 均独立复现；待实施者处置 |

行号指审查快照；后续格式化或修复可能移动位置。以下保留初始观察，不以随后源码变化覆盖原发现。

## API-1：空 typed guard 指针在运行时崩溃

位置：[typed_trigger.hpp](../../include/eventflow/typed_trigger.hpp)，`TypedPattern<T>::where(Fn, identity)`，审查时第 96–112 行；`where<Fn>(identity)` 同样委托到该重载。

`is_nothrow_invocable` 只检查类型，空的 `StatusOr<bool> (*)(const T&) noexcept` 仍符合条件。实现随后创建非空包装 lambda，因此表达式绑定器检查 `bool(node.event_guard)` 时无法识别内部空指针。合法 payload 到达后，`std::invoke(function, typed)` 调用空地址。

独立 Clang 18.1.3、ASan/UBSan/LSan 探针使用常规 Catalog、`key_by`、单事件 Pattern 和正数窗口：

```text
NULL_GUARD_BOUND
AddressSanitizer:DEADLYSIGNAL
ERROR: AddressSanitizer: SEGV
... TypedPattern<ProbeNews>::where ... std::invoke(function, typed)
```

退出码为 1，ASan 定位到审查时 `typed_trigger.hpp:108`。这属于可在声明阶段检测的无效 callback，不应等到 owner 执行时终止进程。`Function<Fn>` 已明确拒绝空静态指针，typed guard 应采用一致边界：模板实参为空时编译期拒绝；运行时指针为空时在绑定前返回 `InvalidArgument`。

定向验收：空指针的运行时重载绑定失败且不调用用户代码；静态空指针被模板约束或静态断言拒绝；合法函数指针及无捕获 noexcept guard 的现有行为保持通过。

## API-2：Clock kind 碰撞会改变调度语义

位置：[computation.cpp](../../src/runtime/computation.cpp)，`TriggerSet::bind` 的 Clock 装配分支，审查时第 269–290 行；[event_processor.cpp](../../src/runtime/event_processor.cpp)，周期策略校验，审查时第 612–621 行。

当前实现校验 policy ID 唯一，并把新 Clock kind 与 `input_relations` 的关系名称比较。它没有比较其他策略的 `event_kind`，也没有比较 source 声明真实采用的 kind。source kind 可以与关系名称不同，因此比较关系名不能完成规范要求的 source/其他 policy kind 冲突检测。

独立探针配置两个 Clock：`first_policy` 每 1 秒、`second_policy` 每 10 秒，均使用 `event_kind = "tick"`。绑定成功；真实 `EventProcessor` 在处理首条 source 后推进至约第 1 秒，两个规则均产生 occurrence：

```text
DUPLICATE_CLOCK_KIND_BIND=1
CLOCK_OCCURRENCES_BEFORE_SECOND_DUE=2
EARLY_CLOCK_RULE=first
EARLY_CLOCK_RULE=second
SOURCE_ALIAS_CLOCK_KIND_BIND=1
```

最后一行对应独立场景：Clock kind 为 `tick`，source Trigger kind 同样为 `tick`，payload relation 名称为 `input`。它也能绑定。源事件可意外命中 Clock 规则，Clock 事件也可意外进入需要不同 payload 的 source 规则。

规范第 4 节明确要求自动 kind 稳定并检查 source/其他 policy 碰撞；显式兼容 kind 也不能使不同 schedule 相互触发。建议绑定器保留各声明的输入域，统一检查新旧周期策略与 source 声明，错误信息包含冲突的 rule/policy 和 kind。

定向验收：相同 Clock kind、source 别名 kind 碰撞在装配发布前失败；两个不同 kind 的 Clock 在各自时点触发；与已有策略的冲突也失败。

## API-3：typed key_by 的分区定义可与 owner 不一致

位置：[trigger.cpp](../../src/runtime/trigger.cpp)，`bind_trigger` 的事件元数据与 `validate_scope`；[computation.cpp](../../src/runtime/computation.cpp)，`validate_session` 和 `TriggerSet::bind`；[event_processor.cpp](../../src/runtime/event_processor.cpp)，owner 读取与 source 输入分区形状检查。

`bind_trigger` 收集 payload relation，并验证同一个规则内部的事件/读取关系形状；但返回的 `BoundTrigger` 不保留这些事件关系供模块验证。`validate_session` 只检查规则 reads 与 `capture_current` reads；底层 owner 同样只知道已转换的规则及 reads。没有 read 的 source/Pattern 因此缺少 source partition 与 owner partition 的比较。

独立探针令 `definition.input_relations` 指向 `PARTITION BY(ticker STRING)`，另建 `PARTITION BY(number INT64)` 的 payload relation。普通 source Trigger 和下列 typed 声明都能绑定到该 String-key owner：

```cpp
auto source = EventRef::source<NumberEvent>({
    .kind = "other", .payload_relation = other}).key_by(&NumberEvent::number);
auto trigger = Trigger::pattern("typed_other", source,
    Pattern::begin<NumberEvent>("first").within(1s));
```

```text
DIFFERENT_SOURCE_PARTITION_BIND=1
DIFFERENT_TYPED_PARTITION_BIND=1
```

`key_by` 不负责路由、也不自动填写 `ProcessorEvent.scope`，这一文档边界是合理的；但是声明的 key 类型仍必须与该 owner 接受的 scope 一致。否则 `key_by(number)` 已通过检查，而 owner 实际只接受 String scope，与启动绑定校验业务 scope 的承诺不符。

定向验收：模块绑定时比较所有有 payload relation 的事件声明与 owner 输入分区形状，并在有 Session 时保持一致；错误发生在私有装配发布前。普通 source、typed Pattern 以及没有显式 MV reads 的情况均应覆盖。无需为此实现 shuffle 或自动输入构造。

## 已核实的实现与证据边界

- 强类型链路实际存在：`TypedEventRef`、`TypedPattern`、匹配投影、`Function<Fn>`、`CapturedTrigger` 和 `TriggerSet::bind` 已串联到既有 Catalog/Request。静态函数适配限定 noexcept `StatusOr<Output>(const Input&)`，输入/输出使用显式 Schema，capture 到函数的类型连接由模板约束检查；未发现通过该入口加载动态闭包的路径。
- 字段适配按名称绑定；实际 Row 的长度、类型、NULL、非有限数和 UTF-8 均有校验，包括 typed struct 未投影的附加 payload 字段。表达式先校验当前 payload，再执行短路条件，避免 `false && ...` 隐藏损坏输入。`TypedRowBinding` 的解码不重新按字符串绑定字段。
- `capture_match` 按显式 label/参数顺序和 required/optional/repeated cardinality 提取已匹配事件。signature 包含 label、输入/输出 Schema 和实现身份。`capture_current` 在 admission 的 RoundContext 中读取同 key、unique relation；缺行返回未就绪。实现将函数放入执行器，不在 builder 内执行模型。
- 模块使用 `Catalog::from_snapshot` 与 `FunctionRegistry::clone`；克隆保留 Registry 容量计数及不可变现有条目，后续注册在私有 map 上进行。相同函数 ID/version 复用需满足实现身份、纯函数标志、单 input alias 和输入输出字段一致；冲突返回 Status。没有发现失败后原 Catalog/Registry 被部分发布的路径。
- 普通 Trigger、Clock 和 Pattern 最终仍由 FSM/E-NFA 执行，definition identity 写入既有恢复检查路径；没有用函数地址或 `typeid` 替代实现版本，也没有新增业务状态图。
- 已阅读的新增单测覆盖字段重排、错误 payload、optional/repeated label、callback Status、函数 Simple/ThreadPool 共享适配、声明原子性、同函数复用、UNKNOWN 后不重复匹配投影及 snapshot 恢复。`computation_test` 的 match 参数恢复测试和 `typed_pattern_demo` 的 A/B/C/D admission barrier 是不同证据，不能互相替代。
- 已逐段比对 README、实现导读与 typed demo：六字段 News、Schema、Pattern、Session、gate、两种 capture 和 40.6/94.5 结果与源码一致。它明确区分构建/绑定/执行，说明 key_by 不路由、候选会话必须预先安装、process(C) 会先尝试旧 pending admission；未发现把这些步骤写成纯 builder 自动完成。
- CMake 的新增库与测试使用项目现有 Clang/C++20 配置；非 Release 开启三个 sanitizer 且诊断失败；GoogleTest 只在 `BUILD_TESTING` 分支的测试链接中出现。审查时看到格式化仍在主线程门禁中进行，本报告未把格式快照作为独立功能缺陷。

## 验证记录与剩余缺口

本 reviewer 自行执行的验证限于两个独立 CMake 探针，链接当前项目已经构建的 sanitizer 库；没有把父线程报告的测试通过当成自己新跑的测试。临时工程与原始探针在 `/tmp/eventflow-trigger-api-review.ofv7jl/`，构建使用 `-DCMAKE_CXX_COMPILER=clang++`、C++20、`-fsanitize=address,undefined,leak`、`-fno-sanitize-recover=all`。

`null_guard`：构建成功，执行出现上述 ASan 非法调用，退出 1。`module_probe`：构建成功，最终执行退出 0，输出上述 Clock 提前触发及跨分区绑定结果，无 sanitizer 诊断。运行 module probe 需把 `cpp3rdlib/lib` 加入 `LD_LIBRARY_PATH` 以加载现有 RocksDB 传递依赖；未下载或添加第三方实现。

本轮仍需实施者记录三个发现的修复/不采纳理由及定向测试证据，并补齐当前源码的全量 sanitizer、Release、格式与示例门禁。特别建议在模块层增加 Registry 同 ID/version 异实现身份及异 Schema 冲突回归；代码路径已检查，但本 reviewer 没有为该分支另造运行探针。

后续证据与处置由实施者追加在本报告，保留初审事实。无需因为修复上述发现自动再开启一轮独立审查。


## 实施者整改记录（同一轮审查）

四项去重发现已完成修复并有定向回归，不另开fresh review：

- 空typed guard：运行时指针为空时保留InvalidArgument，NTTP空指针编译拒绝；9项typed测试通过，见/tmp/eventflow-null-guard-green.log与/tmp/eventflow-static-null-guard-after.log。
- 无关Catalog分区越界：预算推导先检查owner scope，只为兼容分区关系生成envelope；依赖查找与索引有显式校验。无关复合/整数分区表及其级联MV与真实请求共存；新2项、原请求96项、CEP owner100项三san回归通过，见/tmp/eventflow-relation-envelope-green.log、-event_request.log、-event_processor_cep.log。
- Clock kind冲突：模块绑定统一收集已有/新Clock kind以及源表名称和声明的source kind，拒绝互相冲突，且与声明顺序无关；合法1秒与10秒规则仍独立调度。
- Source/owner分区差异：BoundTrigger保留事件关系信息，模块对全部typed/非typed源、无reads/无request的情况也与owner分区形状校验；request另与Session校验。

最后两项新增回归先RED后GREEN，computation_test共9项通过930ms，见/tmp/eventflow-binding-review-red.log、/tmp/eventflow-binding-review-green.log。最终代码格式化后正在执行Bear全量Clang Debug和Release构建，随后执行全量回归及七个示例；最终门禁另记，不能用这里的定向结果代签全部交付。


最终闭环：本轮55个测试目标完成验证（53个初跑通过，两个修正后的完整目标复跑通过），七个Release与解压包示例通过；所有质量门禁通过。源码/产物身份与原始失败、定向修复、最终结果的范围见[重构验证](../verification/trigger-refactor-validation.md)。原独立审查发现保留，本记录完成实施者处置，不构成另一轮fresh review。
