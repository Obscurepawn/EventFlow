# 声明式 Trigger API 重构

Status: complete（本轮声明式 Trigger/typed Pattern 重构及验证完成）
Owner: root
Updated: 2026-09-14

用户已批准按更新后的方案重构生产代码、测试和示例。当前范围包括普通 Trigger、有界强类型 Pattern、静态用户函数、capture_match/capture_current、显式 Session 与完整 Pattern 到 Request 链路。本文是当前实施合同，已移除先前“仅做设计整合”的过时状态；当前公开 API 与验证入口见文末。历史核心版本的通过记录不能替代本轮证据。

## 1. 目标与授权范围

本轮贯通 **强类型输入 → 声明式 Pattern → 匹配输入提取 → 静态用户函数 → 持久化 Request 结果**。用户首先表达业务匹配和计算，不必分别手工装配 FSM 迁移、两份 reads、函数签名、capture 关系和字符串 binding。

序列匹配以 `Pattern` 为主入口；`Trigger` 继续表达事件、条件、Clock 和单次等待。简单等待保留单活跃状态 FSM，多候选匹配使用有界 E-NFA，两者不能根据表达式长短自动互换。它们共享字段、函数、会话和请求声明，最终绑定到同一 Catalog 与现有 owner。完整 CEP 到函数的链路属于本轮验收主线，普通 Trigger 同时保留并迁移既有业务语义。

“编译期固化”首先指 C++ 构建时生成用户函数机器码并检查静态类型；Catalog/SQL Schema、scope 与预算在启动绑定时验证。首版不要求整个作业成为 `constexpr`，也不把启动绑定计算定义称为 C++ 编译期保证。底层 `ProcessorRule`、`ProcessorCepRule`、`RequestPlanDefinition` 和 `RequestTriggerBinding` 保留为高级入口。

静态用户计算函数已基于现有 compiled-in Registry 纳入进程内 API。动态上传闭包、`.so` 插件分发、完整 SQL UDF/UDTF/UDAF 算子族、Process Function、完整 Flink 方言、事件时间 CEP、跨 shard 关联、通用 CDC 和新持久观察队列仍不在本轮范围。本文不改变已完成核心交付的验收结论，也不以静态计算回调宣称完整用户扩展已经交付。

## 2. 原接口成本与重构对应

| 原接口要处理的内容 | 具体成本 | 本轮对应接口 |
| --- | --- | --- |
| `definition.reads` 与 `ProcessorRule::reads` | 同一读依赖分别写名称列表和名称到 RelationId 的映射 | 从字段表达式自动收集；手写 guard 只声明一次 read binding |
| `context.reads.at(...)`、`Row[4]`、`std::get<T>` | 要记住 alias、列序和类型；新闻 payload 与 MV 都有第 4 列但意义不同 | 启动绑定时按 Catalog 解析列名；运行前校验实际 payload，提供强类型访问 |
| `idle/armed`、迁移顺序、失败回退、`$timeout` | 两步业务规则需要自行证明一次消费、复位和到期边界 | 常见等待规则生成确定性的 FSM；允许展开查看 |
| `2'000'000` 与多个 lifetime | 单位不直观；匹配窗口、pending 期限、request 期限容易混淆 | 接受 `std::chrono` 整数 duration，保留三种期限的独立名称 |
| Clock policy、event kind、FSM rule 分开定义 | 多个字符串需要一致，Clock 事件命名还可能碰撞 | `Trigger::clock` 一次生成 policy 和 rule，编译检查 kind 冲突 |
| rule ID、emit、binding ID、plan 名称 | 字符串接错只能到 open 时发现；用户容易把发射等同于计算执行 | `.request(...)` 自动填 rule/emission，plan 仍独立定义 |
| `FunctionSignature`、`FunctionViewDefinition` 与 Row callback | 函数输入输出和版本重复填写，用户函数仍暴露物理行布局 | `Function<Fn>` 单次声明静态函数，按显式 Schema 映射生成 Registry 与 Catalog 信息 |
| CEP match 与 plan capture | 匹配 A/B 后等待受理，读取最新新闻的 plan 可能改用后来的 C | 显式区分 `capture_match` 与 `capture_current`，绑定投影后的函数输入类型 |
| 隐含 candidate 前提 | 即使关闭 once，请求仍依赖活动 candidate；只准备 plan 不足以接入 | `.in_session(session)` 明示业务会话及到期、换代规则 |
| 有界匹配与 owner 预算 | Pattern 很短也可能创建许多路径或超大 occurrence | 高级入口提供受限预设和可检查的完整预算摘要 |

并非需要从零设计 DSL：[fsm.hpp](../include/eventflow/fsm.hpp)已有 `on_event`、`crossing`、`followed_by` 和 guard 组合；[cep.hpp](../include/eventflow/cep.hpp)已有 `CepPattern::event/then/or_else/optional/repeat`。本轮在这些执行器之上补齐声明、Schema 绑定和原子装配能力。

尤其不能直接把新闻示例替换成现有 `followed_by(..., strict_next=true)`：该 helper 成功进入 `accepted`、拒绝进入 `rejected`，需要 reset；新闻示例则回到 `idle`，下一条事件可重新武装。此外该参数拒绝的是下一条指定类型事件，并非所有事件中的下一条。见 [fsm.cpp](../src/runtime/fsm.cpp)与[新闻规则](../examples/news_feature_demo/news_feature_demo.cpp)。

## 3. 字段访问与强类型声明

以下片段使用已存在的公开接口，省略的 Catalog/函数上下文可从[强类型新闻示例](../examples/typed_pattern_demo/main.cpp)和[普通 Trigger 新闻示例](../examples/news_feature_demo/news_feature_demo.cpp)获取。完整 SchemaTraits、会话策略、预算和运行流程均在示例中，不把片段当作独立可执行程序。

```cpp
// Declarations are validated against the Catalog by TriggerSet::bind.
using namespace std::chrono_literals;

const auto news_input = EventRef::source({
    .kind = "news",
    .payload_relation = news});
const auto ready_view = ViewRef(ready);
const auto ready_now = ready_view.column<bool>("ready").is_true();
const auto positive_news = news_input.column<double>("sentiment_score") > 0.0;
```

`ViewRef(ready)`引用同一 Catalog 的 RelationId，不建立另一张关系依赖图。`column<T>`返回带类型期望的不可变表达式声明；`TriggerSet::bind` 在程序启动时检查列名、类型、可空性和 scope，然后绑定物理索引，运行时不再反复按字符串找列。字符串列名存在性不属于 C++ 编译器能够直接证明的条件；统一入口命名为 `bind`，与 C++ 构建期区分，见第 8 节。

`EventRef::source`中的 `payload_relation`只声明 `ProcessorEvent.values`应符合的 visible schema。当前 `values`与 `changes`是两个独立字段；表更新不会自动证明 event payload 正确。便捷层必须生成每个相关 guard 之前的类型/长度检查，不能直接把 arbitrary Row 强转为该关系。将事件与 change 自动绑定属于另外的输入构造 helper。

`is_true()`明确将缺行或 NULL 视为条件不成立；schema 不符、越界或错误类型返回 Status，不能偷偷当成 false。其他 nullable 表达式采用显式 `is_null()`或 `value_or(...)`，避免三值逻辑隐含变化。`&&`与 `||`组成短路表达式节点，保持错误传播顺序。

高级 guard 仍可保留 `StatusOr<bool>(const FsmContext&)` 或 CEP 的 `StatusOr<bool>(const CepContext&)`，但必须单次提交 `{alias, relation}`读声明；绑定器同步生成两份底层 read 声明；普通调用方使用 `ViewRef::column<T>` 或 typed schema 适配。不得推断任意 lambda 捕获了哪些关系或依赖哪些字段。

常用接口接受 `StatusOr<bool>(const News&) noexcept` 一类强类型函数。C++20 首版通过显式 `SchemaTraits<T>` 或等价的字段到成员映射声明列名、类型、可空性和编码，不假定存在可自动反射任意 struct 的机制。事件解码先检查实际 payload，再调用用户函数；涉及 MV 的自定义条件仍须显式提供读依赖。匹配历史和函数输入也使用这一套 Schema/类型适配，避免仅修复 guard 的 `Row[4]` 后又让用户在 capture 中重新处理物理行。

首版表达式只需列、常量、比较、布尔组合和缺失判断。数学聚合继续在 SQL/MV 内完成，不在 Trigger 中重新实现一套 SQL 引擎。

## 4. 简单条件、Clock 与单次等待

普通接口包括 `on_event`、`when`、`clock`、`then_next`、`within`、`emit`、`request`。`then_next`必须明确选择相邻语义；普通 guard 是电平判断，不能暗中解释为 crossing。

```cpp
// The referenced plan and candidate policy already exist in definition.requests.
auto every_news = Trigger::on_event("every_news", news_input);
auto ready_news = Trigger::on_event("ready_news", news_input).when(ready_now);

auto confirmation = Trigger::on_event("positive_news_confirmation", news_input)
    .version(1)
    .when(ready_now)
    .then_next(news_input, NextPolicy::OfKind)
    .when(ready_now && positive_news)
    .within(2s)
    .rearm_on_next_event()
    .emit("confirmed")
    .in_session(SessionRef::candidate(candidate))
    .request({
        .id = "news_confirmation",
        .version = 1,
        .plan = "news_score_and_risk",
        .mode = RequestTriggerMode::Wait,
        .pending_for = 1s});

EF_ASSIGN_OR_RETURN(
    auto bound,
    TriggerSet::bind(snapshot, definition, {confirmation}, runtime));
```

`every_news`和 `ready_news`展示独立备选规则；上例只安装 confirmation。`bind`返回匹配的 Catalog、Registry、计算定义及解释报告，失败不发布一半 rule/binding。该调用发生在 `EventProcessor::open`之前，不对运行中的 owner 增删规则。

上例及后续复用旧 plan 的示意均要求 Registry、plan 和 candidate policy 已定义；实际输入须激活所需 candidate。只注册 plan 并不构成完整接入。`.in_session(news_session)` 显式引用这一业务会话，见第 6.3 节。

`rearm_on_next_event()`明确生成新闻示例的循环：成功、指定类型的确认失败或超时都回到 idle；处理一条数据事件时至多执行一次数据迁移，确认失败的事件不能立即成为新起点。若没有声明完成后的策略，含 `then_next`的 builder 应拒绝编译，避免用户依赖不清楚的默认行为。

`within(2s)`从武装时的实际 processing time 起算，窗口为 `[armed_at, armed_at + 2s)`。相同截止时间的输入先执行到期处理；它不能确认已到期的等待，但仍可能在 idle 中开始新的等待。超时处理和数据迁移是两个不同阶段。

生成的程序应与当前示例保持同样的 `idle/armed`状态、迁移顺序、emit 和 version；不得用多路径 Pattern 自动替换。内部状态名和默认 reset 策略一经参与持久化，就属于版本契约。

```cpp
// A Clock declaration does not create a wall-clock driver.
auto periodic = Trigger::clock("periodic_news_score", {
        .policy_id = "five_second_clock",
        .every = 5s,
        .phase = 0s})
    .version(1)
    .when(ready_now)
    .in_session(SessionRef::candidate(candidate))
    .request({
        .id = "periodic_news",
        .version = 1,
        .plan = "news_score_and_risk",
        .mode = RequestTriggerMode::TryOnceAtFiring,
        .pending_for = 1s});
```

Clock 编译为 `PeriodicPolicy`加匹配该 policy 的 FSM event kind。自动 kind 必须稳定、预留且检查与 source/其他 policy 的碰撞；为兼容已有快照，可显式指定原有 kind，而不是按注册顺序生成新名字。

Clock 保持 per observed scope、不补激活前历史、错过时隙合并并记录 skipped 的现有语义。嵌入式程序仍要驱动 `advance/pump`；builder 不新增定时线程。`.request(...)`必须显式写 mode，教程为 Clock 推荐 TryOnceAtFiring，为普通确认推荐有期限的 Wait。

### 编译到现有结构

| 业务声明 | 绑定产物 | 职责边界 |
| --- | --- | --- |
| `on_event(...).when(...)` | `on_event`生成的 `FsmDefinition`和 `ProcessorRule` | schema binding、表达式与错误定位 |
| `then_next(..., OfKind).within(...).rearm_on_next_event()` | 确定性的 idle/armed 迁移及 timeout | 新 helper 生成表；复用 `FsmProgram` |
| `then_next(..., Strict)` | 指定事件确认优先，其后其他数据事件拒绝迁移 | 明确哪些 host 事件进入流；不能只改一个参数名 |
| `clock(...)` | `PeriodicPolicy`与 `ProcessorRule` | 稳定 event kind 和 policy/rule 接线 |
| `.request(...)` | `RequestTriggerBinding` | 自动填 rule/emission；复用旧 plan 且无输入投影时可生成空 admission factory，`capture_match` 则必须生成有参数的 factory |
| 手写纯 guard | 既有 guard 加声明读映射 | 校验并生成读集；不获得 KV 权限 |

这里的 `Strict`应明确按 EventProcessor 向规则投递的数据事件流判断，包括实际投递的普通 Clock/Expiry kind；`$timeout`仍走专用到期阶段。如果用户只想观察某个 source 子流，需要声明输入域及实现过滤规则，不能在不同场景下悄悄改变 `next`的含义。

## 5. 声明式 CEP 主入口

对于“A1、A2 都可作为开始，之后一个 B 完成两条候选”这类需求，使用独立 `Pattern`入口。公开 API 保留英文 snake_case 命名，不直接照搬 Java 方法大小写。

```cpp
// Multiple starts may coexist for one business scope.
auto pair = Pattern::begin("first", news_input)
    .where(ready_now)
    .next_of_kind("second", news_input)
    .where(ready_now && positive_news)
    .within(2s)
    .after_match(CepSkipStrategy::SkipPastLastEvent);

auto correlated = Trigger::pattern("positive_news_pairs", pair)
    .version(1)
    .limits(CepLimits{
        .max_paths = 64,
        .max_events_per_match = 2})
    .in_session(SessionRef::candidate(candidate))
    .request({
        .id = "news_pair_requests",
        .version = 1,
        .plan = "news_score_and_risk",
        .mode = RequestTriggerMode::Wait,
        .pending_for = 1s});
```

这里只覆盖匹配器局部限制示意；编译还必须检查 owner/outbox/request 的完整预算，不能仅凭 64 条路径断言整套计算定义可受理。Pattern label 用来识别历史捕获，event kind 用来筛选输入，两者分别命名。

| 公开语法 | E-NFA 映射 | 业务含义 |
| --- | --- | --- |
| `begin(label, event).where(p)` | `CepPattern::event(label, kind, guard)` | 开始一条有界候选 |
| `next(label, event)` | `then(..., Strict)` | 当前规则输入流中紧邻的下一条必须通过 |
| `next_of_kind(label, event)` | `then(..., NextOfKind)` | 忽略其他 kind；第一条指定 kind 失败即终止该路径 |
| `followed_by(label, event)` | `then(..., FollowedBy)` | 可跳过不通过的事件，使用后续首次通过者 |
| `followed_by_any(label, event)` | `then(..., FollowedByAny)` | 还保留跳过通过事件的分支，可能产生多组匹配 |
| `within(duration)` | `CepDefinition::within_us` | 必填、正数、processing-time 上界 |
| `optional/or_else/repeat(min,max)` | 现有同名组合器 | 重复上限必须有限，空匹配循环拒绝 |

CEP 请求 binding 的 emission 必须是当前宿主固定的 `"match"`，由 `.request(...)`自动填写。`Trigger::pattern`首版不提供可任意改名的 `.emit(...)`。`emit_cep`目前发布 match，不把 timeout 自动变成可绑定请求的 occurrence；因此暂不承诺 `.on_timeout().request(...)`。

该模式**不与两状态新闻 FSM 等价**：CEP 的一条事件可以结束旧路径，同时成为新路径的起点；确认失败的新闻也可能启动另一条路径。SkipPastLastEvent 只能控制成功匹配后的候选删除，不能普遍补足 FSM 的失败复位语义。需要“一次武装直到确认/失败”的业务应继续选 `Trigger::then_next`。

### 与 Flink 的对应关系

Flink Pattern API 将开始、单事件条件和衔接拆成 `begin/where/next/followedBy/followedByAny`；其中 `next`是严格相邻，`followedBy`跳过不匹配事件，`followedByAny`允许额外匹配。该分层值得采用。官方示例与定义见 [Flink CEP Pattern API](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/#combining-patterns)（2026-09-14 查询）。

EventFlow 的“下一条 news，忽略 quote/trade”必须另叫 `next_of_kind`，不能命名成与 Flink 相同的 `next`。严格性始终相对于送入 matcher 的流；先过滤后的 news 流与未过滤的多类事件流不是同一流。

声明 `news` 子流可以按事件类型筛选，但不能在 matcher 之前过滤掉负面 news，再声称 `.next()` 表示“下一条 news 必须为正面”：负面事件会消失，严格相邻的拒绝条件随之改变。业务条件必须保留在 `.where(...)` 中。

Flink 文档还提供处理时间和事件时间、negative 模式等能力；本轮只映射当前有界 processing-time 子集，不宣称 Flink 兼容。`within`继续按 EventFlow 已有的排他截止合同执行。参见 [Flink CEP 时间](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/#time-in-cep-library)与[当前 CEP 合同](cep-enfa.md)。

<a id="typed-cep-function-pipeline"></a>

### 从匹配到静态用户函数的完整声明

下列片段展示已实现的强类型入口。`news` 是六字段新闻关系，`candidate` 对应模块中已定义的业务会话策略。为避免与可执行示例重复，SchemaTraits 与完整预算见[README](../README.md)和[typed_pattern_demo](../examples/typed_pattern_demo/main.cpp)；绑定仍执行完整校验。两条正面新闻的 CEP 匹配不等价于前面的单等待 FSM。

```cpp
// SchemaTraits, candidate policy and budgets belong to the same computation.
using namespace std::chrono_literals;

struct News {
  std::string ticker;
  std::int64_t timestamp = 0;
  std::string headline;
  std::string text_body;
  double sentiment_score = 0.0;
  double market_price = 0.0;
};

struct ScoreInput {
  double first_sentiment = 0.0;
  double confirm_sentiment = 0.0;
};

struct Score {
  double value = 0.0;
};

StatusOr<bool> is_positive(const News& event) noexcept {
  return event.sentiment_score > 0.0;
}

StatusOr<ScoreInput> make_score_input(const News& first, const News& confirm) noexcept {
  return ScoreInput{
      .first_sentiment = first.sentiment_score,
      .confirm_sentiment = confirm.sentiment_score};
}

StatusOr<Score> calculate_score(const ScoreInput& input) noexcept {
  return Score{
      .value = 0.5 * input.first_sentiment + 0.5 * input.confirm_sentiment};
}

const auto news_events = EventRef::source<News>({
    .kind = "news",
    .payload_relation = news}).key_by(&News::ticker);
const auto news_session = SessionRef::candidate(candidate);

const auto score_function = Function<&calculate_score>{
    .id = "news_score",
    .version = 1,
    .implementation_identity = "news-app-v1"};

auto positive_pair = Pattern::begin<News>("first")
    .where<&is_positive>("news.positive.v1")
    .next("confirm")
    .where<&is_positive>("news.positive.v1")
    .within(2s)
    .after_match(CepSkipStrategy::SkipPastLastEvent);

auto rule = Trigger::pattern("positive_news_pair", news_events, positive_pair)
    .version(1)
    .in_session(news_session)
    .capture_match<&make_score_input>({"first", "confirm"}, "news.pair_input.v1")
    .request(score_function, {
        .mode = RequestTriggerMode::Wait,
        .pending_for = 1s,
        .lifetime = 10s});
```

`key_by` 声明必须与 Catalog 的分区键一致，首版不产生跨 shard shuffle。这个 typed 输入域只向该 Pattern 投递 `news` 数据事件，因此 `.next()` 检查同 key 的下一条新闻；源类型过滤不丢弃 guard 不通过的新闻。内部 deadline 仍由 owner 的时间推进处理。typed 子流过滤与绑定已实现；原始流的 Strict 入口保持自己的语义。声明的 payload 分区形状还须与 owner 和 Session 相容，不能仅验证孤立的 key_by。

`capture_match` 的 label 列表显式对应投影函数的参数顺序，避免框架猜测参数名与 pattern label 的关系。`make_score_input` 是有界纯投影；`calculate_score` 在线程执行器或 Simple 适配器中运行。声明函数时不会执行函数，也不会启动请求。

此链路绑定后应生成一个受控 capture 根关系、相应的 FunctionView 与 RequestPlan，并复用现有 match emission。用户提供的规则/函数稳定身份及语义版本用于生成 binding、plan 和 capture 的确定性身份；不能使用内存地址或注册次序。生成详情应能通过 `explain()` 检查。

## 6. 静态用户函数、输入捕获与请求会话

### 6.1 静态函数声明与单 Catalog 装配

`Function<Fn>` 是静态函数声明，`Fn` 首版支持普通函数或可静态绑定的无捕获 callable。C++ 类型约束检查参数、返回的 `StatusOr<Output>` 和 `noexcept`；`SchemaTraits<Input/Output>` 声明序列化布局，不允许把栈引用、任意裸指针或可变全局状态当成可恢复函数输入。带有算法参数的函数将参数显式固化到计算声明，并纳入实现身份；任意 C++ 函数是否纯、是否有界仍需实现者遵守，类型系统不自动证明这些性质。

一个函数声明统一生成或校验 `FunctionSignature`、typed Row adapter 与 `FunctionViewDefinition`。同一 Registry 的同 ID/version 不允许悄悄覆盖实现；重复引用同一函数复用描述，冲突定义拒绝。参考当前 [FunctionRegistry 接口](../include/eventflow/function_executor.hpp)、[Registry 实现](../src/runtime/function_executor.cpp)与[示例手工装配](../examples/news_feature_demo/news_feature_demo.cpp)。这层 API 已连接静态函数声明与现有执行器，不表示完整动态用户扩展已经交付。

用户源文件使用 Clang 和现有 CMake/Makefile 构建，可形成项目自有静态库后链接进应用/Compute 可执行文件。更新用户模块会重新编译该模块并重新链接；部署节点必须具有相符的实现身份。声明、函数体、Schema 与绑定配方版本共同约束恢复，在途请求不得改用新版实现。首版不要求 JIT、运行时编译或插件上传，也不承诺模板化之后所有运行时派发都会内联。

`.request(score_function, options)` 是单目标计算计划的便捷入口：绑定器根据前一步 capture 输出类型生成所需关系和计划。多目标、共享祖先继续通过同一 Catalog 的函数关系图表达，可以引用显式 plan 名称；不能在 Trigger 中维护第二份可独立修改的 DAG。已有 `.request({.plan = ...})` 保留为复用旧计划的高级入口。

`TriggerSet::bind(snapshot, definition, triggers, runtime)` 在 Catalog/Registry 私有副本中准备全部声明，成功后返回 `BoundComputation`，其字段为 `catalog`、`registry`、`definition`、`explanation`，并提供 `explain()`。失败丢弃整套暂存结果。`EventProcessor::open` 接收成功绑定的整套定义，避免 Registry 已注册一半而 Catalog 创建失败。

### 6.2 显式区分匹配输入与当前视图输入

匹配 A/B 后如果等待受理期间出现 C，原计划的当前读取可能选择 C。两种 capture 使数据来源直接体现在 API 中。[typed_pattern_demo](../examples/typed_pattern_demo/README.md)为两种评分设置显式 `release_gate` 受理屏障：C 先更新 MV，再允许评分受理。仅不调用 pump 不足以保证延迟，因为 process(C) 会先尝试旧 pending 请求。该示例对照 match=40.6、admission-current=94.5，并用 D 验证后续修改不会改写固定输入。

| 公开接口 | 输入来自哪里 | 投影时点与持久化 |
| --- | --- | --- |
| `.capture_match<&make_input>({"first", "confirm"})` | 本次匹配到的具名事件 | 匹配轮做有界纯投影，参数随 intent 持久化；受理时写入 capture 根，不重新选取源事件 |
| `.capture_current<&mapper>(view, "mapper.v1")` | 实际受理时声明的当前同 key 视图 | 在受理准备阶段读取确认版本 R，完成 typed 投影；受理提交后输入固定 |

二者是互斥的输入来源选择，不因 `.request(...)` 是否等待而隐式切换。复用旧 plan 时保留其明确的 capture 配方，不偷偷覆盖；试图同时安装相冲突的 capture 配方应在绑定时拒绝。需要混合匹配字段与当前视图时须另行显式声明投影及两种时间来源，首个闭环不隐含加入混合模式。

`capture_match` 通过现有 `RequestTriggerBinding::make_admission` 从 `TriggerOccurrence::cep_match` 提取 typed 参数，保存到 `RequestAdmission.arguments`，再由生成的 `plan.capture` 在受理时写入受控根关系。这些参数按值编码，计入 argument、intent、outbox 和 snapshot 的数量/字节预算，不保留指向匹配器内存的引用。UNKNOWN 继续确认原批，不重新执行已暂存的投影。路径历史、匹配时的 MV reads 与后续当前视图仍是不同的数据来源。

每个 label 的类型、数量和可选性必须与投影函数参数匹配。单事件 label 可映射为 `const News&`，可选 label 需要显式 optional 参数，重复 label 需要显式集合参数；必需单事件不能默默取重复匹配的第一条。字符串 label 在绑定时验证，静态类型连接在 C++ 构建时检查；框架不分析任意函数体来猜测历史访问。

`capture_current` 仍只读声明的同 scope 关系。必需视图缺行时返回 NotReady，由 Wait/TryOnce 策略决定等待或跳过；当前 `capture_current` 选择一份必需关系；该关系内的可空字段通过 optional 表示，不提供隐式可选关系或混合 capture。错误 Schema、越界或解码失败返回 Status，不伪装成 NotReady。两种模式都不提供触发时全库历史快照。

### 6.3 用业务会话暴露 candidate 前提

当前 [EventProcessor](../src/runtime/event_processor.cpp) 的 `bind_requests` 要求 Registry、plan 和 candidate policy；`freeze_intent` 还要求本 scope 有活动且未到期的 candidate。`once_per_candidate = false` 不会取消这些要求。没有活动 candidate 时，匹配器命中也不等于产生可受理请求。

`.in_session(news_session)` 显式引用业务会话。首版 `SessionRef::candidate(candidate)` 适配已定义的 `CandidateExpiryPolicy`，绑定时校验关系、scope、版本、期限字段与激活配方均一致；一个 processor 当前只有一个 candidate domain，不能让不同 rule 悄悄使用互不相容的会话。完整示例展示所引用 policy 的定义，而不只给一个未定义的 handle。

业务会话的开始、到期、更新和新 epoch 激活由显式策略决定；`SessionRef` 本身不创建、延长或续期会话，Clock 也不隐式激活新 candidate。缺少 session 的新高级声明应在绑定时给出规则级错误；已有底层 plan binding 保留原入口。不能为了隐藏概念而默认每条源消息重建 candidate，这会改变 once、在途请求及多事件关联的语义。更高层 Session builder 的具体语法仍待定。

candidate 到期继续限制 pending intent，并按现有生命周期处理同 candidate 的请求。若未来需要任意 CEP 匹配直接创建无 candidate 请求，必须另定身份、期限、取消、结果可见性与恢复契约；仅增加链式 API 不足以支持这种模式。

### 6.4 受理边界与请求选项

`.request({.plan = ...})`复用已有计划，`.request(function, options)`生成一个明确的单目标计划；两者最终都使用 `RequestPlanDefinition` 的 capture、目标关系、计算预算和 request lifetime。函数依赖从同一 Catalog 反向取得，guard 内不直接调用模型。

```text
Source round:
  input -> stable staged MV -> guard -> FSM/CEP state + occurrence/intent
  -> optional capture_match projection stored as intent arguments
  -> commit together with source position

Later admission:
  use persisted match arguments OR read declared current views at confirmed revision R
  -> capture + manifest + request state + consume occurrence/once
  -> commit R+1
```

Trigger guard 读取当前输入已应用后的稳定 staged MV；它不是本轮前的旧值。occurrence 保留触发时的 reads/event。对 `capture_current` 或现有读取当前关系的 plan，受理延迟后使用的 R 可能包括后来输入；对 `capture_match`，受理时写入 R+1 的根数据必须来自已经保存的匹配参数。二者均记录真实 capture/admission revision，不能把 `within` 或 `.request` 写成“固定触发时全库快照”的保证。

三个期限分别是匹配的 `within`、等待受理的 `pending_for`和计划内的 request lifetime。便捷入口的 `.lifetime = 10s` 绑定到生成计划的请求期限，不能从 `within` 推断；candidate deadline 与可选 `deadline_column`继续对 intent 加绝对上限，不因为 builder 使用 duration 就删除它们。

高级 request 选项应保留 `once_per_candidate`、`required_accepted_binding`、`deadline_column`及自定义纯 `make_admission`。不要提供含义模糊的 `.once()`：FSM 发射一次和 candidate 受理一次是两种不同约束。accepted-once 在成功受理时消耗；capture 未 Ready 不消耗，模型后来失败不归还。

`required_accepted_binding`只要求前置已受理，不代表已成功。Wait 保留有界 pending intent；TryOnceAtFiring 在当次时隙不就绪时跳过，不转换成历史任务积压。UNKNOWN 时仍确认原批，不能重新编译一条规则、重做 capture 或绕过确认缓存继续执行。

## 7. 已实现范围与后置运行时工作

| 能力 | 当前基础 | 本轮状态及边界 |
| --- | --- | --- |
| Source/Clock 条件、单等待、已支持 CEP | FSM/E-NFA、EventProcessor、RequestTriggerBinding 均已存在 | 声明绑定层与 typed guard 已实现，复用 owner |
| 强类型静态用户函数 | compiled-in Registry、FunctionView、Simple/ThreadPool 执行器 | 类型/Schema 适配与单次声明装配已实现；不等于动态插件或完整 UDF 算子族 |
| `capture_match` / `capture_current` | occurrence、admission arguments、受控 RequestCapture 与 plan.capture | 声明式投影、来源选择、类型/数量校验及预算已实现，不提供触发时全库快照 |
| `.request(function, options)` | Catalog 函数图与 RequestPlan | capture 根、函数关系、计划和 binding 的整套私有装配已实现 |
| `.in_session(session)` | 每 processor 一个 candidate policy、scope/epoch/期限校验 | 显式适配现有会话；无 candidate 请求需要另行扩展运行时 |
| 以当前 MV 为条件 | 已支持 declared same-scope unique read | 字段表达式已实现；不能把“读”自动解释成“订阅” |
| 任意 `on_change(view)` / `on_request_terminal(plan)` | Catalog descriptor、ObservationProgram、Engine 私有观察和可信 settlement 基础 | 完整 host 装配后置，不能只新增 builder 方法后宣称可部署 |
| 通用 crossing | 纯 FSM helper 已有 | 对 source/Clock 的采样 crossing 可表达；观察每次关系变化仍依赖通用 host |
| CEP timeout 启动请求 | matcher 返回 timeouts | EventProcessor 目前不将其发布为 request 可绑定 emission，需单独设计和验证 |
| 动态部署或恢复时换规则 | 当前静态计算定义与版本检查 | 本轮不实现热更新、状态迁移或代码分发 |

后续通用观察必须将 `ObservationRuleDefinition`注册在同一 Catalog：source 字段、read schema、sink、plan 的完整 activation targets 和 recipe 身份相互校验，再由 `ObservationProgram::bind`绑定编译实现。表达式依赖可以生成这些声明，但当前 legacy ProcessorRule 不能仅凭生成 descriptor 就被当成已接通 host。

所需 host 工作包括 typed cause 的稳定身份、新旧规则统一 dispatch、防止同一输入重复 step、sink 后继稳定、同批 lifecycle/request 写入，以及 completion/timeout 的未来输出预留、UNKNOWN 和恢复校验。现有 Source 路径预读各规则的 reads，不能直接代替任意观察依赖顺序。

保持 owner 内同步评估、原子写入规则状态与 occurrence。无需建立 CDC 服务、持久 witness FIFO 或第二条 ACK 通道；guard/update/capture/admission factory 不获得直接 KV 写权限，不增加可以任意副作用的 Process Function 回调。详见[同轮观察合同](change-observation.md)。

## 8. C++ 构建、启动绑定与诊断

所有新增项目函数和 lambda 使用 `noexcept`；正常失败返回 `Status/StatusOr<T>`。链式方法只积累有限声明，最终 status-returning factory/bind 完成需要 Catalog 的校验；需要立即失败的操作直接返回 StatusOr，不用异常表达错误。初始化对象使用 C++20 designated fields，代码、注释和诊断采用英文 ASCII。

统一模块入口为 `TriggerSet::bind`；它进行启动绑定，不是 C++ 编译期求值。DDL、Trigger、函数依赖、请求与 capture 统称计算定义；节点地址、资源预算、并发等运行参数才称运行配置。历史底层类型 `ProcessorConfig` 仍混合承载两类内容，公开说明按语义区分。

| 阶段 | 固化内容与可证明的边界 |
| --- | --- |
| C++ 构建时 | 编译用户函数机器码；通过模板/concept 检查参数、返回类型、`noexcept` 和显式 Schema traits 的静态连接；不执行依赖实时输入的计算 |
| 启动绑定时 | 按实际 Catalog 验证列名、类型、scope、label 数量、session、版本和预算；生成规则程序、capture/函数关系与请求计划，随后冻结整套计算定义 |
| 事件运行时 | 校验实际 payload，推进状态与匹配，固定输入并调度已经编译的函数；运行时值、容量和期限仍可能返回 Status |

构建期固化函数实现不保证任意 SQL Schema 在 C++ 编译时已知，也不保证任意 callback 都能进行 `constexpr` 求值。函数 ID/version、implementation identity、声明参数和绑定器语义版本应形成稳定的恢复校验输入；不能用函数地址或 `typeid` 名称替代实现身份。

| 校验时点 | 必须验证 |
| --- | --- |
| C++ 类型检查 | typed guard、匹配投影、用户函数之间的参数和返回类型，`noexcept`、可编码 Schema；不假定能分析任意函数体 |
| 声明绑定 | rule/binding/policy/function ID 与版本、引用计划存在、列名/类型/NULL 策略、scope 一致、读关系 unique_per_partition |
| 表达式与投影绑定 | 事件 kind 与字段布局、历史 label 的存在/类型/数量、布尔短路、读集去重和冲突、不支持的操作明确拒绝 |
| duration 转换 | 正负/零规则、整数到微秒乘法溢出；不可精确表示的亚微秒值拒绝，不静默截断；绝对 deadline 相加仍由运行时检查 |
| 程序编译 | `then_next`策略和完成后行为必填；Pattern 有限 within/repeat、非空循环、有限路径/事件/历史/输出预算 |
| 定义装配 | 重复 rule/emission binding、mode、session/candidate prerequisite、capture 来源、函数签名、plan/status 对应、Clock 事件冲突、sink 权限与反馈边；Catalog/Registry/definition 无部分发布 |
| open/恢复 | 复用现有版本/fingerprint/预算校验；schema 和 callback implementation identity 必须一致 |
| 事件执行 | 校验实际 Row，缺行按已声明语义处理；背压或错误不留下半个规则状态/部分输出 |

`BoundComputation::explain()`返回英文可读的绑定产物：规则版本、观察输入域、解析后的列名/索引、状态/迁移或模式、processing-time 边界、匹配 label 到函数参数、capture_match/capture_current 来源、session policy、函数签名与实现身份、生成关系/plan/binding、三种 lifetime 及预算。它是计算定义绑定结果，不是新的持久业务图。

诊断应定位业务声明，例如 `rule positive_news_confirmation: column sentiment_score requires Float64`。只返回 `FSM declared read set mismatch`不足以帮助使用便捷层的新用户定位源码。

## 9. 兼容路径与验收合同

本轮按以下顺序实现并验证，当前状态与独立审查处置统一记录在第 10 节及[重构验证](verification/trigger-refactor-validation.md)：

1. 在现有重构基础上确定强类型 Schema、Pattern 输入域、函数声明和 session 引用合同，明确 C++ 类型检查与启动绑定边界；保留底层定义入口。
2. 首先贯通一个独立新闻场景：typed source → Pattern → `capture_match` → `Function<Fn>` → 受控 capture/函数关系/Request → 查询结果。完整示例展示 Schema 与 candidate policy，提供 Clang/CMake 的静态链接构建方式。
3. 补齐 `capture_current` 的显式输入来源，以及已有 plan、多目标和共享祖先的绑定；验证延迟受理、函数版本、整个模块绑定失败的原子性与预算。
4. 复用上述基础完善普通事件/Clock/单等待便捷层，并迁移旧新闻 FSM 的等价业务定义。不能以新多候选 Pattern 的结果替代旧单等待语义验证；README 只迁移到实际存在的接口。
5. 通用 `on_change/on_request_terminal`、无 candidate 请求、动态插件等等待对应运行时工作独立完成后再开放。

兼容已有 durable 状态需要 ID/version、内部状态名/迁移顺序、read alias、Clock kind 和 compiler 语义版本稳定。callback body 不能从函数地址或自动 hash 推断；仍需显式 implementation identity/version。没有逐项兼容证据时，以新规则版本/新部署迁移，不能声称新 builder 自动兼容所有旧 snapshot。

本轮至少验证以下行为，而不只比较生成结构：

- 强类型用户函数可随应用静态构建；不兼容参数/返回类型与缺失 `noexcept` 的声明被相应 C++ 约束拒绝，实际 Schema/列/label 错误在绑定时返回 Status。
- 匹配 A/B 后延迟受理并输入 C：`capture_match` 仍使用 A/B，`capture_current` 使用受理时声明的当前视图；已受理输入不受后续数据影响。保存的匹配参数跨 UNKNOWN 确认和 checkpoint 恢复保持不变。
- 缺失或不匹配的 session 在新接口绑定时明确拒绝；关闭 once 仍遵守活动 candidate 与期限限制，Clock 不隐式创建或续期会话。
- 函数身份或 Schema 不匹配不能在恢复时替换旧实现；同一请求的共享祖先只执行一次，固定输入只经 executor 计算，Simple/ThreadPool 按共享契约验证。
- 新模块在函数注册、Catalog 关系定义或 plan 校验失败时无部分发布；匹配参数、生成 capture 根、pending intent 与函数输出纳入现有预算。
- 当前新闻输入轨迹与迁移前业务定义得到相同 emission、request 数、MV/capture 和状态；改变字段顺序后按名称绑定仍正确，错误 schema 返回 Status。
- 原始多类事件域中 `news -> quote -> positive news`可通过 NextOfKind，Strict 拒绝；typed news 子流忽略 quote，但仍保留 guard 不通过的 news。旧 FSM 的 `news -> negative news -> positive news`必须先复位，负面确认事件不能立即重新武装。
- 精确 deadline 先到期；时间溢出拒绝；延迟 Clock 合并、无输入 pump 仍按原语义推进。
- FSM 不产生重叠起点；Pattern 明确覆盖重叠起点、失败事件重新起点、skip 以及多路径预算。
- guard 看到输入后的 MV；延迟 admission 的 capture_revision/admission_revision 为真实 R/R+1；NotReady 不消费 once，受理后失败不归还 once。
- Wait/TryOnce、重复源、UNKNOWN、满 outbox 和恢复保持原合同；相同计算定义下的恢复有具体证据；不把旧手写定义与新绑定身份默认视为字节兼容。

实际实施时使用项目现有 GoogleTest、Clang 和三 sanitizer 门禁；只为新增可观察行为增加测试。本里程碑已由两位 fresh-context reviewer 在同一轮并行审查，四项去重发现均修复并定向复验，不自动开启第二轮。

## 10. 当前状态与交接

最新授权包括强类型输入、静态 Function、两种 capture、显式 Session 和完整 Pattern 到函数链路。生产计算定义、相关测试与示例均已迁移。统一入口为 `TriggerSet::bind`；元数据使用不可变 `definition_identity`，不占业务 register，也不消耗逐候选 register 预算。空 identity 保留原底层 codec 路径，但新生成定义不默认兼容迁移前快照。

| 工作 | Owner | 当前状态 |
| --- | --- | --- |
| 字段表达式、typed schema、非抛异常校验 | implementation team | 已实现并通过本轮对应目标 |
| 普通 Trigger/Clock/单等待、typed/untyped Pattern | implementation team | 已实现并迁移生产与新闻示例 |
| 静态 Function、capture_match/current、Session、原子绑定 | root / implementation team | 已实现；真实 owner、UNKNOWN/快照与输入来源示例已覆盖 |
| README、实现导读和七个示例 | documentation / migration team | 已迁移到实际 API；七个 Release 示例通过 |
| 一轮并行独立审查与四项整改 | API / semantics reviewers、root | 已完成发现、修复和定向验证 |
| Clang Debug/Release、格式/ASCII/noexcept/具名字段、clangd | root / quality audit | 已通过；范围和日志见重构验证 |
| 最终回归收尾与运行包复验 | root | 已完成；55个目标及七个Release/解压示例通过 |

[重构验证](verification/trigger-refactor-validation.md)记录本轮通过范围、首次全量失败与后续修复。首次 55 个 CTest 目标中 53 个通过：`compute_service_test` 命中旧 180 秒目标时限；`feature_computation_demo_test` 的 69 个业务场景通过，一个元数据断言仍检查旧 register 位置。修复只涉及测试断言迁移到 `definition_identity` 并增加跨策略状态解码拒绝，以及目标时限改为 600 秒；业务时间窗口、请求期限、单次 pump 预算未修改。两个完整失败目标的复验尚未全部确认，最终结论保留在验证文档。此前核心版本的 45 个目标日志保持历史状态，不替代本轮证据。

独立报告：[API 审查](reviews/TRIGGER-api-review.md)、[语义审查](reviews/TRIGGER-semantics-review.md)。去重整改为：空 typed guard 非法调用；无关 Catalog 分区造成预算索引越界；Clock/source kind 冲突；源声明分区与 owner/session 不一致。均有 RED/GREEN 或对应负向验证，详见报告与重构验证。

Changed files（按职责汇总）：

- 公开声明：[trigger.hpp](../include/eventflow/trigger.hpp)、[trigger_expression.hpp](../include/eventflow/trigger_expression.hpp)、[trigger_pattern.hpp](../include/eventflow/trigger_pattern.hpp)、[typed_schema.hpp](../include/eventflow/typed_schema.hpp)、[typed_trigger.hpp](../include/eventflow/typed_trigger.hpp)、[static_function.hpp](../include/eventflow/static_function.hpp)、[computation.hpp](../include/eventflow/computation.hpp)。
- 绑定与执行：[trigger.cpp](../src/runtime/trigger.cpp)、[computation.cpp](../src/runtime/computation.cpp)、表达式/Pattern/typed 适配实现，以及 EventProcessor、FSM/CEP definition identity 和 FunctionRegistry 克隆。
- 行为回归：trigger_expression、trigger_pattern、typed_schema、typed_trigger、static_function、trigger、computation、relation_envelope_scope 测试，以及原 owner/request/Compute 与生产示例测试。
- 示例与文档：[typed_pattern_demo](../examples/typed_pattern_demo/README.md)、[news_feature_demo](../examples/news_feature_demo/README.md)、[feature_computation_demo](../examples/feature_computation_demo/README.md)、[README](../README.md)、[实现导读](../docs/implementation_guide.md)；构建目标与格式门禁同步。

本轮最终回归与运行包验证已完成，需求核对和源码/产物身份见[重构验证](verification/trigger-refactor-validation.md)。通用观察 host、动态插件/上传、完整 UDF 家族、高层 Session builder、无 candidate 请求、热迁移与 Kafka Compose 保持后置。


## 本轮完成记录

生产计算定义、相关测试、原news示例和新增typed Pattern示例已迁移；README与实现导读对齐实际接口。独立一轮并行审查发现四项问题，均经失败复现、修复与回归闭环。55个CTest目标完成验证（初跑53个通过，修正两个测试问题后完整复跑两目标通过）；七个Release及七个解压包示例通过；Clang/三sanitizer、格式、ASCII、具名初始化、noexcept与clangd检查通过。详细范围与新产物身份见[本轮验证](verification/trigger-refactor-validation.md)，不沿用旧核心证据。
