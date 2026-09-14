# 运行时模块

本目录继承上层 `AGENTS.md`；修改前先读 [规格索引](../../specs/INDEX.md)及相关合同。
当前 Trigger 重构范围以 [当前状态与交接](../../specs/trigger-api-ergonomics.md#10-当前状态与交接)为准。
强类型输入、静态 `Function`、显式 `SessionRef`、两种 capture 和完整 Pattern 请求链路已纳入当前实施。
不要从规格前半部分的历史提案描述推导这些能力仍被延期；公开 API 以当前头文件为准。

## 职责与入口

- `computation_runtime.cpp`：可注入 BoundComputation 的通用 shard 运行时，拥有 EventProcessor/执行器，保留 UNKNOWN 与尚未确认的 completion；不包含示例业务定义。
- `engine.cpp` / `engine_internal.hpp`：唯一 shard owner、staging、增量关系、提交确认与查询缓存。
- `event_processor.cpp`：源去重、TTL、候选、Clock、FSM/CEP、请求接入及生命周期恢复。
- `request_runtime.cpp` / `request_machine.cpp`：固定输入受理、任务 outbox、attempt fencing、终态与 Latest。
- `function_executor.cpp`：`FunctionRegistry` 和 Simple/ThreadPool 执行契约；worker 不拥有 Engine 或 KV 写权限。
- `fsm.cpp` / `cep.cpp` / `time.cpp`：单活跃状态 FSM、有界多候选 E-NFA、processing-time 调度。
- `trigger*.cpp` / `typed_schema.cpp` / `computation.cpp`：声明、字段绑定、typed Pattern、capture 与整套计算定义装配。
- `observation_program.cpp` / `observation_credit.cpp`：同轮观察程序及预算基础；通用 host 不因这些文件存在而完成。
- `exact_sum.cpp`：可撤回聚合的确定性数值状态，不能替换为依赖更新顺序的浮点累加。
- 公开接口位于 [include/eventflow](../../include/eventflow/)；`TriggerSet::bind` 返回 `BoundComputation`。

## 必须保持的契约

- 同一 shard 同时只有一个未结束 round；关系、生命周期、源进度及对应输出通过同一 owner 原子提交。
- `UNKNOWN` 保留原 commit 身份、完整字节和待发布状态；只确认或重试原批，不重新求值、capture 或开放缓存查询。
- DAG 在本轮全部依赖稳定后传播最终 old/new；guard 读取输入已应用后的 staged MV。
- 每次 dispatch 固定 `now_us`；到期先于同刻数据或 completion，Clock 不隐式创建或续期 candidate。
- FSM 单等待与 CEP 重叠匹配不可互换；原始输入域、typed 子流、Strict 与 OfKind 的事件消费规则必须显式保留。
- `capture_match` 保存匹配参数；`capture_current` 在实际受理的已确认 revision R 读取当前视图，受理批为 R+1。
- occurrence 消费、固定输入、request 状态和 accepted-once 同批；NotReady 不消费 once，受理后失败不归还 once。
- Latest 按 candidate epoch 和 admission sequence 选择符合成功条件的结果，不按完成顺序覆盖。
- `TriggerSet::bind` 使用私有 Catalog/Registry 副本；失败不发布部分函数、关系、计划或 binding。
- 函数、规则、Schema 和 `definition_identity` 参与恢复校验；不能用地址或 `typeid` 名称充当稳定实现身份。
- 预算覆盖路径、历史、intent、未来 completion/timeout、整个 lifecycle 编码及有效 store 限额；超限不遗留半轮状态。
- 查询和状态缓存受节点预算及 owner/epoch/confirmed revision 约束；checkpoint 必须协调源位置、Catalog 与运行时状态。

## 定向验证

在仓库根目录，使用已由 Clang 配置并构建的 Debug 测试目录执行；这些是验证入口，不是本文件的通过记录。

```sh
ctest --test-dir build --output-on-failure -R '^(trigger.*|typed_.*|static_function|computation|relation_envelope_scope)_test$'
ctest --test-dir build --output-on-failure -R '^(engine.*|event_processor.*|event_request|request_.*|function_executor)_test$'
ctest --test-dir build --output-on-failure -R '^(exact_sum|time|fsm|cep|observation_.*)_test$'
ctest --test-dir build --output-on-failure -R '^(compute_service|coordinator_service|core_cluster)_test$'
bash examples/streaming_database/run.sh build
```

按变化选择相关组；关键回归包括延迟受理 A/B/C、UNKNOWN 后匹配投影不重跑、snapshot 重开、预算满时释放终态。
Simple/ThreadPool 或多种 KV 的共享契约必须保留；当前重构不能引用旧核心测试总数作为新代码证据。

## 规格导航

- [运行时合同](../../specs/runtime-contracts.md)、[事件生命周期](../../specs/event-lifecycle.md)、[CEP](../../specs/cep-enfa.md)。
- [请求与函数](../../specs/request-functions.md)、[事件请求接入](../../specs/event-request-integration.md)、[Latest](../../specs/latest-selector.md)。
- [数据库主动消费](../../specs/streaming-database-example.md)。
- [同轮观察](../../specs/change-observation.md)、[Trigger API](../../specs/trigger-api-ergonomics.md)。
