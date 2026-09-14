# 模块指南全局合同审查

Reviewer: guidance_contract_review
Date: 2026-09-14
Result: 通过本次文档合同审查；无需要整改的发现

## 范围与独立性

本轮原始授权为 `/init 请创建各个模块的AGENTS.md`。审查对象为 23 份新增目录指南、根指南的导航与范围说明，以及 `specs/INDEX.md`、`specs/module-agent-guidance.md` 的维护边界。结论只覆盖该文档任务，不重新验收产品或 Trigger 重构。

本审查由独立 fresh-context reviewer 执行，未参与指南编写，未使用其他 reviewer 的结论；与另一位 reviewer 属于同一轮并行全局审查，没有创建追加审查轮次。完整读取根 `AGENTS.md`、适用的规格约定和全部新增指南，并自行核对当前源码。工作树存在其他任务的并行变更，本报告不把全树未跟踪状态或外部代码变化归因于本次初始化。

完整阅读范围：

- `src/AGENTS.md`；`src/core/`、`src/storage/`、`src/sql/`、`src/runtime/`、`src/ingest/`、`src/service/`、`src/client/` 的指南；`src/sql/generated/AGENTS.md`。
- `include/eventflow/AGENTS.md`、`tests/AGENTS.md`。
- `examples/AGENTS.md`；`feature_computation_demo`、`news_feature_demo`、`typed_pattern_demo`、`sql_query_demo`、`condition_trigger_demo`、`clock_trigger_demo`、`checkpoint_recovery_demo` 的指南。
- `cmake/AGENTS.md`、`cpp3rdlib/AGENTS.md`、`docs/AGENTS.md`、`specs/AGENTS.md`。

## 发现与结论

没有发现需要按严重级别登记并整改的问题。未发现与原授权或根规则冲突的模块合同、虚构的公开入口、错误扩大当前实施范围，或将最新已授权接口重新列为延期的情况。

关键核对依据如下。这些是本次阅读源码得到的合同一致性证据，不是运行测试的通过声明。

| 核对项 | 指南与当前依据 | 结论 |
| --- | --- | --- |
| 最新 Trigger 范围 | `specs/trigger-api-ergonomics.md` 第 454 行起的最新授权；`include/eventflow/computation.hpp`；`include/eventflow/trigger.hpp`、`typed_trigger.hpp` | typed Pattern、静态 Function、显式 session 和两种 capture 已属于当前实施；指南保留动态扩展、通用观察 host 和 Process Function 的独立边界，没有以历史草案覆盖新授权 |
| 单 Catalog 与原子绑定 | `src/runtime/computation.cpp` 第 308 行起的 `TriggerSet::bind`：重建私有 Catalog、克隆 Registry、校验 plans 和 EventProcessor definition，最后返回 `BoundComputation` | runtime、SQL 和公共头文件指南的原子装配描述有实现依据 |
| 固定输入与示例前提 | `examples/typed_pattern_demo/main.cpp` 中显式 candidate、`release_gate`、两种 capture 和结果断言；`examples/news_feature_demo/news_feature_demo.cpp` 的单等待与 Clock 声明 | A/B 匹配后等 C 放行、40.6/94.5、D 更新后固定结果，以及前置“已受理”的说明与实际代码相符；未将 Pattern 替换为单等待语义 |
| 公共示例的生产依赖 | `CMakeLists.txt` 第 110-116 行的 `ef_feature_computation_demo` 及两个服务的链接；`DemoConfig` 默认 2s/30s/5s 和生产声明 | 指南正确指出此目录同时属于生产路径，验证范围包含服务与集群调用方 |
| 存储与 UNKNOWN | `include/eventflow/state_store.hpp`、`cached_store.hpp`；`src/storage/state_store.cpp` 的规范 commit 身份、同批 receipt、同步 WAL、未确认写入阻挡与不可写 snapshot | 存储指南没有放松确认前可见性或把 KV snapshot 等同于一致 checkpoint |
| 消息源身份与 ACK | `include/eventflow/message_source.hpp` 的 durable identity、实例 token、连续已交付前缀、UnknownCommit、seek 和预算合同；`tests/message_source_test.cpp` 的共享 fixture | 指南正确区分 broker-neutral 接口、MemMock 与 librdkafka protocol mock；不声称真实 Kafka Compose 已验证 |
| SDK、路由与 checkpoint | `include/eventflow/cluster_client.hpp`；`src/service/coordinator_service.cpp` 的 pending、fanout/quiesce、source cut、control revision 及 restore 校验 | Meta-only 接入、input_committed 与 operation_status 分离、业务 S/control S/KV S+1、静态部署及停机恢复边界一致 |
| 类型与 SQL | `src/core/types.cpp` 的 key 类型和精确整数解析；`src/sql/expression.cpp`、`binder.cpp` 的类型、短路、分区和两侧唯一性约束 | 指南给出的类型/身份/partition 合同有当前实现依据，未扩展为通用 SQL 或跨分区能力 |
| 真实进程证据层级 | `tests/store_contract_test.cpp` 的四适配器参数化；`tests/core_cluster_test.cpp` 的两个 Compute、Memory/Rocks 及全进程恢复；根 CMake 测试注册 | 指南区分进程内 HTTP、真实 KV 子进程与完整集群，不用协议 mock 代替端到端部署 |
| 生成器与依赖 | lexer/parser 文件头均为 ANTLR 4.10；`cpp3rdlib/README.md` 第 79 行记录匹配生成器；根 CMake 使用既有生成文件 | 再生成说明与 bundled runtime 匹配，保留项目外工具和无构建下载边界 |

## 实际验证

本次执行了文件阅读、`rg` 源码检索，以及两个只读 Python 静态检查，退出结果均已检查：

- 目录覆盖、Markdown 本地链接及标题 anchor、代码围栏检查：根指南加 23 份目录指南，共 24 份；146 个本地链接、40 个围栏标记；错误 0。`src/` 和 `examples/` 当前各 7 个一级模块均有指南。
- 将指南中的 CTest `-R` 正则与当前 `CMakeLists.txt` 注册的目标对照：19 个筛选表达式，空匹配 0。该检查只证明入口能选择当前注册的目标，不证明目标已经构建或测试成功。
- 对照当前公共头文件、必要实现、测试 fixture、示例断言及 CMake 接线，确认上表中的合同边界。没有读取私有 PDF，也没有访问或修改 `.reference-repos/`。

本 reviewer 仅新增本报告；没有修改作者指南、作者维护规格、生产/测试/示例代码、构建配置或第三方资产。

## 未验证项与交接

没有运行 C++ 编译、CTest、sanitizer、实际示例进程、Kafka broker、checkpoint 恢复探针、ANTLR 再生成或依赖 hash 全量检查。本次纯文档维护不产生这些产品验证结果，也不沿用旧 45 目标的核心日志证明后续代码。

不存在需要作者整改后再复验的本报告发现。作者仍需汇总本轮另一位独立 reviewer 的结果、记录其发现处置，并同步维护规格的最终状态。并行任务在本审查后继续修改的 API、示例或规格，需要由其实施者同步受影响指南；本报告不预先验证未来状态。
