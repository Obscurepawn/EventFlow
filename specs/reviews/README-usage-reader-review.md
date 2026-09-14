# README 使用链路与配图独立审查

日期：2026-09-15。Reviewer：`readme_reader_review`，以 fresh context 独立检查本次授权、工作树差异、公开接口、实现与验证日志。本报告属于本次同一轮并行审查；未启动额外 reviewer，也未修改 README、源码或图片。

结论：本次文档与图片交付在检查范围内通过；发现的一处轻微术语问题已定向修正，没有遗留阻塞项。此结论不重新宣布产品里程碑或全量质量门禁通过。

## 范围与需求对应

原始需求是补充当前 MV 定义、声明式 Trigger 定义及触发后的 SQL SELECT 审计查询，并给出主要代码和注释。追加需求是删除架构图顶部指定大标题。检查范围为[本次规格](../readme-usage-examples.md)、[根 README](../../README.md)、[示例导航](../../examples/streaming_database/README.md)和[架构 PNG](../../docs/image/eventflow_streaming_architecture.png)。保留用户同期文字修改与删减。

| 需求或挑战 | 当前证据与判断 |
| --- | --- |
| MV 定义及数据含义 | README 三条 DDL 与 [definition.cpp](../../examples/streaming_database/definition.cpp) 的表和两级 MV 归一化空白后完全一致。`PRIMARY KEY (ticker, trade_id)` 保留成交身份，`PARTITION BY (ticker)` 与 `GROUP BY ticker` 对应业务 key。两条不同 `trade_id`、成交量 70 和 50 得到累计成交量 120、成交笔数 2、两倍累计成交量 240；正文已明确“成交笔数”。 |
| 主要代码和省略上下文 | README 明示前三段属于返回 `StatusOr<BoundComputation>` 的 `bind_definition(...) noexcept`，解释 `session`、candidate policy、`definition`、类型、SchemaTraits、capture 与函数的来源，并提供真实源码入口。正文未将三个工厂内部片段宣称为独立可运行程序。SDK helper 有自己的 include、命名空间及参数说明。 |
| 声明式 Trigger 的实际语义 | `on_event(...).when(...)` 读取更新后的 MV，正文正确区分条件满足和阈值穿越。`.in_session(...)` 只引用显式配置会话；`.capture_current(...)` 在受理时读取并固定输入；`.once_per_candidate` 在受理时消费，计算失败不归还。与 [trigger.hpp](../../include/eventflow/trigger.hpp)、[computation.cpp](../../src/runtime/computation.cpp)、[event_processor.cpp](../../src/runtime/event_processor.cpp) 和 [request_runtime.cpp](../../src/runtime/request_runtime.cpp) 对应。 |
| 等待和执行期限 | `freeze_intent` 将待受理上限取为会话截止、binding 截止和评估时间加 `pending_for` 的最小值；README 使用“可在 pending_for 内等待”，对本例成立，没有保证等待满一秒。请求 manifest 的截止为实际受理时间加 lifetime，正文区分两者。Clock 已说明已观察 key、活动会话、过期时隙合并和无完成周期保证。未要求扩写用户不需要的额外边界段落。 |
| 自动审计、触发与结果界限 | `TriggerSet::bind` 自动安装受保护关系。`freeze_intent` 拒绝未发出的情况后，`write_audit` 将 occurrence 写入原轮；`request_id` 在受理前已确定。README 正确说明触发不代表受理或成功，Wait/跳过记录可保留，`read_request` 可能 NotFound，outbox 消费不删除审计。 |
| 查询作用域和列名 | 四条审计 SELECT 的投影字段均存在于 [trigger_audit.cpp](../../src/runtime/trigger_audit.cpp)；业务表 `ticker` 与审计 `scope_key_0` 已区分。`bound.explain()["trigger_audit"]["scope_columns"]` 与生成元数据一致。SDK 强制非空 scope，Engine 将显式 scope 用于分区扫描，且拒绝与 WHERE 推断的 key 冲突，因此 helper 省略 WHERE 合法。复合 key 的审计列说明成立；本 helper 和 `read_request` 示例使用单个字符串 ticker。 |
| 默认保留和预算 | [trigger_audit.hpp](../../include/eventflow/trigger_audit.hpp) 默认 128 行、256 KiB；实现按 `(evaluated_at_us, occurrence_id)` 淘汰旧记录，字节计数包含 KV key 与编码行。README 没有把查询预算当作有序 LIMIT 或分页，也明确查询顺序不保证。UInt64 十进制字符串、时间及 offset 的 Int64 类型均与 schema 一致。 |
| 使用完整链路和能力声明 | 读者可从表/MV、Trigger、统一 bind、启动消息服务和集群、生产消息一路到 SDK SELECT。静态编译与部署边界明确，未将函数声明扩大为动态 UDF/UDTF/UDAF 或上传闭包能力。示例新增导航明确验证器仍以特征和评分断言为主。 |

## 原始发现与处置

| 编号 | 等级 | 原始发现 | 当前处置与复验 |
| --- | --- | --- | --- |
| R1 | P3 | 初审 README 将 `EF_ASSIGN_OR_RETURN` 失败行为写为“返回错误码(StatusOr<T>)”，混合了错误状态与工厂结果类型。宏实际执行 `.status()` 返回。 | 已向 root 报告。当前 README 改为“提前返回错误状态 Status（由工厂的 StatusOr<T> 承载）”，与 [status.hpp](../../include/eventflow/status.hpp) 第 147–151 行一致，复验通过。 |

期限上限曾作为可选澄清点提出；核对实现和本例配置后，现有表述成立，不列为未修复问题，也不要求恢复用户已删减的边界文字。

## 配图独立检查

使用 `view_image` 分别查看原图 `/tmp/eventflow-readme-architecture-before.png` 和最终仓库 PNG，直接比较可见内容：

- 顶部“数据库主动Poll消息系统中的消息”大标题已删除，未替换为其他标题。
- Producer、Message Delivery、静态计算定义、SQL Client、Coordinator/Meta、SourcePoller/IMessageSource、两个 Compute、共享 KV 和三个 shard 标签保留。
- publish、poll/fetch、返回消息、持久确认后的 ACK、按 key 路由、SQL query、控制状态和存储连线方向保留；底部四类颜色/线型图例及异步执行说明保留，未观察到语义改变。
- 根 README 仍使用 `<p align="center">` 和 `width="720"`，图片链接仍指向同一仓库资源。

独立读取 PNG 头确认原图为 1448 × 1086，最终图为 1447 × 1087；存在一像素画布差异，与规格记录一致。本次通过的是标题移除及其余图示语义、阅读布局的目视核对，不宣称未编辑区域逐像素相同。最终 PNG SHA-256：`3616a0d78af18700d39582b9a20b59fd6597bd6a10efebcc93d464c3ab4e4ed5`。

## 验证证据及未验证项

Reviewer 本轮实际执行了只读源码/差异核对、四段 C++ 与编译源的逐字包含检查、恰好三条 DDL 的归一化比较、C++ 围栏 ASCII、Markdown 围栏配对、图片显示 HTML 和 `git diff --check` 检查，均通过。检查编译源为 `/tmp/eventflow-readme-usage-snippets-0ksivn16/snippets.cpp`；未将补齐上下文的事实隐藏为四段独立程序。

读取以下本轮日志，并对照临时 CMakeLists、当前 Debug flags 和 CTest 属性核对范围；未由 reviewer 重复运行构建或测试：

- `/tmp/eventflow-readme-usage-build.log`：三个流式示例目标及两组审计测试目标构建成功。当前构建使用 Clang、C++20、Debug 和 AddressSanitizer/UndefinedBehaviorSanitizer/LeakSanitizer，CTest 配置失败诊断。
- `/tmp/eventflow-readme-usage-snippets.log`：四段文档 C++ 配上真实 Schema/函数/session 上下文后，临时 OBJECT 目标以 Clang 18.1.3 和 `-Wall -Wextra -Werror` 编译成功。OBJECT 编译没有执行新 helper 的网络请求。
- `/tmp/eventflow-readme-usage-ctest.log`：`trigger_audit_test`、`trigger_audit_request_test`、`streaming_database_example` 为 3/3 通过，23.99 秒。前两组源码覆盖触发/等待/跳过/去重/保留/UNKNOWN/恢复等合同；真实五进程示例验证 MV、阈值固定输入、周期成功结果及正常 checkpoint。

未执行全量产品回归、Release、Docker、真实 Kafka、额外故障注入或新 SDK helper 的现场联网执行；五进程 `verify.cpp` 未新增 trigger_audit SELECT 断言，不能将其称为新 helper 的端到端运行证据。网页浏览器实际 Markdown 渲染未检查；图片已目视检查，居中与宽度由 HTML 静态检查支持。规格收尾和全轮报告汇总由 root 完成。
