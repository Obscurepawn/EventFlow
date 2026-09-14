# Clock 示例独立审查

审查日期：2026-09-14。Reviewer：`clock_examples_review`，本次 Clock 示例 milestone 的 fresh-context 独立 reviewer。本轮只读示例、关联运行时、构建与读者文档，仅新增本报告；没有修改生产或示例源码，没有在共享构建目录构建，也没有自行运行示例二进制。

依据：[根约定](../../AGENTS.md)、[示例约定](../../examples/AGENTS.md)、[Clock 授权范围](../clock-example-clarity.md)、[Trigger 当前合同](../trigger-api-ergonomics.md)。目标是两个名称和职责清楚的示例：引擎内静态声明与持续驱动、SDK 对预编译集群规则循环提交推进请求。未扩大到远程上传 C++、动态 UDF 家族、Kafka 或新运行时功能。

## 结论

发现一个阻断引擎示例执行的 P2 问题、两个 P3 导航问题；实施者已在本轮修复，已核对最终源码或文档。引擎定向 CTest 复验通过的日志已读取。当前审查范围没有剩余的已证实功能缺陷；这不替代持续模式信号探针、Release、完整静态门禁或镜像验证。

| 编号 | 优先级 | 原发现 | 当前处置 |
| --- | --- | --- | --- |
| CLOCK-1 | P2 | 引擎在首个时隙前查询结果时没有提供或推导 partition scope，默认和持续模式均提前失败 | 已加 ticker 谓词；已读定向 CTest 通过日志 |
| CLOCK-2 | P3 | Docker README 仍列七类示例和单一 Clock，不能对应新入口的八类示例 | 已分别列出两类 Clock，并明确三个真实集群示例 |
| CLOCK-3 | P3 | 新引擎示例缺少根指南所导航的本地 AGENTS.md | 已补充本地维护和验收约定 |

以下保留原发现；行号按修复前后相应快照说明，不要求再次开启 fresh review 轮次。

## CLOCK-1：首次结果查询缺少 scope

位置：[engine main.cpp](../../examples/engine_clock_trigger_demo/main.cpp)，`run()` 的 `before_tick` 查询；修复前为第 367 行，当前第 366–368 行。

修复前调用为 `query_sql("SELECT request_id FROM " + relations.result)`。生成的结果关系按 ticker 分区，而查询没有 `QueryContext.scope` 或包含 ticker 等值条件的谓词。`Engine::query_trusted` 在 [engine.cpp](../../src/runtime/engine.cpp) 第 1579–1581 行选择 `context.scope` 或 `plan.inferred_scope`，两者均无值时返回 `query requires partition scope`。该查询发生在有限模式和持续模式分支之前，因此两个入口都在首个 Clock 时隙前退出。

复现命令为 `ctest --test-dir build --output-on-failure -R '^engine_clock_trigger_demo$'`。本 reviewer 没有执行该命令；读取实施线程的 `/tmp/eventflow-clock-ctest.log`，其中引擎目标失败并输出上述错误，另外四个相关目标通过。

处置：实施者为首次查询补充 `WHERE ticker='CLOCK'`，当前源码已核对。该谓词允许 Catalog 推导 scope；其他已有相同 ticker 谓词的查询不要求再额外传 `QueryContext`，不能把本问题泛化为所有查询都必须显式传 context。读取 `/tmp/eventflow-clock-engine-retest.log`，定向引擎 CTest 为 1/1 passed，0.84 秒。未用这次复验覆盖或删除先前失败记录。

审查开始时实施者还在修正另一个自查问题：生成的 payload-only input MV 不公开 request identity。最终 `demo_relations()` 从 `BoundComputation::explain()` 获取 capture 根，再按 Catalog 实际字段名构造固定输入查询。已检查当前第 170–192 行及 `verify_tick()` 的使用；该自查发现不冒充本 reviewer 独立发现。

## CLOCK-2：Docker 示例列表未完成拆分

位置：[docker README](../../docker/README.md) 第 22 行。

原文依次列出七类示例，只有一个 Clock，并称“后三项”启动集群；[entrypoint.sh](../../docker/entrypoint.sh) 已改为八项，新增引擎内 Clock 夹在条件示例与 SDK 示例之间。按原文理解，读者无法判断两种 Clock 的部署边界。

静态复现：比较 README 第 22 行与 entrypoint 的 `demo_names`。处置：当前文档分别列出“引擎静态 Clock”“SDK Clock 轮询”，并按名称明确条件触发、SDK Clock 轮询、checkpoint 恢复使用真实集群。已核对，无需为了这次文字修复宣称镜像已重建。

## CLOCK-3：新目录缺少本地维护约定

位置：[engine AGENTS.md](../../examples/engine_clock_trigger_demo/AGENTS.md)。审查首次读取时该文件不存在。

根指南说明每个 demo 有本地指导，`examples/AGENTS.md` 也要求编辑前阅读具体示例的 AGENTS。新增目录已有代码、README、脚本，但缺少此入口。处置：实施者已新增文件，包含静态定义、三个时隙、源位置不变、同一 SystemClock 时间轴、信号退出和构建/CTest 入口；已读取确认。

## 已核对的功能和边界

- 引擎示例在一个 `main.cpp` 中定义 news、两层级联 MV、显式 candidate session、`Trigger::clock`、`capture_current` 和静态 typed Function。`once_per_candidate=false`、最大 Int64 candidate deadline 与单请求 10 秒 lifetime 分别表达不同期限，没有新增永久会话 API。
- 引擎默认仅输入一次新闻，按 5/10/15 秒推进。每轮核对新请求数量及成功状态、实际受理时间、capture/admission 版本、递增受理序号、40/80 固定输入和 60 分结果、旧输入保持、Latest 身份/序号、下一时隙以及 `next_offset=1`。
- 持续循环由应用主动运行，没有隐藏线程：同一个 `SystemClock` 用于 seed 和后续 pump；该时钟以 Unix epoch 为起点、之后使用 steady clock，墙钟校正不会导致回退。信号处理器只写 `volatile sig_atomic_t`，主循环正常退出并销毁资源。每轮排空最多 64 次、等待 100 ms，Status 错误沿调用链返回非零退出。长期保留请求/结果及有界查询会消耗容量，README 已说明演示内存模型和保留策略限制；没有把这次静态审查称为长期运行证明。
- SDK 客户端实际循环提交 `drain(5s/10s/15s)`，每轮之前独立刷新报价。每次 drain 前后比较完整 `source_progress`，报价输入不被混入该不变性比较。每轮恰好出现一个不同的 D 请求、全部 D 成功，读取实际 manifest 检查受理时间、R/R+1 版本和递增序号，并核对该时隙冻结 bid/ask 和 Latest 请求身份/序号。此前 A/B 的成功与每个新时隙之前无提前 D 也有断言。
- SDK `run.sh` 启动 Rocks KV、两个 Compute 和 Coordinator，关闭自动 ticker，执行只访问 Meta 的客户端。启动就绪、客户端退出及所有服务退出状态均有检查，结束保留配置、日志和数据目录。实际周期规则来自服务链接的公共特征计算定义；两层 SDK SQL MV 只展示级联查询，不被描述为新 C++ guard。
- `ClusterClient::drain` 在同一个时间上重复有界 pump；预算耗尽、源输入待确认挡住 Clock 会返回错误，不会创建未来时隙后台任务。README 将持续调度、活动 Session、Ready、前置受理、容量及成功计算分开解释；`query` 不驱动 Clock，任意 C++ 仍须编译进相应应用。
- CMake 有两个独立目标和 install 项；新引擎有限示例注册为 CTest，并继承项目 Clang/sanitizer 策略。SDK 仍由集群脚本验证。Docker entrypoint 的八个名称、两个新 Clock 路径及默认有限运行一致。本轮未验证安装包或镜像中已有二进制是否已经更新。

## 验证证据与剩余边界

本 reviewer 自行执行：两个 run.sh 与 docker entrypoint 的 `bash -n` 通过；两个 main.cpp/run.sh 均为 ASCII、两份 run.sh 可执行；根 README、两个示例 README、docs/README、docker/README 的 Markdown 相对链接目标检查无缺失。该链接检查不证明页内 anchor 或命令运行结果。

本 reviewer 读取实施线程日志：

- `/tmp/eventflow-clock-ctest.log`：最初 4/5 通过，首个引擎目标因 CLOCK-1 失败。
- `/tmp/eventflow-clock-engine-retest.log`：修复后引擎目标 1/1 通过。
- `/tmp/eventflow-clock-sdk-debug.log`：三个时隙审计分别为受理时间 5/10/15 秒，capture/admission 为 23/24、34/35、45/46，admission sequence 为 3、4、5，最终 `SDK_CLOCK_POLLING_DEMO_OK`；运行目录为 `/tmp/eventflow-sdk-clock-polling-demo.gqO7EW`。脚本退出 0 及子进程正常退出由实施线程报告，本 reviewer 未自行运行脚本或观察 wait 的退出码。

审查结束时 main.cpp SHA-256：引擎 `95781cd4a1d0a0df4d03321d43e7080e4096fdb175d7a9ae91f6be7abd33196b`；SDK `8704e4ce6e9626bf38fb9d762e39ae407f2cfb195d77b7fcfd296427899fe3b6`。

剩余验收由实施线程记录：持续模式实际跨时隙运行及 SIGINT/SIGTERM 正常退出、Release 示例、格式/clangd 门禁。镜像重建和镜像内八个示例由独立镜像任务负责。没有自行执行长期运行、故障注入、完整回归、真实 Kafka 或 Docker 探针，本报告不据此宣告完整产品重新验收完成。

## 实施者验收补充

Reviewer 报告结束后，实施线程完成持续模式 SIGINT/SIGTERM、两个 Release 示例、全项目 format-check、两文件 clangd 和八个独立解压运行包脚本；结果与范围见[最终验证](../verification/clock-examples-validation.md)。仅另行修正 SDK README 的旧英文页内锚点，示例 C++ 保持报告中的 SHA。未再发起独立审查轮次，未宣称镜像已重建。
