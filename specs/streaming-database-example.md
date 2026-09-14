# 数据库主动消费与完整集群示例

Status: complete
Owner: root
Updated: 2026-09-15

## 用户授权与结果

用户要求删除旧 examples，重新提供真正的数据库使用流程：独立声明自定义 Trigger 和级联特征 MV，部署集群，向 MessageSource 构造数据，集群持续主动消费，最后通过 SQL 查询并检查结果。新示例不依赖原 feature_computation_demo 的业务定义；Trigger 使用声明式 API，Trigger/MV/断言都要有清楚的英文注释，README 用中文解释。

该授权覆盖持续消费运行循环、可注入的静态计算定义、旧示例退出生产依赖、相关测试与文档更新。动态插件、任意在线图迁移、HA 和真实 Kafka Compose 未自动进入本次范围。

## 设计与不变量

- 保留一个 Coordinator、共享 KV 和至少两个独立 Compute 进程。消费驱动由 Coordinator 托管，复用现有路由和输入一致性入口；示例客户端只负责消息生产、部署控制和 SQL 验证，不能调用 pump/drain 替数据库消费或执行函数。
- 用户进一步指定 HttpMessageSource：独立 HTTP Server 保存测试消息并提供 poll/position/ack 接口，数据库内的 SourcePoller 主动消费；同一驱动接受 MemMock、HTTP 和 Kafka 的 IMessageSource 实现。HTTP 示例服务是消息设施替身，不冒称 Kafka broker。
- 每个消息保持原 source/topic/partition/offset、内容和固定处理时间。输入持久确认后才 ACK；UNKNOWN 重试原消息/原 token，不能前进确认前缀。永久错误可观察且停留原位置，不能静默丢弃。
- poll/inflight/请求体/每轮工作均有预算。暂停和 checkpoint 阻止新输入；恢复根据已保存源位置 seek，不能从更后的 broker commit 跳过 checkpoint 之后的数据。Rocks WAL 保留。
- ComputationRuntime 是通用生产组件；各集群进程通过相同 ComputationModule 绑定 Catalog/Trigger/函数定义并校验身份。静态模块随示例可执行文件编译，示例公开展示定义；不以上传闭包或隐式内置业务图代替自定义。
- 旧例子的生产依赖移除；必要的旧回归场景只保留为 tests 支持材料，不再作为示例或生产默认业务定义。
- 旧示例目录全部退出 examples、CMake、安装入口、Docker示例命令与当前读者导航。历史审查保留原快照，不据此重写历史结果。

## 验收

1. 一条 run.sh 启动消息服务、KV、Coordinator 和两个 Compute；客户端显示声明式定义与 SQL 结果。
2. 部署前写入消息不会被集群提前确认；部署后自动读取，两个不同 shard 的 key 均得到正确级联 MV 和触发计算结果。
3. 追加消息后，无客户端 ingest/pump/drain，指标继续变化；固定请求输入与 once/周期语义按示例明确断言。
4. 共享合同验证 MemMock/HTTP 投递、ACK、暂停和恢复位置。用户在 2026-09-15 明确不增加专门故障注入测试；已有基础契约保留，不新增丢响应、宕机等测试任务。
5. 新定义的 MV/Trigger 及测试有解释语义的注释，README 是一个完整阅读入口。
6. Clang Debug三sanitizer、相关回归、Release、格式、实际多进程示例与正常暂停/恢复/checkpoint 创建有新证据；全进程恢复沿用既有回归，不新增 HTTP 源故障或宕机恢复测试。
7. 最终按根约定进行一轮多位 fresh-context reviewer 并行全局审查；整改后定向验证，记录闭环。

## 工作状态

| 工作 | Owner | 状态 |
| --- | --- | --- |
| 通用计算模块、运行驱动和服务接入 | root | 完成；默认 SQL 与无请求 Trigger 正常回归通过 |
| HttpMessageSource、独立 HTTP Server 与共享合同 | root | 完成；MemMock/HTTP 共 8 个共享用例通过 |
| 数据库持续消费/ACK/暂停/checkpoint | root | 完成；SourcePoller 4 个用例与正常集群示例通过 |
| 独立声明式示例、SQL检查和旧例删除 | root | 完成；Debug 与独立安装的 Release 五进程流程均通过 |
| 文档、Docker入口、回归与独立审查 | root | 完成；唯一一轮双 reviewer 审查整改、61/61 CTest 与静态检查通过 |

## 当前状态与下一步

旧生产服务对 examples/feature_computation_demo 的依赖已断开，八个旧例已退出 examples/CMake/安装/Docker 当前入口。通用 ComputationRuntime 与 database_main 接受静态 ComputationModule，旧业务仅保留在 tests/support/legacy_feature_fixture.* 和测试专用进程中。备份 `/tmp/eventflow-streaming-rewrite-kevwmbii/` 不进入交付目录。

SourcePoller 已完成首个 red-green 验证：初始可编译占位在“仅持久确认后 ACK”行为测试失败，随后实现保留原消息/时间、UNKNOWN 重试、暂停、提交后背压仍确认及永久失败停止。Clang Debug 三 sanitizer 下 source_poller_test 的四个测试通过。首次测试编译遗漏 take 的命名空间、实现返回成员 lvalue 不满足 StatusOr 的 rvalue 接口，已分别修正；不将编译失败作为行为 red 证据。

HTTP adapter、独立有限 retained-log 服务、通用计算运行与 Coordinator 持续消费现已完成。HTTP 共享测试先观察到 MemMock 通过/HTTP 未实现失败，然后实现至 8 个用例通过。没有在用户取消后新增专门故障测试。

新例 `examples/streaming_database/` 独立定义 trades -> trade_totals -> market_features 级联视图、条件/Clock Trigger 和静态函数。首次 Debug 五进程运行记录在 `/tmp/eventflow-streaming-4Y8jgY/`：两个 shard、总量 150/130、固定阈值分数 242/262、后续周期分数 303、源前缀 3/2，五个服务退出码均为 0。客户端只生产/部署/查询/控制，不替数据库 ingest/pump/drain。

README、docs/README、相关读者说明、使用 skill 和模块 AGENTS 已切换到新例。新图 `docs/image/eventflow_streaming_architecture.png` 已目视核对，按用户指定标题改成“数据库主动Poll消息系统中的消息”；最终 SHA-256 为 `63c6542f85329beacaf83467e743153ff1fb0fcb2a50f705ad1e59a7b6453ac4`。用户随后要求缩小 README 图示，已将显示宽度设为 720 像素，点击链接仍打开完整原图。

用户进一步要求图示居中且不限定 HTTP：README 已用居中段落承载图片，图注和组件表改为通用 IMessageSource 与消息系统，明确 HTTP 仅为当前例子实现。图片中的 HttpMessageSource/HTTP Message Server 标签已由独立图片任务替换为 IMessageSource、Message Delivery 和“消息系统（MQ 等）”；root 目视确认标签与方向，尺寸仍为 1448 x 1086。README HTML 结构检查确认居中、width=720 与图片目标存在。

初次全 Debug 构建、Release 构建及 clang-format 检查通过；全量 CTest 首次被 SIGTERM 终止（exit 143），只有 core_cluster/event_request/cep 三项完成的通过记录，不据此声称全量通过。整改后刷新编译并重跑。

唯一一轮并行 fresh-context 全局审查已完成：[运行时审查](reviews/STREAMING-runtime-review.md)与[产品审查](reviews/STREAMING-product-review.md)。默认 SQL/普通无请求 Trigger 分支、尚未消费 partition 的持久起点、未提交任务 owner 清理、CLI 退出状态和文档旧断言共五项已整改。定向正常路径回归、全量 CTest、刷新 Release 示例与静态文档检查均通过，无本轮未完成任务，不自动追加审查轮。

## 变更文件

| 范围 | 主要文件 |
| --- | --- |
| 持续消费与消息设施 | include/eventflow/source_poller.hpp、http_message_server.hpp、message_source.hpp；src/ingest/source_poller.cpp、message_source.cpp、http_message_server.cpp、source_http_protocol.hpp |
| 通用定义与数据库运行 | include/eventflow/computation_runtime.hpp、database_main.hpp、compute_service.hpp、coordinator_service.hpp；src/runtime/computation_runtime.cpp、src/service/compute_service.cpp、coordinator_service.cpp、database_main.cpp、src/main.cpp |
| 完整示例与构建入口 | examples/streaming_database/ 下 8 个文件、examples/AGENTS.md、CMakeLists.txt、docker/entrypoint.sh |
| 测试与旧业务隔离 | tests/source_poller_test.cpp、http_message_source_test.cpp、sql_module_service_test.cpp；tests/support/legacy_feature_fixture.hpp/.cpp、legacy_database_main.cpp；tests/legacy_feature_fixture_test.cpp、compute_service_test.cpp、coordinator_service_test.cpp |
| 当前读者入口与协作指南 | README.md、docs/README.md、core_delivery.md、implementation_guide.md、feature_computation_architecture.md、新架构 PNG、相关模块 AGENTS.md、.skills/use-eventflow/SKILL.md 及 references/ |
| 持久状态与审查 | 本规格、specs/INDEX.md、specs/reviews/STREAMING-runtime-review.md、STREAMING-product-review.md |

前置文档审阅与 AGENTS 初始化记录保留于其各自规格。本轮不把原读者图文/代码差异 C 类条目重新打开；只实施用户随后明确授权的持续消费、通用运行与新例。

## 最终验收证据

| 检查 | 本轮结果 |
| --- | --- |
| Clang Debug 完整构建 | exit 0；address/undefined/leak、不可恢复诊断；无 warning/error；`/tmp/eventflow-streaming-debug-final-build.log` |
| 全量回归 | `ctest --test-dir build --output-on-failure -j 2`，61/61 通过，exit 0，总计 953.47 秒；`/tmp/eventflow-streaming-ctest-final.log` |
| 新正常路径 | sql_module_service_test 两个用例通过；初始行为 red 与整改后 green 分别见 `/tmp/eventflow-streaming-sql-red.log`、`/tmp/eventflow-streaming-sql-green.log` |
| Debug 五进程示例 | CTest streaming_database_example 通过，27.30 秒；`/tmp/eventflow-streaming-ncQhwg/`，预期成功标记与五个 exit 0，无 sanitizer 诊断 |
| Release 完整构建 | exit 0；-O3 -DNDEBUG -flto=thin，无 sanitizer、无 warning/error；`/tmp/eventflow-streaming-release-final-build.log` |
| 独立安装验证 | CMake 安装到 `/tmp/eventflow-streaming-install`，直接运行其中 run.sh；`/tmp/eventflow-streaming-zVfrGy/` 与 `/tmp/eventflow-streaming-release-example.log`，预期成功标记、五个 exit 0 |
| 代码格式/脚本/字符 | clang-format 21.1.2 全项目检查 exit 0；两个 shell 入口 bash -n 通过；157 个项目 C++/header/shell 文件非 ASCII 搜索无命中 |
| 文档/图 | 31 个当前 Markdown、395 本地目标、48 章节 fragment 初轮检查无错误；后续新增链接定向检查通过；最终图 root 目视核对，README HTML 居中/720 宽度/图片存在检查通过 |
| 独立审查 | 上述两份全局报告保留原始发现、处置和最终证据；仅一轮并行 fresh-context 审查 |

全量回归包括 Compute（473.17 秒）、Coordinator（165.10 秒）、真实多进程 core_cluster（110.65 秒，含既有 Rocks 全进程恢复）、旧业务四适配器回归（429.86 秒），以及 HTTP/MemMock/SourcePoller 合同。新 HTTP 示例只验证正常消费、暂停恢复消费和 checkpoint 创建；没有新增 HTTP 服务宕机/故障恢复测试。真实 Kafka Compose、动态扩展函数、Process Function、HA、在线迁移继续后置；本轮没有重新构建已有 Docker 镜像。
