# 数据库主动消费：独立全局产品与交付审查

Reviewer: streaming_product_review（独立 fresh-context reviewer）  
Date: 2026-09-15  
Round: 本里程碑唯一一轮并行全局审查  
Status: complete；原始发现与实施者整改证据均保留

## 范围与原始要求

依据[根约定](../../AGENTS.md)、[当前索引](../INDEX.md)和[授权规格](../streaming-database-example.md)，独立检查当前 CMake/安装入口、默认 SQL 程序、静态模块注入、通用运行时、消息源接口、独立 HTTP 消息服务、五进程示例、业务定义与 SQL 断言、退出清理、Docker 入口、当前文档和架构图片。重点判断是否真正完成“定义业务、部署数据库、生产消息、数据库主动消费、SQL 验证”的使用链路。

本 reviewer 未修改实现、未启动竞争构建、未新增或执行故障注入。用户已明确取消新增专门故障测试，不把它们列为完成门槛。并行运行时 reviewer 负责更细的提交、重放和运行时一致性核查；本报告没有用其他 reviewer 的结论替代自己的检查。

## 发现

### P2：Compute/Coordinator 意外停止监听仍返回成功

- 位置：审查时的 [database_main.cpp](../../src/service/database_main.cpp) 第 190–193、231–237 行；关联 [run.sh](../../examples/streaming_database/run.sh) 的 cleanup。
- 证据：两个服务循环都在 `!stopping && server->is_running()` 为假时结束，随后执行 `server->stop()` 并无条件 `return {}`。因此即使没有 SIGINT/SIGTERM，监听器自行退出也会由 `database_main` 返回 0。相同文件的 KV 分支和独立 [message_server.cpp](../../examples/streaming_database/message_server.cpp) 已正确区分请求停止与监听故障。
- 后果：进程管理方无法由退出状态分辨这类服务故障。若故障出现在验证器最后一次依赖该服务之后，示例 cleanup 的 `wait` 会把它记作正常退出，与“全部子进程失败可观察”的交付要求不符。
- 建议：在 Compute/Coordinator 分支结束时保留停止原因；仅收到停止信号返回成功，否则返回 `Unavailable`。与 KV/HTTP 消息服务保持同一约定即可，无需新增专门故障注入探针。
- 处置：已即时通知主任务；待实施者记录代码变更和定向验证。

### P2：当前架构导航仍宣称新示例覆盖 Rocks 全进程恢复，且链接锚点已不存在

- 位置：审查时的 [feature_computation_architecture.md](../../docs/feature_computation_architecture.md) 第 941 行。
- 证据：正文写“可运行示例……Rocks整集群恢复”，链接为 `README.md#运行与示例`；当前 [docs/README.md](../../docs/README.md) 已无“运行与示例”标题。新 [verify.cpp](../../examples/streaming_database/verify.cpp) 第 307–323 行只执行 pause/checkpoint/resume，没有停止和重建数据库进程；[示例 README](../../examples/streaming_database/README.md) 明确声明不覆盖进程重建恢复。
- 后果：读者通过当前导航会误把 checkpoint 创建当作完整恢复验证，也无法跳转至已删除的示例导航锚点。
- 建议：直接链接 `../examples/streaming_database/README.md`，准确列出自动消费、级联 MV、条件/Clock、固定输入和正常 checkpoint；另外引用 [core_cluster_test.cpp](../../tests/core_cluster_test.cpp) 说明保留的真实恢复回归，并让本轮执行证据由主任务单独记录。
- 处置：已即时通知主任务；待实施者更新正文与链接。

## 原始要求到当前实现的核对

| 要求 | 当前源码证据与结论 |
| --- | --- |
| 删除旧 examples，旧业务只能留在测试支持层 | `rg --files examples` 仅列出 `streaming_database` 及两层 AGENTS。CMake 第 149–154 行的 legacy fixture/server 位于 `BUILD_TESTING` 内；生产与示例 Release 链接命令未链接 legacy fixture 或 GoogleTest。`src/`、`include/`、新示例中没有旧业务名称引用。 |
| 实际 KV、Coordinator、两个 Compute，另有 HTTP 服务 | `run.sh` 启动独立消息服务、Rocks KV、两个独立 Compute 和 Coordinator，动态端口、三份 JSON 服务配置；两个业务 shard 加一个控制 shard。 |
| 独立声明式 Trigger 和级联 MV，有可理解注释 | `definition.cpp` 自建 Catalog，定义 `trades -> trade_totals -> market_features`、session、阈值 once、200 ms Clock、固定输入捕获和静态评分函数。两级 MV、Trigger 和捕获边界均有英文解释。 |
| 通用生产运行组件，不内置示例业务 | `eventflow` 只链接 `ef_database_main`；示例额外链接自己的 `ef_streaming_definition`。`ComputationModule` 提供名字、版本、bind；Coordinator 与 Compute 检查模块身份、版本及 Catalog fingerprint。默认 `sql` binder 由通用 runtime 提供；无 requests 的运行路径需要结合并行 reviewer 的整改及新证据完成最后验收。 |
| 数据库自己持续消费与执行，客户端不推进计算 | `SourceSubscription` 注入 `IMessageSource`，Coordinator 托管 `SourcePoller`。`verify.cpp` 只有 HTTP publish、部署/暂停/恢复/checkpoint 和 SQL/源位置查询，未调用数据库 ingest/pump/drain。 |
| 部署前不 ACK、两个 shard 的准确结果 | verify 第 236–264 行检查 STAGING 的源位置为 -1，然后 deploy；断言总量 120/130、两倍总量 240/260、阈值结果 242/262，并检查实际路由 shard。 |
| 追加输入继续计算，once、固定输入与周期语义 | verify 第 266–305 行观察无消息时新增周期结果；暂停后追加 30，检查源位置和 SQL 不变，恢复后断言 150/3/300 与周期评分 303；原阈值结果与完整 capture 行保持一致。 |
| 共享接口和正常适配器合同 | `HttpMessageSource` 实现现有 `IMessageSource`；`http_message_source_test.cpp` 用同一参数化测试覆盖 HTTP/MemMock 的原字节、ACK 前缀、pause/seek、预算和 close。没有以 HTTP 合同替代真实 Kafka broker 的声明。 |
| 生命周期与清理 | run.sh 使用 EXIT/INT/TERM trap，倒序停止依赖、检查 wait 返回码、超时后报错，保留配置/日志/数据。已通过 Bash 语法检查；服务入口停止原因存在上方 P2。 |
| 当前 README、图和示例指南同步 | 新 README 展示完整时间线、静态编译边界及 checkpoint 限制；examples 两层 AGENTS 指向新例。图片已实际打开，标题精确为“数据库主动Poll消息系统中的消息”，poll/ACK 从 Coordinator 指向消息服务，消息返回方向相反，两个 Compute 和共享 KV 清楚分离。架构文档仍有上方残留。 |
| 旧 Docker/安装入口退出 | CMake 仅安装当前四个程序、新示例和随包依赖；Docker 通过 `examples/*/run.sh` 动态发现，不再写死旧名称。 |

## 本 reviewer 实际检查的证据

- 阅读了当前原始需求、公开接口及上述生产、示例、测试、构建和文档源码；没有从历史验收数量推导当前通过。
- `bash -n examples/streaming_database/run.sh` 与 `bash -n docker/entrypoint.sh` 均成功；新 run.sh 具有可执行权限。
- 对 13 份当前入口/示例/skill 引用文档做本地目标存在性检查，未发现不存在的目标路径；上方旧锚点另经实际标题核对发现。此检查不证明外部 URL 可访问。
- 检查 `build-release/CMakeFiles/eventflow.dir/link.txt` 和 `eventflow_streaming_database.dir/link.txt`：均使用 Clang、`-O3 -DNDEBUG -flto=thin`，无 sanitizer；仅示例数据库链接其业务定义，两者均未链接 legacy/GoogleTest。此证据是审查期间构建产物，不代表随后整改后的二进制已刷新。
- 读取 `/tmp/eventflow-streaming-4Y8jgY/verify.log`，确认该次运行包含两个 shard 的 SQL 结果、STAGING 无 ACK、无新输入的周期结果、暂停追加保留、固定输入及 `STREAMING_DATABASE_OK ... periodic_score=303 source_next_offsets=3,2`。这不是 reviewer 新启动的运行，也不代表之后修改已验证。
- `/tmp/eventflow-streaming-debug-build.log` 与 `/tmp/eventflow-streaming-release-build.log` 末尾均到达已构建目标；主任务随后正在修改源码，需要刷新。主任务报告全 CTest 初跑被外部 SIGTERM 中止；reviewer 实际读到的日志只含 `core_cluster_test`、`event_request_test`、`cep_test` 三项 Passed，不能宣称全 60 项通过。

## 剩余边界与交接

主任务需处理上方两项发现，以及同轮运行时 reviewer 提交的问题，并补充整改后的必要 Debug/Release、相关正常路径回归、完整 CTest 和真实示例证据，更新 active spec 的状态。按用户要求，不新增专门故障注入，也不自动开启第二轮 fresh-context 审查。

HTTP 服务是有界内存消息设施，退出即丢失日志；新示例验证 checkpoint 创建，未验证全进程重建。真实 Kafka、HA、动态 UDF/UDTF/UDAF、Process Function、在线迁移仍属后续范围。这些边界已在当前入口文档中说明，不作为本轮额外实现要求。

## 实施者处置与验证

2026-09-15，root 完成两项整改：

- CLI 的 Compute/Coordinator 分支在停止后检查停止信号；未收到 SIGINT/SIGTERM 而退出监听时返回 Unavailable。代码与 KV/HTTP 消息服务保持同一语义。没有新增故障注入探针；新 Release 安装示例的五个进程均在请求关闭后返回 0。
- 架构文档直接链接新示例，准确区分正常暂停/恢复消费、checkpoint 创建与 tests/core_cluster_test.cpp 的既有 Rocks 全进程恢复。另修正 src/sql/AGENTS.md、tests/AGENTS.md 的旧 Trigger 规格锚点。静态检查覆盖 31 份 Markdown、395 个本地目标及 48 个章节 fragment，错误为 0；README 中新增 HTML 图片链接的目标另经存在性核对。

用户指定的图标题、缩小显示、居中与泛化消息设施均已完成：图内文字为“数据库主动Poll消息系统中的消息”；改用 IMessageSource、Message Delivery 和“消息系统（MQ 等）”，不限定 HTTP。README 使用居中段落和 width=720 显示，点击仍打开 1448 x 1086 原图；图注同步泛化，HTTP 仅是当前例子实现。root 已目视检查最终 PNG，并用 HTMLParser 检查居中/宽度/图片链接；最终图 SHA-256 为 `63c6542f85329beacaf83467e743153ff1fb0fcb2a50f705ad1e59a7b6453ac4`。CMake 安装保留 run.sh 执行权限。用户手写的个人介绍、演进计划和联系方式保留。

整改后 Clang Debug 和 Release 全构建均 exit 0、没有 warning/error；Debug 使用 address/undefined/leak 与不可恢复诊断，Release 命令实查为 -O3 -DNDEBUG -flto=thin、无 sanitizer。clang-format 全项目检查与 Bash 语法检查 exit 0。默认 SQL 与普通无请求 Trigger 的 2 个新增正常用例通过。

运行 `cmake --install build-release --prefix /tmp/eventflow-streaming-install` 后，直接执行包内 `examples/streaming_database/run.sh`，预期总量/阈值/周期/源位置断言全部通过，五个服务均 exit 0；日志 `/tmp/eventflow-streaming-zVfrGy/`，总体日志 `/tmp/eventflow-streaming-release-example.log`。完整 61 个 CTest 仍在运行，结果待追加，不在这里提前声明通过。没有重开 fresh-context 审查轮，也没有把安装示例当作 Docker 镜像重新构建或真实 Kafka 证据。

最终门禁（2026-09-15）：全量 CTest 已 exit 0，61/61 通过，总计 953.47 秒；日志 `/tmp/eventflow-streaming-ctest-final.log`。其中新 Debug 示例通过（27.30 秒），日志 `/tmp/eventflow-streaming-ncQhwg/`，预期 SQL/源位置断言及五个进程 exit 0，无 sanitizer 诊断。两个产品发现和同轮运行时发现均已整改并记录；本轮规格已完成，无待办交付项。后续仅按用户新授权推进已声明的范围外能力。
