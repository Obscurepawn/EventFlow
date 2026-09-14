# Clock 示例：声明与驱动分开

Status: done
Owner: root

用户要求把Clock的两种接入方式分到两个example目录，并用名称区分SDK客户端轮询推进。原clock_trigger_demo迁为sdk_clock_polling_demo；新增engine_clock_trigger_demo，直接展示声明式Clock、关系与函数定义、多个周期和可停止的持续运行。

保持现有边界：Clock按已观察的key重复调度；无固定次数，但时钟须持续推进。请求另受Session/Ready/once/容量约束。SDK推进已有内置Clock，不声称远程上传任意C++定义。旧集群用例的source prefix、实际capture/admission和Latest断言保留；不能只改名称却沿用“query自己触发模型”的说明。

验收：两个目录各自有代码/README/run.sh；引擎示例直接使用Trigger::clock，默认验证至少三个时隙，--continuous可由SIGINT/SIGTERM停止；SDK示例循环推进多个时隙，逐次检查计算与源确认边界；Clang三sanitizer、相关回归、Release运行、格式/ASCII/clangd和一轮fresh-context示例审查。

| 工作 | 状态 |
| --- | --- |
| 目录与调用入口调整 | 已实现 |
| 引擎声明式多周期/持续示例 | 已实现并通过有限/信号退出测试 |
| SDK轮询多周期与原断言 | Debug/Release 真实四进程通过 |
| README与导航/构建同步 | 已完成 |
| 验证与独立审查 | 已完成；八个独立解压用例通过 |

## 实施分工与当前状态

- engine_clock_example：新增引擎静态定义、多周期及持续模式。
- sdk_clock_example：原集群示例改名、三轮客户端推进、每轮源确认/捕获/Latest 断言。
- root：CMake、README 两种入口、文档导航、包入口同步与验证。
- 本轮不改变公共 API 或运行时语义。docker 入口同步纳入第八个示例，镜像重新构建与验证仍由独立 reviewer-image 任务记录，不能用本地测试替代镜像证据。

已修改：README.md、CMakeLists.txt、examples/AGENTS.md、docs/README.md、docs/core_delivery.md、docs/implementation_guide.md、docker/entrypoint.sh、docker/README.md、Dockerfile 及当前规格/索引；目录实现由两位 agent 并行完成。

## 本轮审查与修复

独立报告：[CLOCK-examples-review.md](reviews/CLOCK-examples-review.md)。初次引擎 CTest 缺少分区查询范围，后为初始化查询补充 ticker 谓词，定向复验通过；没有修改底层查询合同。两项导航问题（新例 AGENTS、Docker 八示例枚举）已修复。本轮只进行一次 fresh review，不把实施者检查算作额外审查。

Debug 原五目标运行四个通过，新引擎目标失败；修正后该目标 1/1 通过。SDK Debug/Release 两次真实四进程脚本退出 0；引擎 Release 默认三时隙通过，Debug 持续模式分别在首次真实时隙后接收 SIGINT/SIGTERM，均退出 0。格式、ASCII、Bash、链接、两文件 clangd 均通过；Bear 记录本轮实际编译并更新本地数据库，与 CMake 的 98 个唯一翻译单元完全对应。最终制品、源码清单和日志摘要见[本轮验证](verification/clock-examples-validation.md)。

## 完成状态

两个目录、README 的两种驱动解释、三个周期的请求/源边界验证、可停止的持续模式、Clang 三 sanitizer、Release、格式/clangd、一次独立审查及更新运行包均已完成。本轮没有未完成的代码项；镜像任务需用包含八个示例的最新源码重建并另行验收。
