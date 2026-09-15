# 流式数据库示例默认使用上传 UDF

Status: complete
Owner: root
Updated: 2026-09-16

## 授权与目标

用户提出将 examples/streaming_database 改为上传 UDF 的实现，并询问制品如何分发到 Compute。本轮让该路径成为默认完整 SDK 上传示例，沿用已交付生产接口；不更改生产分发协议。

用户随后明确要求上传 UDF 作为主要使用方式：根 README 中原有静态编译的完整描述与代码全部改为上传版。根 README 的 Schema、业务 case、计算图、函数实现、声明/部署和源码导航均以 streaming_database 上传版为主线；static_streaming 仅保留为独立参考，不占根 README 主流程。

## 决定与约束

- streaming_database 接收原 native_udf 的定义、SDK 验证与首次部署/checkpoint 恢复流程，使用通用 eventflow Server。
- 原静态模块完整示例保留到 static_streaming，避免移除可用的静态 API 学习路径。消息服务源码共用 streaming_database/message_server.cpp。
- native_udf 保留 README/run.sh 转发兼容入口，不重复维护一份业务实现或重复注册同一端到端 CTest。
- 原生制品通过 SDK 上传到 Coordinator，与注册记录同批保存于 KV 控制 shard。deploy 下发函数引用和定义，各 Compute 经 RemoteStateStore/HTTP 按摘要从共享 KV 读取，再验证并加载。不是每个计算请求重新下载；恢复从 checkpoint 制品重建。
- 保留前轮生产代码和历史验收记录；调整示例组织、构建和相关导航，并按默认上传用法更新图中文字。

## 验收

- [x] streaming_database/run.sh 完成真实五进程 SDK 上传、提交、首次部署、51.5/128/53 与恢复后 54.75 验证。
- [x] static_streaming 保留原静态模块行为；native_udf 转发路径可运行。
- [x] CMake 构建/安装/CTest、根 README、相关静态文档指针和示例指南一致，没有原静态路径被误读为上传版。
- [x] Debug 三 sanitizer、Release/安装运行、相关测试与格式/链接/脚本检查。
- [x] 同轮两位 fresh-context reviewer 独立审查，处置后定向验证。

## 当前工作

Owner: root。迁移、说明、图片与 skill 已同步完成。Debug 4/4、Release 与独立安装运行、恢复与转发兼容均通过；独立审查发现已处置，最终证据见下文。

用户进一步要求 README 用自然、具体的语言说明操作，避免“Compute 安装时从 KV 读取制品”等措辞。本轮改写为文件上传、读取和加载的实际动作；保留必要的接口名与一致性含义，并同步图中文字。

用户补充了 Coding Agent 章节的写法：先加载 .skills/use-eventflow，再描述业务需求；demo prompt 只包含新闻场景、计算规则和 SQL 查询目标，删除实现/部署/验证检查清单。README 已按该要求修改。


## 最终变更与验证

- 默认 streaming_database 使用上传版 definition/verify/run，通用 Server 在启动后接收 .so 和计算规则；原 main.cpp 与静态定义/验证保留在 static_streaming。native_udf 仅为 README/run.sh 转发，不重复业务代码或端到端 CTest。
- CMake 重新对应源文件、静态参考目标、安装目录和两个实际集群 CTest；消息服务源码仍共用 streaming_database/message_server.cpp。
- 根 README 全部使用上传版说明和四段实际 C++ 代码，正文与图片改用文件、读取、加载等直接表述。静态参考的旧教程链接转向 static_streaming；文档导航与 use-eventflow skill 默认流程同步。
- Debug 相关目标构建 exit=0，4/4 CTest 通过，50.12 秒；`/tmp/eventflow-streaming-default-build.log`、`/tmp/eventflow-streaming-default-ctest.log`。两条集群链均为实际进程，不是 mock。
- Release 构建、独立安装目录上传/恢复运行 exit=0；`/tmp/eventflow-streaming-default-release-build.log`、`/tmp/eventflow-streaming-default-install.log`。结果 51.5/128/53，恢复后 54.75，旧固定结果 51.5，各进程 exit=0。
- 旧 native_udf 命令转发运行通过，`/tmp/eventflow-streaming-default-alias.log`；安装目录的 static_streaming 也完成实际运行，`/tmp/eventflow-streaming-default-static-release.log`，51.5/128/53、所有进程 exit=0。
- 四段 README C++ 片段经 Clang/C++20、Werror、三 sanitizer 编译通过，`/tmp/eventflow-streaming-readme-snippets.log`；格式、shell 语法、git diff --check、当前文档路径/围栏和 skill quick_validate 通过。
- 架构与首次部署图经 Archify 9/9 showcase 验证/导出，最终 PNG 在 1100 px 宽度检查无截断；记录和图片身份在 `/tmp/eventflow-streaming-reorganize-zql2hhwv/`，这里只交付静态 PNG，不声称纵向时序 HTML 满足全屏显示。
- 同轮独立 [代码审查](reviews/STREAMING-DEFAULT-code-review.md)、[读者审查](reviews/STREAMING-DEFAULT-reader-review.md)完成；三项去重问题（审计名称/API、skill 默认流程、文档导航旧能力说明）已修正并定向检查。用户最后的 prompt 文案调整为纯文档修改，未再启动审查轮次。

生产分发代码本轮未改，未重跑全量 65 项测试，也未据旧数量声称本轮全面验收。原文件备份 `/tmp/eventflow-streaming-reorganize-zql2hhwv/` 保留了迁移前两个目录。无剩余任务。
