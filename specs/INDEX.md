# 开发规格索引

## 最近完成的 README 使用示例补充

[README 使用示例](readme-usage-examples.md)已完成：补充当前物化视图 DDL、声明式 Trigger 和内置触发审计的 SQL/SDK 查询，明确“成交笔数”，并移除架构图顶部大标题。四段 C++ 片段编译、3/3 相关回归、图文检查及同轮双 reviewer 独立审查完成；不改变产品 API 或既有里程碑状态。

用户追加的“Schema → 业务 case → 合并 DAG → Graph-Based 建模理由 → 代码”说明也已完成：单张图明确特征依赖、两类触发条件和评分目标，解释级联 MV 与 CEP 状态机的图结构。Mermaid 实际渲染、静态检查及同轮双 reviewer 审查完成，两处布局发现已定向修复；前次验证保留为对应范围的历史证据。

## 最近完成的数据库接入与示例重写

[数据库主动消费与完整集群示例](streaming-database-example.md)已完成：旧 examples 退出，通用 ComputationModule/Runtime、Coordinator 托管 SourcePoller、HTTP 消息源和独立五进程示例接通。声明式 Trigger/级联 MV、自动消费与 SQL 断言经 Debug/Release 实跑验证；61/61 CTest、独立安装运行、文图检查及一轮并行双 reviewer 审查整改完成。README 架构图采用通用 IMessageSource/消息系统，居中以 720 像素显示。真实 Kafka 与新增故障注入仍按用户决定后置。

## 最近完成的源码可读性维护

[源码逻辑分段与审阅注释](source-spacing.md)已完成：36 个手写源码文件补充逻辑分段，在 24 处增加英文注释。仅插入空行/注释、非注释 token 一致性、37 文件 clang-format 与一轮双 reviewer 独立审查通过；见[本轮验证](verification/source-readability-validation.md)。

## 最近完成的 Clock 示例调整

[Clock 定义与驱动示例](clock-example-clarity.md)已完成：拆分引擎静态定义与 SDK 主动轮询两个目录，各验证三个周期；引擎示例提供可停止的持续运行模式。README 明确两种入口及 SDK 的推进请求边界。一轮独立审查整改、Debug/Release、clangd 与八个独立解压示例均完成，见[本轮验证](verification/clock-examples-validation.md)。

## 当前镜像交付

[Docker 源码与编译环境](reviewer-source-environment.md)已按最新授权完成：镜像只准备工具链、第三方依赖和源码，默认显示帮助，编译/示例/测试由使用者显式执行。实际镜像构建、默认入口/清单验证和一轮双 reviewer 审查完成，见[本轮验证](verification/reviewer-source-environment-validation.md)。

此前[Reviewer Docker 镜像](reviewer-image.md)的预编译示例镜像、[完整验证](verification/reviewer-product-validation.md)与[环境验证](verification/reviewer-image-validation.md)作为历史快照保留，不代表本轮默认行为或当前源码验收。Kafka 集成、常驻 Compose 和原生 ARM 继续后置。

## 最近完成的内置触发审计

[默认 Trigger 审计关系](trigger-audit-table.md)已完成：自动安装只读 trigger_audit，记录实际发出的名称、时间和关联请求，支持单 key SQL 历史、有界保留和一致恢复。README/skill已同步，60个测试目标与八个独立运行包示例完成验证，并通过一轮独立审查；见[本轮验证](verification/trigger-audit-validation.md)。

## 开发协作维护

[EventFlow 使用 skill](eventflow-usage-skill.md)已完成：在 `.skills/use-eventflow/` 提供应用接入工作流、现有 API 语义、五类计算示例和 reviewer 的显式加载入口。一轮独立接入审查已整改，消费者 Debug 三 sanitizer/Release 与七个现有示例均通过。

[读者文档与配图可读性优化](reader-doc-polish.md)已完成：整理入口、术语、目录和示例，新增 8 张概览/重排图，保留原详图；静态检查及一轮并行独立审查整改完成。C 类图文/代码差异保持后置，本任务未改代码。

[模块 AGENTS.md 初始化](module-agent-guidance.md)已完成：新增 23 份目录指南，根文件增加导航；静态检查及一轮双 reviewer 并行独立审查通过。本次仅修改指导文档，不改变下述产品交付或重构状态。

[读者文档、配图与代码一致性审阅](reviews/reader-documentation-review.md)保留最初的文字、图片和代码差异发现。用户后续批准文字/图片可读性优化，已按上方规格完成；C 类事项暂不处理，原审阅不作为当前代码完全一致的声明。

## 最近完成的重构

[声明式 Trigger API](trigger-api-ergonomics.md)已实现普通 Trigger、typed/untyped Pattern、静态 Function、显式 Session、capture_match/capture_current 与原子 `TriggerSet::bind`，并迁移生产、测试和七个示例。一轮双 reviewer 并行审查的四项去重发现已修复。Debug/Release 构建、静态质量与七个 Release 示例通过；55个CTest目标全部完成验证（53个初跑通过，两个完整目标修正后复验通过），七个独立解压包示例也通过，见[本轮验证](verification/trigger-refactor-validation.md)。历史核心记录不替代这轮验收。

## 当前交付

[核心交付](core-delivery.md)是当前唯一验收状态入口：真实共享 KV、Coordinator/Meta、多个 Compute、级联 MV、条件/Clock、固定输入异步请求、checkpoint 恢复和六个示例。最终源码 45/45 全量测试、Debug/Release 与六个示例已通过；运行包已在独立解压目录完成六个示例验证，见[最终验证](verification/final-validation.md)。多个独立全局 reviewer 已并行完成一轮，合理发现已修复并有定向证据，不自动再开一轮。

[产品范围](product-scope.md)保留用户决策、工程规范及完整产品待办；[运行时契约](runtime-contracts.md)保留详细接口与不变量。当前核心收敛不代表 UDF/UDTF/UDAF、在线迁移、复杂 Job、Kafka Compose 或性能/镜像验收完成；Process Function 已移出当前范围。

## 子系统合同

| 规格 | 当前范围 |
| --- | --- |
| [SQL 与增量关系](sql-runtime.md) | SQL/binder/Catalog、身份、聚合、lookup join 和原子增量维护已实现 |
| [事件生命周期](event-lifecycle.md) | 输入身份、时间、TTL、简单 FSM、缓存和存储 snapshot 基线已实现 |
| [有界 E-NFA CEP](cep-enfa.md) | processing-time 单 owner 的多候选匹配与恢复已实现 |
| [请求与函数视图](request-functions.md) | 单 Catalog、固定输入、共享祖先、线程执行器与有效预算已接通核心 |
| [Latest selector](latest-selector.md) | typed candidate/status、成功门槛、受理顺序和 SQL 后继已实现 |
| [事件与请求接入](event-request-integration.md) | 同 owner 的 source/fence/admit/completion、once 与 UNKNOWN 合同已实现 |
| [同轮变化观察](change-observation.md) | Catalog/程序/Engine/settlement 基础已实现；通用 host 与完整预算调度后置 |
| [控制面与集群](control-plane.md) | 静态核心已实现；完整元数据版本、动态 Job 与迁移协议保留为后续目标 |

## 审查与历史材料

阶段完成记录：[设计](reviews/M0-design-review.md)、[存储](reviews/M1-review.md)、[SQL](reviews/M2-review.md)、[生命周期](reviews/M3-review.md)、[CEP](reviews/CEP-owner-fresh-review.md)。请求/预算等限定审查由各子系统规格链接到[审查目录](reviews/)。

最终并行全局审查：[架构](reviews/CORE-global-architecture-review.md)、[工程](reviews/CORE-global-engineering-review.md)。报告保留原发现与对应快照，实施者追加修复、验证和剩余缺口；历史绿灯不替代当前源码门禁。

[archive/](archive/)保存早期接口提案供追溯。继续开发以本目录当前合同和主树源码为准，不从旧工作目录或提案覆盖实现。


## Trigger 后续边界

当前强类型函数与匹配输入已属于上方重构范围。[Trigger 规格](trigger-api-ergonomics.md)保留通用观察 host、高层 Session builder、无 candidate 请求、动态插件及在线迁移等后置项；不将这些目标写成当前可用 API。
