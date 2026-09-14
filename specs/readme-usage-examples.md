# README 的物化视图、声明式 Trigger 与审计查询示例

Status: done（示例补充与追加 Schema / case / DAG 说明均完成）
Owner: root
Updated: 2026-09-15

## 授权与范围

用户要求在根 README 补充当前如何定义物化视图、使用声明式 API 定义 Trigger，以及触发后如何通过 SQL SELECT 查询触发记录，并附主要代码和注释。

沿用 `examples/streaming_database/definition.cpp` 的成交业务与现有公开接口。只更新读者文档、架构 PNG 及本次状态/审查记录，不改变生产、示例或测试代码，不重开已完成的产品里程碑。

用户随后要求删除 README 架构图顶部的“数据库主动Poll消息系统中的消息”大标题；只编辑该 PNG 的标题，保留其余图示、README 居中显示与 720 像素宽度。使用内置 imagegen，完成后目视检查并覆盖原资源路径。

用户进一步询问“笔数”是否表示两笔交易；README 改用“成交笔数”，并明确两条不同 trade_id 的成交记录对应 trade_count=2，累计成交量另为 120。保留用户并行修改的其他文字和删减，仅对本次示例及审查指出的 Status 术语作定向修订。

## 验收

- [x] 展示 `Catalog::create/define_sql` 定义输入表及两级 MV，解释依赖和按 key 增量维护。
- [x] 展示条件 Trigger、Clock、显式 session、固定输入、静态函数请求及统一 bind；省略的上下文有真实源码入口。
- [x] 展示内置 `trigger_audit` 的 SQL 与 SDK 查询，说明 scope、触发/受理/成功区别、字段及有界保留。
- [x] 正文中文，代码和注释英文 ASCII；本地链接、anchor、代码围栏和 diff 检查通过。
- [x] 对照现有实现与针对性验证，由两位 fresh-context reviewer 在同一轮并行审查；处置发现并记录证据与边界。
- [x] 架构图大标题已删除，其他模块、标签、连线及 README 显示方式保持一致。

## 工作状态

| 工作 | Owner | 状态 |
| --- | --- | --- |
| 当前规格、公开接口及示例核对 | root | 完成 |
| README 示例与必要导航 | root | 完成，术语修订和独立审阅已闭环 |
| 文档片段与现有针对性验证 | root | 完成，下方记录范围 |
| 架构图标题删除 | root | 完成，root 和两位 reviewer 已目视核对 |
| 一轮并行独立审查 | root / reviewers | 同轮双 reviewer 完成，唯一去重发现已修复 |

## 当前状态与决定

采用现有 `trades -> trade_totals -> market_features` 及 `volume_threshold` / `periodic_score`，不引入新业务 API。请求仍需显式 candidate session；`capture_current` 读取受理时的 MV；审计记录 emitted occurrence，不等同于成功结果。已有产品验证只作为历史背景，不作为本次检查结论。

Changed files：`README.md`、`examples/streaming_database/README.md`（审计查询导航）、`docs/image/eventflow_streaming_architecture.png`、`specs/INDEX.md`、本规格及本次审查记录。

## 验证、剩余缺口与下一步

已读取当前 Catalog/Trigger/ClusterClient/audit schema、流式示例及相关合同。

- 三个示例目标、`trigger_audit_test` 和 `trigger_audit_request_test` 通过当前 Clang Debug 构建；日志 `/tmp/eventflow-readme-usage-build.log`。
- 从 README 提取全部四个 C++ 片段，复用原示例 Schema/函数/session 上下文，临时 CMake OBJECT 目标以 Clang 18.1.3、C++20、`-Wall -Wextra -Werror` 及三 sanitizer 编译通过；日志 `/tmp/eventflow-readme-usage-snippets.log`，临时源 `/tmp/eventflow-readme-usage-snippets-0ksivn16/snippets.cpp`。这证明片段编译，不声称新增 helper 已运行联网调用。
- `ctest --test-dir build --output-on-failure -R '^(trigger_audit_test|trigger_audit_request_test|streaming_database_example)$' -j 2`：3/3 通过，exit 0，共 23.99 秒；日志 `/tmp/eventflow-readme-usage-ctest.log`。包含既有触发审计合同及真实五进程正常示例，不是全量产品回归。
- 三条 README DDL 与当前 definition.cpp 归一化空白后完全一致；四个审计 SELECT 的投影字段均匹配当前内置 schema。
- 初轮四个 Markdown、86 个本地目标、2 个 anchor、19 个代码围栏及围栏内 ASCII 检查通过；收尾检查扩大到本轮两份审查报告，日志 `/tmp/eventflow-readme-usage-docs.log`，并再次检查 `git diff --check`。

最终四段 README C++ 与已编译临时源重新组装后逐字相同，片段源 SHA-256：`3bc731434e92e6451fc439a142f932eb0d93d65f2a03eb5747c6fe6b495371e0`。文字修订没有改变编译对象。

一轮并行 fresh-context [API/语义审查](reviews/README-usage-api-review.md)和[读者/配图审查](reviews/README-usage-reader-review.md)均完成。去重发现只有 R1/P3：宏失败路径混称错误码与 StatusOr；现已明确返回 Status、由工厂 StatusOr 承载，两位 reviewer 均重新对照宏确认修复。原发现保留于报告。期限上限的可选扩写经源码核对不属于错误，不扩大正文。

前次示例补充范围无剩余任务，无需为该范围再开审查轮；下方追加说明单独跟踪用户的新授权。未运行完整回归、Release、Docker 或真实 Kafka；本次没有代码行为修改。新 SDK helper 已编译但没有现场联网运行，已有五进程验证器仍主要检查 MV 和评分，不冒充新 helper 的执行证据。


## 图片编辑记录

使用内置 imagegen，编辑后覆盖 `docs/image/eventflow_streaming_architecture.png`；生成源保留在 `/home/obscure/.codex/generated_images/01a0a112-1102-7be1-93b0-f420b0ec9b3c/exec-f30bb53b-c88a-4067-b796-27125851f057.png`。root 已目视确认标题删除，其他模块、标签、箭头和图例保持；README 居中与 width=720 检查通过。原图 1448 x 1086，生成图 1447 x 1087，存在 1 像素的画布尺寸差异，不宣称未编辑区域逐像素相同。最终 PNG SHA-256：`3616a0d78af18700d39582b9a20b59fd6597bd6a10efebcc93d464c3ab4e4ed5`。

本次最终提示词原文：

> Use case: precise-object-edit. Asset type: existing EventFlow README architecture diagram. Image 1 is the edit target. Primary request: remove only the large dark-blue heading at the very top that reads exactly "数据库主动Poll消息系统中的消息". Erase that heading and seamlessly restore the surrounding white/off-white background in that top band. Keep the canvas size and composition, all other text, every box, arrow, label, icon, color, position, line weight, and the bottom legend unchanged. The diagram content below the heading must remain faithful to the original. Do not replace the heading with any new text. Do not redesign, crop, rescale, reorder, relabel, add, or remove any other diagram element. Preserve the original 1448 by 1086 canvas if possible.

## 追加说明：先讲 Schema、业务 case 与计算 DAG

Status: done
Owner: root

用户要求在 MV / Trigger 代码前，先简要描述数据 Schema，再讲清业务 case，用同一张图画出当前 demo 的 feature computation DAG、组合触发条件以及各 Trigger 触发哪个特征计算。随后追加简要解释 MV 为什么适合计算依赖、CEP 为什么适合事件依赖，让无背景读者理解方案选择。顺序为 Schema -> case -> DAG -> 建模理由 -> code。

用户进一步指定核心表述：CEP 背后的状态机是一张图，级联物化视图也是一张图，允许使用 Graph-Based 术语。新增理由以 Feature Computation Graph 与 Trigger State Graph 的节点/边对照开头，说明两图通过触发请求连接；CEP 状态图可以有环，不泛化为任意任务 DAG 调度器。

继续使用当前成交 demo：trades、trade_totals、market_features；阈值规则为成交事件、更新后总量 >= 100、活动 session 和 once 约束，周期规则为 200 ms Clock 与活动 session。两条规则独立请求同一个静态评分函数，各自捕获受理时的输入并保存独立结果；不能画成两规则必须同时满足，不能虚构 CEP 序列。

仅在 README 新增概览与原始 Mermaid 图，配中文说明、英文 ASCII 图源码。当前架构 PNG 与现有代码片段保持原样。Changed files：README.md、本规格、INDEX，以及本轮追加说明的独立审查记录。

- [x] 输入字段、类型、主键、分区和成交笔数含义明确。
- [x] 业务目标、两类触发及 70/50/30 输入的结果先于代码说明。
- [x] 单张 DAG 区分持续 MV 数据依赖、触发控制边、受理时 capture 和 score 目标，保持每请求独立性。
- [x] 用具体例子解释关系运算/引用与计算图、事件条件/顺序/窗口与 CEP；区分事件模式和任意任务依赖调度，以及本 demo 的简单 Trigger 与真正序列 Pattern。
- [x] Mermaid 实际渲染及目视检查、结构/链接/围栏/ASCII 检查通过，保留用户同期改动。
- [x] 本次追加范围的一轮并行 fresh-context 审查完成并处置发现，不重跑无关系统测试。

已核对 example definition、generated request 装配、公开接口及原解释，并读取 RisingWave 的 CREATE MATERIALIZED VIEW 和 Flink 1.19 CEP 官方文档，引用仅支持通用建模说明，不用外部系统能力证明当前实现。概览与 Graph-Based 解释已补充；渲染、图结构检查及同轮双 reviewer 独立审查均完成。本节不沿用前次 C++ 编译或 CTest 作为 Mermaid 的验证证据。

追加说明验证记录：

- 图源位于 README 的唯一 Mermaid 围栏；图节点/边为英文 ASCII，中文正文解释字段和图例。保留用户同步修改的链接标签与其他文字。
- 使用本机既有 Mermaid 11.17.2、Playwright 和 Chromium 实际解析/渲染，未向项目添加第三方依赖或构建下载。命令 `node /tmp/eventflow-readme-case-render.mjs`，日志 `/tmp/eventflow-readme-case-dag-final-render.log`，产物 `/tmp/eventflow-readme-case-dag-final.{mmd,svg,png,json}`。
- 初审图为 10 个节点、13 条边；图结构无环，两条 Trigger -> 独立 capture -> score 路径和 MV 依赖检查通过。初审图源 SHA-256 为 `d62a0d63dfcae7935ed8c08d32d822e40ede3e1cc94dc74bfefe05482a9eda00`，旧产物保留；该结构检查不代表初审视觉布局无缺陷。
- 初稿审计旁路与较长边标签发生布局拥挤，删去图中非本次核心要求的审计旁路、缩短请求边标签后重新渲染；正文仍说明审计与成功的区别。root 已目视核对最终图。
- 浏览器 MCP 首次渲染调用长时间未返回，已终止，改用上述本地渲染完成；不把终止的调用作为成功证据。
- 当前 Schema -> case -> 单图 -> Graph-Based 解释 -> code 顺序、围栏及 ASCII 检查通过。最初顺序检查因用户同步保存旧段落未通过，现已在最新文件上保留用户文案并补足 Graph-Based 说明。

独立审查的定向处置：

- R1/P2：两条正确的依赖边在自动布局中穿过无关节点，导致输入来源可能被误读。将持续 MV、阈值路径与周期路径分组，保留 10 个业务节点和 13 条业务边端点后修复；原图和中间分组版均保留。
- R2/P3：分组后的进入箭头穿过组标题。移除辅助分组标题文字，保留边框、业务标签和正文图例，避免文字与连线重叠。
- 采样证据纠正：第一版临时脚本未匹配带前缀的 SVG path ID，跳过全部路径；撤回其空 collisions 作为验证证据。脚本改用 data-id、规范化节点 ID，并强制检查路径/节点覆盖计数。独立 reviewer 以原图复现两处碰撞，再确认分组版无非端点业务节点碰撞。
- 最终图源及渲染产物为 `/tmp/eventflow-case-dag-clean.{mmd,svg,png,json}`；图源 SHA-256 为 `239e8c9868d87b1ddc5cd04461e4aa4aa0ec3badfe0739047adf453788b1ef1c`，与 README 逐字相同。修正后的 `node /tmp/eventflow-case-dag-layout-check.mjs clean` 实际检查 13 条路径、10 个节点，碰撞为 0；日志 `/tmp/eventflow-case-dag-clean-render.log`。root 已重新目视核对。
- `python3 /tmp/eventflow-readme-case-check.py` 校验阅读顺序、单图、依赖图无环/分支独立、算术、本地链接/围栏/ASCII、当前图源等于渲染对象，以及既有 C++ 片段与架构 PNG 未变；日志 `/tmp/eventflow-readme-case-docs.log`。

同轮[语义审查](reviews/README-case-graph-semantics-review.md)和[读者/配图审查](reviews/README-case-graph-reader-review.md)完成；R1/R2 均已定向复验关闭。最终 reader 独立采样也确认 13 条路径、10 个节点、0 处非端点节点碰撞及 0 处标题相交。原发现、无效证据说明与处置均保留，不再开新审查轮。

本次追加授权无剩余任务。`python3 /tmp/eventflow-readme-case-check.py` 与 `git diff --check` 已通过，交付前刷新日志。此轮没有运行 C++ 构建/测试，现有代码片段和架构 PNG 没有因本次追加说明改变。
