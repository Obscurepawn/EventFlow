# 读者文档与配图可读性优化

Status: done
Owner: root
Updated: 2026-09-14

## 授权与范围

用户在全局审阅后授权先修文字与图片，明确暂不处理图文与代码差异。实施依据为[读者审阅](reviews/reader-documentation-review.md)中的 T 类和纯呈现 G 类问题；C01-C17 保留为后续决定，不修改代码、不重新定义业务行为，也不把旧设计自动替换为源码现状。

修改根 README、docs 面向读者的文档和相关示例 README：改善入口、目录、术语、上下文、段落顺序及图形可读性。PNG 使用 imagegen 生成可读版本或概览，保留原详图；Mermaid/文本图可调整布局、分组和解释，但保留涉及 C 类差异的关系与语义。第三方依赖清单及资产保持原样；依赖 README 的优化仅在有必要且能保留清单一致性时实施。

## 不变量与验收

- 作者个人前言、未来规划、联系方式和开发方法保留，不改写为统一宣传文案。
- API 示例、命令、公式、期限、输入轨迹与结果语义保留；代码块使用英文 ASCII，中文说明放在正文或图注。
- 保留读者需要的具体技术内容，通过顺序和解释降低理解成本，不把全文缩成能力列表。
- C 类差异不因润色被悄悄解决或删除；必要时保留明确的原设计/历史注记。
- 图片在项目内落盘；正文概览与详细图有明确入口。不得将项目资源只留在工具默认目录。
- 受影响本地链接/anchor、围栏、示例代码块、图片引用和重要文字检查通过；实际检查图片结果。
- 完成一轮并行 fresh-context 全局文档审查，记录发现、处置、验证和边界。纯文档修改不运行无关 C++ 构建，不声称产品测试新增通过。

## 任务

| 工作 | Owner | 状态 |
| --- | --- | --- |
| 根 README、术语与入口整理 | root | 完成 |
| 调研文档目录、上下文与措辞 | research author | 完成 |
| 实现导读、运行手册、文档导航 | guide author | 完成 |
| 架构文档与七个示例说明 | architecture author | 完成；保留并行 Clock 改名 |
| PNG 概览/重排、原详图链接与图注 | root / guide author | 完成，新增 8 张，原 24 张保留 |
| 静态检查及一轮独立审查 | root / independent reviewers | 完成；局部发现已处置并定向复核 |

## 当前状态与交接

本轮已完成文字与图片可读性维护。根 README 先给运行和结果，再展开 API；作者个人规划、联系意图与开发方法保留。调研目录、层级、术语和跨文档章节引用已修正；导读与示例补充必要上下文，输入时间线和结果先于高级构建细节。

C01-C17 未作为本轮实现任务处理。涉及这些事项的原文或图结构保留；新增 typed 图注只在图旁重复已有正文的受理前提，没有改变边或 API。Rocks/线程池等旧开发记录移到明确历史注记，未借润色改写其快照事实。

## Changed files

- 根 README.md。
- docs/README.md、core_delivery.md、implementation_guide.md、feature_computation_architecture.md、research.md。
- 原七个示例 README；Clock 按并行工作后的 examples/sdk_clock_polling_demo/README.md 处理。engine_clock_trigger_demo、Docker 和相应实现由并行任务新增，本任务仅核对相关链接，未接管其功能。
- docs/image 下新增 news_feature_overview.png、timely_architecture_overview.png、materialize_architecture_overview.png、eventflow_architecture_overview.png、eventflow_sql_overview.png、eventflow_request_overview.png、incremental_shared_computation_readable.png、risingwave_temporal_join_readable.png。
- 本规格、specs/INDEX.md、原审阅的后续处置说明、本轮两份独立审查报告及两份图片生成提示词记录。

六张 overview 在正文提供原详图入口。Temporal Join 和 Incremental 的 readable 图属于同信息重排，原版本保留为资产历史，不作为额外详图入口；旧 Incremental 的歧义句不重新作为教程结论。未删除任何原图。

所有 PNG 使用内置 imagegen；初稿及局部修正共选定 8 张。生成结果在实际查看后复制到项目，未使用 Python 绘图或 API fallback。提示词与来源保存在[调研图记录](verification/reader-image-prompts.json)和[EventFlow 图记录](verification/reader-eventflow-image-prompts.json)；这些是过程证据，不是产品运行依赖。

## 验证与独立审查

- 最终 CommonMark/HTML 静态检查覆盖当前 15 份读者 Markdown（含并行新增示例与未改的依赖说明）：333 处本地引用有效，151 个围栏内容为 ASCII，检查退出码 0。标题 anchor 按可见标题文字生成，处理了 URL 编码、波浪线围栏和绝对 file:line；没有把 C++ lambda 当链接。
- 8 张新 PNG 均实际查看；SQL 查询箭头、Timely 输入标签、Materialize 共享存储/双向进度线及请求图遗漏的原有结果存在条件，均经定向修正后复核。新文件可解析，原 24 张 PNG 的 SHA256 全部未变。
- 原案例和受保护的 C 类表述定向检查通过；研究中的源/capture 同批承诺、Always Retry、README 共享祖先原保证及条件示例原输出仍保留。可执行 API 代码未因中文润色被重写；11 个研究示意围栏仅做等义英文/ASCII 标签与箭头转换。
- [独立文字审查](reviews/READER-polish-text-review.md)与[独立视觉/工程审查](reviews/READER-polish-visual-review.md)在同一 fresh-context 轮并行完成。历史证据标签、示例计数、章节编号、gate 图注、内部阶段编号、Timely 邻文和 Materialize 方向发现已处理；两份报告保留原发现，并追加实施者处置。没有再开第二轮。
- 本任务没有编辑生产、测试或构建代码。工作区并行任务修改了 CMake、Clock 目录，并在独立审查后继续修改 8 个 Catalog/Engine/请求相关源文件；这些变化已保留，不归因于本任务，也不声称全工作区源码一直未变。独立审查报告的源码 hash 只对应各自核对时点。

未运行 C++ 构建或产品测试，未渲染实际 Markdown 浏览器页面或 Mermaid。PNG 已通过实际图像预览；不能据此宣称全部窗口尺寸和主题已经验收。此次完成只表示已授权的文档呈现改动与审查处置完成，不表示 C 类差异已解决。

## 交接

文字与图片优化完成，C 类事项保持后置。用户随后明确要求将整个 specs/verification 目录加入 Git 忽略规则，已在根 .gitignore 添加 /specs/verification/。目录此前没有已跟踪文件，本地内容保留；此操作没有删除或移动验证记录。后续准备独立源码交付时，可另行整理对本地验证记录的引用。
