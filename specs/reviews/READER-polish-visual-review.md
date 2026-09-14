# 读者文档优化：独立全局视觉与文档工程审查

Status: review_complete_pending_targeted_fixes
Reviewer: reader_polish_final_visual_review
Reviewed: 2026-09-14

## 审查范围与依据

本报告是[读者文档与配图可读性优化](../reader-doc-polish.md)唯一一轮并行 fresh-context 全局审查中的独立视觉与文档工程审查。开始时独立读取根、docs、specs 的 AGENTS、规格索引、本轮规格及[原始读者审阅](reader-documentation-review.md)，未依赖实施者的通过结论，也未另开子审查轮次。

用户批准的是文字与图片可读性改进。C01-C17 仍待后续决定；尤其 C09-C11 不以本轮配图为由补齐 capture、重构逐函数完成循环或重新定义 Latest 提交。本审查只比较原图与新图的呈现，检查是否因简化而新增歧义，不重新宣布产品功能、构建或测试通过。

检查包括新增八张 PNG、根 README 与研究/实现导读中的图片入口、保留原图、读者 Markdown 的本地链接与锚点、代码围栏。并行进行的 Clock 示例改名、新引擎 Clock 示例和 Docker 工作仅随当前文档做链接存在性检查，不评审其行为，不修改或回滚其文件。本审查只写本报告。

## 发现与处置

| 编号 | 级别 | 独立发现 | 建议与当前处置 |
| --- | --- | --- | --- |
| V01 | 中 | 新 Timely 图在每个应用进程内各展开一个 Worker；图后文字仍写“图中每个应用进程包含多个 worker”。定位：research 的“顶层架构：应用进程与 Worker”。 | root 已采纳：明确概览每进程仅展开一个 Worker，多 Worker 结构见原详图。交付前由 root 修正并追加定向复核证据；本报告不以采纳意图代替已修复。 |
| V02 | 中 | 新 Materialize 图的紫色“部署与进度”箭头只有从 environmentd 向 Replica 的下行方向；原详图的“命令 / 进度与结果”是双向。图后正文也明确 Controller 接收进度与结果。简化后会把进度误读为下发消息。 | root 已采纳：用 imagegen 定向恢复紫色双向连接并标“部署命令 / 进度反馈”，同步图注；保留其他图形。交付前需实际预览修订图并追加证据。 |
| V03 | 低，非阻塞，已处置 | RisingWave Temporal Join 与 Incremental 共享依赖原图仍存在，但其正文只展示重排图，没有原版本入口。 | 两张属于相同信息重排，旧图并非包含更多细节。root 决定在维护规格明确两者不是详图，不把旧 Incremental 的歧义重新暴露为权威说明。接受该处置；六张真正概览均保留详图入口。 |

V01-V02 是本轮新增呈现的局部问题，不是 C 类行为差异。原请求图仍有 C09-C11 登记的既有歧义，本轮未要求借机改语义。

## PNG 实际检查

通过 `view_image` 实际打开新增图片。除请求图为 1189×1323 外，其余七张为 1536×1024，均为 RGB、不含透明通道。工具中的新图预览保留上述像素尺寸，检查标题、节点正文、连线端点、图例、裁切及相邻说明；没有把元数据检查当成实际看图。

research 的新闻概览指定 1000px 宽，另外四张新图指定 1100px，且有 `max-width: 100%` 和白底。根 README 与实现导读的三张 EventFlow 图用普通 Markdown 图片，实际宽度由页面容器决定。1000/1536 和 1100/1536 分别约为 65% 和 72%；这些只是比例推算，本轮没有在浏览器中按这些 CSS 宽度重现页面，不能据此声明手机、900px 列宽、深浅主题全部通过。

| 新图 | 实际视觉判断与原语义边界 |
| --- | --- |
| [news_feature_overview.png](../../docs/image/news_feature_overview.png) | 已查看。1→6 采用两排顺序，3→4 绕行于框外；业务输入、持续状态、触发、固定输入、共享计算、查询六部分的字和箭头清楚。底部保留持续状态可变与请求固定输入的区别，详图入口存在。没有把概览用于证明细节中的 C01/C13。 |
| [timely_architecture_overview.png](../../docs/image/timely_architecture_overview.png) | 已查看。只展开左 Worker，右侧保留同样机制；蓝色双向记录与紫色双向进度可区分，Probe 和实际输出没有混为同一节点。图自身无明显裁切，图旁数量说明见 V01。 |
| [materialize_architecture_overview.png](../../docs/image/materialize_architecture_overview.png) | 已查看。environmentd、Cluster、Replica、clusterd/Workers、Persist 与后端层次比原图更易辨认，SQL 请求与结果分开表示；新增下行进度歧义见 V02。 |
| [incremental_shared_computation_readable.png](../../docs/image/incremental_shared_computation_readable.png) | 已查看。peak 到两个观察输出的共享分叉明确；灰色无观察依赖节点与底部限定避免把“无直接 Observer”误写为绝不计算。stabilize 的调度虚线与数据依赖能区分。 |
| [risingwave_temporal_join_readable.png](../../docs/image/risingwave_temporal_join_readable.png) | 已查看。上排持续维护 MV，下排由新增 Trigger 驱动；虚线明确标“左侧 Trigger 变化时读取”，底部说明市场更新不会独立发起推理。与原图的 lookup 触发方向一致。 |
| [eventflow_architecture_overview.png](../../docs/image/eventflow_architecture_overview.png) | 已查看。应用→Meta→Compute→共享 KV、两个业务 shard 与 control shard 清楚；Function pool 位于各 Compute 内，保留单 shard owner。Compute A 到 Data shard 0 的线较折返但端点可辨，没有连入其他 shard；原详图入口存在。 |
| [eventflow_sql_overview.png](../../docs/image/eventflow_sql_overview.png) | 已在实施者确认最终配色后实际打开，并与原 SQL 图对照。底部 SELECT 的三条查询箭头均为蓝色，与“调用 / 数据”图例一致；更新区的读取确认状态及原子提交仍为绿色。四层区分解析/绑定、Catalog、增量更新及查询，文字与端点可辨，未发现新增阻塞。 |
| [eventflow_request_overview.png](../../docs/image/eventflow_request_overview.png) | 已查看。编号调整为1→2→3→4→5→6，主要调用线没有穿过标题，DAG 虚线与返回线分开。仍保留当前输入捕获、executor 框内逻辑 DAG、逐函数返回 owner 的文字，以及绿色 Latest 框和各自提交图例；它们对应原图 C09-C11，未被本轮悄悄改成另一合同。 |

另实际打开原新闻、Timely、Materialize、EventFlow 架构/SQL/请求、RisingWave Temporal Join、Incremental 共享依赖八张原图做视觉对照。超大原新闻图在工具中缩放预览，只比较整体结构及来源，不声称已逐字复审其所有细字。

最终 SQL 预览对象 SHA256：`6eaee5da5df3a41cd05021f4cd5529eb4006ba0f7f494c93bac7bfab88a6ccc2`。V02 的发现对应修正前 Materialize 概览 SHA256：`e3493878758263c8f6b6016fc64c645d069c56225ef05c5ead6204164a0ba7a8`；之后修改应由 root 追加新对象的定向验证，不覆盖原发现。

## 本地静态证据

使用环境中已安装的 `markdown_it` 解析 CommonMark token，使用 HTMLParser 提取 HTML 的 href/src；没有安装工具、联网、构建或执行文档内的产品示例。链接检查排除 fenced code 和 inline code，标题 anchor 使用渲染后的标题文本，因此标题内的仓库链接不会进入 anchor；对本地绝对 `file:line` 先分离行号并核对文件存在与行号范围。

本轮独立 checker 还以短输入验证以下分支：反引号围栏、波浪号围栏、链接标题、绝对文件加行号，以及围栏中 `[](int x)`/`[x](int y)` 不会被当成 Markdown 链接。自检全部通过。

当前全量读者检查快照为15份 Markdown：根 README、docs 内除 AGENTS 外5份、8份示例 README、cpp3rdlib README。共检查409个链接或图片引用，其中330个本地引用、18个绝对 file:line；151个代码围栏。失效路径、失效本地标题 anchor、越界引用行号及未闭合围栏均为0。HTTP 链接未访问，存在性检查不证明命令可执行或源码行为正确。

实际识别到7段 Mermaid：架构文档3段、news 示例2段、typed Pattern 示例1段，以及 research 中使用 `~~~mermaid` 的1段。没有沿用原审阅“6段”的数量。只核对围栏与源码入口，未运行 Mermaid 渲染器，不能声明布局、连线遮挡或 Mermaid 语法渲染通过。

原图保留检查读取首次审阅生成的 `/tmp/eventflow-reader-review-jrvqtvyv/baseline.json`，对其中全部24张 PNG 重新计算 SHA256，结果24/24一致、0缺失、0改动；命令退出码0。当前图片目录共有32张 PNG。此前根 README/implementation_guide 的架构详图、SQL详图、请求详图，以及 research 的新闻、Timely、Materialize 详图入口均指向仍存在的原文件。

## 剩余检查与通过边界

独立全局审查已完成：八张新 PNG 和八张对应原图均已实际打开；本地静态检查及原图保留检查通过。V01/V02 已获 root 采纳，仍须由实施者完成限定修正、实际检查结果并在本报告追加证据；目前不宣称这两项已修复。整改只做定向复核，不发起新的 fresh-context 审查轮次。

本报告的通过边界仅为本次获批的读者呈现与静态文档工程，不包括 C01-C17 的解决、不包括完整产品能力、外部系统最新版事实、C++构建/测试、真实 Kafka或容器验收。保留原详图与后续差异记录，不等于所有图文/代码已完全一致。

## 实施者处置与定向复核

Owner: root；本节为实施者追加，独立发现原文保持不变。

- V01 已修正：research 图后明确“概览在每个进程中只展开一个 worker，详细图再展示同一进程中的多个 worker”。已通过文字定位复查。
- V02 已修正：通过内置 imagegen 定向调整 Materialize 的两条紫色连接为双向，标签为“部署命令 / 进度反馈”；实际查看生成图，确认两端箭头、共享 Persist 线和其他标签保留，再按字节复制到项目。最终 SHA256 经工具核验为 `128e629113fc23f255f1e5012db4c323894b5d87d19b0f76dd5dc141aeda1a80`。
- V03 保持已接受的处置：六张概览提供详图，两个重排版本只保留原文件及 provenance，不将旧歧义文本重新作为教程入口。

最终图片和提示词来源见 [reader-image-prompts.json](../verification/reader-image-prompts.json) 与 [reader-eventflow-image-prompts.json](../verification/reader-eventflow-image-prompts.json)。整改后只做定向验证，没有追加 fresh-context 轮次。实际 Markdown 页面和 Mermaid 渲染仍未作为通过证据。
