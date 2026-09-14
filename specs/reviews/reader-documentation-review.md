# EventFlow 读者文档、配图与代码一致性审阅

Status: review_complete_pending_decisions
Owner: root
Reviewed: 2026-09-14

本记录供用户决定后续修改。审阅以第一次接触 EventFlow、懂基本编程和数据库但不了解项目开发历史的读者为对象。读者文档描述是差异核对的基准；源码说明当前行为，specs 只提供决策背景，不自动决定哪一方应当修改。

本轮未修改被审阅的文字、图片或代码。所有建议和实现差异均待用户决定，不能把本记录当成已批准的改写或开发规格。

后续处置：上述为原审阅时状态。用户之后批准先修文字和图片，明确暂不处理 C 类差异；已按[可读性优化规格](../reader-doc-polish.md)完成实施及一轮并行独立审查。下方原始发现与行号继续保留供追溯，不覆盖为“从未存在问题”。

## 1. 主要结论

最值得先处理的是下面五类问题：

1. **当前实现、早期设计和开发记录混在一起。** 架构文档仍写线程池接入中；调研前文和结尾对快照读取、提交原子性有不同表述。读者需要自行判断哪段有效。
2. **先给内部术语，再给业务含义。** owner、candidate、occurrence、受理、epoch、固定输入在入口段落中同时出现；读者不容易分清“条件成立”“请求已保存”“函数执行完成”。
3. **阅读入口过重。** 根 README 的快速上手在第 174 行才开始，结果解释在第 395 行后；中间的大段类型声明适合 API 教程。调研目录还有不存在的章节。
4. **部分图同时承担总览、实现说明和异常协议说明。** 细字和长连线需要放大才看清；重要限定恰好写在最小的字里。请求图的编号顺序、返回路径和提交颜色还有具体歧义。
5. **确有需要决定的图文与代码差异。** 包括提交分阶段、共享函数重试、两种 capture、SDK 路由、远程任务、恢复后的分片归属和 KV 格式。它们不能全部当成文字润色处理。

建议保留现有文档中的具体输入时间线、预期结果、SQL 例子、失败反例及源码入口。它们已经提供了很好的学习材料；主要需要调整顺序、补充定义，并把不同层次的信息分开。

## 2. 覆盖范围与检查方法

文字覆盖 14 份面向读者的 Markdown：根 README、docs 内除 AGENTS 外的 5 份文档、7 个示例 README、cpp3rdlib README。AGENTS、specs 和审查日志作为背景材料，不按入门教程标准评判。未读取私有题面 PDF，未修改参考仓库。

图形覆盖 docs/image 下全部 24 张 PNG：20 张被上述文档引用，4 张未被引用；均由审阅者实际打开查看。另检查 6 段 Mermaid 和文中的文本流程图。Mermaid 只检查源码结构和语义，未完成浏览器渲染；PNG 的缩放意见依据图像预览、原始尺寸及正文指定宽度，不等于实际 Markdown 页面在所有窗口和主题下的视觉验收。

源码核对集中在文档涉及的公开入口和关键链路：Meta/SDK、Compute 装配、线程池、SQL/KV 编码、Trigger/capture、请求受理与完成、Latest、源确认和 checkpoint。没有重新运行构建、CTest、真实服务或外部产品验证；测试入口仅用于后续决策和验证规划。

分工：root 审阅根 README、示例、依赖说明、4 张未引用图片并汇总；reader_research_review 通读调研及 10 张配图；reader_architecture_review 通读产品架构、3 段 Mermaid 及 7 张组件图；reader_guides_review 通读实现导读、运行手册、文档导航及 3 张 EventFlow 配图。之后按新增要求补充了限定源码核对。这是本次读者审阅，不是产品里程碑验收。

下文位置为仓库相对路径和本次快照行号。优先级“高/中/低”表示阅读阻碍或决策影响，不是代码缺陷严重级别。

## 3. 逐文档判断

| 文档 | 优先级 | 主要需要优化的部分 | 建议保留 |
| --- | --- | --- | --- |
| [README.md](../../README.md) | 高 | 3-9 行开篇中英术语密集；174 行才到快速上手；193-286 行类型展开先于结果；369 行先用 A/B/C 后解释；444 行出现 occurrence/电平/crossing | 项目定位、作者前言与方法说明、接入方式表、目录导航、实际环境要求；建议调整位置，保留个人表达 |
| [docs/README.md](../../docs/README.md) | 中 | 第 3 行定义过密；第 9 行“题面”依赖私有背景；导航缺少根 README 优先推荐的 typed Pattern 示例 | 按读者问题组织的短导航 |
| [docs/core_delivery.md](../../docs/core_delivery.md) | 中 | 第 75-101 行先给位置编码 Row 和内置表查询，104 行才说明字段；第 137 行恢复步骤堆入内部协议名 | 构建、四终端启动、HTTP 输入、预期结果、恢复到新目录的完整步骤 |
| [docs/implementation_guide.md](../../docs/implementation_guide.md) | 高 | 第 3 行未定义术语；92/473 行 A/B/C/D 上下文；319/365/451 行片段的变量前提；437/644 行历史纠偏与限制堆叠 | SQL 到 AST、键值布局、增量更新、两种输入捕获、Latest 乱序完成例子和源码路径 |
| [docs/feature_computation_architecture.md](../../docs/feature_computation_architecture.md) | 高 | 第 5-9 行历史运行/审查状态；第 54-67 行调研修订任务；主图混合当前与目标；提交语义缺少先行解释 | 两类依赖的设计理由、具体键值例子、110/95 的捕获差异、严格下一条新闻反例 |
| [docs/research.md](../../docs/research.md) | 高 | 目录/编号；第 3 章历史范围到结尾才修订；首图过密；结论突然引入项目内部协议 | 逐系统机制分析、offset 100/101 恢复例子、frontier/capability 轨迹、实际重算对照表 |
| [examples/sql_query_demo/README.md](../../examples/sql_query_demo/README.md) | 低 | 第 13-17 行高级构建变量可后移；末尾与其他示例的能力对比可以缩短 | 90+30=120、撤回后变为 30、查询结果变空的完整例子；可作为入门写法参照 |
| [examples/typed_pattern_demo/README.md](../../examples/typed_pattern_demo/README.md) | 中 | 第 15/47 行 MemOnly/题面；117/171 行 binding/intent；Mermaid 把 gate 画到函数节点 | 第 17-43 行先看结果、A/B/C/D 表、40.6/94.5/-11 对照，整体结构很好 |
| [examples/news_feature_demo/README.md](../../examples/news_feature_demo/README.md) | 中 | 第 11 行“假接口”、105 行“前面明确配置”但前文没有配置示例；226 行“武装确认”；228 行迭代历史 | 条件/确认/失败/Clock 表，输入时间线与同一次请求的固定输入 |
| [examples/feature_computation_demo/README.md](../../examples/feature_computation_demo/README.md) | 中 | 第 3 行候选/受理/固定输入先于解释；图中 A/B/C/D 在第 65 行才解释；31-40 行文本图连线对齐 | 四条业务路径、Ready 精确条件、新闻修订/候选到期的时间线 |
| [examples/condition_trigger_demo/README.md](../../examples/condition_trigger_demo/README.md) | 中 | 第 13-28 行构建细节先于业务；第 111 行 TriggerWitness 与本例目标关系弱；输出示例应与当前程序核对 | 价格 100.0 到 100.3、成交量 110、条件仅受理一次的对照 |
| [examples/clock_trigger_demo/README.md](../../examples/clock_trigger_demo/README.md) | 中 | 高级启动选项可后移；第 92-99 行审计字段缺少业务解释；重复说明全部四条路径 | 4.9s 更新报价、5s 无输入计算、源位置不变的对照 |
| [examples/checkpoint_recovery_demo/README.md](../../examples/checkpoint_recovery_demo/README.md) | 中 | 恢复主题之前重复全部触发策略；第 98 行 owner 静止/冻结控制状态需要解释 | 两代进程、两个 key、重放去重、110 到 135 的可观察结果 |
| [cpp3rdlib/README.md](../../cpp3rdlib/README.md) | 低 | 第 38 行之后准备历史与使用说明可以分区；正常使用者只需先看到平台、版本、链接、校验方法 | 精确版本、许可证、来源、ABI 和 SHA256；这里的技术细节有明确用途，无需统一简化 |

示例共有的改进是：在一条运行命令后先给“会发生什么”和预期结果，再列完整配置变量、打包分支和源码验证范围。各示例仍应保持可独立运行，不必为减少重复而让读者频繁跳转。

## 4. 文字问题与可直接讨论的改写

### T01：入口先讲一件具体的事，再引入名词

位置：README.md:3-9；docs/README.md:3；docs/implementation_guide.md:3；架构文档:15-17。

现有入口同时使用 Catalog、MV、Trigger、owner、固定输入和 Latest。建议先给完整过程：

> EventFlow 接收持续到来的新闻和行情，更新统计特征；条件满足或定时器到期时，保存本次计算所需的数据，再交给后台函数计算。结果保存为可通过 SQL 查询的记录。

随后解释 Catalog 记录依赖、MV 保存持续更新的查询结果、Trigger 决定何时创建请求。必要技术名保留，避免让读者从英文名称推断项目特有含义。

### T02：根 README 的上手路径应更短

位置：README.md:11-174、193-286、395-402。

建议顺序为“项目解决什么问题 → 支持的接入方式 → 环境前提 → 一条运行命令和结果 → 简化架构 → 深入阅读入口”。作者前言、开发方法和目录树保留为独立章节；完整 SchemaTraits、期限/会话/gate 细节放在专门示例或 API 教程中。无需删除个人前言，也不把项目介绍改成统一营销文案。

### T03：修复调研目录和编号

位置：docs/research.md:21-24、132、326、375、383、402。

目录列出正文不存在的第 5、6、7 章；第 132 行引用的附录名称也不再存在。术语章节出现重复 2.4、从 2.4.3 回退到 2.3.4/2.3.5，以及同级章节的 Markdown 层级不一致。需要以实际正文重建目录，同时同步标题编号与 anchor。它直接影响阅读导航，不只是排版偏好。

### T04：历史状态与当前说明分开

位置：架构文档:5-9、54-67、801、882；docs/research.md:674-687、2009-2013；implementation_guide.md:437、644。

“3 项去重 P1”“旧日志不代签”“M5 由 MetaService……”要求读者知道开发流程。建议把历史审查、阶段号和修订任务留在 specs；读者文档用明确的当前行为与未来目标表。历史研究目标如果继续保留，应在所在章节入口标明，避免读到结尾才发现需要重解前文。

涉及功能差异的部分不能只按上述建议直接改写，须先处理第 6 节的逐项决策。

### T05：在示例前就地交代数据和变量

位置：core_delivery.md:75-111；implementation_guide.md:319、365、451；news_feature_demo/README.md:105。

运行手册可在输入命令前补充：

> 集群的内置定义已包含 trade 表和汇总成交量的 trade_volume 视图。下面输入股票代码 DEMO 的一笔交易：交易编号为 1，成交量为 120。row 按 ticker、trade_id、volume 的顺序填写。

API 片段可以不独立编译，但应说明 news/ready 是哪些关系 ID、哪些变量来自上一节，并链接完整定义。避免使用“前面明确配置”指代只存在于源码中的配置。

### T06：先讲清触发、受理和完成的差别

位置：README.md:369、490；架构文档:575-624；implementation_guide.md:479-511。

> 条件成立时，系统先记下一条触发记录。输入和执行条件准备好后，系统保存请求及其固定输入，这一步称为受理。后台函数随后执行；结果经校验并保存后，请求才算完成。

这段只作为解释草案。源更新和受理应当是一次还是多次提交，仍须按 C01 由用户决定；不能用通俗改写掩盖该语义选择。

### T07：A/B/C/D 的两套含义不要混用

位置：架构文档:52；implementation_guide.md:92、473；README.md:369；feature_computation_demo/README.md:23-65。

一套 A/B/C/D 指新闻到达顺序，另一套指文本编码/首次预测/条件确认/周期预测的计划 ID。建议图和正文优先使用业务名称；需要展示持久 ID 时写“首次预测（计划 B）”，新闻时间线写“新闻 A”。新闻四步表应放在第一次引用 A/B/C 之前。

### T08：把抽象短语改成可观察动作

以下是按语境使用的建议，不做全仓机械替换：

| 当前用词与位置 | 建议说法或首次解释 |
| --- | --- |
| “发射 occurrence”“电平判断”“不隐含 crossing”；README:444 | “产生一条触发记录（TriggerOccurrence）”；条件每次为真都可触发；若仅在 false 变 true 时触发，需要另定义状态变化规则 |
| “单次武装确认”；news 示例:226 | “进入等待状态后，用下一条新闻确认” |
| “运输重复”；research:2003 | “消息重传造成的重复” |
| “同 key 有状态闭包共置”；架构:418 | “同一个业务 key 所需的有状态计算放在同一个执行者中” |
| “一致切面”“源切面”；恢复章节 | “同一恢复点对应的源位置和各节点状态”；随后再介绍 consistent cut 的专业名称 |
| “owner 静止”；checkpoint 示例:98 | “负责该分片的执行者已停止推进，可保存一致状态” |
| “支持 out-of-memory 的运算”；research:659 | “支持工作集超过内存容量的计算，将部分状态保存在磁盘或远端存储” |
| “Monotonic Atomic View”“Mem/IO overhead”；research:633 | 直接描述“一次读取不混入不相容的新旧值”及其内存、存储和实现成本，若保留术语则补定义 |
| “Durable active book”“Catalog target closure”；EventFlow 图 | “持久化的活动请求记录”“本次请求目标依赖的全部节点” |
| “future release budget”；请求图 | “为完成、取消和超时处理预留容量” |

callback 的“闭包”、clock 的“时钟漂移”等本身有准确技术含义，不应因命中关键词就替换。epoch 还需区分请求候选版本、owner 版本和外部系统 checkpoint 区间，不能全译为同一个“周期”。

### T09：操作手册先给结果，再解释恢复协议

位置：core_delivery.md:137；checkpoint 示例:98。

> Coordinator 暂停接收新输入，等待各 Compute 到达可以保存状态的位置，再把源消息位置、数据版本、Catalog 和运行状态登记到同一个 checkpoint。成功后集群保持暂停；继续运行调用 resume，从快照恢复则按下面的停机步骤操作。

版本/receipt/冻结意图的细节放在后续段落或实现导读；有关恢复行为的选择仍以 C07/C08 决策为准。

### T10：保留边界，但减少回应旧评审的措辞

位置：news 示例:11；implementation_guide.md:48、437、644；多示例末尾 TriggerWitness 说明。

“假接口”“凑齐一个成功部署”“不代签”容易使新读者猜测作者在反驳什么。可以直接说明当前支持的入口、成功条件和恢复要求。与该示例无关的历史见证记录等限制，可移到统一能力说明并保留链接。

## 5. 图片与流程图

### 5.1 优先优化的图形问题

| 编号 | 优先级 | 图片或图块 | 具体问题与待选方案 |
| --- | --- | --- | --- |
| G01 | 高 | news_feature_trigger_graph.png；research:73 | 原图 3815×6560，正文指定 800px 宽，约缩为原宽 21%；细字、四组 Trigger、长距离 Clock/版本/超时连线同时出现。建议入口增加一张“输入→持续状态→触发→固定输入→函数→结果”简图，原图保留为可放大详图 |
| G02 | 高 | eventflow_architecture.png；README:163、implementation_guide:17 | 首次介绍进程时同时展开负缓存、receipt、键格式和恢复限制；两个 Compute 内部几乎重复。建议首页只展开三类服务和主路径，详细内部结构保留在导读 |
| G03 | 高 | eventflow_request_lifecycle.png；implementation_guide:443 | 2 号框只写当前输入捕获；4 号框把整个逻辑 DAG 包在执行器内；5/6 号绿色区域容易读成两次提交。具体代码差异见 C09-C11，先决定语义再改图 |
| G04 | 中 | eventflow_request_lifecycle.png | 编号上排为 1/2/5、中排 3/6、下排 4；3→4 的蓝线穿过标题说明。建议主过程按一个方向排列，循环返回单独画出 |
| G05 | 中 | eventflow_sql_pipeline.png 及请求图图例 | SQL 最下方纯查询路径使用绿色，但统一图例将绿色解释为 confirmed commit/storage boundary；请求图又把所有绿色区域说成独立提交。建议颜色各有唯一含义，查询/返回使用普通调用线 |
| G06 | 高 | timely_top_level_architecture.png | 两套 Worker 内部重复展开，数据与进度往返线多；建议只详画一个 Worker，并编号“记录进入→算子执行→跨 Worker 传递→输出”，进度另用一条解释线 |
| G07 | 高 | materialize_top_level_architecture.png | Cluster/Replica/clusterd/Worker 层次同时展开，紫色“进度与结果”与蓝色“记录与结果”图例有重叠。建议分清控制反馈和查询结果，并就地解释层级 |
| G08 | 中 | incremental_shared_computation_graph.png；research:1853 | 图中“没有 Observer，不执行”过于绝对；同图无直接 Observer 的 peak 仍被其他观察结果依赖。建议写成“不被任何观察结果依赖，本例无需计算”，避免误教机制 |
| G09 | 中 | typed_pattern_demo Mermaid:99 | release_gate 的虚线落在函数节点，看起来只限制函数执行；正文说明它限制请求受理，进而影响 capture_current 的读取时点。建议增加受理节点，把前置条件画到该节点；匹配参数仍在匹配阶段保存 |
| G10 | 中 | news_feature_demo Mermaid:124 | score/risk 直接连各自 Latest，主体图没有显示整请求成功门槛。建议增加共同的成功条件或正常字号图注，避免读成单函数完成即独立发布 |
| G11 | 中 | 架构 Mermaid:280 | SDK 直连、跨 Compute 函数分发和消息源接入混在主图，需按 C03-C05 逐项决定；不能只统一配色就认为已修正 |

首张新闻图有透明背景（RGBA，alpha 范围 0-255），其他 PNG 没有透明像素。该图 HTML 未固定白底；深色页面中的细连线对比需在后续实际预览确认。本轮未据此断言所有渲染器一定显示错误。

三张 EventFlow 图原宽均为 1800px；若页面按 900px 列宽展示，图内文字尺寸会减半。这是比例估算。后续应在实际阅读宽度下验收，不能只检查原始大图是否清晰。

### 5.2 全部 24 张 PNG 的检查记录

图片均位于 [docs/image](../../docs/image/)。高优先图的细项见上表；“保留”指本次未发现显著阅读障碍，不表示外部系统事实已重新验证。

| 图片 | 文档引用位置 | 判断与建议 |
| --- | --- | --- |
| news_feature_trigger_graph.png | research:73，800px | 高；入口简图与详图分开，处理透明底对比，见 G01 |
| stream_processing_components_overview.png | research:696，699px | 大体可保留；“Incremental：依赖失效”宜区分需要重算与永久 invalid |
| flink_cluster_architecture.png | research:959，1100px | 中；控制/数据/快照分类好，SlotPool/PekkoRpcService 等细节可移配套表 |
| flink_job_execution_pipeline.png | research:991，1200px | 中；编号主线好，ExecutionGraph 的 A0/A1/B0/B1 交会连线及 Slot 映射需更清楚 |
| flink_trigger_inference_flow.png | research:1062，522px | 可保留；主流程和字号清楚 |
| flink_next_news_state_machine.png | research:1076，410px | 轻改；解释 Waiting/Armed/Consumed 及业务 session，和 Incremental 版本保持一致 |
| risingwave_top_level_architecture.png | research:1157，1200px | 大体保留；补 Hummock 的职责说明，长读取线可加编号 |
| risingwave_temporal_join.png | research:1255，1100px | 中；2568×348 的横长图缩小时难读，可改成上下两层；保留“左侧变化时读取”的标签 |
| timely_top_level_architecture.png | research:1319，1200px | 高；减少重复 Worker 内部和跨区连线，见 G06 |
| timely_dual_input_operator.png | research:1401，728px | 大体保留；“完成通知”明确为“逻辑时间完成通知” |
| materialize_top_level_architecture.png | research:1506，1200px | 高；区分层级和反馈/结果箭头，见 G07 |
| materialize_external_inference_flow.png | research:1610，456px | 轻改；查询请求和返回特征分别标明，避免动作与箭头方向歧义 |
| csp_top_level_architecture.png | research:1691，1200px | 中；保留构图/运行和线程边界，编号输入队列到输出主路径；解释 rank、active tick |
| csp_shared_feature_dependencies.png | research:1721，634px | 轻改；把实线/passive、虚线/Trigger 的解释补到图旁 |
| incremental_top_level_architecture.png | research:1832，1200px | 中；突出设置输入→stabilize→读取，两个 heap 和 cutoff 细节后置 |
| incremental_shared_computation_graph.png | research:1853，1016px | 中；修正 Observer 条件的孤立表述，见 G08 |
| incremental_next_news_state_machine.png | research:1925，485px | 可保留主体；与 Flink 图统一状态及正/负新闻分支说明 |
| eventflow_architecture.png | README:163；导读:17 | 高；主页简化，导读保留详图，见 G02 |
| eventflow_sql_pipeline.png | 导读:167 | 中；分区清楚，修改绿色查询箭头含义及 one disposable owner round 等措辞 |
| eventflow_request_lifecycle.png | 导读:443 | 高；两种 capture、逐函数返回、顺序和提交颜色一起处理，见 G03-G05 |
| risingwave_query_dataflow.png | 当前读者文档未引用 | 保留待决定；较简洁，可作分图素材，但 SQL 请求/结果两条线需分别标注 |
| materialize_incremental_query_flow.png | 当前读者文档未引用 | 保留待决定；纵向流程较清楚，可作详图简化素材，需附层次说明 |
| csp_event_execution_flow.png | 当前读者文档未引用 | 保留待决定；主线清楚，active/passive 和 rank 需要图例 |
| timely_worker_communication_flow.png | 当前读者文档未引用 | 保留待决定；比总览简洁，数据与 capability 变化的线标签需区分 |

4 张未引用图片没有删除，也不建议仅因未引用就直接清理。可以由用户选择作为旧素材保留、重新引入，或另行清理。

### 5.3 Mermaid 与文本图补充

6 段 Mermaid 分别在架构文档:236、280、721，news 示例:109、124，typed Pattern 示例:99。架构双图需在图前解释线型并按业务层分组；严格下一条新闻的状态图结构清楚，保留主体。示例内 Mermaid 源码继续遵守英文/ASCII 约定，中文业务解释可放图注；不机械把节点标签全部改中文。

文本图检查中，feature_computation_demo/README.md:31-40 有长行和对齐问题，根 README:494 的共享依赖连线也可简化。SQL 示例的短树和输入/输出表已经很清楚，没必要全部改成复杂图片。

## 6. 图文与当前代码差异：逐项待用户决定

本节按读者文档描述建立对照，保留文档是“当前声明”“建议”还是“目标”的上下文。源码证据只说明实际做法。下面的处理方案均未执行；specs 与某一方一致不构成自动选择它的理由。

### C01：源处理与请求受理是否必须同一次提交

- 文档：research.md:2011 写“捕获结果及请求状态与本轮状态同 shard 原子提交”，2013 又把关系、FSM、Timer、请求、进度列为同批。
- 代码：[event_processor.cpp](../../src/runtime/event_processor.cpp) 第 2326-2363 行完成源/lifecycle 提交；第 3161 行另调 requests->admit。[request_runtime.cpp](../../src/runtime/request_runtime.cpp) 第 735-786 行另起 apply，768-769 行保存 capture_revision=R、admission_revision=R+1。
- 差异影响：源确认时请求可能尚未受理；延迟受理的当前输入可能晚于触发时所见输入。它影响 API、ACK、背压和恢复，不能靠改一个“同批”词草率消除。
- 待决定：保留文档的一次提交语义并重新设计实现；或明确采用分阶段提交并同步文档。若改代码，需要重新定义输入未 Ready 时的源进度、UNKNOWN 和 checkpoint。
- 后续验证：源提交后延迟受理、UNKNOWN 原批确认、input_committed 与后续失败分离、R/R+1 及恢复回归。

### C02：共享祖先“只执行一次”的保证范围

- 文档：README.md:508，以及 news 示例:146，写同一请求共享祖先只执行一次。
- 代码：[request_machine.cpp](../../src/runtime/request_machine.cpp) 第 700-709 行允许在 max_attempts 内重新 Ready，第 956 行再次领取时增加 attempt；固定输入不变。
- 差异影响：正常调度复用同一个逻辑节点；失败/超时可能导致物理函数再次运行。读者可能错误地将回调视为可安全执行一次性外部副作用。
- 待决定：将文档保证限定为正常调度的逻辑复用；或要求更强的执行保证，明确失败后是否允许重试及外部效果如何处理，再决定代码方案。
- 已有验证入口：RequestMachineTest.RetryKeepsFrozenInputsAndFencesThePreviousAttempt、CatalogDiamondExecutesSharedAncestorOnceThroughActualSimpleExecutor。本轮未运行。

### C03：SDK 是否直接访问 Compute

- 文档：架构 Mermaid:287 画“缓存路由后的数据请求”从 SDK 到 Compute；同文后段说明其属于后续方向。
- 代码：[cluster_client.cpp](../../src/client/cluster_client.cpp) 第 270-283 行只有 Meta HTTP 客户端，请求路径固定为 /v1/meta/；discover 返回路由信息，未实现数据请求直连。
- 待决定：实现并验收 SDK 路由缓存/失效/原身份重试；或将主图只画当前 Meta 代理路径，把直连保留在目标图。根 README 的接入表和 EventFlow PNG 当前画法与代码相符。
- 验证入口：ClusterClient.BootstrapDiscoveryUsesMetaOnlyAndPreservesDecimalIdentity；若改为直连，需增加路由更新和错误节点验证。

### C04：Compute 之间是否分发函数任务

- 文档：架构 Mermaid:290 有 Compute 间“固定输入函数任务/完成消息”双向边；约 402 行才说明后续范围。
- 代码：[compute_service.cpp](../../src/service/compute_service.cpp) 第 408 行安装本地 driver；[feature_computation_demo.cpp](../../examples/feature_computation_demo/feature_computation_demo.cpp) 第 568-572 行创建本地 ThreadPoolFunctionExecutor；Compute 当前 endpoint 没有远程函数任务接口。
- 待决定：实施跨进程任务路由、结果校验、版本分发、取消和恢复协议；或把该边从当前架构图移到未来设计。当前函数 worker 位于 Compute 内部。

### C05：Kafka 适配器是否已经接入集群自动消费

- 文档：架构 Mermaid:283 画 MemMock/Kafka 直接进入 Coordinator；约 903 行把 restore 后按 manifest seek 写入恢复流程。
- 代码：存在 IMessageSource/KafkaMessageSource，但当前 Coordinator 和 CLI 没有相应消费驱动/seek 接线；集群通过 SDK ingest 投递，恢复示例也由 SDK 按原身份重放。
- 待决定：增加实际 broker-neutral 消费驱动及 Kafka 联调；或区分“已有独立适配器”“当前 SDK 投递”“未来自动消费/seek”。真实 Kafka 集成不能用协议 mock 替代。

### C06：线程池是否仍未接入

- 文档：架构:7、402、645 仍写线程池接入进行中；同文约 915 行和实现导读:532 已写真实线程池。
- 代码：Compute::install 使用 threaded=true，实际进入 ThreadPoolFunctionExecutor::create，见 C04；默认每执行器 2 个 worker，按 shard 创建。
- 待决定：同步历史状态说明，并决定是否保留历史快照段落。这里是实现进展超过旧文字，并未发现缺失线程池代码。
- 验证入口：ComputeExecutorMode.LogicalReadyTimeDoesNotDelayBackgroundWorkersUntilTheWallClockCatchesUp。本轮只核对实现和测试源码。

### C07：恢复时是否允许更换分片所属节点

- 文档：架构:846-856 将停机、更新 assignment、新 owner 读取状态写成首版流程。
- 代码：[coordinator_service.cpp](../../src/service/coordinator_service.cpp) 第 216-254、808-835 行校验原逻辑 node ID 与 shard 归属；允许 endpoint 改变，不能据此重新分配 shard。
- 待决定：实现有停顿的 owner 迁移协议；或将当前恢复限定为原逻辑部署恢复，把 assignment 变更标为目标。
- 验证要求：保持 node/shard、仅改 endpoint 应可恢复；把 shard 转给另一个 node 的行为需单独定义和测试。未执行这些探针。

### C08：建议 KV 格式与实际持久格式不同

- 文档：架构:494 明确写“建议首版键布局”；498-511 行含 database_id、M/revision_or_receipt 和 null_bitmap。
- 代码：[key_codec.cpp](../../src/core/key_codec.cpp) 第 71-92 行实际为 D + shard + T/S/I + object_id + encoded_parts，没有 database_id；[types.cpp](../../src/core/types.cpp) 第 417-428 行关系值采用 format/schema_id/schema_version/values 的 JSON 封装。
- 待决定：保留为设计备选并就地链接实际布局；更新为当前格式；或正式实施格式变化。最后一种需要数据兼容、恢复、迁移与版本计划，不能只调整编码函数。

### C09：请求图是否应该同时表示两种 capture

- 图：请求图第 2 步只写 Capture currently Ready inputs at one row boundary。
- 文档：实现导读:447-493 与 typed 示例明确区分 capture_match/current。
- 代码：[computation.cpp](../../src/runtime/computation.cpp) 第 210-216 行 Match 读取保存的 admission.arguments，Current 才读取当前关系；232-240 行在匹配阶段生成并保存参数。
- 待决定：将通用图补齐两种来源，或把图明确标为 capture_current 场景。现代码实现了正文中的两种模式，不应仅凭此简图删掉其中一种。
- 验证入口：Computation.MatchArgumentsSurviveUnknownAndSnapshotWithoutRepeatingProjection 与 typed_pattern_demo 的 40.6/94.5 轨迹。

### C10：函数 DAG 在执行器中独立跑完，还是逐节点返回 owner

- 图：请求图把四函数 DAG 放进 executor 框，显式返回线集中在 risk 一侧；图内小字又说每次完成返回 owner。
- 代码：[function_executor.cpp](../../src/runtime/function_executor.cpp) 第 484 行调用单个 task callback；生产 driver 第 811-840 行逐个 complete_request；[request_runtime.cpp](../../src/runtime/request_runtime.cpp) 第 843 行起通过 owner 写入输出并刷新依赖，确认后才派发后继。
- 待决定：按当前逐节点提交机制重画循环；或将完整 DAG 托管给执行器，并重新定义中间结果、失败、缓存与恢复协议。这是职责变化，不是单纯换一根箭头。
- 验证入口：RequestMachineTest.CompletionOnlyPreparesOwnedRowAndRequiresSqlRefreshForDescendants。

### C11：Latest 是否另有一次独立提交

- 图：第 5 步已写 output/status/SQL/Latest 同一次 commit，第 6 步又以绿色独立框解释 Latest；副标题说每个绿色边界是独立 owner commit。
- 代码：[engine.cpp](../../src/runtime/engine.cpp) 第 1119、1180、1200 行在同一个 RoundState 中计算 selector 并写入公开 Latest；它随输出及请求状态的当前原子轮发布。
- 待决定：把第 6 框改成第 5 步的规则说明；或要求独立发布阶段并先定义可见性差异。前者符合图内已有 COMMIT 文本和当前代码，建议优先考虑，但未执行。
- 验证入口：RequestLatestContract 的 AdmissionClaimAndCompletionPublishWholeRequestStatus、AdmissionOrderWinsAcrossReverseCompletionAndLaterCancellation。

### C12：“Always Retry”是否包含永久错误和无限重试

- 文档：research.md:675、687 写通过 Always Retry Process 最终达到一致性；2013 行又加上原子提交与 UNKNOWN 限定。
- 代码：[request_machine.cpp](../../src/runtime/request_machine.cpp) 第 700-725 行受 max_attempts 和请求期限约束，1009-1015 行仅对部分错误重试；Engine 对 UNKNOWN 重送完整原批，提交被拒绝后可以停止该 shard。
- 待决定：把文档的重试范围限定为可恢复错误、次数和期限；或要求持续重试，并先定义永久错误、背压、取消及资源释放。两者都不能用“始终重试”概括所有失败路径。

### C13：“不支持快照读”是否排除单分片当前一致读取

- 文档：research.md:674 明确说不支持在同一严格快照读取多个特征；2009 行又允许同 shard 受控捕获。文档自身存在不同范围表述。
- 代码：[state_store.cpp](../../src/storage/state_store.cpp) 第 326-342 行在同 shard 锁内 get_many 并返回一个 revision；请求受理记录实际捕获版本。没有任意历史 as-of，也没有跨调用/跨 shard 的事务快照。
- 待决定：采用“同分片当前一致读取，无通用历史 MVCC”的明确描述；或选择另一种读取合同并确定实现范围。这里是能力边界的定义问题，不能只把 Snapshot 和 MVCC 当成同义词。

### C14：动态 UDF 与 Process Function 目标是否进入实施

- 文档：research.md:681-682 在“目标”中列出动态 UDF、UDAF、Process Function，并要求不重新构建整个系统。它没有宣称已有相应 API，但没有就近标明当前状态。
- 代码：[function_executor.hpp](../../include/eventflow/function_executor.hpp) 第 38 行起的 Registry 管理 compiled-in callbacks；当前 typed Function 也是随应用编译的静态函数。
- 待决定：保留为有明确状态的长期研究目标；或授权实施相应扩展并另定 ABI、生命周期、状态、部署和恢复合同。不能把现有静态 Function 直接当作已实现动态家族。

### C15：Watermark 是否驱动事件时间窗口和 CEP 发布

- 文档：research.md:670 将 Watermark 与 CEP、窗口结果发布联系起来；2011 行又区分处理时间 Timer 和事件时间进度。前者是能力目标，未给出当前可用的事件时间算子入口。
- 代码：[time.cpp](../../src/runtime/time.cpp) 第 228-264 行的独立 WatermarkTracker 有未知分区进度、idle、乱序容忍逻辑；[coordinator_service.cpp](../../src/service/coordinator_service.cpp) 第 465 行起另行汇总已出现分区的事件时间进度。426-431 行转发 Compute 的是 now_us，当前 EventProcessor 不通过 Watermark 驱动事件时间窗口/CEP。
- 差异影响：类中具备某项策略，不代表集群已接通该策略；当前条件和 CEP 仍按处理时间推进。
- 待决定：文档明确当前水位用于进度记录和恢复元数据；或进一步实现水位驱动的事件时间计算，定义迟到输入、空闲分区、结果修订和恢复规则后再修改代码。

### C16：ShardTask/ExecutorGraph 是概念层次还是实际对象

- 文档：架构:340-370 写“首版采用以下层次”，使用 ShardTask、ExecutorGraph、TriggerFSMExecutor、MaterializeExecutor 等名称。
- 代码：公开头和 src 中没有这些同名类型。实际 [compute_service.cpp](../../src/service/compute_service.cpp) 第 203-208 行的 Shard 持有 cache、mutex 和 FeatureComputationDemo，再由其创建 EventProcessor/Engine/RequestRuntime。
- 待决定：将该段明确写成概念层次并提供到实际类型的映射；更新为当前对象关系；或按原物理算子/ExecutorGraph 设计重新组织实现。单纯为匹配文档重命名类，并不能实现其描述的执行架构。

### C17：条件示例的成功输出字段名不一致

- 文档：condition_trigger_demo/README.md:101 写 CONDITION_TRIGGER_DEMO_OK c_requests=1。
- 代码：[condition_trigger_demo/main.cpp](../../examples/condition_trigger_demo/main.cpp) 第 256 行输出 condition_confirmation_requests=1；请求统计行还包含 succeeded/total 后缀。
- 待决定：更新文档示例为当前输出；或恢复文档中的输出接口并同步调用方。该差异可以局部处理，不需要重新设计计算语义。本轮没有执行脚本，事实来自打印代码。

### 核对后暂未发现差异的部分

最新实现导读对业务 N 分片加一个 control shard、STAGING DDL、全部安装确认后 RUNNING、Rocks 新目录恢复和 WAL 保留的描述，有对应代码支持。typed Pattern 输入域、显式 Session、私有副本原子 bind，以及 Latest 的成功/epoch/受理顺序门槛，也在当前实现中找到对应路径。以上是静态核对结论，不是新的测试通过声明。

## 7. 建议的决策顺序

1. **先决定会影响产品行为的差异**：C01 提交阶段、C02/C12 重试保证、C03-C05 网络与源接入、C07 恢复归属、C08 存储格式、C10 执行器职责、C13-C16 读取/扩展/时间/执行层次。为它们改代码可能形成独立开发任务。
2. **再决定文档组织和明显过时处置**：T01-T07、C06。保留作者前言和技术细节，优先改善入口与定义顺序。
3. **据已选语义更新图**：G01-G11、C09/C11。图片信息结构、配色和文字必须一起检查，不能先美化再补语义。
4. **最后统一措辞与样式**：T08-T10、低优先图、构建选项位置和未引用资源。保持必要术语与示例 ASCII 约定。

用户可直接按 T/G/C 编号选择要处理的项目，并指定差异应改文档、改图、改代码，还是继续保留为后续目标。本轮没有默认批准任何一种处理。

## 8. 验证与边界

本次只提供文件阅读、图像检查和限定源码对照证据。读者文档与图片的编辑前 SHA256 基线位于临时目录 `/tmp/eventflow-reader-review-jrvqtvyv/baseline.json`，用于交付前检查未改原文/图片；临时目录不是后续工作的唯一记录。

最终对照发现根 README 有本任务之外的并行编辑：增加 Ray/CPU/GPU 后续方向，并调整联系段落样式，文件由 606 行变为 607 行。已重新读取并同步本报告的根 README 定位，相关发现仍成立；该新增内容明确属于后续规划，不登记为当前代码缺陷。其余 38 项基线文件（含全部 PNG）保持一致。本任务对原文/图片/代码没有发起写入，未回滚并行编辑。

报告已汇总 10 类文字问题、11 类图形问题和 17 项图文/实现差异或范围决策；相互关联的条目通过编号交叉引用，不把它们计算成互不重叠的代码错误。

未运行产品测试，未核实 Flink/RisingWave/Timely/Materialize/CSP/Incremental 外部最新版的全部事实，未验证 Mermaid 实际布局或浏览器深浅主题表现。源码审阅未发现必须立即修改才能完成本次审阅的实现错误，也不等于证明全部实现正确。

本轮修改文件仅为本报告与 specs/INDEX.md 的审阅入口。后续行动等待用户逐项决定。
