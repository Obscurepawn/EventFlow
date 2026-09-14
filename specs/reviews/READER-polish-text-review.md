# 读者文档润色：独立文字与阅读路径审查

Status: reviewed_with_findings
Reviewer: reader_polish_final_text_review
Reviewed: 2026-09-14

本报告属于本次唯一一轮并行 fresh-context 全局文档审查。审查者未参与被审阅正文的编写；原始 T/G/C 审阅报告仅作为需求与范围背景，不作为本轮独立验收证据。

## 范围与结论

已完整阅读根 README、docs 的 5 份读者文档，以及 SQL、typed Pattern、news feature、feature computation、condition、SDK Clock polling、checkpoint recovery 共 7 份原示例 README，合计 13 份。Clock 目录按当前的 `sdk_clock_polling_demo` 读取；并行增加的 `engine_clock_trigger_demo`、Docker 文档、CMake 和新 Clock 实现不属于本报告验收范围，仅确认相关导航目标存在。

本轮文字可读性有实质改善：首页第 9 行即进入快速开始，运行命令和 40.6/94.5/-11 的结果解释先于完整类型声明；术语在业务场景旁解释；运行手册在位置编码的 Row 输入前说明字段；各示例把业务时间线与结果移到高级构建参数前。架构文档先讲四条业务路径，再介绍对象和进程；调研目录已经对应现有正文，各组件保留具体机制和计算轨迹。

尚有 5 项可局部收尾的问题，见下表。没有发现需要改动产品代码才能完成本次文字润色的问题。等级只表示本次读者文档的阅读影响，不是产品代码缺陷等级；中等级问题建议在文档交付前处理，低等级问题可用简短文字或链接收尾。

## 具体发现

| ID | 等级 | 路径与审阅位置 | 问题、影响与建议 | 初始状态 |
| --- | --- | --- | --- | --- |
| R01 | 中 | [运行手册](../../docs/core_delivery.md) 第 184 行；[条件示例](../../examples/condition_trigger_demo/README.md)与[恢复示例](../../examples/checkpoint_recovery_demo/README.md)第 100 行；[架构文档](../../docs/feature_computation_architecture.md)第 941 行 | 运行手册仍把 `final-validation.md` 称为“当前代码对应”的验证；两份示例称其为“最终代码”的结果。这是历史核心快照，后续 Trigger/Clock 的证据由 `specs/INDEX.md` 分开记录，不能覆盖当前变化。运行手册“六个示例”、架构文档“五个可运行示例”也与当前八项导航不一致。建议明确称为历史核心验证，当前能力/证据链接到规格索引；入口数量直接省略。此项不涉及 C 类行为选择。 | 已通知 root，待处置 |
| R02 | 低 | [架构文档](../../docs/feature_computation_architecture.md)第 981、1003 行 | Sources 表编号 2 和脚注 `[^2]` 仍引用 research 的 `§2.4.9`。调研已经把 Version/Snapshot 章节改为 `§2.5.9`，旧编号不存在。建议同步两处为 `§2.5.9`；这属于 T03 的跨文档引用收尾。 | 已通知 root，待处置 |
| R03 | 中 | [typed Pattern 示例](../../examples/typed_pattern_demo/README.md)第 97–117 行 | Mermaid 的 release gate 虚线仍落到两个 `Static score function` 节点，图上的 capture_current 在其上游，容易理解成先读取当前输入、仅让函数等待。后面的正文准确解释了受理门槛，但初读该图仍有 G09 所述歧义。若保留现有箭头，至少紧邻图明确“虚线表示整个请求的受理前置条件，包括 capture_current 的读取时点，并非只限制函数执行”。不要求改 API、代码或任何 C 类选择。 | 已通知 root，待处置 |
| R04 | 低 | [调研文档](../../docs/research.md)第 1295–1299、1907–1911 行 | Temporal Join 与 Incremental 共享计算图换成 readable 版本后，原来的 `risingwave_temporal_join.png`、`incremental_shared_computation_graph.png` 已没有正文入口。原 PNG 文件仍保留，但读者无法像其他新概览一样从图旁回到原图。按本轮“概览与详细图有明确入口”的验收要求，建议各补一个“原图”链接。 | 已通知 root，待处置 |
| R05 | 低 | [架构文档](../../docs/feature_computation_architecture.md)第 825、906 行 | 正文仍出现没有定义的内部阶段名“M5由MetaService……”和“P1 必须支持……”。它们迫使读者猜测开发阶段，与本轮 T04 的目的不符。建议用相应业务范围表述，或把内部编号留在历史注记。此建议不要求改掉 C06 中特意保留的线程池历史状态。 | 已通知 root，待处置 |

## 内容保留与范围核对

- 根 README 的原个人前言正文（现“未来演进迭代”）、联系方式及 `Spec Coding方式` 整节，与本轮编辑前副本逐段比较完全一致；仅阅读顺序和前言标题变化。没有把作者方法与个人规划压缩成宣传文案。
- 调研保留六字段新闻、A/B/C/D 四条业务路径，以及 5 秒周期、120 秒历史、候选 5 分钟、确认 30 秒、Ready 2 秒等独立参数。offset 100/101 恢复反例、Timely capability/frontier 轨迹、Incremental 共享 peak 和 cutoff 重算表仍完整存在。
- 示例保留 SQL 的 90+30=120、撤回后 30 与空结果；typed Pattern 的 A/B、C、D 和 40.6/94.5/-11；新闻确认的负面失败、再次等待、超时和 Clock；条件示例的 mid 100.0 到 100.3、volume 110 和一次受理；checkpoint 的重复输入及总量 110 到 135。SDK Clock 三周期轨迹属于并行 Clock 更新，未将其变化归功于本轮润色。
- `capture_match`、`capture_current`、两秒匹配窗口、一秒待受理期限、十秒请求生命周期、Session、gate 已受理而非已成功、Clock 无输入推进与 Latest 整请求成功门槛，在文字中继续明确。没有发现文字重排把进程内 C++ 定义改成可向集群上传回调的说明。
- C01/C12/C13 的旧研究表述仍保留在 research 第 3 章及结论；C02 的“同一请求只执行一次”仍见根 README 与 news 示例，未被暗中改成新的保证；架构图仍有 C03–C05 的相关连线，C06 历史状态、C07 assignment 调整流程、C08 建议 KV 格式、C14 扩展目标、C15 水位目标、C16 ShardTask/ExecutorGraph 概念均未被自动替换成源码现状。C17 的 `c_requests=1` 输出仍保留。本报告不裁决这些差异。
- C09–C11 涉及 PNG 的完整 capture 来源、执行器职责和提交边界；24 张原 PNG 的哈希均未改变。新概览是否无意决定这些语义由并行视觉 reviewer 核对，本报告不以原图保留推导新图已通过。

## 本次实际检查

1. 读取根、docs、specs 的 AGENTS，以及 `specs/INDEX.md`、`reader-doc-polish.md` 和原始 T/G/C 报告；随后通读上述 13 份最终读者稿。
2. 用独立 Python 静态检查器提取 Markdown/HTML 本地链接和图片引用，检查路径、GitHub 风格标题 slug 与显式 anchor。共检查 328 次本地引用，未发现缺失目标或 anchor。架构文档的 18 个本机绝对源码定位单独去除 `:行号` 后验证目标；它们属于明确标为本地快照的引用，不能据此声称在 GitHub 或其他机器上可直接打开。
3. 检查全部 145 段反引号或波浪线围栏，均已闭合；围栏内容均为 ASCII。章节编号的普通文字引用另由通读检查，因此发现 R02；路径存在性检查无法发现这种编号错误。
4. 对本轮临时基线中 131 个 `src/`、`include/`、`tests/` 文件逐个重算 SHA256，没有差异。24 张原 PNG 逐个重算 SHA256，也没有差异。没有把并行 CMake/Clock/Docker 变化当成本任务越界。
5. 从编辑前根 README 副本取原个人前言正文和 `Spec Coding方式` 节，与当前对应内容逐段比较，均完全保留。

基线来自 `/tmp/eventflow-reader-polish-wk8cdt65/baseline.json` 与同目录 `README.before-root-edit.md`。本报告保存检查结论，临时目录不是后续唯一记录。初次辅助脚本在个人章节旧标题定位处中止；链接、围栏与哈希统计已在中止前输出，随后使用实际正文起点单独重跑了个人段落比较，得到上述一致结果。

## 局限与交接

未运行 C++ 构建、产品测试、服务或示例；未把旧验证次数当作本次测试通过。未访问私有 PDF、未写 `.reference-repos/`，未验证外部产品的最新 API 或重新检查外部网页。本轮未承担逐 PNG 视觉主审，也未在浏览器渲染 Mermaid；只检查图的引用、相邻文字与源码结构。

只有根 README 有可直接比较的完整编辑前副本；其他文档的内容保留判断来自原始审阅中的具体案例、当前全篇阅读和现存源码基线，不能称为逐字历史比对。报告行号对应发现时的快照，作者完成局部修正后可能变化。

审查者本轮只写本报告。请实施者保留上述初始发现，追加处置与定向验证记录；不需要因此再发起新一轮 fresh-context 审查。

## 实施者处置与定向复核

Owner: root；本节由实施者追加，保留上方独立审查原结论。

| 发现 | 最终处置 | 定向证据 |
| --- | --- | --- |
| R01 | 已修正 | 运行手册、条件/恢复示例明确链接“历史核心验证”，补充 specs/INDEX 当前证据入口；去掉六个/五个的导航计数。没有改动产品行为或 C17 输出 |
| R02 | 已修正 | 架构文档表格和脚注两处均为 §2.5.9，与 research 的 Version/Snapshot 标题一致 |
| R03 | 已修正呈现 | typed 图前说明虚线约束整个请求受理及 capture_current 读取时点；只是就近重述已有正文，Mermaid 边与 C 类事项不变 |
| R04 | 已解释并保留原资源 | Temporal Join、Incremental 属于同信息重排，不是“概览加详图”。原图保留在资产目录和生成来源记录；旧 Incremental 的歧义标签不作为另一个权威入口。六张真正概览均有原详图入口 |
| R05 | 已修正 | 未定义的 M5 改为 MetaService 的 Schema 管理职责；P1 改为“受控恢复”；保留原能力要求和历史注记 |

完成上述修改后重新核对引用、围栏、保护内容及图片保留情况。检查结果统一记入 [reader-doc-polish.md](../reader-doc-polish.md)；没有新开独立审查轮，也没有运行产品构建或测试。
