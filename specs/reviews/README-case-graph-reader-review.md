# README Schema、业务 case 与计算图：独立读者及配图审查

Date: 2026-09-15
Reviewer: case_graph_reader（fresh-context 独立 reader / visual challenger）
Status: 完成；R1 图示遮挡与 R2 分组标题相交均已在同一轮定向复核关闭，本轮范围没有未处置发现。

## 范围与依据

本轮只检查 [README](../../README.md) 新增的“先看示例：数据 Schema、业务 case 与计算图”：阅读顺序、字段和案例、两条 Trigger 的独立性、固定输入与目标函数、Graph-Based 解释，以及实际 Mermaid 渲染的文字和箭头。

授权顺序为 Schema -> case -> 同张 DAG -> 建模理由 -> code；原始要求与当前状态见[本次规格](../readme-usage-examples.md)。已阅读根、specs 和示例目录的 AGENTS。只读 [definition.cpp](../../examples/streaming_database/definition.cpp) 与 [verify.cpp](../../examples/streaming_database/verify.cpp) 作为源码证据。reviewer 只写本报告，没有修改 README、图片、源码或其他规格；不重审前次架构 PNG 编辑、SDK 示例和全产品质量门禁。

## 原始发现与处置

### R1 / P2：自动布局让两条边穿过无关节点，模糊真实输入来源

原图源对应 README 中 `session -. "session / once gate" .-> threshold` 和 `features -->|"read at admission"| periodic_input`。两条业务边的声明正确，但实际 PNG 上有节点遮挡，不能仅以 Mermaid 解析成功认定图示清晰。

- `market_features -> periodic_input` 的数据实线从蓝色 MV 出发后穿入紫色 `periodic_score` 矩形，再从其下方继续到橙色请求。读者可能看成 Trigger 直接提供固定特征输入，无法直观看出该输入来自 `market_features`。
- `trading_session -> volume_threshold` 的控制虚线穿过 `trade_totals` 矩形下部。连线在 MV 下方重新出现，容易误判 session gate 的来源。

PNG 目视结果由 SVG 坐标进一步确认，坐标使用 SVG viewBox：

| 对象 | 初审几何证据 |
| --- | --- |
| `periodic` 矩形 | `x=816.238..1076.238, y=360..510` |
| `L_features_periodic_input_0` | 从 `(766.238,473.261)` 经 `(807.995,485.551)` 后进入上述矩形，再继续到 `periodic_input` |
| `totals` 矩形 | `x=435.695..695.695, y=160..286` |
| `L_session_threshold_0` | 从 `(745.695,246.900)` 向左下，经 `(676.706,259.583)` 进入上述矩形 |

初审建议仅调整 Mermaid 布局或分组，使两条边绕开无关节点；保留 10 个业务节点、13 条业务边及现有正文语义。实施者接受发现并调整布局，R1 已按下方定向复核证据关闭。原发现保留，没有重开独立审查轮。

### R2 / P3：分组修订版的两条控制线经过组标题文字

R1 调整后的 `/tmp/eventflow-case-dag-branches.png` 通过分组避开了无关业务节点，但 `trades -> threshold` 经过 `Threshold Trigger and score` 标题，`session -> periodic` 经过 `Periodic Trigger and scores` 标题。节点内文字仍完整，业务含义不变；组标题处文字和线叠在一起降低可读性。建议仅为组标题留出空间或调整位置。该问题由本轮 R1 定向复核发现，没有开启新的独立审查轮。

实施者将三个辅助分组标题改为空白，保留分组边框、全部业务节点/边和正文图例；下方最终 PNG 与独立路径采样均确认文字相交已消除，R2 关闭。

## 本轮定向复核记录

实施者提供 `/tmp/eventflow-case-dag-branches.{mmd,svg,png,json}`，保留初审 `/tmp/eventflow-readme-case-dag-final.*`。reviewer 已目视修订 PNG，并独立确认 README 当前 Mermaid 与修订 `.mmd` 一致、10 个业务节点和 13 条边的端点集合与初稿完全相同，R1 两条线已绕开无关节点。修订图源 SHA-256 为 `d87c2bd840ea5e09099deb13ce02ab65b45c642514225197e059624f1192d301`。

复核时还发现实施者第一版 `/tmp/eventflow-case-dag-layout-check.mjs` 的证据采集缺陷：它用 `path.id.startsWith('L_...')` 匹配边，但实际 ID 带 `case_dag_branches-` 前缀，因此会跳过全部路径；原 `collisions=[]` 不能单独证明已检测且没有碰撞。reviewer 使用 SVG 的 `data-id` 匹配边并规范化节点 ID，在只读 Chromium 中重新按每 2 像素采样，显式检查 13 条边、10 个节点：

| 快照 | 检查边数 / 节点数 | 非端点业务节点相交 | 组标题相交 |
| --- | --- | --- | --- |
| 初审 `eventflow-readme-case-dag-final` | `13 / 10` | 恰好重现 R1 两处 | `0` |
| 分组 `eventflow-case-dag-branches` | `13 / 10` | `0` | 恰好检出 R2 两处 |

该独立复测以原图作为能检出已知问题的对照，直接支持 R1 已修复；不把采样近似声称为全部平台的几何证明。实施者随后修正本地脚本的 ID 匹配，并要求实际检查 13 条边及 10 个节点。最终日志已输出该计数，不再沿用原空采样结果作为证据。

### 最终快照与结论

reviewer 已通过 `view_image` 重新目视 `/tmp/eventflow-case-dag-clean.png`。新图中两条 R1 路径可从真实来源完整追踪到目标；辅助标题已无文字可供连线遮挡；节点标签完整，控制条件、两类固定输入和两个评分结果都可辨认。没有发现新的截字、标签覆盖或误导性箭头。

独立只读 Chromium 复测最终 SVG，仍按每 2 像素采样，结果为 `checked_edges=13, checked_nodes=10, collisions=[], title_hits=[]`。实施者的最终日志 `/tmp/eventflow-case-dag-clean-render.log` 也报告 `parsed=true`、`checked_edges=13`、`checked_nodes=10`、`collisions=[]`。已再次确认最终 Mermaid 与当前 README 一致、英文 ASCII、10 个业务节点与 13 条边的端点集合不变，标题顺序仍为 Schema -> case -> DAG -> Graph-Based 解释 -> code。

| 最终检查对象 | SHA-256 |
| --- | --- |
| `README.md` | `9de9e84ed6b72ffd3c4f0d6a1724708f59099b161f018cd9021dd7fad5fa7bd1` |
| `/tmp/eventflow-case-dag-clean.mmd` | `239e8c9868d87b1ddc5cd04461e4aa4aa0ec3badfe0739047adf453788b1ef1c` |
| `/tmp/eventflow-case-dag-clean.png` | `62cdb03f2f3d0b9b9ccab36eafbfdce69edbc2a8f37ada047e62b5c9f13d6b89` |
| `/tmp/eventflow-case-dag-clean.svg` | `fc6cf6973a7aaa619c4f4cd859788fca53e919c79f4ad7144c73625ba656c407` |

本轮结论：新增导读的阅读顺序、案例与来源解释符合授权，图示整改已完成定向复核，没有本 reviewer 范围内的剩余阻塞项。此结论不扩大到下方明确未验证的产品行为。

## 已核对且未发现问题的内容

- **阅读顺序**：字段表先于业务目标，70/50/30 时间线先于合并图，Graph-Based 的节点/边解释先于实际 DDL/Trigger 代码。新读者无需先读代码才能知道输入、结果和触发目标。
- **Schema**：`ticker STRING NOT NULL`、`trade_id INT64 NOT NULL`、`volume INT64 NOT NULL`、主键 `(ticker, trade_id)` 与分区键 `ticker` 均对应 `definition.cpp:77-84`。同一 ticker 下不同成交编号对应不同成交记录；`COUNT(*)` 表示成交笔数，`SUM(volume)` 表示累计成交量。
- **特征与评分**：`definition.cpp:86-97` 定义两级 MV；`definition.cpp:65-70` 的评分为 `doubled_volume + trade_count`。三笔样例对应 `70/1/140`、`120/2/240`、`150/3/300`，评分分别为 `141`、`242`、`303`，算术一致。
- **假设与现场结果区分**：正文明确第二笔后的阈值请求已受理才追加第三笔，不保证每个中间状态都有周期结果。`verify.cpp:252-263` 先等待并检查阈值评分及固定输入，再在 `289` 行追加 `30`，`297-303` 行检查周期评分 `303` 和阈值输入仍为原值。`141` 被写作“若捕获此时输入”，没有冒充验证器实际断言过的结果。
- **两条 Trigger 独立**：`definition.cpp:131-146` 为事件、更新后 `total_volume >= 100`、活动 session 和 once 的阈值请求；`151-167` 为 200 ms Clock、活动 session 和 `TryOnceAtFiring` 的周期请求。两者各用一个 request id、各自 capture，同指 `trade_score / calculate_score`。图中没有将两条规则合为 AND，也没有声明周期评分等待阈值评分完成。
- **时钟和会话边界**：首笔成交创建 session 对应 `definition.cpp:114-127`；每个已观察 ticker 的 200 ms 时隙及可能合并逾期时隙对应 `148-167`。正文没有承诺严格墙钟完成时限。
- **Graph-Based 解释**：明确计算图边表示数据依赖，CEP 状态图边表示事件/条件驱动的状态转换；说明状态图允许循环；“先 A 后 B、2 秒内”作为模式解释，随后明确当前 demo 实际使用条件 Trigger 和 Clock Trigger，没有虚构已运行的 CEP 序列。
- **来源适配**：当前 [RisingWave MV 文档](https://docs.risingwave.com/sql/commands/sql-create-mv)支持实时增量维护及级联 MV 的概念说明；[Flink 1.19 CEP 文档](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/libs/cep/)支持事件条件、序列、重复和 `within` 时间约束的说明。这些来源没有被当作 EventFlow 当前实现的运行证据。
- **文字与标签**：初审 PNG 内各节点文字完整，长标签换行后仍在节点框内，没有发现文字截断或文字标签相互覆盖。初审时仍存在 R1 连线路径问题，因此当时没有判定整图通过；最终视觉结论见上方整改复核。

## 初审证据

README 快照 SHA-256：`45190cc5b43e62ae8929ee858af1052c3afb3e6308a4a9c41ee5ce87a0a32182`。

| 文件 | SHA-256 |
| --- | --- |
| `examples/streaming_database/definition.cpp` | `aba0f675c329de518ce1c6d0c48461473f41df8ada8e0162fdaa585ccc54b378` |
| `examples/streaming_database/verify.cpp` | `003e3fd635d7ad68948377326b495c15a5dde465976ef53af9fe8895c52dd715` |
| `/tmp/eventflow-readme-case-dag-final.mmd` | `d62a0d63dfcae7935ed8c08d32d822e40ede3e1cc94dc74bfefe05482a9eda00` |
| `/tmp/eventflow-readme-case-dag-final.png` | `e0bc855540d8bf28925a85b0c055088c11db41d056b109b60778d02ac6e4dbc5` |
| `/tmp/eventflow-readme-case-dag-final.svg` | `c48ab4a80724eb9704ea321a0a653bd7957b92a857c6d94a48b2dfe39d748379` |

实施者使用本地 Mermaid 11.17.2 与 Chromium 渲染，reviewer 阅读实际 PNG、SVG、JSON 及 `/tmp/eventflow-readme-case-dag-final-render.log`。日志报告 `parsed=true`、10 节点和 13 条边，viewBox 为 `0 0 1084.23828125 1214`；reviewer 独立从当前 README 提取 Mermaid，确认与 `.mmd` 一致（忽略尾部换行）。对 SVG 的标准 XML 读取曾因内嵌 HTML 标签失败；随后使用 HTMLParser 读取路径和矩形属性，上述几何证据来自该成功读取，未把 XML 读取失败解释为已交付产品问题。

已实际运行 Python 静态检查：五个相关标题顺序、新增段落 Mermaid 围栏配对、Mermaid ASCII、Pattern 本地链接存在，以及 README 与渲染图源一致性，均通过。源码与图证据只用于本轮指定结论，不替代系统执行证据。

## 未验证范围与下一步

本 reviewer 未重新构建 C++、运行 GoogleTest/多进程示例、检查真实 Kafka、恢复或性能，也未复验旧架构 PNG 和 SDK helper 的执行。新增段落的审计语句未在本 reviewer 范围内重新追踪底层审计提交代码；没有据此新增运行正确性声明。Mermaid 的视觉证据绑定本地上述渲染器和快照，未声称所有 Markdown 平台都有相同布局。

下一步：由 root 将本报告与同轮另一位 reviewer 的结论写回本次规格和 INDEX。本 reviewer 无剩余修改或复核任务，不重复源码或全产品审查。
