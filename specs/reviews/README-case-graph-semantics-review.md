# README 业务 case 与图语义独立审查

Date: 2026-09-15
Reviewer: case_graph_semantics（独立 fresh-context reviewer）
Status: 语义及图源核对通过；R1 已完成同轮定向复核

## 范围与要求

本轮仅检查 [README](../../README.md) 新增的“先看示例：数据 Schema、业务 case 与计算图”，以及 [本次追加授权](../readme-usage-examples.md#追加说明先讲-schema业务-case-与计算-dag)。原 SDK 示例、架构 PNG 标题修改和历史系统测试均不重新验收。

要求是先 Schema，再业务 case、合并 DAG、Graph-Based 建模理由，最后进入原有代码。图中需要同时表达特征依赖、组合条件和明确的评分目标，并保留两条 Trigger 的独立请求及受理时 capture 语义。审查遵守根 AGENTS.md、specs/AGENTS.md，未修改 README、生产代码或其他文件。

## 核对结果

| 核对点 | 结论与当前源码证据 |
| --- | --- |
| Schema 与物化链路 | `ticker STRING NOT NULL`、`trade_id INT64 NOT NULL`、`volume INT64 NOT NULL`、复合主键及 ticker 分区均与 [definition.cpp](../../examples/streaming_database/definition.cpp) 77–97 行一致。`COUNT(*)` 是不同成交记录的笔数；两个 MV 分别聚合与派生两倍总量。 |
| 70 / 50 / 30 时间线 | 70/1/140、120/2/240、150/3/300 和评分 141、242、303 的算术正确。[verify.cpp](../../examples/streaming_database/verify.cpp) 237–263、289–303 行先等待阈值评分 242，再追加 30，之后断言阈值固定输入不变而周期得到 303。README 明确此时序假设及周期不保证覆盖每个中间状态。这里只读取验证器源码，没有执行该验证器。 |
| 阈值与会话 | definition.cpp 102–146 行由首笔成交激活 ticker candidate；其 deadline 为 Int64 最大值。成交事件、更新后的总量 >= 100、活动会话与 once 条件组合正确；`.in_session` 不负责创建会话。[event_processor.cpp](../../src/runtime/event_processor.cpp) 908–983 行过滤无效/到期 candidate、已消费 once 及重复 pending intent。 |
| Wait / TryOnce / capture | definition.cpp 142–167 行对应 Wait、1 秒 pending、once=true 与每 200 ms 的 TryOnceAtFiring、once=false。[request_runtime.cpp](../../src/runtime/request_runtime.cpp) 815–833 行在受理准备阶段读取已确认视图；[computation.cpp](../../src/runtime/computation.cpp) 228–251 行随后按 scope/request_id 固定输入。[event_processor.cpp](../../src/runtime/event_processor.cpp) 3620–3654、3687–3699 行区分等待和跳过，并在受理 companion commit 中消费 once。图的 `capture_current at admission` 与该边界一致。 |
| 两个绿色评分目标 | definition.cpp 139–141 与 160–162 行引用相同 `trade_score` / `calculate_score` / version / implementation identity。[computation.cpp](../../src/runtime/computation.cpp) 25–33、128–134、182–225 行按 Trigger 身份生成独立 capture、function relation、plan、status 和 latest，并复用函数注册。两规则没有 `required_accepted_binding`；图没有阈值与周期之间的先后边或 AND 汇合。两个绿色框表示独立请求的函数执行，正文也明确说明。 |
| Graph-Based 解释 | MV 的节点是输入/关系/函数计算，边是数据依赖；CEP 的节点是匹配状态，边是状态转移。正文用 A 后 B 的有界事件序列解释记忆和超时，明确状态图可有环、函数就绪由计算图及请求执行器管理，没有把无序任务 AND 或一般任务 DAG 等同 CEP。它明确当前 demo 仅使用条件和 Clock Trigger，没有声称图中是实际序列 Pattern。[trigger_pattern.hpp](../../include/eventflow/trigger_pattern.hpp) 10–39 行提供独立的顺序、组合、有限重复和强制 processing-time within 接口；[当前 Trigger 合同](../trigger-api-ergonomics.md)与[有界 CEP 合同](../cep-enfa.md)支持这些边界。 |

另行读取了 [RisingWave CREATE MATERIALIZED VIEW](https://docs.risingwave.com/sql/commands/sql-create-mv) 的流式增量维护/级联说明，及 [Flink 1.19 CEP](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/libs/cep/) 的条件、衔接和 within 说明；两份引用支持通用建模解释，不用于证明 EventFlow 已实现外部系统的全部能力。

## 独立发现与处置

### R1：图中的 capture 数据边被 Clock 节点遮挡，存在可读性歧义

初审图中 `features -> periodic_input` 的实线穿过/被 `periodic` 节点遮挡，容易误读为特征数据先经过 Clock，再到捕获输入。`session -> threshold` 的长虚线也与 MV 区域靠得很近。实际 Mermaid 边集合正确，这不是实现语义错误；但用户要求同图清楚区分持续数据依赖和触发控制边，建议通过分组或布局使这条 capture 边可连续追踪。

证据：目视 `/tmp/eventflow-readme-case-dag-final.png`；对应 JSON 的该边起点仍是 `features`、终点仍是 `periodic_input`。已向 root 报告；root 正在同一审查轮内调整布局，保持 10 个业务节点及 13 条业务边。此处保留原发现，后续追加定向复核，不开始新的 fresh-review 轮次。

## 本轮实际静态与渲染证据

独立 Python 检查已完成：从当前 README 提取审查节及唯一 Mermaid；与 `.mmd` 去首尾空白后逐字一致；图源码为 ASCII；围栏闭合；新增相对链接存在；Schema/case/DAG/建模理由顺序正确。读取渲染 JSON 确认 parsed=true、10 个业务节点、13 条业务边；逐点遍历确认无环，两 Trigger 之间无可达依赖，各捕获框仅连接自己的评分目标。独立目视 PNG，发现 R1。

初审对象指纹（用于保留整改前证据）：

| 对象 | SHA-256 |
| --- | --- |
| README 新增审查节（从三级标题到下一个三级标题前） | `a86e43ca30454e29d3fdf64c3e9e6daf5b0cbc9066dfcaa9e12755c5694f2bae` |
| Mermaid 围栏正文（不含末尾换行） | `9cbcb263384fbf54b8d9353997f0f994f2ab564be05d4ca1df12c4e6f8142606` |
| `/tmp/eventflow-readme-case-dag-final.png` | `e0bc855540d8bf28925a85b0c055088c11db41d056b109b60778d02ac6e4dbc5` |
| `/tmp/eventflow-readme-case-dag-final.svg` | `c48ab4a80724eb9704ea321a0a653bd7957b92a857c6d94a48b2dfe39d748379` |
| `examples/streaming_database/definition.cpp` | `aba0f675c329de518ce1c6d0c48461473f41df8ada8e0162fdaa585ccc54b378` |
| `examples/streaming_database/verify.cpp` | `003e3fd635d7ad68948377326b495c15a5dde465976ef53af9fe8895c52dd715` |
| `src/runtime/computation.cpp` | `db4a0420a2af0dce544a30e74d419311289bc91529152a3460aa31dffca09e5a` |
| `src/runtime/event_processor.cpp` | `19c789436615ccc5250bdf43c65f1d5da612e5f5406435827e902c09a0a6dc3b` |
| `src/runtime/request_runtime.cpp` | `830db18ad6683cd10277c02c64ed19287aa9393d35081ac01c28436e15c7197f` |
| `include/eventflow/trigger_pattern.hpp` | `918bd711ab8ccfdaf80e5a0b17f72a1ef2e904b2e01fab14355bc6e4a5961f40` |

本轮没有编译 C++、运行 CTest、启动集群、执行 Kafka/恢复/故障注入或重新验证旧 PNG。没有将历史 3/3 测试视为这张图通过的证据。浏览器中的 GitHub 最终渲染也未实测；当前视觉证据来自本地 Mermaid 渲染产物。

## 同轮定向复核与最终状态

root 将同一张图分为持续物化特征、阈值评分和周期评分三个区域，当前渲染产物改用 `/tmp/eventflow-case-dag-branches.{mmd,svg,png,json}`，原始产物保留。

独立复核结果：当前 README Mermaid 与新 `.mmd` 去首尾空白后逐字一致；10 个业务节点的标签完全不变；13 条业务边的起止节点集合完全不变。最初定向脚本同时要求所有边标签逐字一致，因此在三处标签精简上失败；随后逐项确认 `read updated total_volume -> read total_volume`、`session / once gate -> session / once`、`session gate -> active session` 均不改变业务含义，修正为“端点及节点标签不变，边标签仅允许这三处精简”的检查后通过。更新后 ASCII、围栏和本节相对链接检查通过。

已独立目视新 PNG：`market_features` 到两个 capture 输入的实线可连续追踪，周期分支的数据边从 Clock 下方进入 `periodic_score_request`，不再穿过 Clock 框；session 的控制边也不再被 MV 框遮挡。图中仍有普通连线交叉，但没有将交点画成依赖节点，也没有引入两个 Trigger 的串行关系。渲染 JSON 的 `collisions` 为空。R1 已关闭。

| 布局整改版检查对象（后续仅清理辅助标题） | SHA-256 |
| --- | --- |
| README 新增审查节 | `851a300a3605f272fc29b99920f3e22aa7ecaf621253f171552145e885ad3485` |
| Mermaid 围栏正文（不含末尾换行） | `c73ff4a0b8301278a6e595e5796819cb03ecc8567b7425270918edee551e37ec` |
| `/tmp/eventflow-case-dag-branches.mmd` | `d87c2bd840ea5e09099deb13ce02ab65b45c642514225197e059624f1192d301` |
| `/tmp/eventflow-case-dag-branches.png` | `1506daac234eeb43c6acd22732f9e7cf11bcb8eb494c15e521fe5630d98a5596` |
| `/tmp/eventflow-case-dag-branches.svg` | `4c209260ca9b778e78f6d7bbb454fdded6bc61a677446d78f139aba6122c6bc4` |
| `/tmp/eventflow-case-dag-branches.json` | `97112b22e923a182a7e9855848c26b61268b065704e02c765ef1994f79e31470` |

### 同轮最终标题清理

最终图源为 `/tmp/eventflow-case-dag-clean.mmd`。一次轻量定向核对确认：相较已审 branches 版，只有三个 `subgraph` 的辅助标题改为空白；业务节点标签、13 条边的端点及标签均不变。当前 README 围栏与 clean 图源一致且为 ASCII。将当前审查节的 Mermaid 换回 branches 图源后，所得正文指纹恢复为上表的 `851a300a...ad3485`，验证了图外正文未变。独立目视 clean PNG：辅助标题已移除，捕获边可追踪，未重新引入 R1。

采样证据更正：早期 branches JSON 只记录 `collisions=[]`，没有边覆盖计数；root 后续发现当时脚本未匹配实际 SVG id，该空列表不能单独证明无碰撞。已读取修正后的 `/tmp/eventflow-case-dag-layout-check.mjs`，确认未匹配路径或覆盖不足会失败；最终 clean JSON 实际记录 `checked_edges=13`、`checked_nodes=10`、`collisions=[]`。本 reviewer 独立核对脚本与结果及目视 PNG，没有把原空采样当作通过证据，也没有重新启动全量语义审查。

| 最终检查对象 | SHA-256 |
| --- | --- |
| README 新增审查节 | `7b964353763a1a8c2defe899022905e283d49c7b41f1ed1e2906a67fd01f8e87` |
| Mermaid 围栏正文（不含末尾换行） | `cbb8d87217afe53c9d4e814a84482d03d42f41a5e3d081846c84c610901a913f` |
| `/tmp/eventflow-case-dag-clean.mmd` | `239e8c9868d87b1ddc5cd04461e4aa4aa0ec3badfe0739047adf453788b1ef1c` |
| `/tmp/eventflow-case-dag-clean.png` | `62cdb03f2f3d0b9b9ccab36eafbfdce69edbc2a8f37ada047e62b5c9f13d6b89` |
| `/tmp/eventflow-case-dag-clean.svg` | `fc6cf6973a7aaa619c4f4cd859788fca53e919c79f4ad7144c73625ba656c407` |
| `/tmp/eventflow-case-dag-clean.json` | `b3661a3aa1a95c63540493dc7f02015d63858a6959785ef1934b7b9976b8e4de` |

Changed files：仅本报告。报告相对链接和行尾空白检查通过。该授权范围没有未关闭的语义发现；生产代码、测试和其他用户改动未由本 reviewer 修改。未验证范围仍以上节为准，整体任务状态由 root 在对应规格中收尾。
