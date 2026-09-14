# README 架构与消息处理时序图

Status: complete
Owner: root
Updated: 2026-09-15

## 授权与最终范围

用户要求用 archify 重画根 README 系统架构图，并增加消息处理、特征计算与 Trigger 状态维护的时序图。后续指定保留原图细节、使用本地无界面浏览器、不添加 HTML 交互入口，最终只交付 PNG。时序素材最终改为用户提供的根目录 `trigger.svg` 对应 PNG，原样保留其纵向布局、文字和箭头。

最后一次清理授权只涵盖本次在 `docs/image/` 新增、没有被 Markdown 使用的文件；存量图片及用户输入文件不纳入清理。并行新闻示例修改保留，不修改其 owner 或验收状态。

## 图文合同与验收

- [x] 对照旧图及当前源码，保留静态 ComputationModule/单 Catalog、生产/拉取/ACK/重放、Coordinator、不同 shard owner、共享 KV 及控制状态。
- [x] 在架构 PNG 中展开 Engine/MV、Trigger/CEP/Timer、RequestRuntime/Machine、函数线程池、CachedStateStore；RemoteStateStore 位于下层 HTTP 路径。
- [x] 架构图最终 validate/deliver 为 9/9 showcase、0 errors、0 warnings；四尺寸本地 visual-check 通过，实际读取 PNG 与 1100 px 嵌入截图。
- [x] 将用户指定 trigger.svg 原样转换为 3240×3330 PNG，检查源 hash 不变、PNG 完整可读及 CRC；不借用已退出候选的 HTML 检查声明。
- [x] README 相邻正文区分输入提交、发出/受理、固定输入、函数完成、UNKNOWN 与 checkpoint；明确用户选图为阶段示意，当前成功路径实际先 submit 再 ACK，ACK 不等待函数完成。
- [x] 同轮两位 fresh-context reviewer 并行审阅，三项发现保留并记录定向处置；不重复开启审阅轮。
- [x] README 只引用两张 PNG，链接与围栏检查通过；本次 20 个未引用中间文件移出仓库，存量图未修改。

| 工作 | Owner | 状态 |
| --- | --- | --- |
| 当前源码与原图核对 | root | 完成 |
| 详细架构图与用户指定时序 PNG | root | 完成 |
| README 接入与范围内清理 | root | 完成 |
| 同轮独立审阅与定向复验 | root / reviewers | 完成 |

## Changed files

- `README.md`：系统结构段落的新图、职责说明与时序解释；其他业务示例变更属于并行任务。
- `docs/image/eventflow-system-architecture.png`：4000×2560 详细架构图。
- `docs/image/eventflow-message-sequence.png`：3240×3330，来自用户指定 SVG。
- 本规格、`specs/INDEX.md`、[验证记录](reviews/README-diagrams-validation.md)、[语义审阅](reviews/README-diagrams-semantics-review.md)、[视觉审阅](reviews/README-diagrams-visual-review.md)。

## 证据与边界

最终 PNG、生成规格/检查用 HTML、用户源 SVG 的 SHA-256 与检查范围记录在验证文件中。独立审阅保留原发现，作者补充最终图片与定向复验；用户后来指定的时序 SVG 没有被冒称为独立 reviewer 已审过的新快照。

本轮图示只在 `docs/image/` 新增两张被 README 引用的 PNG。未引用的 HTML、SVG、JSON、截图与机器回执已移到仓库外临时目录；用户提供 SVG 的样式和内容在转换时保持不变，记录保留源 hash。没有重跑产品 C++ 构建、CTest、真实 Kafka，也未改变系统既有里程碑或后置范围。

无剩余实施任务。后续若需修改图中文字，按用户当前交付偏好重新生成静态 PNG；本轮没有新增版本化图源或交互页面。
