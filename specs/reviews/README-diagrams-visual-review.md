# README 图示独立需求与视觉审阅

日期：2026-09-15。审阅者：`diagram_visual_review`，本轮独立 fresh-context reviewer。范围为本次 [README 图示规格](../readme-archify-diagrams.md)、[README 系统结构段落](../../README.md#系统结构)、两张新增 PNG、原架构图，以及作者本轮生成的交付和浏览器检查材料。本报告不修改图稿、图片、README 或并行新闻示例文件。

结论：原图的主要细节已保留，新增时序图能辨认各轮提交与异步结果返回；未发现节点被线穿过、文字互相遮挡或 PNG 带出 Viewer 控件。存在一项需要处置的可读性发现：架构图缩小显示时说明文字明显过小。机器回执通过与这一目视发现分别记录，不能互相覆盖。

## 原发现与处置状态

### V1 / P2：架构图在桌面默认比例下的说明文字过小

**位置与证据**：[架构 PNG](../../docs/image/eventflow-system-architecture.png)；同名前缀的 `visual-check.1440x900.light.png`。已实际打开截图，节点副说明、边标签和 Compute A 的 owner 边框说明明显小于常规正文，读者需要放大才能精读。回执记录该视口图宽 944、作者 viewBox 宽 1380，最小节点说明仅约 **6.16 px**；例如“持续生产业务事件”属于该字号。2048×1320 深色截图中的同类文字较清楚，但仍偏小。

[README](../../README.md) 为架构 PNG 指定 `width="1100"`；按当前 1380 宽 viewBox 与 9 px 副说明计算，其投影约 **7.17 px**。这是几何推导，不是已完成的 README 页面渲染测量。原始 PNG 为 4140×2640，实际打开后文字边缘清楚；提高导出分辨率无法单独改善嵌入后的物理字号。

**影响**：保留了文字内容，但在入口文档中难以直接阅读这些细节。`visual-check` 的 6 px 下限已经满足，仍不能据此把主观可读性判为通过。

**建议与状态**：收紧图稿中的间距和画布留白、保留或增大文字尺寸，再查看最终 PNG 及实际嵌入比例。不要通过删去语义细节或进一步缩小字体解决。已向作者报告；作者确认将在同一轮整改，当前报告保留原发现，尚不宣称修复完成。

本次目视状态：`visual_review: failed`，原因是 V1。这是针对下方锁定版本的结论；整改后应追加定向复验与新哈希，不删除本发现，也不自动开启另一轮独立审阅。

## 需求覆盖与视觉观察

| 核对项 | 实际观察 |
| --- | --- |
| 保留原架构图细节 | 对照 `eventflow_streaming_architecture.png`，仍包括静态 ComputationModule/Catalog、生产端、消息系统 topic/partition/offset、poll/返回/确认后 ACK、SQL 客户端、Coordinator/SourcePoller、按 key 路由、多 Compute、共享 Memory/RocksDB KV 与控制状态/checkpoint。原图中 offset 100/101/102 等样例记录和 shard 编号改为概括表达，没有因此丢失其消息位置及业务/控制 shard 含义。 |
| Compute 内部展开 | Compute A 明确包含 MV、Trigger/CEP/Timer、请求状态机、函数线程池、有界缓存；Compute B…N 明示具有相同内部结构、负责其他 shard。比原图补充了请求固定输入、once、owner/epoch、UNKNOWN 和函数完成返回路径。具体实现命名与调用先后以同轮语义 reviewer 的审阅为准。 |
| 新增时序图 | 六条生命线和四个阶段均可辨认；能沿箭头跟随拉取、返回投递身份、路由、特征稳定后检查 Trigger、输入提交、请求受理、输入确认、ACK、函数派发及完成结果提交。图中 ACK 文案明确无需等待函数完成。 |
| 线条与标签 | 实际查看两张最终 PNG、两图 1440×900 浅色及 2048×1320 深色截图。没有观察到不相关节点被连线穿过、主要标签互相遮挡、文字被节点或页面裁断。架构右侧的控制与其他 Compute 路由较密，但仍能辨认。 |
| PNG 导出 | 两张 PNG 仅含图形、必要分区和图例；没有 HTML 标题栏、主题/导出按钮、导航浮层、搜索框或说明卡残留。PNG 不会自动携带 HTML 下方补充卡片；README 的相邻文字保留了 owner、UNKNOWN、ACK 与 checkpoint 的主要边界。 |
| README 引用与围栏 | 系统结构段落恰好引用两张本地 PNG，文件存在，没有 HTML/SVG/JSON 链接或交互入口。读取整份 README 的围栏检查得到 12 对，未发现未闭合围栏。本项不覆盖并行新闻示例的业务语义。 |
| 最终只保留 PNG | 审阅时仍有图稿、HTML、SVG、回执及截图，作者已说明正在移出。它们属于审阅过程中的中间状态，不记录为最终交付缺陷。最终仍须核对本次新增图示只留下两张目标 PNG，原有图片可保留。 |

## 锁定版本与检查证据

本审阅直接读取文件，使用 Python 标准库重新计算 SHA-256、字节数和 PNG 尺寸，并验证两张最终 PNG 的每个 chunk CRC。JSON/HTML 的当前哈希及字节数均与各自 `.delivery.json` 一致；HTML 同时与 `.visual-check.json` 绑定值一致。

| 对象 | 尺寸或字节数 | SHA-256 |
| --- | --- | --- |
| 架构 PNG | 4140×2640；692085 bytes | `56eb9f6707d4946a4256255c618e5e7d0ea7fd7e05ddda2f04af8bce6578653c` |
| 时序 PNG | 5520×2600；679834 bytes | `eb0fef6c9dff46fe330b59557901472e4ff9494fb97137aef469c42bbe154603` |
| 架构 JSON | 8381 bytes | `83e811d741c03206f591a605a508a92d2b65b8bbbe1bf19219c65bbae4157e65` |
| 架构 HTML | 821997 bytes | `88e0376c21be628e1667b3854d368f436e6caf1def46284f3c61583251922421` |
| 时序 JSON | 5028 bytes | `8ae74da058485d9772731475d21cbf6b58f493b55437848818eb66a57f28301e` |
| 时序 HTML | 811580 bytes | `05c10e9d7411f342f8beb32fd28b53397fd66de59a69c231eaa200b3f8601b64` |

README 系统结构段落的 SHA-256 为 `1303695ebb783118b02ac46345e17434c7caf832df0e2a79da81deb64e8b232c`，范围从“系统结构”标题到下一二级标题之前。独立记录该段落，避免并行新闻示例修改使整份 README 哈希失去比较意义。

两份交付回执均为 `ok: true`，`showcase` 为 9/9、0 errors、0 warnings。两份浏览器回执均为 `status: pass`、`evidenceKind: automated-browser`、READ/Still 状态，覆盖 1440×900、1600×1000、1920×1080、2048×1320；各视口 `scrollWidth/scrollHeight` 等于视口宽高，无溢出。四种浅/深色截图组合均存在且尺寸吻合。浏览器可执行文件是本地 `/home/obscure/.cache/ms-playwright/chromium-1232/chrome-linux64/chrome`。回执的 `visualReview: pending` 正确保留了机器证据边界。

本 reviewer 实际目视的四张浏览器截图哈希如下，另两张目标 PNG 已在上表绑定：

| 截图 | SHA-256 |
| --- | --- |
| 架构 1440×900 light | `75bcfc89c95350c3fb36541697af7adfd56bc95e1a2650660c2054de52081990` |
| 架构 2048×1320 dark | `f75800837d8263cf4a870743ae9db87528091e31100a39b8fb4db7e7ed3283a9` |
| 时序 1440×900 light | `30cb33d588fd4bb83727a2b785d97bc71fcad80cd854db858fb6d45e68a41f7f` |
| 时序 2048×1320 dark | `f387d4bb1537ccb65c35389ef7019846a671f0618e971c84a69e9b71f967c739` |

## 未验证项与下一步

- 本 reviewer 核验了现有交付与浏览器回执，没有重新运行 `deliver` 或 `visual-check`；目视结果来自实际图片读取，不把作者自查或机器回执称为独立视觉判断。
- 仓库外补充的 1100 px PNG 嵌入页面首次使用本地 Chromium CLI 截图时，在 25 秒后超时，未生成截图；因此不声称已通过实际 GitHub/Markdown 渲染环境检查。没有使用用户 Edge 或任何 MCP 浏览器。
- 未检查移动端、其他 Markdown 渲染器、交互功能和深色 PNG 导出；最终交付范围为静态 PNG。未重新核验全量生产源码语义、构建、C++ 测试、真实 Kafka 或产品历史验收。
- 作者完成同轮定向修正后，应追加 V1 处置、最终 PNG 目视与新哈希。交付前再核对 README 引用、围栏、本次两张 PNG 及中间文件清理；当前中间状态不作为最终缺陷。

## 实施者同轮处置（2026-09-15）

- V1 定向调整：架构图作者坐标宽度由 1380 收紧到 1000，保留所有组件和语义连线；以新回执重新导出 PNG。最终 1440×900 的最小节点正文约 6.96 px（原约 6.16 px）；已另外用本地无界面 Chromium 把最终 PNG 以 1100 px 宽嵌入页面并读取实际截图，主要节点与连线可辨。图内长副说明仍小于常规正文，README 的职责表保留完整说明。
- 已目视最终架构 PNG 和实际嵌入截图；实施者定向 `visual_review: passed`。该结论与上方原快照的独立 `failed` 分开，不改写原发现。
- 用户随后选定根目录 `trigger.svg`，最终时序 PNG 是该文件原样转换的纵向图；源 SVG SHA-256 与转换前后相同，作者已读取最终全图。原 reviewer 的时序 PNG 哈希与 HTML 四尺寸回执不适用于该新选图。
- 最终架构 PNG：`b334abf8dc3a910a52a71b7338806a70355a6798de8db27acc93a49d00911406`；最终时序 PNG：`7cfac776ed14204935ace0cf17a44af55c4147324045601f134ea2cf5b630652`。两图解码/CRC、Markdown 引用、围栏及本次中间文件清理均已定向复验。
- 最终[验证记录](README-diagrams-validation.md)说明源图选择、严格调用顺序与阶段图的差异及验证范围；没有重新开展 fresh-context 审阅。
