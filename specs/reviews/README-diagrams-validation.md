# README 图示验证

Updated: 2026-09-15
Status: complete

## 最终范围

本轮交付根 README 的详细架构图及消息处理时序图，仅新增两张 PNG。时序图按用户最后指定的 `trigger.svg` 原样转为 PNG；先前自绘时序图的校验回执属于已退出的候选，不作为最终时序图的验收证据。

仅清理本轮在 `docs/image/` 新增且未被 Markdown/HTML 资源引用的 20 个中间文件；本轮清理未修改存量图片、并行新闻示例文件或用户提供的输入素材。没有增加 HTML/SVG 交互入口。

## 最终产物

| 图片 | 尺寸 | 字节数 | SHA-256 |
| --- | --- | --- | --- |
| `eventflow-system-architecture.png` | 4000×2560 | 758872 | `b334abf8dc3a910a52a71b7338806a70355a6798de8db27acc93a49d00911406` |
| `eventflow-message-sequence.png` | 3240×3330 | 355989 | `7cfac776ed14204935ace0cf17a44af55c4147324045601f134ea2cf5b630652` |

## 架构图

- 最终 validate / deliver：9/9 showcase，composition 0 errors / 0 warnings，退出码 0。
- specification SHA-256：`39f6c646b1fd18bec7d8b6ecedd9ab62040b51bd2d9e0e5fd9a6f6670f769373`，8395 bytes。
- 检查用 HTML SHA-256：`24ea74d090ae7b64d468234fec4e4e8754527255f25cf837f226c25317c08bf9`，820005 bytes。
- `visual-check` 退出码 0；1440×900、1600×1000、1920×1080、2048×1320 的宽高 containment、可读性与 Viewer 布局均通过，浅/深色截图分别生成。
- 目视最终 PNG、两端尺寸浅/深色截图和 1100 px 嵌入截图，确认主要节点与链路可辨、无相互遮挡；这部分为实施者定向复验，不冒称第二轮独立审阅。
- 有界缓存显式标为 CachedStateStore，下层 HTTP 连线标为 RemoteStateStore；缩小作者坐标与留白，保留全部 12 个组件。最终 PNG 来自自带 canonical 导出，无 Viewer 控件。

## 用户指定的时序图转换

- 源文件：根目录 `trigger.svg`；SHA-256 `b476f939f67c398c67eb4ec55ec386f7689b65602bb5b9d7e917b91e65311689`，155536 bytes，转换前后字节相同。
- 本地无界面 Chromium 直接载入 SVG，按原 1080×1110 viewBox 渲染，CDP 截取 scale=3，得到 3240×3330 PNG；未改动 SVG 的样式、布局、文字或箭头。
- 读取最终 PNG 全图，确认六条生命线和完整时间轴可见，保留用户选定的纵向版式。
- 原 SVG 按阶段将 ACK 放在函数派发前；依用户要求保留原图，README 紧邻图片说明当前就绪成功路径实际先 submit，再返回输入确认及 ACK，且 ACK 无需等待函数完成。此图片是阶段示意，不将它声称为严格调用顺序一致的源码 trace。
- 常规 Playwright 对独立 SVG 的截图入口超时，没有把失败输出当作成功；改为同一本地 Chromium 的 CDP screenshot 后正常输出并核对。最终时序图不套用已退出候选的 9/9 或四尺寸 HTML 检查声明。

## 通用检查与独立审阅

- 两张最终 PNG 解码和每个 PNG chunk CRC 均通过，尺寸及 SHA-256 与最终记录一致。
- README 的本地文件引用目标存在，围栏闭合；系统结构段落恰好包含两张 PNG，未引用 HTML/SVG/JSON。
- 同轮并行 fresh-context 审阅：[语义报告](../reviews/README-diagrams-semantics-review.md)、[视觉报告](../reviews/README-diagrams-visual-review.md)。保留三项原发现与实施者处置，不启动第二轮。
- 仅此文档范围完成；没有重跑 C++ 构建、CTest 或真实 Kafka，没有用历史测试数量证明本轮图片正确。

## 本地工具与中间证据

- Archify doctor 检查通过，Node.js v20.19.6。
- 后续检查使用本地 `/home/obscure/.cache/ms-playwright/chromium-1232/chrome-linux64/chrome`，通过 `ARCHIFY_CHROME` 指定；未再使用用户浏览器。
- 已退出的候选、最终架构回执、完整浏览器检查和转换回执移到 `/tmp/eventflow-readme-diagrams-20260915-yvbtaf3a`，为可丢弃的本地中间材料，不是仓库交付文件。
- 用户提供 SVG 在转换时字节保持不变，源 hash 见上；本轮新增 `docs/image/` 文件仅剩两张被 README 引用的 PNG。

这份精简验证记录置于非忽略的 reviews/，随推送和源码压缩包交付；详细机器回执与截图保持为仓库外临时材料。
