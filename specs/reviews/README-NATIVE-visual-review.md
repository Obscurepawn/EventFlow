# README 原生 UDF 图文：独立视觉与交付审查

日期：2026-09-16。Reviewer：独立 fresh-context visual reviewer。同轮全局审查之一。

## 范围与结论

读取根、docs、specs 协作约定、[规格索引](../INDEX.md)、[本轮规格](../readme-native-udf-sync.md)与完整 [README](../../README.md)，检查三张交付 PNG、实际 1100px 嵌入截图、回执与新增 SDK 片段。未修改 README、图片、产品代码或 skill 文件。

三张静态图在本次 1100px 检查中完整可读，没有发现正文或箭头被裁断的阻断项。发现一处低风险陈旧措辞，已交实施者处理。此结论覆盖文档与静态图交付，不重新宣布产品测试或独立 HTML 查看器全部通过。

## 原发现

| 编号 | 级别 | 原文与问题 | 建议及处置 |
| --- | --- | --- | --- |
| V1 | 低 | 审查时 README 第 461 行仍称“当前采用静态分片和已编译的计算定义”。相邻文字已介绍 SDK 提交结构化定义，这一概括容易继续暗示计算声明必须随 Server 编译。 | 建议改为“当前采用静态分片，计算定义在首次部署时固定”。已通知实施者；由实施者追加实际处置与定向验证。 |

## 实际视觉检查

通过图像查看工具逐张打开 `/tmp/eventflow-readme-native-7l5v05wr/` 下的 `architecture-embedded.png`、`deployment-embedded.png`、`runtime-embedded.png`，按实际 1100px 输出审阅。脚本读取 PNG 头，并逐字节比较仓库图片与本轮临时导出，三张均一致。

| 交付图片 | 原图尺寸 | 嵌入尺寸 | 观察 |
| --- | --- | --- | --- |
| [系统架构](../../docs/image/eventflow-system-architecture.png) | 4000 × 2560 | 1100 × 704 | 保留消息生产与读取、Coordinator/SourcePoller、多 Compute、MV、Trigger/CEP/Timer、请求机、线程池、有界缓存和 KV。UDF 上传、KV 制品及加载路径可辨，箭头未穿过节点正文。次要说明字号较小，但该宽度仍可辨。 |
| [首次部署](../../docs/image/eventflow-udf-deployment.png) | 3600 × 3704 | 1100 × 1132 | 注册、提交定义、首次部署三个分组完整。注册记录与字节原子保存、FunctionRef 返回、Compute 读取库、全部安装确认后 RUNNING 的先后关系可读。底部返回与图例未被截断。 |
| [运行时消息处理](../../docs/image/eventflow-message-sequence.png) | 3600 × 3824 | 1100 × 1168 | 输入轮、受理派发、源确认、后台完成四个阶段完整。持久领取在 submit 前，ACK 与 completion 分开，UNKNOWN 原批确认文字清楚。未简化成仅展示 UDF 调用。 |

README 三个 `img` 均使用本地 PNG、`width="1100"` 和描述性 alt；相邻正文解释部署前 STAGING、进程内执行、提交能力与静态 Pattern 的差别，并说明完成发生在 ACK 后仅是一条可能时序。README 未引入本轮临时 HTML/JSON 链接。

原图 SHA256：

- architecture：`95521c80e0fe80abf5f7c290b843624f3e6eed56df57500462db042352005f66`
- deployment：`296c72f4a803610ff71a2b5ca974737636097e2420edd7e18bc406e62a4f7a2d`
- runtime：`99e370b105de060c5e3b124f9d417402fe950929c9529b9d9ac2ceec2ea0d7aa`

## 回执与独立验证

- 检查三份当前 `*-delivery.json`：各为 `9/9`、`showcase`、零 errors/warnings。这是生成工具结构检查，不能单独替代目视检查。
- 检查 `readme-width.json`：三图宽度及 `scrollWidth` 均为 1100；与实际截图尺寸一致。
- `architecture-browser.json` 的 HTML SHA 与当前临时 HTML 一致；记录的四尺寸 containment 通过。
- `deployment-browser.json` 与 `runtime-browser.json` 都是此前候选的失败记录，原 containment 因纵向滚动未通过；其 artifact SHA 也与最终临时 HTML 不同。不得把这些记录写成最终时序 HTML 全视口检查通过。当前交付是完整静态 PNG，视觉证据是上述最终导出和完整嵌入截图。
- 独立扫描 README Markdown 链接及 HTML `src`/`href`：本地目标缺失数为 0。围栏标记数为 28，人工阅读未见未闭合代码块。
- 检查新增 SDK 片段的 `snippet.cpp` 与 README 调用顺序一致，包装在 `Status ... noexcept` 中并包含相应公开头和示例定义。独立执行 `cmake --build /tmp/eventflow-readme-native-7l5v05wr/build --clean-first --parallel 2`，Clang CMake 目标 `readme_native_snippet` 重新编译成功，退出码 0。

## 未验证与边界

本审查没有重跑集群、checkpoint、65/65 产品回归或真实 Kafka；片段检查为编译，未执行部署。未承诺窄屏 GitHub 容器下仍有同等字号，也未把 1100px 独立嵌入页描述为实际 GitHub 页面渲染。图片可单独打开原图阅读，README 相邻说明也保留关键协议与能力边界。源码语义的深入核对由同轮另一位独立 reviewer 覆盖。

## 实施者处置

V1 已与语义审阅发现一并修正：明确静态模块或 SDK 提交两种定义来源，以及首次部署固定的边界。三张 PNG 未因文案修正再次改动，最终源码/图像身份和验证范围见 specs/readme-native-udf-sync.md。本轮没有将旧候选 HTML 的浏览器结果套用到最终时序图，也未再启动独立审阅轮次。
