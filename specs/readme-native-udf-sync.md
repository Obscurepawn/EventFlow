# README 与生产 UDF 图文同步

Status: complete
Owner: root
Updated: 2026-09-16

## 授权与范围

用户要求生产代码完成后同步 README.md 中的图片和文字。本轮对齐 [生产 UDF 接入](native-udf-delivery.md)：服务已启动、STAGING 上传受信任 C++ `.so`、提交结构化定义、首次部署、从 KV 加载及 checkpoint 重载。保留静态 ComputationModule 示例与系统内部细节。

沿用既有静态 PNG 偏好：更新系统架构和消息处理图，补充首次部署时序图；README 不新增交互 HTML 入口。图源、浏览器检查和回执放仓库外临时目录。保留用户原 trigger.svg 和其他存量图片。

## 验收与工作状态

- [x] 移除“尚不支持 UDF/必须与内核联合编译”的过时当前描述，区分已实现切片与完整扩展族、在线迁移。
- [x] 增加实际 SDK 调用和运行入口，静态流程仍有明确标注；摘要、首次部署、C++ ABI、进程内执行和恢复边界与代码一致。
- [x] 图片呈现完整组件、上传/提交/部署、KV 制品和加载，消息图区分派发、ACK 与异步完成；不把运行时图简化成只剩 UDF。
- [x] 图示验证、静态导出和实际图像/README 宽度目视检查；链接、围栏、新增代码片段编译与文档静态检查。
- [x] 同轮两位 fresh-context reviewer 审阅图文，记录发现与定向处置。

已完成 README 当前能力、快速开始、SDK 片段、静态流程标注、组件说明与三张 PNG 同步。图源和临时证据目录 `/tmp/eventflow-readme-native-7l5v05wr/`。本轮只修改文档/图片，不重跑或改写此前 65/65 产品验收记录。


## 最终变更

- README.md：两种接入方式、原生 UDF 快速开始、实际 SDK 注册/提交/部署片段、首次部署时序、静态业务流程标注、架构/消息语义和当前源码导航。保留原作者背景讨论与静态 C++ 业务代码。
- docs/image/eventflow-system-architecture.png：保留十二个组件，补充 SDK 定义、KV 原子注册、制品加载与恢复。
- docs/image/eventflow-message-sequence.png：更新运行阶段顺序，明确先 submit 再确认输入/ACK；completion 与 ACK 不存在等待依赖。
- docs/image/eventflow-udf-deployment.png：新增注册、提交和首次部署时序。
- docs/AGENTS.md：读者文档指南改为引用最新生产 UDF 规格。
- 本规格、INDEX 和两份独立审阅报告。

## 验证证据

1. 三张图最终 validate/deliver 均为 9/9 showcase、0 composition errors、0 warnings；原生 PNG 导出回执均为 canonical=true。来源与回执保存在上述临时目录，图源/HTML 不进入 README。
2. 架构 HTML 的四尺寸自动 browser check 通过，回执绑定当前 HTML。两张纵向时序图初版 HTML 的全屏 containment 检查因纵向滚动失败；该记录属于旧候选，不能作为最终版本的通过或完整检查证据。本轮最终交付是静态 PNG，不交付这些 HTML，也不承诺它们的全屏 Viewer 布局。
3. 最终三张 PNG 通过本地 Chromium 在 README 指定的 1100 px 宽度实际渲染，scrollWidth 均为 1100；全图高度分别为 704/1132/1168，无水平溢出或图像裁切。作者及两位 reviewer 均检查实际图像，主要标签、箭头和阶段完整可读。时序图去掉细小辅助注释，将必要含义并入主标签。
4. 最终 PNG 逐 chunk CRC、尺寸与 SHA-256 检查通过；README 的三个图片引用、全部本地路径、代码围栏和 git diff --check 通过。
5. 新增 SDK 片段从 README 提取，放入 Status/noexcept 工厂，Clang/C++20、-Wall/-Wextra/-Wpedantic/-Werror 与三 sanitizer 编译通过；视觉 reviewer 又独立 clean-first 重编译通过。该检查只证明片段与当前 API 的编译一致性，不冒称重新运行产品测试。
6. 同轮 fresh-context [语义审阅](reviews/README-NATIVE-semantics-review.md)、[视觉审阅](reviews/README-NATIVE-visual-review.md)完成。唯一去重问题为“已编译的计算定义”的残留表述，已修正并定向检查，不再开启新一轮审阅。

用户旧图源 trigger.svg 在当前工作树中不存在，本轮未创建、修改或删除该输入；除上述三张交付图外，其他存量图片未改动。中间 HTML、JSON、截图和构建片段保存在仓库外。既有 .skills/use-eventflow 内容仍面向静态模块，README 的原生接入提示已直接引用当前原生示例与规格，避免要求 Agent 先采用旧能力边界。

## 最终图片身份

| 文件 | 尺寸 | 字节数 | SHA-256 |
| --- | --- | --- | --- |
| docs/image/eventflow-udf-deployment.png | 3600 x 3704 | 549995 | `296c72f4a803610ff71a2b5ca974737636097e2420edd7e18bc406e62a4f7a2d` |
| docs/image/eventflow-system-architecture.png | 4000 x 2560 | 756070 | `95521c80e0fe80abf5f7c290b843624f3e6eed56df57500462db042352005f66` |
| docs/image/eventflow-message-sequence.png | 3600 x 3824 | 584918 | `99e370b105de060c5e3b124f9d417402fe950929c9529b9d9ac2ceec2ea0d7aa` |

本轮图文同步已完成，无剩余任务；后续 API 行为变化须按新源码刷新图文，不沿用这次图像或历史测试证据。
