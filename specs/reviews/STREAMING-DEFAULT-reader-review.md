# 默认上传 UDF 示例：独立读者与使用说明审查

日期：2026-09-16。Reviewer：streaming_readme_review（独立 fresh context）。

## 范围与结论

按根目录、docs、examples、specs 的 AGENTS.md 和 [本轮规格](../streaming-udf-default.md)审查默认示例迁移、根 README 的上传主流程、公开 API、UDF 分发解释、配图和验证证据。使用 develop-with-specs 工作流保存报告；不修改生产实现、个人前言或其他作者文案。

根 README 已以独立 `.so`、`SubmittedComputation`、SDK 上传/提交/首次部署为主线。关于文件保存到共享 KV、Compute 部署和恢复时读取并加载的中文解释具体自然，未发现必须继续重写的措辞。两项文档发现需要实施者定向修正后再关闭本轮验收。

## 发现

### R1：文档导航仍否认上传接口，并把默认示例描述成静态模块（P2）

- 位置：`docs/README.md:11,28,36`。
- 事实：导航链接和命令指向当前 `examples/streaming_database`，正文却要求静态函数和相同计算模块，声称“当前没有动态上传函数”且把动态 UDF 全部归入后置范围。
- 影响：读者从根 README 进入文档导航后，会获得相反的当前能力和使用流程。
- 建议：默认流程改为独立 `.so` 上传、提交定义、首次部署；静态模块仅链接到 `static_streaming` 作为参考；后置项限定为运行中更新与完整扩展族等尚未实现部分。
- 处置：已通知实施者，待修正及文字/链接定向核对。

### R2：根 README 的审计说明残留静态示例的名称与取值入口（P2）

- 位置：`README.md:329,355`。
- 事实一：周期规则的 `event_kind` 写作 `news_clock`；上传定义由 `src/runtime/submitted_computation.cpp:296-299` 生成 `item.name + "_clock"`，当前实际应为 `periodic_impact_clock`。
- 事实二：业务 key 映射指向 `bound.explain()`，但根 README 上传流程只获得 `submit_computation` 返回的 `staged` JSON；对应入口为 `staged["explanation"]["trigger_audit"]["scope_columns"]`。`README.md:397` 已正确说明 explanation 来自提交返回值。
- 影响：读者核对审计行时看到错误的预期名称；照正文定位 API 时需要跳回静态绑定流程。
- 建议：同步实际周期事件名与 SDK 提交返回值，不改变实现。
- 处置：已通知实施者，待定向核对。

## 已核对的依据

- `examples/streaming_database/definition.cpp` 与根 README 声明代码一致，新闻 Schema、30 分钟 session、阈值和 200 ms Clock、三字段固定输入均匹配。生产 UDF 返回 `Row`，静态参考的 typed 计算定义已移至 `examples/static_streaming`。
- `coordinator_service.cpp:884-939` 在 STAGING 检查、加载上传文件，同批保存注册引用和原始字节，提交定义时读取 UDF 并绑定；`compute_service.cpp:413-425,480-482` 在部署检查和分片初始化中加载提交定义；`submitted_computation.cpp:338-364` 从 control shard 按摘要读取 `.so`、校验摘要并调用 UdfLibrary。
- `examples/native_udf/run.sh` 只转发默认脚本；CMake 分别注册默认上传示例与静态示例，未为兼容入口重复注册端到端测试。
- 实际查看两张图的 1100 px 嵌入版本：`/tmp/eventflow-streaming-reorganize-zql2hhwv/architecture-embedded.png` 与 `deployment-embedded.png`。节点、箭头和文字可辨认，上传保存、部署读取及恢复路径符合当前解释。原 PNG 尺寸分别为 4000×2560、3600×3704；嵌入尺寸分别为 1100×704、1100×1132。
- 独立检查根 README、docs/README 和三个示例 README 中的本地链接及图片路径，均存在；本次未执行完整 Markdown 渲染或所有 anchor 检查。
- 阅读 `/tmp/eventflow-streaming-readme-snippets.log`：Clang 18.1.3、CMake 成功编译四个 README C++ 片段。
- 阅读 `/tmp/eventflow-streaming-default-ctest.log`：默认上传示例、静态示例、udf_test、submitted_computation_test 共 4/4 通过，50.12 秒。
- 阅读 `/tmp/eventflow-streaming-default-alias.log`：兼容入口得到恢复后 54.75、旧固定结果 51.5、no_reupload=true，四个数据库进程和消息进程正常退出。

## 未验证项与交接

本 reviewer 未重新运行集群或完整构建；以上编译与端到端结果是本轮实际日志的独立核阅，不能描述为 reviewer 新跑的测试。Release 构建/安装和所有脚本实现质量由并行工程 reviewer 与实施者验收；本次不宣称真实 Kafka、消息服务宕机恢复、运行中改图或完整扩展族通过。

下一步：实施者修正 R1/R2，在本报告追加对应处置与定向证据；不自动发起第二轮 fresh review。

## 实施者处置

文档导航已改为 SDK 上传、提交和首次部署，不再否认原生 UDF 接口；审计说明改为 staged["explanation"] 和 periodic_impact_clock。根 README 的 Coding Agent 章节按用户补充要求，明确先加载 .skills/use-eventflow，再用新闻业务 demo prompt 描述需求，删除技术交付清单。更新后的链接、围栏和 skill 校验通过，未改动已验证的运行代码，不新增审阅轮次。
