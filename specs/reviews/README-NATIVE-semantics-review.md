# README 生产 UDF 图文同步：独立语义审查

Reviewer: readme_udf_semantics（fresh context）
Date: 2026-09-16
Scope: 本轮 README 和三张 PNG 与当前生产接口、运行时及原生 UDF 验收范围的一致性。未修改 README、生产代码或图片。

## 结论与发现

未发现首次部署流程、原子注册、加载执行、输入确认或 checkpoint 恢复边界的阻断性误述。发现一处应在本轮交付前消除的当前能力文案冲突：

- **P2 / 文案一致性：README.md:461 仍写“当前采用静态分片和已编译的计算定义”。** 本句对整个当前系统作限定，会使读者认为结构化计算定义仍必须预编译，与本轮新增 SDK 提交入口和同段后半句相冲突。生产 `CoordinatorService` 在 `submit_computation` 中接收并绑定序列化定义，编译要求属于 C++ 函数 `.so` 或静态模块。建议改为“当前采用静态分片；计算定义可由静态模块提供，或在首次部署前通过 SDK 提交”，保留运行中迁移后置边界。发现时尚未处置；实施者应追加实际修改及定向核对结果。

## 源码核对

| 合同 | 本轮独立读取的证据与判断 |
| --- | --- |
| STAGING 与控制状态 | `src/service/coordinator_service.cpp:885`、`:922` 限制上传/提交为启用后的首次 STAGING，显式恢复场景也不能注册；`:493` 持久化 INSTALLING、安装并验证所有目标，再保存 RUNNING、恢复拉取。`:998` 仅允许 RUNNING/PAUSED/QUIESCED 查询；README 没有承诺提交后即可查询运行结果。 |
| 原子注册与 UNKNOWN | 同文件 `save` / `resolve`（`:267` 起）将控制记录和制品 mutation 放入 control shard 的同一个 `CommitBatch`，UNKNOWN 保留 `pending_batch`。`:885` 起校验同 ID/version 的内容身份，限制 16 个版本，同字节重复返回原引用。图示及正文相符。 |
| 提交与单 Catalog | `include/eventflow/submitted_computation.hpp` 明确 DDL、输入关系、有限 session、typed 条件、Clock 与当前字段 capture。`src/runtime/submitted_computation.cpp` 绑定后调用 `TriggerSet::bind`；Coordinator 先加载并完整绑定，再保存 staging 定义及 Catalog 指纹。README 已把详细旧业务示例标记为静态 API，未把 Pattern/capture_match 宣称为提交 API。 |
| 原生 C++ ABI 与执行位置 | `include/eventflow/udf.hpp:7` 的 1 MiB 常量；`src/runtime/udf.cpp:127` 校验 ELF、构建身份、入口和签名，通过 memfd/dlopen 加载；`:175` 回调捕获共享 library，使用 C++ `FunctionInputs` / `StatusOr<Row>`，摘要进入 implementation identity。README 同 ABI、受信任、进程内线程池且无沙箱的边界正确。 |
| 部署/恢复加载 | `src/runtime/submitted_computation.cpp:338` 根据精确 FunctionRef 检查注册列表、从控制 shard 读取字节、核对摘要再打开库；Compute install（`src/service/compute_service.cpp:414` 起）绑定并核对 Catalog 指纹。Coordinator 恢复路径验证 checkpoint cut，再以已保存定义重装。正文没有把普通 KV snapshot 或消息服务存活等同于完整宕机恢复。 |
| submit、输入确认、ACK、completion | `src/runtime/computation_runtime.cpp:170` 持久领取后向执行器 submit；`:209` 的 ingest 成功输入继续 pump 才返回输入状态。`src/service/coordinator_service.cpp:635` 起固定投递身份/时间、读取 Compute 明确确认、保存控制进度；`src/ingest/source_poller.cpp:56` 起仅在输入确认后 ack 原 token，后续函数额度不足不撤销输入。completion 由 runtime 后台读取并提交。消息图先 submit 后 ACK，正文明确 completion 可以与确认并行且 ACK 不等待函数完成，符合本条成功路径。 |
| 示例与恢复数值 | 独立检查 `examples/native_udf/run.sh` 的五进程、四个数据库进程重启和不存在的恢复 UDF 路径；检查 `verify.cpp` 的 51.5/128/53/54.75 与固定旧结果断言。README 的数值和范围与示例及已完成生产规格一致。 |

## 图片与验证证据

- 使用 `view_image` 独立打开最终 `docs/image/eventflow-system-architecture.png`、`eventflow-message-sequence.png`、`eventflow-udf-deployment.png`。组件保留 Coordinator、多个 Compute、MV、Trigger/CEP/Timer、RequestRuntime、缓存、函数线程池和共享 KV；新增制品上传、原子存储与部署/恢复加载连线。三图语义与相邻说明一致。
- 首次部署图把 STAGED 响应与最终 RUNNING 区分，未暗示已运行计算图可变；消息图把受理、持久领取、submit、ACK 和独立 completion 提交分开。细分读写往返被压缩为示意，README 已说明提交边界仍独立。
- 独立读取 `/tmp/eventflow-readme-native-7l5v05wr/snippet-build.log`：Clang 18.1.3 成功构建 `readme_native_snippet` 对象目标。此证据验证新增片段的编译，不等于本 reviewer 重新执行集群。
- 查看当前 README diff，确认静态完整业务流程仍保留，入口、范围摘要、系统图说明及消息时序已同步。

## 未验证项与交接

本 reviewer 没有重跑产品全量测试、Release 构建或五进程示例；此前 65/65 属于 `native-udf-delivery.md` 所记录的生产交付证据，本次文档审查不刷新该结果。浏览器布局、导出回执及 README 宽度的专项检查由同轮视觉 reviewer 承担。没有要求扩大 UDTF/UDAF、运行时图迁移或消息服务持久恢复范围。

下一步：实施者处理上方 P2，追加定向文案验证与处置记录；无需另起 fresh-context 轮次。

## 实施者处置

该残留措辞已修正为：计算定义可以由静态模块提供，也可以在服务启动后通过 SDK 上传 UDF 并提交，随后在首次部署时固定。当前 README 已无“当前采用静态分片和已编译的计算定义”原句。该项与视觉审阅 V1 去重为同一问题，定向文字检查、链接与围栏检查通过；不重新发起审阅轮次。
