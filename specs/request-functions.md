# 统一视图图中的请求计算

Status: active（核心已实现，最终门禁待确认；通用 host 与用户扩展后置）
Owner: root
Updated: 2026-09-14

## 目标

在现有Catalog/MV图、EventProcessor唯一owner与同shard提交上完成固定输入的异步函数视图、共享祖先、请求/attempt终态和A/B/C/D示例。依据[运行时契约](runtime-contracts.md)及[独立接口挑战](archive/request-history.md#function-design)。不建立第二份可独立编辑的业务计算DAG。

用户最新优先级以[五小时核心规格](core-delivery.md)为准：核心与三个demo先交付。此阶段使用版本固定的内部确定性函数；Simple/ThreadPool 共用执行契约，真实 Compute 已接入线程池。动态.so上传/分发及UDF/UDTF/UDAF三类用户扩展后置。用户已将Process Function及其专门调研移出本次交付；不能以内部函数或FSM回调宣称三类扩展已完成。

## 采用的接口与不变量

1. `RelationKind`显式区分基表、SQL视图、FunctionView和LatestView；基表有PublicInput/RequestCapture/RuntimeStatus写角色。普通apply/hook不能绕开受控捕获或函数结果入口。
2. RequestCapture定义业务partition前缀加一个request_id的完整identity；request_id不加入business partition。`identity_domain`及规范identity_fields防止同类型不同语义身份被错误对齐。
3. 单输入SQL projection/filter保留完整identity domain，改变identity的聚合不保留。FunctionView的多个输入按同完整identity点读对齐；全部依赖来自Catalog，函数输出由host添加规范identity可见列，worker只返回payload。
4. Catalog持久描述包含函数名称/版本/实现身份及精确签名，参与fingerprint/deepclone/from_snapshot验证；普通SQL默认metadata保持原编码兼容。受控关系只通过绑定Registry的private owner启用，不能误当外部可写基表。
5. RequestRuntime属于EventProcessor的唯一Engine owner。源输入/MV/FSM/Trigger先提交；后续受理在已确认R捕获，R+1原子保存Manifest、固定输入、record/index、occurrence消费和once。各次claim/completion另有原子批，函数结果、请求状态、Latest及后继变化在对应完成轮一致发布。完成消息有独立入口，不伪造SourceIdentity。
6. 节点状态区分“尚未完成”和“已完成但SQL无输出”；必需无值依赖按声明终结/跳过，不能永久等待。共享祖先正常只受理一次；attempt严格fencing，旧回包和终态后的重复无副作用。
7. 函数池只执行固定typed输入；成功提交TaskOutbox后才派发。输入满不能阻塞completion/timeout/cancel释放额度。Simple和真实线程池共用契约；函数版本在任务存续期固定。
8. Latest是Catalog中的受控selector，按当前candidate epoch和request admission_seq选择，不能按完成先后覆盖新结果。B/C once在成功受理时消耗，不能把FSM emit等同Request accepted。
9. UNKNOWN沿用完整原批次，不能重捕获输入、重复逻辑发布或把cache命中当绕过fencing的路径。可重放的durable outbox是恢复来源。

## 实现入口与执行边界

- Catalog 保存完整身份域、写角色、Function/Latest 元数据和 canonical codec；公开/null-Registry Engine open 拒绝受控关系，EventProcessor 通过 private owner 入口绑定 Registry。
- RequestRuntime 借用同一个 Engine，活动恢复使用有界 active book，终态历史保存在 KV。EventProcessor v2 提供 expiry fence、Timer/notice debt、admit/once、completion/cancel/timeout/claim 与统一 pending resolver。
- [特征计算定义与 driver](../examples/feature_computation_demo/feature_computation_demo.cpp)、[接口](../examples/feature_computation_demo/feature_computation_demo.hpp)、[示例](../examples/feature_computation_demo/README.md)和[测试](../examples/feature_computation_demo/feature_computation_demo_test.cpp)均在主树。文本编码 A、首次预测 B、条件确认 C、周期预测 D 属于应用声明；Engine 不分支判断业务字段。
- Driver 保留原 UNKNOWN 确认、durable source position/active 集合同步、已 poll completion 的有界保留和 durable outbox 派发。已确认子轮或批量完成不能在后续失败时丢失。
- Compute 使用真实线程函数池，独立示例使用 Simple；owner 只执行有界推进和结果提交，callback 在线程池中执行。SDK drain 使用固定逻辑时间等待已确认的后续工作，不自动重试不确定控制操作。
- 文本编码完成持久化 readiness dirty，后续控制轮受理首次预测。待处理条件确认不占用首次预测独立的 level 槽；这不是每次关系 crossing 的持久事件历史。

## 有效存储预算

运行时open必须查询适配器实际StoreLimits，未报告能力不能猜默认值。公开解析函数给Meta取得有效EngineConfig：mutation/page数量取请求值与适配器上限的较小值；完整canonical batch先保留最大revision的owner/commit ID header及每个允许mutation的17字节put framing，其余空间与max_read_bytes共同限制raw max_bytes。这里采用保守可同时成立的预算矩形，无法留下正空间的配置在初始化前拒绝，不把数量/字节两个独立上限误认为可以同时打满。

EventProcessor的control/request未来状态预留必须读取这个有效配置；不是继续拿SDK期望值做证明。Engine保留原max_rows语义（轮内总行数，不是单次read-key数量），单点读只需max_read_keys>=1，scan page按实际能力裁剪。Remote额外保守计入16MiB JSON wire开销；scan输入prefix/after另有每次调用约束，通用响应接收器当前不声称有16MiB硬cap。Processor默认lifecycle上限现为1MiB，Demo显式512KiB；Demo使用默认Engine raw预算并由适配器进一步收敛，不提高存储上限来隐藏矛盾。

单独 record/index/status 检查不足以证明终态可提交。完整 lifecycle、函数/Latest/SQL/GROUP BY 状态、Engine marker、必要读取、canonical 批头及 mutation framing、Remote wire 必须共同计入。普通输入满不能阻止已获准请求的 completion/cancel/timeout 或完整 expiry fence；恢复也要逐项验证有效配置与已受理工作相容。

限定整改统一使用 reserve_transition 覆盖有无 RequestStatus 的请求，并复用 Engine metadata 的最大时间宽度；新增四 KV 回归先失败再通过。对应[有效预算审查](reviews/M4-effective-budget-review.md)保留 279 项契约、12 项应用关键路径、4 项终态原因断言、5 个独立 probe，以及 60 组 canonical/wire 边界。接近 16MiB 的 scan 使用 30s 客户端超时，不能写成 5s 延迟保证。

## 已有审查证据

以下报告只覆盖其明确快照与范围，不是最终头文件修改后的重跑。

| 证据 | 已确认的限定结果 | 尚不证明 |
| --- | --- | --- |
| [执行层review](reviews/M4-execution-fresh-review.md)、[RequestMachine记录](archive/request-history.md#request-machine)、[RequestRuntime owner review](reviews/M4-request-owner-fresh-review.md) | 函数保留容量、终态编码、resolver/私有index权限及active增长预留等已复现问题关闭 | 新上层集成和底层实际能力匹配 |
| [Latest整改复验](reviews/M4-latest-remediation-review.md) | 7个独立强化探针关闭SQL/host scoped scan缺T与两个mutation计数反例 | 完整byte/read/store/companion预算；报告并未无条件关闭F2 |
| [EventProcessor预算整改](reviews/M4-event-request-budget-remediation.md) | 冻结版本的all-candidate expiry SQL字节遗漏、单periodic多输出低估两反例关闭 | 当时尚未加入的TryOnce/API及所有存储组合 |
| [owner整体review](reviews/M4-owner-fresh-review.md) | 指定v2快照503项配套回归及8个对抗probe通过，覆盖TryOnce、absolute cap、partial rounds和有界恢复 | M4完整资源合同、M5与系统最终交付；报告使用的RequestLatest是53项版本，主树保留后来的57项测试 |
| [app整改review](reviews/M4-app-remediation-review.md) | 关闭driver UNKNOWN/offset同步和批量poll后续completion丢失两项P1；包含四KV、独立故障probe与main证据 | 新有效预算、Release、通用订阅和三类进程 |
| [StoreLimits实现](archive/storage-history.md#store-limits)、[独立review](reviews/M4-store-limits-fresh-review.md) | 能力发现API与decoder的限定契约已审查，重复Content-Encoding问题关闭 | Engine/Processor有效预算及Remote新增保守裁剪，必须用后续证据验证 |

## 当前状态与下一步

核心 Catalog/Request/Latest、线程执行器、有效预算及 Meta/KV/多 Compute 已接通。历史有效预算阶段的 35/35 全量及 Release 证据见对应报告，最终代码的 44 个 CTest 目标、Release、示例和工具链核对统一记录在[核心交付](core-delivery.md#验证证据与剩余门槛)。最终全局审查已完成一轮，合理发现已有修复，当前等待 root 完成最终门禁记录。

Catalog/纯程序/Engine 同轮 observer 基础已实现；完整 host 绑定与输出预留调度、三类用户扩展、在线迁移/复杂 Job、Kafka Compose 后置。观察仍采用同轮同步状态/Trigger 提交，不恢复持久 witness 队列方案。当前 Manifest 保存 capture/admission 版本、受理时间/序号与候选 epoch，occurrence 消费后没有完整历史 TriggerWitness 存档。
