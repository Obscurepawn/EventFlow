# 数据库主动消费全局审查：运行时与接口

日期：2026-09-15。审查者：独立 fresh-context runtime reviewer。本报告属于本里程碑唯一一轮并行全局审查，原始发现以下方源码快照为准；实施者可追加处置与定向验证，不覆盖原发现。

## 范围与结论

对照根及 src/ingest、src/service、src/runtime、include/eventflow、tests、specs 的 AGENTS.md，以及 [本轮规格](../streaming-database-example.md)，只读检查 IMessageSource/HTTP 协议、SourcePoller、Coordinator 的输入/ACK/暂停/checkpoint/恢复、通用 ComputationRuntime、Compute 安装与缓存边界、现有测试和日志。

发现 1 项 P1 和 2 项 P2，修复前不建议宣布本轮完整交付。正常 streaming_market 示例主路径成立，但它总是启用 request integration，不能证明通用无请求模块可用。没有提出 HA、动态 UDF、真实 Kafka Compose 等额外完成门槛；遵守用户决定，没有新增或执行专门故障注入探针，也没有另开审查轮或重构建。

## 原始发现

### R1 / P1：公开默认 SQL 模块和普通无请求 Trigger 模块无法完成绑定、部署与推进

证据：

- [computation_runtime.cpp](../../src/runtime/computation_runtime.cpp):26-52 的默认 `sql` binder 对非空输入执行 `TriggerSet::bind(..., {})`；[computation.cpp](../../src/runtime/computation.cpp):342-346 明确拒绝空 Trigger 列表。因此普通 SQL DDL 在绑定阶段即报 `A computation requires one to 64 Trigger declarations`。
- 即使改掉空列表问题，普通无 request 的 TriggerSet 产物允许 `registry == nullptr`，见 computation.cpp:351-359、500-502；ComputationRuntime::open 在 computation_runtime.cpp:59 无条件拒绝该产物。
- 即使补齐 registry，Compute 的 shard_report 在 [compute_service.cpp](../../src/service/compute_service.cpp):337 无条件调用 `active_requests()`；install 在 466-467 以 report 作为成功响应，导致无请求部署失败。
- ComputationRuntime::pump 在 computation_runtime.cpp:226-234 无条件调用 `EventProcessor::pump()`，Compute::pump 在 compute_service.cpp:626-627 无条件枚举 active_requests。上述请求接口均经过 [event_processor.cpp](../../src/runtime/event_processor.cpp):3972-3976 的 request_ready，在未启用请求时返回 `request integration is disabled`；普通时间推进已有 `EventProcessor::advance`。

影响：公开的默认 ComputationModule（name=sql）不能作为普通 SQL 数据库部署；仅声明普通 Trigger 而不创建异步函数请求的静态模块也不能由新的通用生产运行时托管。这违反本轮“通用生产组件、应用注入完整静态定义”的要求，并非仅缺少某个示例能力。

建议：默认 SQL 直接形成合法 BoundComputation；无请求模块无需强制配置函数执行器/registry，或者由运行时提供合法空 registry。report、pump、quiesce 的请求专用枚举按实际 requests 配置分支，无请求时使用普通 advance。定向验证应覆盖普通 SQL 和普通 Trigger 的正常绑定/安装/输入/时钟/暂停/checkpoint 流程；这属于功能回归，不要求增加故障注入。

当前证据限制：只读源码推导。现有 compute_service/coordinator_service 测试均显式注入 legacy_feature_module，新示例使用带 request 的 streaming_market，不能覆盖该默认分支。

### R2 / P2：checkpoint 未固定尚无已处理消息的 partition 起点

证据：

- [coordinator_service.cpp](../../src/service/coordinator_service.cpp):141-149 的 source_description 只持久化 source_id、relation、topic、partition，省略 SourceAssignment.next_offset。
- prepare_source 在同文件 238-243 对没有 state.progress 的 partition 使用本次启动配置的 `assignment.next_offset.value_or(0)`；有 progress 才使用已保存 next_offset。
- 恢复时 1078-1083 仅比较上述 source_description；checkpoint.source_progress 在 791 仅复制已出现于 state.progress 的 partition，verify_source_cut 的 411-431 也不补充未消费 assignment。

完整轨迹：某个已配置 partition 尚未消费任何消息，在初始 next_offset=0 时创建有效 checkpoint；全部进程停止后，以相同 source_id/topic/partition/relation、next_offset=100 恢复。持久 subscription 比较仍相等，该 partition 没有 progress，prepare_source 会 seek(100)，跳过原冻结切面应当回放的 0..99。显式恢复配置改变不应静默改变同一个 checkpoint 的源切面。

建议：持久记录每个 assignment 的有效初始 replay 起点，并把它纳入 subscription 身份；或 checkpoint 为所有已分配 partition 记录下一位置（包括尚未产生输入的 partition），恢复只使用保存值。对已处理 partition 继续优先采用数据库进度，而不使用较新的 broker commit。

当前证据限制：只读源码推导，没有执行改变恢复配置的探针。现有 HTTP/Mem 共享恢复合同覆盖已投递位置，不覆盖 Coordinator 对尚未消费 assignment 的 checkpoint 配置固定。

实施处置（2026-09-15）：按主实施者分配完成独立小修，source_description 将每个 assignment 的 `next_offset.value_or(0)` 编码为规范十进制字符串，写入持久 source_subscription。这样未指定 offset 与显式 0 表示同一有效起点，而恢复时改变该起点会被既有 subscription 比较拒绝。prepare_source 的已保存 progress 优先逻辑保持不变。代码依据为 [coordinator_service.cpp](../../src/service/coordinator_service.cpp):141-152；仅核对源码内容，本子任务没有编译、运行测试或新增故障探针，行为验证由 root 追加。

### R3 / P2：已 claim、未 submit 的任务取消后残留 owner 映射，无长期容量上界

证据：

- [computation_runtime.cpp](../../src/runtime/computation_runtime.cpp):94-96 将所有已确认 Running 节点写入 task_owners_。
- claim_ready 在同文件 170-179 先为全部 Ready request claim，直到 180-193 才向执行器 submit；前段读到的 executor.active_tasks 不随 claim 增加，可产生超过实际执行器容量的已 claim 任务。执行器满时 189-190 退出，部分 Running 任务尚未 submitted。
- 这些任务之后超时或被取消，consume 的 102-104 因 `!submitted_.contains(id)` 直接跳过。synchronize 的 118-141 虽收集最新 running 集合，只清理已有 completions_ 或 cancel submitted_，从不清理已经不在 running、且从未 submitted 的 task_owners_。
- forget 仅在 synchronize 清理本地 completion 或 run_functions 消费 completion 时执行（130、275）；从未交给执行器的任务永远没有 completion 触发清理。

影响：有限 active request/outbox/executor 预算不能约束这个不断累积的本地映射；多轮正常背压和超时即可使内存随历史任务数增长。它是旧运行代码迁入通用生产组件时仍保留的漏洞，而非新代码才可能出现的故障假设。

建议：同步已确认状态时移除不再 running、又未 submitted/待处理 completion 的 owner；对可能仍从执行器返回的已提交任务保留映射，直到安全丢弃或消费 completion。也可先清理从未提交的 cancel_task_ids，但应同时处理已通过公开 processor 接口确认的状态变化。

当前证据限制：只读所有插入/删除路径及 RequestMachine/RequestRuntime 的 outbox、超时/取消合同，没有新增负载或故障测试。

## 已确认的设计路径与证据边界

- SourcePoller 只保留一条 SourceMessage，第一次 poll 固定时间；计算未确认时重试原内容/时间，ACK 不确定时只重试原 token。input_committed 与后续 operation_status 分离，后续函数背压不撤销已确认输入。
- Coordinator 先持久保存原 pending_input/dispatch time，再转发 Compute；确认后更新 source progress 并清 pending。SourcePoller 只有该步骤成功才 ACK，Meta UNKNOWN 先 resolve 原 batch。后台在保留未确认消息时限制时钟推进。
- pause/checkpoint 与后台消费共享 gate。checkpoint 拒绝未确认 input，并在 worker quiesce、源切面对账、revision/receipt 固定之后创建 KV snapshot。恢复校验真实冻结 control revision 和 worker receipts，已处理 partition 的 seek 使用数据库切面。HTTP 服务日志只在该进程存活期间保留，不代表持久 broker。
- Compute 名称/版本安装检查、Catalog fingerprint、持久 shard owner receipt 和 EventProcessor 的定义恢复校验仍在；缓存按 shard 分摊节点预算，UNKNOWN 保留边界未被新 Runtime 查询代理绕过。静态回调实现身份仍由应用提供的稳定版本/identity 负责，不是二进制代码一致性的自动证明。

只读现有证据：

- `/tmp/eventflow-streaming-4Y8jgY/verify.log` 有 `STREAMING_DATABASE_OK shards=2 totals=150,130 threshold_scores=242,262 periodic_score=303 source_next_offsets=3,2`；日志包括暂停后追加、SQL 固定输入检查及已完成 checkpoint。该记录证明正常示例与创建 checkpoint，单靠它不能宣称新 HTTP source 的全进程恢复已跑过。
- 阅读了 source_poller_test 的四个用例和 HttpAndMemorySource 的四个双后端用例。其结构覆盖输入确认、固定时间、暂停、永久错误保留、二进制/空值、已交付前缀、seek 撤销 token、poll 预算与关闭。实际运行成功由实施者提供日志，本审查未重跑。
- 审查时 `/tmp/eventflow-streaming-ctest.log` 明确记录 core_cluster_test 通过（109.69 秒）、event_request_test 和 cep_test 通过；全量 60 目标仍在执行。core_cluster_test 源码保留 RocksSnapshotRestartsAllProcessesAndContinuesTheExactSourcePrefix 既有全进程恢复用例，使用测试业务模块及 SDK 接入，不等价于新自动 HTTP 消费恢复。
- 未执行构建、测试、额外 fault probe 或真实 Kafka。Release 与全量 CTest 不在本报告中声明通过；格式与用户文档交付由同轮另一审查方向及实施者负责。

## 实施者处置

2026-09-15，root 已完成本轮三项整改：

- R1：默认 SQL 直接构造 BoundComputation；没有 requests 时不创建函数执行器，使用 EventProcessor::advance 推进，Compute 状态与 quiesce 跳过请求专用枚举。新增 sql_module_service_test 两个正常路径用例：默认模块经真实 HTTP Coordinator/Compute 消费 MemMock、查询级联 MV 并暂停/checkpoint/resume；普通 Trigger 在没有 Registry/request plan 时发出 occurrence。初始运行分别复现空 Trigger 绑定和强制 Registry 错误；整改后 2 个用例通过，CTest exit 0。测试最初用 0 创建 checkpoint 被现有单调时钟规则拒绝，改为读取 status.now_us 后成功；这是测试使用方式修正，不改变产品时钟合同。
- R2：持久 source_subscription 加入每个 assignment 的有效初始 next_offset 字符串，现有 progress 继续优先。正常自动消费/checkpoint 示例以及上述 SQL 模块测试已经运行；按用户决定，没有追加修改 offset 的故障/恢复配置探针。
- R3：未提交任务的 cancel_task_ids 立即 forget；同步已确认请求状态时，清除不再 Running 且不在 submitted 的 owner，保留仍可能返回 completion 的任务。清理条件在已确认 active_requests 和本地 completion 消费之后执行，没有越过 UNKNOWN。针对该清理路径没有新增故障或负载测试；保留服务/请求/执行器回归。

整改后的 Clang Debug 完整构建与 Release 完整构建均 exit 0，无编译 warning/error；clang-format 全项目检查 exit 0。Release 安装到 `/tmp/eventflow-streaming-install` 后直接运行其中的 run.sh，得到预期成功标记，五个进程退出码均为 0，日志 `/tmp/eventflow-streaming-zVfrGy/`。全量 61 个 CTest 目标正在执行，完成结果由下文追加；本记录不把在跑门禁算作通过，也不启动第二轮 fresh-context 审查。

最终门禁（2026-09-15）：`ctest --test-dir build --output-on-failure -j 2` 已 exit 0，61/61 目标通过，总计 953.47 秒，日志 `/tmp/eventflow-streaming-ctest-final.log`。其中 Compute 473.17 秒、Coordinator 165.10 秒、core_cluster 110.65 秒、旧业务四适配器 429.86 秒；HTTP/MemMock、SourcePoller、默认 SQL/普通 Trigger 与 Debug 新例全部通过。新例日志 `/tmp/eventflow-streaming-ncQhwg/` 给出预期成功标记及五个 exit 0，无 sanitizer 诊断。三项发现均完成实施与相应验证，本轮无未处理发现；没有增加用户取消的专门故障测试。
