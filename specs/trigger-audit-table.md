# 默认 Trigger 审计关系

Status: done
Owner: root

用户要求在使用 skill 完成后实现：每次 Trigger 满足条件并触发 Feature Computation 时留下审计记录，写入默认元数据关系，一次触发对应一行，可通过 SQL SELECT 列出历史触发；README 展示 reviewer 的查询方式。实现后同步使用 skill 的 API/示例能力。

先完成 specs/eventflow-usage-skill.md，再核对现有 occurrence/request/commit 生命周期。设计须明确触发、待受理、已受理和完成的区别，以及重放/UNKNOWN 下去重、一致性、每 key 查询与有限保留边界。暂不以新增用户需求为由扩大到全局跨 key SQL、通用 CDC 或外部审计服务。

当前功能已实现并验证；下方保留设计过程，最终交付结果与证据见文末。

## 初步设计与实施边界

使用 skill 已完成。默认关系名 trigger_audit，由 TriggerSet::bind 在私有 Catalog 末尾生成，通过现有 SQL/SDK 单 key 查询。新增独立 BaseWriteRole::TriggerAudit，普通输入、hook、request metadata 写入口不能伪造记录；只由私有生命周期能力在触发原批中写入。保留既有源/Clock/CEP 与 UNKNOWN 确认协议。

分区列使用 scope_key_0、scope_key_1 等规范名称，对应 owner 的业务 key 类型；这样即使用户的字段名叫 rule_id 等也不冲突。explain 提供原字段到审计字段的映射。默认保留最近128行、每scope编码行总计不超过256KiB，单条记录受max_occurrence_bytes约束；插入和淘汰同批。数字审计字段中的UInt64用十进制字符串保存，时间/offset为Int64。完整payload、guard读取值和CEP历史不默认复制。

历史选择已解决：用户确认记录实际发出的 occurrence（请求可能等待或跳过），不以成功受理或计算成功为记录门槛。

计划分工：Catalog角色/codec与合同测试；私有Engine审计写能力及权限/原子性测试；root负责最终记录语义、默认schema/binder、运行时/保留策略、SDK示例与恢复测试、README/skill同步及整体验证。一轮fresh-context审查在实现和实际证据准备后进行。

## 已确认语义与 schema

用户已选择“记录实际发出的触发，不等计算成功”，并明确表必须内置、无须显式添加，schema由实现者设计。默认 TriggerSet::bind 自动生成和安装；继续保留原始低层 ProcessorConfig 的 relation=0 未接线兼容模式，正常声明式/生产集群调用不需要任何 audit 注册或DDL。

口径：在 freeze_intent 确认本 occurrence 可保留并发出后记录（普通/Clock/CEP）。条件失败、once 已消费或无活动会话导致根本未发出的 occurrence 不记行。已发出但随后 Wait/NotReady/TryOnce skip 的记录仍保留；request_id 是已确定的请求身份，受理前或被跳过时 read_request 可以仍为 NotFound。行仅表示 emitted，不追加“计算成功”假字段。ACK/消费 outbox 不删除历史审计。LevelRequestBinding/手工直接受理不产生声明式 occurrence，不冒充规则发出日志，既有request_status/read_request仍可审计它们。

表名 trigger_audit；scope_key_i 根据业务key类型非空；主键(scope_key_i...,occurrence_id)，partition=(scope_key_i...)。后续列按顺序为：occurrence_id/string、rule_id/string、rule_version/string、emission/string、event_kind/string、occurred_at_us/int64、evaluated_at_us/int64、scheduled_at_us/nullable int64、skipped_slots/string、firing_revision/string、request_id/nullable string、binding_id/nullable string、plan/nullable string、candidate_epoch/nullable string、request_mode/nullable string、intent_deadline_us/nullable int64、source_id/nullable string、source_topic/nullable string、source_partition/nullable int64、source_offset/nullable int64。UInt64字段使用规范十进制字符串。firing_revision 为包含该行与触发状态的确认批版本，不等于后续capture/admission版本。

公开 TriggerAuditConfig 包含 relation=0、max_rows_per_scope=128、max_bytes_per_scope=256KiB；ProcessorConfig.trigger_audit 默认初始化，binder赋实际relation ID。保留字节计数包括KV key+encoded row，与scan预算对应。同 scope 同轮只扫描一次，按(evaluated_at_us,occurrence_id)稳定排序后保留最新记录，插入/淘汰同批。单行超过 max_occurrence_bytes 或预算不能承受时返回ResourceExhausted并回滚本轮，不静默丢失记录。

实现组件 RED 已记录：Catalog role原本不支持（已green6/6），Engine lifecycle writer原本拒绝（已通过完整合同验证），默认table原本Relation not found。日志前缀 /tmp/eventflow-audit-。

## 最终状态

内置表、Schema、权限、原子记录、保留、恢复、SDK/进程内查询与README/skill同步均完成。60个测试目标均完成验证（59个首跑通过，Coordinator整组超时调整后完整复跑通过），Release及八个独立解压包用例通过，格式/noexcept/具名初始化/Bear/clangd实际LSP检查通过，一轮独立fresh-context审查完成。

完整证据见[最终验证](verification/trigger-audit-validation.md)和[审查](reviews/TRIGGER-audit-review.md)。无剩余本任务代码项；镜像与其他任务状态保留。

Changed files：CMakeLists.txt；include/eventflow/{catalog,event_processor,trigger_audit}.hpp；src/sql/{catalog,catalog_codec}.cpp；src/runtime/{engine.cpp,engine_internal.hpp,event_processor.cpp,computation.cpp,trigger_audit.cpp,trigger_audit_internal.hpp}；tests/trigger_audit{_catalog,_engine,_request,}_test.cpp；三个Clock/恢复示例的main.cpp与README；根README、docs/implementation_guide.md、.skills/use-eventflow的审计语义与引用；本规格、INDEX、审查及验证记录。
