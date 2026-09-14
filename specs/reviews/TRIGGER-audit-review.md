# 默认 Trigger 审计关系独立审查

Reviewer: audit_fresh_review
Review round: 本 milestone 的一次独立 fresh-context 审查
Reviewed at: 2026-09-14 14:05 UTC
Status: 审查完成；未发现确定的 P0/P1/P2 缺陷，整轮验收仍等待实施者完成全量验证

## 范围与结论

按[当前授权规格](../trigger-audit-table.md)检查：自动内置表、无需用户 DDL、每个实际发出的 occurrence 留一行、不等待计算成功、SQL 查询名称和时间，以及 README／使用 skill 同步。审查基于当前源码与实际日志；未启动或修改共享构建，未修改生产、测试或示例源码，也未另开审查轮次。

**没有需要实施者修复的确定代码发现。** 当前默认 `TriggerSet::bind` 路径符合授权语义。以下结论不把低层 `ProcessorConfig.trigger_audit.relation=0` 的兼容模式当作默认入口，也不把明确后置的跨 key SQL、永久归档、动态 UDF 或 Kafka Compose 当成本任务缺陷。

## 要求、接口与实现核对

| 要求／不变量 | 当前实现证据 | 审查判断 |
| --- | --- | --- |
| 自动安装，无用户 DDL | `src/runtime/computation.cpp:421` 在私有 Catalog 中定义 `trigger_audit`，随后整体校验并返回 `BoundComputation`；重名失败，不发布半个 Catalog | 符合；`trigger_audit_test.cpp` 覆盖默认建表及重名原子失败 |
| 业务 key 与元数据名称不冲突 | `src/runtime/trigger_audit.cpp:10` 生成规范 `scope_key_N`，partition 为业务 scope，主键额外包含 occurrence_id；binder explain 记录源列映射 | 符合；复合 key 使用源字段名 rule_id/request_id 的用例覆盖冲突风险 |
| 仅实际发出的触发 | `event_processor.cpp:1925`／`:1981` 先调用 freeze_intent，retain 为 true 才进入 outbox 和本轮 occurrences；`:2387`／`:2772` 写同一批审计 | 符合；false predicate、once 消费、Wait、TryOnce skip 与 typed CEP 均有定向断言 |
| request_id 在受理前可关联 | `event_processor.cpp:837` 冻结由 binding ID/version 与 occurrence ID 组成的身份；`:1879` 直接写该 intent 身份；request_round 复制 intent，不重新生成请求身份 | 符合；等待时 NotFound、随后受理／完成和 snapshot 恢复均比较同一身份 |
| 同 shard 原子提交 | 审计写通过 `EngineRuntimeAccess::write_trigger_audit` 进入同一 RoundContext；FSM/CEP、outbox、源去重状态和审计在 apply_lifecycle 的同一 publish 批提交 | 符合；ResourceExhausted 回滚、实际输入贡献及 audit revision 均有断言 |
| 受保护写权限 | `engine.cpp:1723` 检查 lifecycle frame，拒绝 runtime/metadata/observer frame；普通输入、emit 和 raw stage 入口不能写入该角色 | 符合；独立写角色合同测试覆盖 Memory、Rocks 和二者 HTTP 适配器 |
| Catalog 与恢复编码 | Catalog 接受独立 TriggerAudit base role，拒绝混入 request domain／view／生命周期注解；codec 保留 role；schema 和保留设置进入定义指纹 | 符合；普通角色历史字段布局保持原样，改变保留策略后恢复被拒绝 |
| UNKNOWN、消费与重放 | pending state 保留原 occurrences；UNKNOWN 确认重试原 commit ID 和 mutation bytes，未确认前拒绝查询；ACK 只删除 outbox，不改审计表 | 符合；source replay 不重新进入发出路径，SDK／checkpoint 日志可见历史保留 |
| 有界保留 | 每 scope 一次 staged scan；按 `(evaluated_at_us, occurrence_id)` 决定淘汰，计算 KV key＋encoded row 字节，插入／淘汰在同批；单行超限返回 ResourceExhausted | 符合；行数、字节、不同 key、ACK 独立性与整轮回滚已有测试 |
| Clock／CEP 时间与来源 | FSM 写 emission time，CEP 写 match completion time，evaluation 单独保存；Clock 的 source 字段为空；scheduled_at_us 按 occurrence 中已有值保存 | 符合已声明边界；普通 Clock 延迟到 17 秒时保存 15 秒 slot 和 skipped=2 的测试覆盖 |
| SQL、README 与使用 skill | README 给出 Trigger 名称、各时间、request_id 查询和 SDK scope；skill 说明 NotFound、有限保留、单 key、非时间排序、旧部署不自动迁移 | 与实现一致；未把 emitted 写成 succeeded，未要求应用建表或挂 sink |

## 实际证据

- 已读取 `/tmp/eventflow-audit-full-debug-build.log` 的完成尾部，显示项目全部目标构建至 100%。本 reviewer 没有独立重建或运行该构建，不把日志读取称作独立编译验证。
- `/tmp/eventflow-audit-catalog-green.log`：6 个 Catalog 用例全部通过。
- `/tmp/eventflow-audit-contracts-green.log`：5 个 CTest 目标中 4 个通过（engine Clock、Catalog、普通 audit、audit Engine），旧的 request audit 测试在 CEP 用例中因 `occurrence has no request binding` 终止。该失败日志真实保留，不能从文件名推断整组通过。
- `/tmp/eventflow-audit-engine-green.log` 也保留更早一次测试构造失败：JOIN 缺少 singleton 证明。当前源码已使用 `audit_count JOIN input`，而较新的 contracts 日志记录 audit Engine 目标通过。这里不把旧失败误报为当前实现缺陷。
- 最新 `/tmp/eventflow-audit-full-ctest.log` 在本次读取时记录 `trigger_audit_request_test` 通过（1.73 秒），覆盖当前全部 5 个 request audit 用例；`feature_computation_demo_test` 通过（540.29 秒）。这证明该 request 目标相对旧失败日志已有更新证据，但 60 个目标的整轮 CTest 当时仍在运行。
- 已读取 `/tmp/eventflow-audit-sdk-debug.log`：真实 SDK Clock 演示依次查询 5／10／15 秒的 audit 行，比较同一 request_id、slot、evaluation、source=null，且 firing_revision 不晚于 capture_revision；日志以 `SDK_CLOCK_POLLING_DEMO_OK` 结束。
- 已读取 `/tmp/eventflow-audit-recovery-debug.log`：两代真实 KV／Coordinator／两个 Compute 进程；两个 key 各恢复 2 行审计历史；原 offset 重放不增加记录，后续输入继续推进；日志以 `RESTORE VERIFIED` 结束。源码还逐行比较完整审计 JSON，并校验 checkpoint 的 source positions／watermark。
- 本 reviewer 实际执行轻量静态检查，结果 `AUDIT_REVIEW_STATIC_OK`：skill 本地链接存在，所查 README／skill 围栏闭合，新增 audit 头文件／实现／测试均为 ASCII。

## 未验证边界与交接

1. 全量 60 CTest 的最终结果、Release 构建和最终格式检查由实施者追加新证据；本报告不宣布它们通过，也不单凭已通过测试宣布 milestone 完成。
2. 本 reviewer 没有独立注入 Clock／延迟 CEP 发出时的 UNKNOWN，也没有穷举多个同时间 occurrence 恰好触及保留上限的组合。相关一致性判断来自共同 lifecycle writer、原批确认与保留算法的源码检查，加已有 source UNKNOWN 和行／字节保留测试；这是覆盖边界，不是已确认缺陷。
3. 256 KiB 是每 scope 的 KV key＋encoded row 预算，不代表后端索引开销或全节点容量。全节点／轮预算仍由现有 owner/store 限制；不足时允许整个发出轮返回 ResourceExhausted。
4. 表没有无限历史或全 shard 查询承诺。CEP 的 scheduled_at_us 只保留已有 occurrence 元数据；README／skill 未承诺恢复完整 CEP 因果链，完整 payload／guard 快照不在此表范围。
5. 完成本轮剩余验证并同步 active spec／INDEX 状态后即可交接；若后续仅修复测试或文档问题，做对应定向复验即可，无须自动重开 fresh-context 审查轮。

## 检查对象指纹

以下 SHA-256 来自本次实际读取后的文件，用于识别审查快照；不是二进制等价或整树完整性证明。

| 文件 | SHA-256 |
| --- | --- |
| `src/runtime/trigger_audit.cpp` | `3eebfc70adac9a6324b4dba65f6218e8a1daf0ad1c4c9babae6a279bbb0bfd0b` |
| `src/runtime/event_processor.cpp` | `1d583a5e2dec7802f26a65eea0f020ee7ac9ab97a1ac84232d8c3a79f0197462` |
| `src/runtime/engine.cpp` | `8e077afd2922d4544ed79a0ed247bdbd80479e070ba12db5805e83c6e925d98b` |
| `src/runtime/computation.cpp` | `2540cfccb003de17bc0d1e84772cb7942968fd2352ea3c14b11e99601e341f05` |
| `src/sql/catalog.cpp` | `63991e07880b4e82fffc3ebb29129ea498ad6e40f24f1546473a82e3dfb03c53` |
| `src/sql/catalog_codec.cpp` | `937a491490998ac052e19bd6d6e18eb020efb513555aa9903b630b8dd338534f` |
| `tests/trigger_audit_request_test.cpp` | `6074d1f3b99e21d7dbf03a9847343058846d4ada620118732ff22b8cac826cd5` |
| `README.md` | `c662617337f35326073f2c801ebeeaf34739db1ed113c430b0a960ff5e03861e` |
| `.skills/use-eventflow/references/trigger-audit.md` | `c834ced48b9105e84efdb13a85cf50e7dc07be33be8287c1a74353a1db57d3f7` |

## 实施者补充的最终证据

本轮剩余验证已完成：全量首跑59/60通过，Coordinator触及180秒整组上限；调整为与重型合同目标相同的600秒后，该完整目标256.16秒通过。Release构建、八个独立解压包示例、全项目format及变动范围AST检查、103个源文件的Bear数据库、13个文件的clangd实际LSP解析/索引/定义跳转均通过，详见[最终验证](../verification/trigger-audit-validation.md)。未修改本报告原始证据，也未再启动独立审查轮。审查后只有CTest整组超时设置及验证/文档记录变化，没有改变受审生产逻辑。
