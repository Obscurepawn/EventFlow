# M3 生命周期与基础组件完成记录

日期：2026-09-14。已完成本阶段的确定性EFSM、processing-time生命周期、输入适配、状态/query缓存和KV snapshot基础；集群驱动、global watermark/late策略与一致checkpoint仍在M5。用户正在讨论的E-NFA CEP增强单独跟踪，不能视为已有功能。

## 独立审查

| 范围 | 记录 | 结果 |
| --- | --- | --- |
| 时间、watermark组件、消息源、snapshot | [IO/time](M3-io-time-component-review.md) | snapshot可写打开与发布fsync重试两项P2均修复复验关闭 |
| EFSM组件 | [FSM](M3-fsm-component-review.md) | 最终状态编码预算问题修复；独立19项通过，后续fixed-now语义由生命周期gate覆盖 |
| 状态cache | [cache](M3-cache-component-review.md) | 初始39项、跨round保留/写穿后59项独立验证通过 |
| SQL query cache | [query cache](M3-query-cache-component-review.md) | 28项独立验证，包括实际clone调用计数；公开计划校验保留 |
| 完整EventProcessor生命周期 | [fresh生命周期gate](M3-lifecycle-fresh-review.md) | delayed timeout时间/新相对Timer问题修复，原反例不变；129/129、无skip、无sanitizer诊断，PASS |

## 主构建与真实协议证据

主构建均为Clang、ASan/UBSan/LSan。EventProcessor共97项共享契约，覆盖Memory、RocksDB、HTTP→Memory、HTTP→Rocks；修复后root实际重跑通过，日志`/tmp/eventflow-processor-limit-fix-test.log`。首次新增回归误用了超过outbox上限的查询limit，已只修正测试参数并复跑，不把该用法错误称为生产缺陷。

状态cache与Engine组合另有4项HTTP Memory/Rocks集成，通过真实增量图验证跨轮命中和UNKNOWN阻断/原批确认。Timer/watermark18项，EFSM当前17项，snapshot19项，source30通过/4个不适用Mem分支skip，状态cache51项，query cache22项；对应独立审查的额外探针不冒充仓库测试数量。

带M4元数据准备的matching主构建19个CTest中18个首次通过，EventProcessor参数修复后独立重跑通过。当前格式检查通过。Bear数据库已扩展为Clang全部当前TU，随后新增文件继续追加；main及Engine头文件clangd检查0 errors。

## 本阶段成立的语义

- 去重先于时间、MV和FSM；同身份不同payload拒绝，超出去重保留窗口的旧身份明确失败。
- 一次延迟advance固定now，先清理全部已到期TTL，再以当前稳定MV执行timeout/clock；回调和新相对等待使用now，原deadline保存在scheduled_time_us。
- 每个已提交subround独立可见；后续输入失败不能撤销先前已完成轮次。durable occurrence outbox保证这些输出仍能恢复。
- UNKNOWN保留完整批次与运行时副本，确认后才发布；完成通知不重复交付已确认轮次。
- 当前Latest输入语义为每key当前行的typed upsert、TTL续期与撤回，不声称已有任意arg-max SQL或M4 LatestSuccess selector。

M3基线完成不代表完整项目交付。M4统一函数视图/Request/attempt/A-B-C-D、M5真实三类进程/Job生命周期/SDK/DDL/全局checkpoint、后续扩展及最终多个fresh-context全局审查继续执行。
