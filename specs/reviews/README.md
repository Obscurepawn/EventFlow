# 审查与整改证据

本目录保留独立 reviewer 的问题、探针、源码指纹，以及后续整改/里程碑记录。
**初审失败与后续关闭报告应一起阅读。** 每份结论只覆盖其列明的版本和范围；
当前交付状态以[核心规格](../core-delivery.md)及[最终验证](../verification/final-validation.md)为准。
整理目录没有发起新审查轮，也没有将旧组件通过视为最终全局通过。

## 最终并行全局审查

| 报告 | 职责 |
| --- | --- |
| [核心架构审查](CORE-global-architecture-review.md) | 原始要求、真实部署/图/触发/源因果与接口语义；原发现及 owner 处置保留 |
| [核心工程与恢复审查](CORE-global-engineering-review.md) | 真实故障探针、checkpoint、恢复、缓存与构建证据；原发现及 owner 处置保留 |

## 独立审查与整改复验

| 范围 | 原审查 | 对应整改或补充 |
| --- | --- | --- |
| SQL/增量运行时 | [运行时审查](M2-runtime-fresh-review.md) | [R1-R5 整改复核](M2-runtime-remediation-review.md) |
| FSM | [FSM 组件](M3-fsm-component-review.md) | 宿主语义另见生命周期审查 |
| 输入、时间、snapshot | [I/O 与时间组件](M3-io-time-component-review.md) | 后续集群 checkpoint 由全局报告覆盖 |
| 状态 cache | [KV cache 组件](M3-cache-component-review.md) | 跨 round 写穿复验保留在报告末尾 |
| SQL query cache | [计划缓存组件](M3-query-cache-component-review.md) | 范围和独立探针在原报告 |
| EventProcessor 生命周期 | [生命周期里程碑](M3-lifecycle-fresh-review.md) | fixed-now timeout 整改保留在原报告 |
| 有界 E-NFA | [CEP 宿主](CEP-owner-fresh-review.md) | 控制容量及恢复反例的修复/复验保留在原报告 |
| 函数执行与 Engine bridge | [执行桥接](M4-execution-fresh-review.md) | completion 容量问题的处置保留在原报告 |
| RequestRuntime | [Request owner](M4-request-owner-fresh-review.md) | owner resolver、命名空间、Catalog 生命周期及容量的 R1-R4 闭环 |
| Latest 元数据 | [Latest Catalog](M4-latest-catalog-fresh-review.md) | 隔离元数据范围，不代替后续 runtime |
| Latest runtime | [Latest 运行时](M4-latest-runtime-fresh-review.md) | [限定整改](M4-latest-remediation-review.md)；完整预算另见有效预算报告 |
| 请求生命周期接入 | [EventProcessor 请求 owner](M4-owner-fresh-review.md) | [控制预算限定整改](M4-event-request-budget-remediation.md) |
| 应用预测轨迹 | [应用轨迹初审](M4-app-fresh-review.md) | [UNKNOWN/完成批所有权整改](M4-app-remediation-review.md) |
| 存储能力查询 | [StoreLimits](M4-store-limits-fresh-review.md) | 严格响应帧整改保留在原报告 |
| 实际预算闭包 | [Store 到 runtime 预算](M4-effective-budget-review.md) | 恢复 mutation 和无状态表记录增长预留处置 |
| Catalog wire codec | [Catalog codec](M5-catalog-codec-review.md) | 定义范围/配置一致性探针和整改 |

## 里程碑与处置汇总

这些文件汇总阶段审查和验收，不等同于额外独立 reviewer 原始报告：

- [初始设计 challenge 与处置](M0-design-review.md)
- [类型、KV 和构建阶段](M1-review.md)
- [SQL 阶段完成记录](M2-review.md)
- [生命周期阶段完成记录](M3-review.md)
- [调研第四章的独立审阅汇总](research-ch4-synthesis-review.md)

## 实现历史已经合并

原十二份 `*implementation-notes.md` 是实现者交接，不是独立审查，已与两份旧提案
合并为三个[历史主题](../archive/README.md)。保留重要决策、反例、日志和哈希，删除重复的
API 罗列、临时接线指令及过时待办；实际独立审查与整改文件没有删除。
