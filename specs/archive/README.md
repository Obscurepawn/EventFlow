# 设计与实现历史

这里仅保留已经采纳的设计选择、关键反例和组件阶段的证据入口。现行规范从
[specs 索引](../INDEX.md)进入；最终代码的验证从[验证报告](../verification/final-validation.md)进入。
历史中的“当时尚未接入”和旧测试数量不代表当前状态，也不构成新的独立审查。

原来两份提案和十二份实现交接合并为三个主题；重复接口、临时分工、构建接线说明和
已被后续工作取代的待办删除。独立审查及整改记录完整保存在[审查目录](../reviews/README.md)。

| 主题 | 保留内容 |
| --- | --- |
| [触发器与生命周期](trigger-history.md) | Flink 源码参考、E-NFA/FSM 语义、状态读取、原子推进、控制容量反例及 CEP 验证 |
| [存储与缓存](storage-history.md) | 确认状态缓存、SQL 计划缓存、snapshot、StoreLimits 与有效预算历史 |
| [请求与函数视图](request-history.md) | 单一 Catalog、身份域、私有 Engine 写入口、执行器所有权、请求状态/持久化与审查证据 |

旧引用的精确替代位置：

| 已删除文件 | 保留位置 |
| --- | --- |
| `cep-contract-proposal.md` | [E-NFA 设计](trigger-history.md#cep-design) |
| `CEP-component-implementation-notes.md` | [CEP 组件](trigger-history.md#cep-component) |
| `CEP-owner-implementation-notes.md` | [CEP 宿主与容量整改](trigger-history.md#cep-owner) |
| `M3-event-processor-implementation-notes.md` | [事件生命周期](trigger-history.md#event-processor) |
| `M3-cache-implementation-notes.md` | [确认状态缓存](storage-history.md#kv-cache) |
| `M3-query-cache-implementation-notes.md` | [SQL 计划缓存](storage-history.md#query-cache) |
| `M3-snapshot-implementation-notes.md` | [存储快照](storage-history.md#snapshots) |
| `M4-store-limits-implementation-notes.md` | [实际存储预算](storage-history.md#store-limits) |
| `function-view-contract-proposal.md` | [函数视图设计](request-history.md#function-design) |
| `M4-function-catalog-implementation-notes.md` | [函数 Catalog](request-history.md#function-catalog) |
| `M4-engine-bridge-implementation-notes.md` | [Engine 私有桥接](request-history.md#engine-bridge) |
| `M4-function-executor-implementation-notes.md` | [函数执行器](request-history.md#function-executor) |
| `M4-request-machine-implementation-notes.md` | [请求状态机](request-history.md#request-machine) |
| `M4-request-runtime-implementation-notes.md` | [请求持久 owner](request-history.md#request-runtime) |

独立审查原文可能仍用旧文件名描述审查时阅读的材料；上表保留其来源对应关系。
没有改写旧审查的结论、源码哈希或运行结果，也没有把曾经失败的检查删成“始终通过”。
