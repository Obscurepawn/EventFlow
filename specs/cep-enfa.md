# 有界 E-NFA CEP

Status: done（processing-time 单 owner 范围）
Owner: root
Updated: 2026-09-14

## 已确认目标

用户明确要求纳入当前核心交付，保留简单时钟/状态条件。依据[源码调研与契约提案](archive/trigger-history.md#cep-design)，增加真实多候选E-NFA，不将已完成的EFSM改名冒充NFA。

## 实现边界

- 模式编译到TAKE、IGNORE和PROCEED（ε）转换；一个输入在单条路径最多被消费一次，支持多条候选同时推进。
- 首个正向语法子集：顺序、严格/宽松衔接、分支、optional、有限重复、within；明确拒绝未支持的negative/greedy等语法。NoSkip、SkipPastLastEvent为基础策略。
- 每条路径拥有独立寄存器/历史引用；事件/历史可共享不可变存储。guard能读取本路径历史以及本轮预声明的同key只读MV快照，不能让试探分支修改共享业务状态。
- processing time先实现，固定now、到期先于同刻数据；原deadline仅用于审计。事件时间重排不在该 matcher 范围；集群 watermark 另由控制面维护，不混同两种时间域。
- 活跃路径、buffer事件/字节、转换次数、单轮匹配输出均有预算；超限整体不推进，不静默丢弃合法匹配。Engine需要可丢弃的prepare/WouldBlock路径，不能把可恢复背压变成shard永久失败。
- 源去重在matcher之前；匹配ID稳定，不使用内存地址或临时分支分配顺序。状态与共享引用的codec校验版本、身份及引用完整性，并与outbox/source progress同批保存。
- `strict next news`会忽略Trade/Quote，但第一条News必须消费；不能直接换成原始流的`.next()`，也不能错误跳过负面News。

## 必须保留的轨迹

- `A1,A2,B` 同时产生多个候选，optional 不重复消费，分支寄存器隔离。
- 当前 MV 总和与路径匹配历史总和分别读取；guard 不将两者混为一谈。
- skip/within 边界、恢复、UNKNOWN 原批重试、满预算无半轮输出。
- pending match 与同轮 FSM alarm 合计受输出数量和完整 lifecycle 编码预算约束；准入/恢复提前拒绝未来无法发布的状态。
- `max_occurrences=1` 的链式 Timer 可以先发布、ACK，再继续下一控制轮；不能因未来预留重新死锁。

## 容量整改与审查证据

[实现记录](archive/trigger-history.md#cep-owner)和[独立审查](reviews/CEP-owner-fresh-review.md)保存两个已关闭反例：pending CEP match 加 FSM alarm 超过共同 outbox 容量；pending 到 published 的完整 lifecycle 编码增长未预留。修复计算可先消费现有 outbox 后的未来控制轮，联合约束 count、bytes 和 snapshot 编码空间，没有提高默认限额或静默跳过匹配。

独立冻结源码验证共 229 项：matcher 23、四 KV 宿主 100、旧生命周期 97、EngineLifecycle 3、独立 probe 6，0 skip，无 warning/三 sanitizer 诊断，测试后源码哈希一致。具体快照和 `/tmp/eventflow-cep-fresh-review/final-*.log` 由审查报告保留；这是 CEP 阶段证据，不替代当前整个集群门禁。

## 当前边界与下一步

接口和实现在 [cep.hpp](../include/eventflow/cep.hpp)、[cep.cpp](../src/runtime/cep.cpp)及 EventProcessor/Engine 私有生命周期入口；CMake 已注册 matcher 与宿主测试。发布前可回滚的生命周期 ResourceExhausted 会丢弃候选轮，普通公开 hook 保留其既定 fail-stop 行为。

用户 guard/read/sink 必须遵守声明预算，不能静态保证任意 C++ 回调永远守约。周期 CEP 按声明 state/match 上界进行保守控制预算；宿主预算不匹配时 open/准入拒绝，不能假设 Clock 永不 TAKE。

请求模式、同轮观察、集群 watermark/checkpoint 已在其他规格继续演进，完整通用观察 host 仍后置。本 matcher 不提供事件时间重排、完整 Flink 方言或自动 HA；最终源码验证入口为[核心交付](core-delivery.md#验证证据与剩余门槛)。
