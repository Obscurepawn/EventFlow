# 事件生命周期

Status: done（事件生命周期基线；集群保证由控制面规格补充）
Owner: root
Updated: 2026-09-14

## 目标与边界

将M2持续关系执行与真实输入、时间、FSM、窗口和有界缓存接通。所有可恢复状态必须进入同shard提交；适配器或组件测试不代替最终三类进程端对端验收。KV snapshot在此实现存储边界，分布式一致checkpoint由M5协调。

## 接口与不变量

- `IMessageSource`区分持久输入身份与当前实例delivery token。MemMock/Kafka共用手动ack、已交付连续前缀、pause/resume/seek契约；Kafka关闭自动commit和offset store。Schema转换独立于source。
- `IClock`提供单调Unix微秒时间；ManualClock显式advance，SystemClock恢复时以`max(wall, restored)`锚定，再用steady elapsed推进。一个dispatch读取一次。
- `TimerQueue`是有界可复制状态。唯一timer ID、generation、deadline和可选period；取消只匹配generation。`advance(t)`先交付所有`deadline <= t`，同deadline按ID排序，周期timer合并到最新应到slot并记录skipped。数量预算不足或溢出时不改变队列；snapshot保存当前时间与全部timer。上层在round成功后才替换内存副本。
- `WatermarkTracker`保存固定源分区进度、显式idle和允许乱序跨度。没有进度的活跃分区阻塞推进；全idle保留现值；恢复活跃分区不能使watermark回退。记录使用到达前全局watermark判late，迟到策略必须显式使用；默认不把迟到输入用于event-time窗口，但处理时间关系可继续使用。idle是显式控制事件，避免测试依赖睡眠。
- FSM定义为有限状态+优先级转换，guard仅读声明的当前稳定MV/事件字段；step返回新状态/emit/timer意图。strict-next只消费指定事件类型的第一条；恰好deadline时先timeout。业务去重发生在FSM前。
- 窗口/TTL生成typed delete，恢复timer携带row generation，旧到期不能删除新版本。Latest通过同一Catalog中的关系体现，结果可供后续SQL派生。
- 状态缓存有总bytes/entries预算，按确认revision/owner epoch失效；UNKNOWN后不能从cache继续执行。计划缓存仅存内部已验证、不可变的绑定结果，公共BoundPlan仍检查来源与结构。
- `create_snapshot(snapshot_id)`冻结全部KV状态及shard receipt/revision，ID重试返回原snapshot；Rocks执行flush和checkpoint，Memory复制。恢复创建新store，不覆盖活动实例。Coordinator后续绑定源processed positions、Catalog、FSM/timer/request并原子发布manifest。

## 固定时间与延迟定时器

一次延迟advance(now)先删除全部deadline<=now的TTL，再用当前稳定MV执行timeout/clock，最后接受同now数据。FsmEvent.time_us、状态entered_at和新相对Timer都使用实际now；原deadline单独保存在scheduled_time_us，不能让callback观察过去时间或把新状态的等待期回填到过去。新相对timeout从now+duration开始，因此一次延迟唤醒不追溯执行多个状态超时；如需历史轨迹，确定性回放驱动应逐个advance到原deadline。周期时隙仍可合并并单独记录slot/skipped。该语义由独立生命周期审查的两项四后端反例推动修复，整改复验已完成。

简单规则使用确定性扩展 FSM：每 key/每 rule 一个活跃状态，携带 typed 寄存器和 Timer，按声明顺序选择首条匹配转换。多候选匹配由另行实现的[有界 E-NFA](cep-enfa.md)提供，不能把简单 FSM 改名冒充 NFA。

## 验证与当前状态

生命周期基线已接入主构建，独立 gate 整改后 129/129、root 97/97 通过，组件审查和处置统一见[阶段完成记录](reviews/M3-review.md)。以下报告保留限定范围的独立证据：

- [输入、snapshot 与时间](reviews/M3-io-time-component-review.md)：原 token/持久身份、手动确认、snapshot marker 与父目录 fsync、watermark 恢复边界。
- [FSM](reviews/M3-fsm-component-review.md)：最终时间/sequence 编码增长预算整改。
- [缓存](reviews/M3-cache-component-review.md)：跨轮保留、写穿、UNKNOWN fencing 和 Engine/Remote 集成。

MemMock 与真实 librdkafka adapter 已实现，Kafka 协议 mock 不代表真实 broker；固定版本 Compose 集成按核心收敛决定后置。存储 snapshot 是本组件边界，真实 Meta checkpoint 和三角色恢复已另行实现，见[控制面](control-plane.md)。WatermarkTracker 的显式 idle/固定源集合能力不等于当前集群已暴露全部策略。

## 下一步

保持本基线的 fixed-now、TTL 撤回、去重和 UNKNOWN 合同。请求模式的 fence/debt/释放通道由[事件与请求接入](event-request-integration.md)细化，最终组合验证只在[核心交付](core-delivery.md#验证证据与剩余门槛)登记，不再保留相互矛盾的组件施工日志。
