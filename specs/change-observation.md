# 同轮关系变化与异步完成观察

Status: active（基础已实现；完整通用 host 绑定和预算调度后置）
Owner: root
Updated: 2026-09-14

## 1. 当前决策与范围

用户确认：FSM/E-NFA可以主动读取声明的同key MV。采用**输入/完成→MV稳定→规则主动读→next state/Trigger及sink后继→同一批提交**；不引入延迟witness FIFO、独立逻辑时钟或观察队列ACK。TriggerOccurrence 在待处理期间保留不可变 reads/event/state provenance，不以之后重读 latest 代替；消费后的完整历史存档尚未实现。RelationChange仅复用Engine内部临时RowChange选择适用规则；如规则需要before/after，使用内部typed事件值。首版不实现通用CDC系统、对外变更流协议、持久观察队列、CDC replay log或独立服务；Kafka输入与原一致checkpoint要求保留。

这细化[原架构§7.4/§8](../docs/feature_computation_architecture.md)与[运行时契约](runtime-contracts.md)。保留灵活状态条件、crossing、有限E-NFA、Timer、源/视图/异步完成订阅和关系sink；一份Catalog、一个shard owner、无MVCC/HA。UDF/UDTF/UDAF在核心后实现；不增加用户Process Function API。

## 2. 当前代码与改动边界

| 源码 | 已有能力 | 本增量 |
| --- | --- | --- |
| [EventProcessor](../src/runtime/event_processor.cpp)的read_views/emit/emit_cep | 声明alias的同scope唯一关系点读；FSM/CEP状态、occurrence/sink可同批 | 从显式source/change/completion/Clock/Expiry绑定调度规则；不再依赖level dirty替代变化 |
| [Engine](../src/runtime/engine.cpp)及[私有接口](../src/runtime/engine_internal.hpp) | prepare→stabilize→finalize→stabilize，owned lifecycle hook；一次publish | 增加最终状态稳定后的有界观察执行阶段；每个规则sink传播后再开放其后继规则 |
| [RequestRuntime](../src/runtime/request_runtime.cpp) | final状态决定Latest；原批UNKNOWN、attempt fencing、固定输入 | 提供已验证的attempt/request状态转移摘要；完成在观察及输出预留内同批提交 |
| [FSM](../src/runtime/fsm.cpp)、[CEP](../src/runtime/cep.cpp) | 纯step/advance返回候选状态，声明reads与资源上限 | 复用这些纯接口；观察时刻就是当前owner dispatch的实际now |

Catalog 声明/codec、纯观察程序与 credit 基础、Engine 私有观察阶段、可信 attempt/request settlement 已实现。以下合同中的完整 EventProcessor host 装配与未来输出预算调度尚未全部落地，按核心范围后置。原队列设计已经废弃，不作为交付要求。

## 3. Catalog声明与host绑定

公共纯声明位于[observation_definition.hpp](../include/eventflow/observation_definition.hpp)：ObservationInputKind、ObservationProgramKind、ObservationInputDefinition、ObservationReadDefinition、ObservationRuleDefinition、ObservationRuleDescriptor。Catalog::define_observation登记；snapshot/fingerprint/from_snapshot/canonical codec统一保留。没有订阅时旧canonical字节不变。

- 整个规则由一份声明指定Fsm/Cep、name/version、recipe_id/version、implementation_identity、program_fingerprint；program_fingerprint为完整规范声明内容，回调地址/函数体不序列化。EventProcessor必须用相同字段及精确输入/reads/sink/targets匹配已编译recipe，不能只按名字找callback。
- Source绑定声明source_kind和typed source_fields；RelationChange引用Catalog关系及insert/update/delete mask；AttemptSettled/RequestTerminal同时声明plan及其精确completion_targets；Clock声明policy_id；Expiry声明相应关系。非Source的event布局由Catalog生成，不能由用户伪造host状态字段。
- scope_fields声明非空业务分区；reads包含alias、relation、精确visible fields及正字节上界，只允许同scope unique_per_partition关系。条件主动点读当前staging稳定值，缺行是nullopt，不把缺行当0。
- 可选sink_relation只指向同scope PublicInput关系；activation_targets是实际Request计划的完整目标关系引用，host必须与实际plan相等。不能只存plan字符串而省去完成/请求激活反馈边。
- inputs/reads/event/sink及程序声明有有限数量/编码预算；无法证明的变量长度用明确上界约束，实际超界在可回滚阶段拒绝。

Catalog的订阅查找和rule_order都是派生索引。依赖图包含已有关系依赖、关系/reads/完成targets→规则以及规则→sink/activation_targets；拒绝因果反馈、缺失关系、跨scope和不匹配签名。定义与合法sink链仍在一个Catalog中，不维护第二份可编辑业务图。

现有关系定义有完整绑定前缀。Catalog观察descriptor记录after_relation_count以恢复关系与观察声明的插入顺序；全Catalog版本计数包括观察声明。关系仍可在观察声明后追加，旧ID及无观察时的canonical保持。

## 4. 一轮中的主动读与执行顺序

1. owner为公开操作固定实际now，先处理按既有契约必须先行的到期fence；UNKNOWN先确认原批。
2. 应用本轮source或被认可的completion，完成关系SQL传播、Request finalize、typed status与Latest稳定。
3. 根据本轮typed cause及关系变化选择eligible规则，按Catalog派生的依赖顺序执行。规则可主动读取已声明关系；guard/update不能越过声明、重入Engine、直接KV或执行外部I/O。
4. step产生next FSM/CEP state、Timer及TriggerOccurrence；sink写关系后必须把其SQL后继完全稳定，再执行依赖该sink或读其后继的规则。不能把所有guard先求值再统一写sink，否则后继读到旧值。
5. 记录最终规则状态、审计reads/event、occurrence/request intent、sink及Engine metadata，完成总预算核验后一次publish。已确认后才派发函数或公开输出。

同批有多条适用事件时，为规则定义固定顺序：Source、关系变化（RelationId/规范identity顺序）、AttemptSettled（node/attempt顺序）、RequestTerminal、Clock/Expiry。依赖排序优先于无关规则的ID顺序；同一规则的同一事件不重复执行，显式声明两个不同输入则按声明语义执行。

必须先等一个被观察关系的所有本轮上游写入者完成，才确定它的最终net变化并运行下游规则。一个sink导致另一规则的read祖先改变，也必须纳入排序；仅根据最初source roots做一次遍历不足。root可采用拓扑waves或有界worklist，但不能反复执行有副作用的同一逻辑step来“趋于稳定”。

不同提交的`10→120→10`逐次进入观察，不丢crossing。某关系在同一提交内净变化为零时，不声称存在两个已提交中间变化；业务需要逐输入观察时不能把这些输入先合并成一个事务。Trigger内部sink链的次序按完整依赖计划定义，而不是随机遍历顺序。

没有SourceIdentity的纯MV/sink/TTL变化也能触发RelationChange。有效completion即使SQL无输出/无RowChange，仍可通过可信AttemptSettled/RequestTerminal事件触发；旧attempt、重复回包和未接纳worker消息不能伪造完成。worker payload不提供成功phase或candidate身份。

## 5. Engine/RequestRuntime私有执行合同

root提供私有RuntimeEvent/RuntimeCause及观察上下文；它们不包含用户callback代码。最终观察阶段能：读取声明关系、调用已经绑定的纯program、stage授权owner状态、返回已声明sink关系写；每波sink写由Engine检查权限并稳定。

现有“final seal只允许stage S/I、不写关系”的版本不足以运行通用sink，不能简单在seal调用emit后跳过稳定。应区分可传播关系输出的观察阶段与最终只读审计/提交封口。一个规则执行期间的reads稳定；后继规则在其依赖输出稳定后才获取自己的reads。

观察规则的state实例只由新调度入口驱动，旧source/Timer循环不能再次step同一个实例。未注册通用订阅的旧规则保留原行为。新旧规则如有sink/read依赖仍需纳入同轮排序，不能通过不同入口绕过环检查。

RequestRuntime不在早于最终Latest的companion里读取Latest。原RequestCommitHook仍可提供lifecycle候选状态；新观察阶段在整Request phase与Latest确定后执行。多个owner候选写同一lifecycle/index时必须合并成一次更新，不互相覆盖。

## 6. Timer、CEP与真实Request捕获

统一实际dispatch processing time，不产生第二套observed_at逻辑时间。FsmProgram::step仍先advance(now,当前稳定reads)，CepProgram::step仍先expire(now)再匹配；deadline保存scheduled审计，延迟Timer guard在实际执行轮读视图，不伪造deadline时刻的历史快照。

无新source时，现有owner Timer/pump调度仍能推进到期FSM/CEP、周期与expiry；周期合并错过slot并记录skipped。规则具备Timer不要求人为伪造source；订阅Clock/Expiry使用明确的内部事件身份。到期与completion的先后继续遵守实际期限先fence、释放控制工作不被普通trigger队列挡住。

TriggerOccurrence持有当轮固定条件reads、事件、规则版本和转移事实。触发→admission若因Ready/额度延迟，真正Request capture仍使用实际受理now与稳定revision R，R+1提交，不把TriggerWitness当InputBundle。candidate epoch/cap按原事实和当前期限检查，不能把旧触发重新绑定到新候选；已过期intent明确记录终态原因。

## 7. 同批预算与完成释放

同步step会产生状态与输出，因此必须在接受异步工作前为其最坏合法观察后果预留，而不是等completion到了才发现outbox已满。使用实际有效EngineConfig/StoreLimits，分别计入rows、reads、mutations、完整canonical批头/每mutation及Remote wire。

- 配置时计算有限无环关系/观察/sink/activation闭包；FSM按max_steps、CEP按路径/匹配/状态/输出上界，包含完整未来lifecycle、当前scope状态及可变长事件/读集/输出。超大或无有限上界配置明确拒绝。
- source、普通Timer/candidate/TTL新增时先验证本轮及未来不可绕过expiry fence的额度；无法容纳时不提交source position。不能调用未来guard探测预算。
- Request admission预留不经claim也会发生的cancel/deadline/终态及其观察输出；claim/retry在task可派发前预留该attempt一次settlement与相关完整闭包。本轮Accepted/Running变化与未来结果各自记账；同轮旧attempt超时、新attempt claim不得复用同一额度。
- normal occupied与future reserved合计不超过容量；普通新输入不能占用已获准任务的终态/输出额度。完成将预留转为真实state/outbox占用，unused部分仅在提交确认后释放。
- active Request已终态不表示它发布的未ACK occurrence可不占空间。下一请求不得因active slot释放重用尚被输出占用的预算；ACK/intent到期清理必须能释放该占用。
- 守约callback与有效声明下，普通trigger队列满不能阻止已获准completion/cancel/timeout同批保存状态与输出。用户guard/update真实错误或违反声明仍明确fail-stop，不以跳过规则来制造成功；纯callback不提供外部副作用exactly-once。

资源上界包含sink再传播和后继规则，不只计算第一个step。无法实现其最坏合法状态时，配置或上游准入提前拒绝；不能把永久无法完成的Request标成已成功受理。

## 8. UNKNOWN、恢复与验收

UNKNOWN保留整批MV、next规则state、witness、sink、Request/outbox和额度候选；只确认原批，不重新读最新MV或再次step。仅已确认效果进入committed_rounds，不能重复返回之前确认的子轮。确定未提交且无UNKNOWN的重试可重新执行纯callback，逻辑状态/输出仍只提交一次。

恢复按既有有界lifecycle/request状态与额度重建，检查Catalog/recipe/signature及版本，不能用当前callback重解释旧状态。没有新的观察队列；Trigger/outbox仍由原确认与重放身份管理。全局一致 checkpoint 已在静态核心另行实现；本组件测试不代替控制面及真实恢复证据。

必验轨迹：

1. `10→120→10`跨三次提交，crossing一次且审计reads=120；同批净零不生成虚构已提交变化；纯MV轮无需source。
2. completion的最终status→Latest→SQL稳定后NFA主动读到新值，整Request失败不发布成功Latest；有效无RowChange完成仍可观察，旧attempt/重复不重触发。
3. rule A sink→SQL→rule B的reads，B只看到A稳定后新值；多上游写同一读祖先不会过早step；循环与跨scope拒绝；sink与全部规则状态同批。
4. FSM/有界E-NFA混合Source/Clock/Expiry/RelationChange/Completion，deadline恰界、无输入到期、strict/NextOfKind/FollowedBy、周期skip保持实际processing-time语义。
5. 满active/outbox和最大合法结果下，已获准完成/取消/超时能同批提交并释放；旧未ACK输出继续计费，retry/new claim不借旧credit；真实有效Store/Remote边界提前拒绝。
6. 观察/最终publish UNKNOWN后确认：callback不重跑、next state/sink/outbox/once只提交一次；重启按原版本恢复，损坏或缺recipe拒绝。
7. Trigger事实与延后InputBundle不同：保留原条件值，实际admission按当前R/now捕获，旧candidate/过期cap不能重绑。

## 9. 已实现基础与剩余工作

| 基础 | 主树入口与范围 |
| --- | --- |
| Catalog 声明与 codec | [observation_definition.hpp](../include/eventflow/observation_definition.hpp)、Catalog 和 catalog_codec；六类 typed input、精确 reads/recipe、sink/activation 因果与前缀重建，无观察时 canonical 不变 |
| 纯程序与额度 | [observation_program.hpp](../include/eventflow/observation_program.hpp)、[observation_credit.hpp](../include/eventflow/observation_credit.hpp)；绑定 FSM/CEP 声明、状态校验和有界多轴额度预留，尚不等于完整 host 生命周期调度 |
| Engine 同轮阶段 | [engine_internal.hpp](../src/runtime/engine_internal.hpp)中的 RuntimeObserver/RuntimeCause、install_observer、stage_observer_state；按派生顺序在最终 SQL/Latest 后读取，验证 sink 权限并稳定后继；UNKNOWN 不重跑 callback |
| 可信请求原因 | [request_machine.hpp](../include/eventflow/request_machine.hpp)的 RequestAttemptSettlement 与 RequestRuntime 接入；只为实际接纳的 settlement/首次请求终态生成原因，旧或重复回包不能伪造 |
| 回归入口 | catalog_observation_test、observation_program_test、observation_credit_test、engine_observation_test、request_observation_test 已注册；覆盖 codec、纯程序、私有阶段和四 KV 请求观察基础 |

Catalog 限制为 256 个规则、每规则 64 个 input/read/target、4096 个关系节点、64 KiB/64 层 canonical 程序声明；event/read/sink 字节上界必须为正且不超过 16 MiB。RelationChange 双侧布局另计 `2 * field_budget + 3`；这些是结构/编码界限，不是整个进程 RSS 硬上限。隐藏 partition 或投影改名使用 canonical partition 类型，用户不填写物理隐藏列名。activation 检查覆盖请求 capture/SQL 祖先及 RequestStatus，不能只连接最终 target 而漏掉反馈。

Catalog 基础阶段的 `/tmp/eventflow-observation-foundation/observation-final.log` 与 `regression-final.log` 保留当时 12 项及四个原 Catalog 目标证据；后续 Engine/request 观察基础有新增测试，最终全量范围统一见[核心交付](core-delivery.md#验证证据与剩余门槛)，不把旧数量作为当前覆盖总数。

下一阶段需要完成编译 recipe 与 EventProcessor 实际实例的精确装配、新旧规则统一调度、完整 source/Timer/completion 观察闭包的未来输出预留与恢复校验，按上文七条轨迹验证真实 host。静态集群目前使用内置特征计算的简单条件/Clock；仅有 Catalog 往返、私有 observer 测试或可合并 level dirty 都不能宣称任意规则已经可部署。没有新增持久观察队列或第二套业务图。
