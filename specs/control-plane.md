# MetaService 控制面与多进程一致边界

Status: active（静态核心已实现；完整版本、Job 与迁移协议后置）
Owner: root
Updated: 2026-09-14

## 1. 已实现核心与完整产品合同

后续授权补充：2026-09-16 用户选择实施 [生产 C++ UDF 上传与首次部署](native-udf-delivery.md)，增加服务启动后 STAGING 上传、结构化定义提交与首次部署。该记录是此有限切片的最新状态；下文在线迁移、多 Job 与完整扩展族合同继续后置。

MetaService 是 Schema、配置和必要元数据的唯一权威，Compute 只安装、执行和缓存明确版本。部署保持 1 Coordinator、1 KV、N Compute，共 N+2 个进程；共享 KV、同 key 单 owner、无 MVCC/HA/跨 key 计算不变。依据[产品范围](product-scope.md)、[运行时契约](runtime-contracts.md)和[架构](../docs/feature_computation_architecture.md)。

| 当前实现 | 真实边界 |
| --- | --- |
| [CoordinatorService](../src/service/coordinator_service.cpp) | 在独立 control shard 持久保存 format=2 集群记录、固定 recipe/DDL/Catalog、assignment、源前缀、水位、pending 原投递和 checkpoint；单控制 gate 串行变更 |
| [ComputeService](../src/service/compute_service.cpp) | 静态 shard 安装/输入/推进/查询/暂停/恢复屏障；每 shard 单 owner、共享 Remote KV、节点预算分配的 cache、真实线程函数池 |
| [ClusterClient](../include/eventflow/cluster_client.hpp) | 只配置 Meta 地址，通过其代理 discover/catalog/ddl/deploy/ingest/query/request/pump/drain/pause/resume/checkpoint/status；尚无直接路由刷新或多 Job API |
| [CLI](../src/main.cpp) | kv、compute、coordinator 三个角色；Compute 的静态配置包含 KV endpoint/shards，Meta 推送部署；当前启动顺序为 KV、Compute、Coordinator，再 deploy |
| 一致 checkpoint | 冻结原投递和 worker/control revision 后建立 KV snapshot；显式停止全部进程，从 Rocks snapshot 创建新目录并验证冻结意图后恢复 |

当前只支持启动前 DDL 和单一版本静态部署，运行中在线 DDL、任意用户代码上传、复杂 Job/unregister、独立 Schema 演化、配置热更新及节点自动发现均未作为已实现能力。worker 必须校验相同全局 shard_count、图版本/指纹与声明；全部确认后才开放 RUNNING。

下文第 2-9 节保留完整产品的元数据、版本和生命周期合同。其中 DeploymentImage、IMetaStore、操作 handle、动态 Job/路由等是后续接口模型，不代表同名类型或 endpoint 已存在。静态核心通过较小的整体 Meta 记录实现其子集；现有外部 API、源串行准入和实际恢复流程以[核心交付](core-delivery.md)为准。后续实现不能破坏这里保留的身份、单 owner、版本和恢复约束。

## 2. 首版收敛决策

1. 一个部署只运行一个 Coordinator。控制面使用一个串行操作队列，所有影响图、配置、源、生命周期、路由和 checkpoint 的变更通过该队列。普通输入在控制屏障之间并行发往不同 shard。
2. 一个物理 shard 只有一个 ShardTask 和一个 Engine；其 Catalog 是部署统一图。多个 Job 是图中明确归属的子图，不创建多个写同一 shard 的 Engine。
3. 对象与版本不可变，激活的是引用它们的 DeploymentImage。首版 DDL/语义配置变更使用全局停顿屏障，全部目标 worker 确认后才发布 READY。失败保留暂停并前向修复。
4. 数据面固定逻辑 shard 数与 shard→Compute 静态映射。1/2/4 Compute 等价测试分别启动独立部署，不隐含在线变更 shard 数、重分片或自动接管。
5. Meta 也通过唯一共享 KVService 持久化。启动时预留 control shard：`data_shard_count=N`、`control_shard=N`、KV `shard_count=N+1`；Engine 与 shard_for 仍使用 N，不能把业务 key hash 到 control shard。IMetaStore 组合 IStateStore，使用独立 owner token/revision，既不新增服务，也不创建第二个写业务 shard 的 Engine。
6. 首版版本变更先 drain 已受理请求，达到有界截止后按显式策略取消并提交终态。新旧版本不并行维护同一可变关系；旧版本定义/签名/结果解码信息仍保留引用。将来允许旧 Request 与新版本并存时，必须通过本文版本固定契约，不能退回按最新版本重解释。

第 5 项保持用户选择的共享 KV 方向。control shard 的 key 仍遵循存储公开 namespace 校验，由 Meta 独占其普通数据命名空间；不得访问/覆盖 store 私有 receipt/config key。Compute只获得所属业务shard的owner身份，不能提交control shard。KV预配置control owner解决bootstrap先后关系；不存在先从未启动的Meta读取自己的初始owner配置的循环。

语义版本管理不等于任意图迁移。运行时持久 fingerprint 绑定完整 Catalog；静态核心只支持启动前 DDL。后续完整 Job 提交需提供追加独立子图的受控兼容 rebind，见 §6.4，不能把启动前 DDL 当作动态提交已经完成。

## 3. 元数据模型与身份

下表是待实现的 Meta 记录模型，不是现有 C++ 类型清单。新增持久记录带 `format_version`。数字溢出、未知版本、重复 ID、依赖丢失、未知 enum、非法 UTF-8 或超预算明确返回 Status，不能只解析成功就接受。新增控制面 API 封套内 64 位 ID/offset/revision 使用规范十进制字符串，避免 SDK 使用浮点 JSON number 丢失身份精度。现有 Catalog canonical codec 保留其 JSON 整数格式与 fingerprint：跨 API 以原始 canonical 文本载荷传递，SDK 不解析后再序列化改写该载荷；Meta/Compute 使用现有精确整数 decoder 校验。

| 记录 | 必要字段及意义 |
| --- | --- |
| ClusterMetadata | cluster_id、meta_format_version、partition_hash_version、logical_shard_count、各 ID allocator 高水位、当前 restore_generation、clean_run 标志 |
| NamespaceDescriptor | namespace_id、名称、状态、所属部署；作为名称解析和资源归属域，首版不是多租户安全承诺 |
| SchemaVersion | schema_id/version、稳定 field ID、类型/nullable、PK/partition key、row/key encoding_version、兼容性分类 |
| CatalogVersion | 不可变完整规范定义、稳定 relation/state ID、依赖和激活边、Schema/函数引用、fingerprint；包括 MV、Trigger/CEP、Capture、Request、结果与 latest 规则 |
| JobVersion | job_id/version、namespace_id、对象归属集合、Catalog 引用、输入映射、触发/请求计划、资源配置引用；不等于一个 Request |
| JobDeployment | job_id、active_job_version、deployment_epoch、graph_generation、runtime namespace映射、运行状态、最后 stop/checkpoint 边界、各 worker 状态、原因、当前 operation_id |
| DeploymentImage | image_id、graph_generation、精确 Catalog/Job/Schema/Config/Source/Extension 版本引用、assignment 及 runtime namespace 映射；安装内容冻结后不原地修改，active pointer 只指向已确认镜像 |
| ConfigVersion | config_id/version、作用域及类型化内容、校验后默认值、生效模式；定义不可变，desired/active 引用与回执由部署状态记录。保存完整有效值，不让各 Compute 自行补不同默认值；校验CEP/宿主/控制轮等嵌套预算相容性，不能把永久无法容纳一个key的配置伪装成临时队列拥塞 |
| SourceDefinition | source_id/version、job_id、adapter_kind、broker 配置/凭据引用、topic/partition 固定 assignment、Schema 与解析器版本、时间/迟到策略、起始与恢复策略 |
| AssignmentVersion | route_version、assignment_epoch、shard→node_id/endpoint、owner token 身份、目标/实际启动状态；Compute 进程 incarnation 与业务身份分开 |
| ExtensionVersion | extension_id、family、定义与版本、精确签名、输入/输出 Schema 引用、implementation_identity、artifact digest/size/location、执行能力声明与兼容性元数据 |
| ControlOperation | operation_id、规范请求/内容身份、expected_meta_revision、对象/目标版本、屏障 ID、阶段、每 worker/每 shard 回执、失败/repair 原因 |
| CheckpointManifest | checkpoint_id、KV snapshot_id、冻结 DeploymentImage、source/global prefix、watermark、shard/runtime 摘要、控制操作和恢复所需身份，详见 §9 |
| VersionReferences | active deployment、inflight Request、持久结果/历史和 checkpoint 对版本的引用；删除的检查依据 |

MetaService 负责定义及引用真相，Compute 上报的运行状态是待验证证据。Meta 不在每个 Request 提交中跨进程维护一个假定原子的引用计数：部署先固定整个版本，解除引用只在屏障处根据权威 KV 的 Request 状态和各 shard 回执对账。失联/dirty worker 的引用保持保守占用；不能因心跳消失删版本。

Schema 的独立记录与 Catalog 内嵌 Schema 是同一份定义的索引和部署副本：Meta 在 bind/发布时核对 ID、版本和完整规范字段，Compute 在 install 时核对引用与 payload 一致，不能任选其中一份为准。当前 Schema/Field 数字身份能够随 snapshot 往返；“稳定字段 ID”尚不表示跨 Schema 演化已经实现，未来演化不能因列重排复用旧字段身份。首版仍可拒绝已有 Schema 的变更。

### 3.1 namespace 与多 Job 隔离

- 用户名称解析使用 `(namespace_id, job_id, local_name)`，Catalog 的 relation/state ID 在部署内全局唯一并单调分配，不随进程重启、unregister 或重试重新生成。首版 SDK/解析边界在 bind 前把名字解析为全局对象；不暗示当前 SQL grammar 已支持 database.schema.table 语法。
- 当前 [key_codec.cpp](../src/core/key_codec.cpp) 的物理前缀是 `D | shard | kind | object_id`，没有架构示意中的 database_id。首版可保留该编码，以单部署存储域和全局唯一 object_id 保证隔离；不能仅在文档增加 namespace 字样就宣称实现了物理隔离。
- Meta 分配运行时私有 state object ID，保留 Engine 元数据 ID；Processor、Request、CEP 的 ID 与 Catalog 聚合 state ID 不冲突。默认 `0xffff0000`/`0xffff0001` 仅适合单实例组件，不作为每 Job 固定分配策略。分配器耗尽明确拒绝，禁止 wrap/reuse。
- Request、Timer、occurrence、source_id 的规范身份包含持久 Job/对象作用域。同一个业务 key 在两个 Job 可以有不同状态；各 Job 的输入只更新自己的子图，stop 后另一个 Job 的输入不能间接推进已停止 Job。
- 首版每 Job 独占可写关系、MV/触发运行时及 source subscription；相同 Kafka topic 被两个 Job 使用时采用不同持久 source_id 与独立 group/assignment。不开启跨 Job 可变依赖或多个 Job 同写一个基表；提交时明确拒绝。共享不可变 Schema、函数/制品定义允许，必须记录引用。
- 需要共享可变基表/MV的后续方案须定义依赖方与生产方生命周期，不能以“共享元数据”默许停止后的关系继续变化。统一 Catalog 仍保留依赖保护，显式 drop/清理不能越过任意有效引用。

### 3.2 用户扩展元数据与本次边界

当前核心只激活已编译的内部确定性函数，版本和签名仍由 Meta 下发并与各 Compute 的本地 FunctionRegistry 精确匹配；缺少实现的 worker 返回安装失败。ArtifactLocation 可以是 `builtin://...`，implementation_identity 需对应确定的构建/函数实现身份，不能只记录函数名字。

后续 UDF、UDTF、UDAF 共用 ExtensionVersion与同一Catalog生命周期；Process Function已由用户暂缓，只保留未来元数据扩展位置。制品以不可变内容身份定位，位置可以更新为同一内容的另一存储地址，但内容改变必须是新版本；不能在原路径替换 `.so` 后让旧版本悄悄加载新代码。元数据保留制品长度、内容摘要、目标平台、需要的 runtime ABI 与签名版本、分发/校验状态以及引用。

family 对应的详细签名保留版本化描述载荷：UDF 的标量输入/输出，UDTF 的有界行输出身份与撤回声明，UDAF 的状态 schema/codec 与 add/retract/merge/finalize 能力。本次不实现或激活Process Function，也不把其专门研究作为验收前置；将来恢复此需求时再确定状态/Timer/输出/checkpoint及C++ ABI契约。仅能存储定义不表示支持执行；不支持的 family/ABI 在部署时明确拒绝。

制品上传、远程分发、签名安全、动态加载与三类实际算子仍是核心完成后的工作。MetaService 预留记录与版本检查，不能以该表存在代替 AC12。

## 4. 持久化、bootstrap 与恢复资格

### 4.1 MetaStore 与原子控制操作

目标接口为 `IMetaStore::read_snapshot()`、`commit(MetaBatch)`、`resolve_pending_commit()`；工厂使用 `open/create/from_snapshot`，项目生产函数使用 `noexcept` 和 `Status/StatusOr<T>`。接口为目标概念名，尚未加入头文件。适配器组合IStateStore对control shard读写：单元测试使用MemoryStateStore，真实部署经RemoteStateStore访问KVService的Rocks后端；不额外引入Coordinator本地Rocks库或第二套落盘协议。

每个 MetaBatch 在一个提交域原子保存对象更新、operation 阶段、meta_revision 和操作回执；复用现有 IStateStore 时使用其规范 `make_commit_id(owner, shard, expected_revision)`。业务 operation_id 和存储 commit_id 分开：一个 DDL 多阶段可产生多个 MetaBatch，但 API 重试始终找同一个 ControlOperation。

同 operation_id 同规范请求返回当前/最终回执；同 ID 不同内容返回 Conflict。响应丢失不产生新 ID。MetaStore UNKNOWN 时停止后续控制变更及新准入，保留完整原 MetaBatch，先确认或重试；单靠内存状态或目录存在不能宣称已发布。进程存活期以原批确认；异常退出后遵循 dirty-run 人工恢复，不声称能自动重建丢失的 UNKNOWN 批。

控制操作 journal 是 DDL/生命周期进度记录，不是重新为业务消息建立完整 redo log。持久后端保留 RocksDB WAL 与同步写；内存适配器只证明契约，不承诺 Coordinator 进程重启后的持久性。

### 4.2 最小 bootstrap 配置

Coordinator 启动仅需本地监听地址、期望 cluster_id、KV endpoint、data/control shard引导参数、control owner token、静态节点引导清单和本进程日志配置。Compute 启动仅需 node_id、监听地址、MetaService bootstrap 地址以及本机硬内存/线程资源上限。其 Schema、Job 配置、激活图和分片 assignment 必须从 Meta 获取。KV必须独立读取固定N+1 shard和各owner清单启动，随后Meta验证其配置；本地引导文件只提供启动根，不作为日常Schema/Job配置第二权威。

首次创建部署时 Meta 原子落盘 ClusterMetadata、静态 assignments 和有效配置。已有元数据时启动参数只用于定位与核对；不以命令行默认值覆盖持久 Job/source/Catalog。cluster_id、shard_count、hash/codec 版本或 owner 身份不匹配时拒绝启动并报具体差异。

启动顺序为 KV 可用 → Coordinator 读取 MetaStore 并进入 RECOVERING → Compute 注册 incarnation/能力并逐 shard 验证 KV 与目标图 → Coordinator核对全部目标回执 → 先恢复 Timer/请求与源位置，再允许运行。注册是静态名单内的握手，不是自动选主或自动新增 owner。Coordinator 不可用时暂停外部准入；正在进行的 owner 提交可以确认至有界切面，Compute 不自行修改部署状态。

`clean_run=false`、未闭环 DDL、未确定 snapshot 或不匹配的 shard revision 都阻止普通 resume。健康检查可报告进程存活，但 ready 必须为 false，并暴露待人工 `repair`/`restore` 的操作。首次开始运行前持久标 dirty；只有全部节点与 Meta/KV 已达到记录的受控停止边界才能标 clean。单 Job STOPPED 不等于整个部署 clean。

## 5. API、发现与重试

以下是完整产品的目标 API 模型；当前已提供第 1 节列出的静态 Meta/SDK endpoints，不能把下表名称当作全部可调用接口。统一 HTTP/1.1 + JSON，复用已有 transport；元数据 API 与 Compute/KV 应用接口保持传输抽象。接口消息有 protocol_version、长度/批量/超时预算。操作创建返回 operation handle，客户端通过 get_operation 查询完成；HTTP 成功只表明某阶段已受理时，必须在 body 明示阶段，不返回暗示部署完成的 READY。

| 目标 API | 核心输入 | 成功结果/约束 |
| --- | --- | --- |
| get_cluster / discover | bootstrap、可选已知 route_version | cluster_id、restore_generation、目标/有效图版本、node endpoints、assignment 与 Job 部署状态 |
| get_schema / get_catalog / get_config / get_source / get_extension | 精确 ID/version | 不可变定义、内容身份；不以 latest 自动替换精确版本；定义存在不等于部署已激活 |
| get_deployment / get_operation | job_id 或 operation_id | 一次权威读取中的 meta_revision、desired/active image 与配置引用、冻结目标集合及各 worker 安装/生效状态 |
| register_schema / register_extension | operation_id、namespace、声明与期望元数据 revision | 已验证的不可变定义 ID/version 与登记回执；不支持的扩展 family 可保存声明但不得激活，动态制品上传/执行仍后置 |
| submit_job / apply_ddl | operation_id、namespace/job、expected version、声明 | Job/DDL operation handle；SQL 与 SDK 转同一声明路径 |
| update_config | operation_id、scope、expected_config_version、patch | 完整候选 ConfigVersion 与生效类别；未知字段拒绝 |
| stop_job / resume_job / unregister_job | operation_id、job_id、expected_deployment_epoch、策略 | 持久阶段与最终 Job 状态；默认 retain_data=true |
| repair_operation | 原 operation_id、明确修复动作 | 查询并继续原阶段/原目标；变更目标内容需新 operation 且先解除旧阻塞 |
| create_checkpoint / restore_checkpoint | operation_id、checkpoint_id | 完成 manifest 或恢复操作；restore 需受控停机 |
| ingest / query | 精确 Job 部署、Catalog/Schema 版本、业务 scope/身份 | 转到唯一 shard owner；单 key 结果带 revision/catalog_version |

| 路径 | MetaService 的责任 | 消费者的责任 |
| --- | --- | --- |
| 定义/配置写入 | 串行校验、统一 ID 分配、依赖和预算检查、持久版本/操作；SQL 与 SDK 共用 Catalog | 客户端保留 operation_id；Compute 不接受独立 Schema/配置注册 |
| 元数据读取 | 返回精确版本；名称或 active 查询在一次 read_snapshot 中解析并返回 meta_revision/image_id | SDK/Planner 据返回版本固定后续读取；分次查询不拼接不同 active image |
| 部署安装 | 冻结完整 DeploymentImage，提供 canonical 定义及必要 recipe，记录 prepare/install/activate 回执，全部确认后 READY | Compute 校验 Schema/图/配置/实现身份和本机能力，安装同一镜像，回报实际版本；失败保持屏障 |
| 输入与结果读取 | 固定 Job/图/Schema/配置、解析 source 与路由；历史请求定位原 generation 与 decoder | owner 在 round 边界读写，结果带原版本；SDK 缓存元数据可直接解码，缺版本时回 Meta 精确读取 |

不可变定义可缓存到其引用释放；active 指针与部署状态必须按 meta_revision/image 刷新。查询开始时固定一个已 READY 的镜像，Meta/Planner 只按其中的 Schema 和 Catalog bind，Compute 拒绝与当前安装镜像不匹配的计划。期间发生切换时返回可识别的版本冲突，由 SDK 刷新并重新 bind；不能把旧绑定计划发送给新图。历史结果由持久记录选择原 Schema/函数版本，旧 decoder 不存在时明确拒绝；保留定义版本不提供业务行的 MVCC 历史快照。

SDK 路由缓存键包含 cluster_id、restore_generation、job_id/deployment_epoch、route_version；请求携带 assignment_epoch、目标 shard、精确 Schema/图版本和原输入/Request 身份。Compute 核对 Job 正在运行、图版本与 owner context 后才准入。路由过期返回结构化 Conflict + 可刷新标识，Job 停止/注销返回 FailedPrecondition；两者不能混成成功或静默转发到任意 owner。

SDK 在明确路由过期时刷新 Meta 并有限次数重试，原 delivery/source identity、payload、request_id 不变；不修改事件业务版本。HTTP 超时后查询/重投同身份，可能已提交的写不自动生成新 operation/commit。原部署已注销或恢复代际已改变时，SDK返回明确失效；不能把旧 Job 的写自动转换为新部署的一次输入。

首版外部消息与 SDK ingest 通过 Coordinator 统一准入，便于维护源前缀和控制屏障；查询可由 Coordinator 转发或通过验证的路由直接到 Compute。同 key 查询由 owner 排队获得当前轮边界。跨 shard 查询如开放，只能声明逐 shard 当前结果；不承诺同一时刻快照。DDL屏障期间普通数据查询统一暂拒，保留元数据/操作状态读取，避免看到半安装图。

## 6. 串行化与 DDL 状态机

### 6.1 两级顺序

控制面持久状态由一个操作执行器串行写；网络请求线程只受理/查询，不直接修改 Catalog。每个 shard 的输入、Timer、函数完成、配置应用和控制命令进入同一 owner 队列。控制屏障的回执表示它之前已受理的操作达到确认切面，不能只表示 RPC 处理线程收到了消息。

| 操作 | 首版影响域 | 屏障内允许的进展 |
| --- | --- | --- |
| DDL/Job版本/语义配置变更 | 全部署 | 已接纳输入提交、UNKNOWN确认、请求完成/取消；停止新增输入与触发 |
| checkpoint/全部署停止 | 全部署 | 排空至冻结切面，冻结后业务shard写禁止；仅当前operation的control shard阶段记录可推进 |
| stop/unregister 单 Job | 该 Job 的输入/触发/请求；控制状态仍全局串行 | 其他无依赖 Job 可运行；同 shard通过唯一owner交错执行 |
| source assignment/seek/解析器变更 | 所属 Job，需 stop；涉及全局图时升级全部署 | 确认旧 token、保存边界后重开适配器 |
| 路由/节点 endpoint 更新 | 全部署受控屏障 | 无自动失效接管；新 endpoint 生效前旧实例已退出/禁止准入 |

控制队列执行某个长操作期间仍可查询状态、确认该操作的回执、接收排空所需的内部完成；其它修改返回 Busy/FailedPrecondition 或进入有界等待队列。不能锁住 completion/ack 通道等待自己排空。配置、DDL、checkpoint、source seek 和 unregister 不能各自拿局部锁后交错发布。

### 6.2 DDL/安装流程

```text
VALIDATING -> PREPARING -> QUIESCING -> INSTALLING -> ACTIVATING -> READY
                 |             |             |             |
                 +-------------+-------------+-------------+-> REPAIR_REQUIRED
```

1. VALIDATING：在当前权威 Catalog 上解析/bind/检查依赖、Schema、同 key 分片与全部预算；冻结完整候选 DeploymentImage，持久分配对象 ID/目标版本和规范内容。未产生副作用的非法请求可直接 REJECTED。
2. PREPARING：持久操作和目标 worker/shard 集合；发 prepare 验证版本 codec、内部函数身份、资源和初始化可行性，不开放候选图写入。操作 ID/内容一致的重复 prepare 幂等。
3. QUIESCING：Coordinator 暂停 sources 与 SDK 准入；确认已交付输入和全部 UNKNOWN；停新触发，完成或显式取消已受理请求；冻结 Timer 与数据查询。每 shard 回执含当前图、revision、最后 commit 身份、源/运行状态和屏障 ID。
4. INSTALLING：所有目标 shard 安装同一候选，必要 MV 对已保留关系做停顿下有界初始化。不能获得所需历史或超预算则停在 REPAIR_REQUIRED。安装回执在数据 KV 与该 shard 的新图/初始化结果一起保存；不是只有 Meta 内存布尔值。RPC 重试先读回执，不能再次执行非幂等 backfill。
5. ACTIVATING：全部 shard 确认 INSTALLED 后 Meta 持久决定激活目标版本，向全部目标发 activate，但数据入口仍关闭。所有 worker 回执其有效版本一致后，Meta 原子公布 active pointer/READY 和新的路由版本。Compute 只为与已公布部署匹配的请求工作；Coordinator随后释放屏障和源准入。
6. 任何副作用后的失败/UNKNOWN：保留原 operation、候选定义、各 shard 阶段和全局暂停。repair 核对各 shard 持久状态，以原目标向前补齐；恢复过程中重新确认各回执。某节点已装新图时不能普通 open 旧 Catalog，不能删除进度并声称已回滚。

READY 的必要条件是目标集合内每个 worker 的全部 assigned shard 已安装和确认激活该版本，包括没有业务数据的 shard。目标列表由 PREPARING 冻结，不能通过临时移除失败节点凑齐成功。READY 后个别节点短暂未收到释放屏障最多导致其暂不可用；它不能执行旧版本。释放屏障失败需持久可重试状态，不能假报所有节点已经消费。

当前 Engine::install 在单 shard 成功后直接替换本地 Catalog；M5可在服务准入屏障下复用这个行为，但仍需持久安装回执与恢复查询。不能让每个 worker 独立分配 relation ID/读取默认配置后期待图碰巧相同。

### 6.3 在途版本与配置边界

Request 受理时固定 Job/Catalog/plan/function/Schema/implementation_identity/config 版本，重试和回包只使用该绑定。已存历史结果保留原 Schema 解码。旧 attempt、旧 deployment_epoch 或取消后结果不产生新写；不能把版本不符的回包交给当前版本函数解释。

首版切换采取 drain-or-cancel，因此不要求两个 Engine 并行解释同一 shard 的不同活跃 Catalog。取消纯 C++ 运行函数不能强杀线程：Request终态持久化可拒绝晚回包，但 physical task 与制品引用在回调退出前保留；释放资源计数必须等实际退出。若不能达到有界停止条件，操作失败并维持暂停，不能宣布完成。

| 配置类型 | 首版生效规则 |
| --- | --- |
| 日志级别、指标采样等不改变语义的本机运维项 | 允许明确 allowlist 的热更新；目标/实际版本可观测，不更改已受理事件的计算语义 |
| state/plan/function cache 预算 | 通过控制命令在 round 边界生效，先淘汰再确认；不能丢 UNKNOWN 原批、冻结输入、完成保留容量或运行中任务来满足缩容 |
| 输入/Request/结果/Timer/CEP 配额、线程池并发 | 首版要求暂停并验证可达新预算；低于当前不可释放占用则拒绝或保留暂停，不能静默丢状态 |
| Schema、图、函数版本、窗口/TTL/触发阈值、watermark/late 策略、输入解析器 | 作为版本化语义变更走 DDL屏障；不能任意热写一个配置字段改变旧请求解释 |
| shard_count/hash/PK/partition/key codec、KV数据目录、owner身份 | 首版普通 update_config 明确拒绝；需要新部署或另行实现并验证受控迁移 |
| broker/group/source身份、assignment、恢复位置 | 仅在所属 Job STOPPED、无未确认 token 时变更/重开；持久源身份不能为了重试随意换新 |

添加 nullable 字段也不能只凭“可兼容”跳过安装和当前 codec 验证；是否支持该演化以实际 decoder/backfill 证据为准。不支持的 Schema 演化明确拒绝，CREATE 新版本/新关系不等于已有 ALTER 能力。

Compute 总预算覆盖所有 shard/Job 的状态、计划、函数结果缓存，并单列不可淘汰的 pending commit、输入、Request 与 completion 保留容量。Meta 的节点分配之和不能超过 node 上报的本机硬上限。状态缓存绑定 owner/assignment epoch 和已确认 revision；UNKNOWN 即使缓存全命中也停止推进。图版本激活淘汰不兼容计划/状态解释，函数缓存 key 保留精确版本与输入身份。

### 6.4 可实装切片与受控兼容 rebind

当前EventProcessor持久state fingerprint与RequestRuntime index都绑定完整Catalog；RequestRuntime还校验plan配置/版本，旧终态record绑定原Catalog。现有请求 owner lease 要求 RequestRuntime 存活期间禁止 Engine::install 激活新 Catalog。M5必须先drain/cancel、释放物理任务并销毁该RequestRuntime，再install；不能装新Catalog后直接拿旧配置/namespace重开而忽略fingerprint不符。

当前静态核心允许启动前 DDL 构建图，激活后提供部署级 pause/resume/checkpoint，尚无预声明多 Job 的 submit/stop/unregister。后续可先增加不改全局 Catalog 的 Job 生命周期，再实现下面的动态追加路径；前一阶段不能替代后一阶段的验收。

最终M5需增加最小append-only兼容路径：全局drain后，只追加新的独立Job/对象，旧Relation ID、定义、Schema、依赖、state布局与旧Job recipe必须是新图的完整不变前缀；禁止修改旧对象语义或偷偷删除已注销对象。Meta发布新的graph_generation与显式MigrationRecord，记录旧/新fingerprint、每shard进度和namespace映射。每shard受控安装时显式rebind旧Processor状态指纹，必须完整保留其source positions、dedup、FSM/CEP、Timer、outbox和处理时间，不能以创建新空namespace替代迁移。

RequestRuntime采用最小路径：旧active index必须已空，旧runtime namespace和终态archive保留在原graph_generation；新generation为请求index分配新namespace与新fingerprint，不尝试用新Catalog自动解码旧record。历史查询通过Meta的archive映射选择精确旧Catalog/plan/Schema decoder；未实现对应历史reader时明确拒绝该跨generation查询，不返回按新布局解释的错误结果。request identity/capture domain应带稳定Job及部署generation作用域，next_sequence/latest排序不能重置后覆盖此前结果。

stop/resume本身保留原graph_generation及runtime namespace，不是迁移；job deployment_epoch只fence外部准入，不能直接改动仍绑定旧owner_epoch的RequestRuntime配置。实际迁移必须同时验证运行时身份、晚回包fencing及新namespace引用。MigrationRecord与新的Engine图/状态在各shard安装提交域中原子落盘，Meta只在所有目标成功后READY；部分成功同样pause+repair，不能放开混合指纹。

任意ALTER、旧Job阈值/窗口/Schema/函数语义变更以及非空active Request跨Catalog并行，首版可明确拒绝；其完整迁移协议不在本最小append-only路径承诺内。append-only rebind及历史版本读取是M5后续增量任务，仍需独立实现/测试，不能以本段设计代替完成。

## 7. Unbounded Job 生命周期

```text
CREATING -> RUNNING -> STOPPING -> STOPPED -> RESUMING -> RUNNING
                |          |         |
                |          |         +-> UNREGISTERING -> UNREGISTERED
                +----------+-------------------------> REPAIR_REQUIRED
```

JobVersion 是长期任务定义；JobDeployment 是本次部署状态；一次触发生成的 Request 有独立有限终态。RUNNING 且无数据时可以等待、推进 Timer，但不表示队列/缓存/状态无限。

- `stop`：先持久 STOPPING 并关闭该 Job 准入/新触发，pause 全部该 Job source partitions；让已交付输入完成，确认 UNKNOWN，继续必要完成/取消通道；冻结 Timer。记录每 shard 的最后确认切面和全局源前缀，drain 或按调用中固定策略取消 Request，全部目标回执后公布 STOPPED。超时保留 STOPPING/REPAIR_REQUIRED，不能自动越过未确认输入。
- STOPPED：不消费新输入、不触发、不推进该 Job Timer或物化关系；其它 Job 和 Compute 进程可继续运行。保留持久 Timer/deadline、算子状态、结果、source positions。暂停时间仍计入 processing-time 期限；resume 时先执行到期规则，周期合并遵循既有 skipped_slots 契约。
- `resume`：只接受可恢复 STOPPED，验证原图/配置/源可重放位置与节点；重建该 Job 在唯一 ShardTask 内的运行时，seek 保存的 next_offset，恢复时间与到期先于新数据规则；全部目标准备成功后提高 deployment_epoch、持久 RUNNING，再开放准入。不会把整个共享 KV 回滚到单 Job 的停止时刻。
- `unregister`：RUNNING 时先完成同一 stop 流程；随后移除该 Job 的准入、订阅、Timer调度、Executor/运行时和缓存引用，等待物理任务资源按契约释放。全部节点确认后持久 UNREGISTERED tombstone，提升 job deployment_epoch 和 route_version；默认不删除物化数据、结果或恢复所需版本元数据，不退出整个 Compute。
- 对已 STOPPED 再 stop、已 UNREGISTERED 再 unregister 返回幂等最终状态。对旧 deployment_epoch 发来的写明确拒绝。物理 shard assignment_epoch 不因一个 Job 注销而改变，否则会误伤同 shard 的其它 Job；它仅表达物理 owner 更换。job deployment_epoch 承担该 Job 的 fencing。
- UNREGISTERED 不等于 STOPPED；resume 明确拒绝。后续重新部署保留数据必须显式声明可复用的原 Job/图/源边界与新 deployment_epoch并通过校验，不能隐式把 tombstone 当新空任务。普通新建使用新的 job_id 与对象 ID。
- 显式 `purge_data` 不属于 stop/unregister 的隐含动作。若提供此 API，需验证部署已注销、无活动依赖/历史策略/checkpoint/inflight 版本引用，按目标对象执行可恢复删除；未实现时清晰拒绝，不能递归删除共享定义。

## 8. Source 确认、全局交付前缀与 watermark

Coordinator 每个持久 `(source_id, topic, partition)` 维护已交付 ledger；entry 包含 offset、规范 payload 身份、路由 shard 和完成状态。先有 ledger 后发往 Compute；已完成必须来自目标 shard 的持久提交确认，而非 RPC 到达、Task排队或函数开始执行。单源分区按投递顺序发往各 shard，对每个目标保序；跨源同 key 顺序仍是唯一 owner 的接纳顺序。

`global_delivered_prefix` 表示该源分区**已实际交付序列中从头连续全部完成的前缀**，携带 `next_offset` 与明确的起始/seek 边界。它不是所有 shard 最大 next_offset，也不是 Kafka 最新 group commit，更不是要求 offset 数值逐一无洞；broker合法跳号仍按已交付记录次序跟踪。例：offset 10→shard A 未完成、offset 12→shard B 已完成，不能 ack 12 越过 10；10完成后可确认至13。若尚未交付一条记录，沿用确定的起始/恢复 next_offset，不能用 -1 当有效恢复位置。

现有 EventProcessor::ProcessedPosition 只表示单 shard 已接纳最大 offset+1。Meta可保存它作审计/对账，却必须另外实现 ledger/prefix；把各 shard 值取 max/min 都不足以重建真实全局交付边界。

完成后的 ledger entry 依次 ack 原 DeliveryToken。ack UNKNOWN 只重试原 token，未确定前不 seek/下一 ack；计算成功但 broker ack 失败时不撤销已提交计算。DeliveryToken 是当前适配器实例能力，重启后不复用旧 instance_id/delivery_id；seek 后重新 poll 生成 token，durable source identity 保持不变并由 Compute去重。适配器预取但尚未向 Coordinator交付的记录不计入 checkpoint 完成前缀，恢复从保存位置重新读取。

watermark 按 Job 的源分区集合维护，定义随 Source/Config 版本保存。每条输入用到达前 watermark 判 late；未知活跃分区阻塞，idle排除，全idle不凭空推进，重新活跃不回退。源 idle变更也经控制队列与输入接纳顺序协调。checkpoint先排空已交付记录，再保存 WatermarkTracker编解码状态，因此 snapshot 中 watermark不超前于未提交输入。尚未接入具体事件时间算子的策略不能只靠 WatermarkTracker存在宣称完成。

## 9. 一致 checkpoint、受控重启与 restore

### 9.1 全局停顿算法

```text
PREPARING -> PAUSING -> DRAINING -> FROZEN -> SNAPSHOTTING -> PUBLISHED -> RELEASED
                 Any uncertain step -> REPAIR_REQUIRED / remain paused
```

1. 控制队列冻结操作 ID、checkpoint ID与目标集合，禁止 DDL/config/source/lifecycle/route 变更；暂停所有 source 和 SDK 输入。处于 DDL 部分安装等非正常图状态时先 repair，不能直接把它作为运行 checkpoint。
2. 排空所有已经交付的输入，确认全部业务/Meta UNKNOWN 和 broker ack UNKNOWN。首版不把存在 gap 的 ledger 或未交付 mailbox装成可恢复切面；排不空则保持暂停并失败。每个源的 processed next_offset来自 global_delivered_prefix。
3. 停止新增触发，drain已受理 Request，或按提前指定策略逐个取消并提交终态；处理已受理完成。冻结 processing-time/Timer/TTL/CEP推进，停止后台写。首版 checkpoint 使用无活动物理任务/未确认completion的边界；若以后要保留在途 Request，必须把冻结输入、attempt/outbox与重派发语义纳入实际恢复测试。
4. 每业务shard在唯一owner队列产生FROZEN回执：barrier_id、catalog/config/job版本、owner/epoch、revision、last_commit_id、processing_time、source positions、Timer/FSM/CEP/Request/outbox状态摘要。回执之后禁止业务数据写入，直到同屏障release；暂停外部输入本身不足以冻结Timer或函数完成。control shard继续保存当前checkpoint operation阶段，其他控制变更保持排队/拒绝。
5. Meta冻结精确DeploymentImage和控制状态（含source prefixes/watermark/Job状态），在control shard保存PREPARED image后调用唯一KVService::create_snapshot(snapshot_id)。存储返回的N个业务shard revision/receipt/owner必须与FROZEN回执一致；control shard单独记录snapshot内revision/receipt，不要求等于之后发布manifest时的control revision。snapshot期间串行Meta执行器不并发发起其它control提交，快照返回后才推进本operation。
6. Meta以一个原子 MetaBatch发布不可变完整 CheckpointManifest和操作PUBLISHED；snapshot丢响应时查snapshot_status或重试同ID。Meta提交UNKNOWN时继续保持冻结并确认原批，不能释放屏障后凭目录存在推断可恢复。
7. manifest确定持久后才能报告 checkpoint完成并release原屏障；release分发可幂等重试，持久记录未释放节点。一个成功的KV snapshot但没有PUBLISHED manifest只是孤立存储快照，不可作为一致checkpoint。

### 9.2 manifest 的最小内容

| 类别 | 冻结内容 |
| --- | --- |
| 格式/身份 | format_version、cluster_id、checkpoint_id、barrier_id、restore_generation、KV snapshot_id与存储格式 |
| Catalog/config | 精确规范 Catalog/Schema/Trigger/CEP/Request定义、JobVersion/配置/源版本、函数签名/implementation_identity、必要制品引用；包括可独立恢复的控制image或不可变内容引用集合 |
| 分片 | logical_shard_count、hash/codec版本、每 shard owner/assignment_epoch、revision、last_commit_id、processing_time_us和运行状态摘要 |
| 输入 | 每 source partition 的global_delivered_prefix/next_offset、起始/seek边界、确认broker位置作审计、无未决ledger/ack的证明 |
| 时间 | WatermarkTracker序列化、max_observed/idle集合、已发布watermark、Timer/FSM/CEP状态所属KV切面及停止期间时间语义 |
| 请求 | 冻结输入、request/attempt/终态/outbox所属切面；首版 drain/cancel策略与零活动任务回执；旧版本/结果保留引用 |
| 控制 | Job停止/运行意图、active DeploymentImage、ID分配高水位、节点目标集合、引用与保留策略；不含当前进程的可复用DeliveryToken |

Meta 与业务状态位于同一个 KVService，但不假设跨 shard 运行时事务：先冻结业务和本次控制意图，再创建不可变 snapshot，最后发布完成状态。snapshot 不必包含之后才写入的完成标志，但必须保留可验证的本次冻结控制 image。静态核心在 restore 时核对快照内的意图、实际 control revision 与 worker 切面；不能仅凭 snapshot 目录存在或旧 live DB 中的标志恢复。后续多版本产品可以导出包含所需不可变引用的恢复 bundle，但必须保持同一冻结切面的校验。

### 9.3 受控 restart 与 checkpoint restore 的区别

正常受控 restart 在原活动KV继续，要求clean边界、原owner配置和完整Meta记录；从保存的source位置恢复，不丢弃checkpoint之后已经提交的业务数据。普通resume不处理dirty-run。

显式restore是操作员选择返回一个已发布checkpoint，允许放弃该切面之后的活动数据。首版先停止全部Compute和Coordinator写入，保留原目录，使用RocksStateStore::from_snapshot创建新的活动目录；绝不把snapshot目录直接写开，也不覆盖旧活动库。校验manifest与KV snapshot中的所有owner/revision/catalog，按冻结控制image恢复生效对象，再加载运行时并从保存位置seek。

完整多 Job/版本产品还必须保证 restore_generation 和 ID allocator 高水位不回退到可能重用旧身份的值。若使用恢复 bundle，需带上这些高水位、已选 manifest 及必要 operation/tombstone，并在开放新部署前提交新的恢复代际；缺失或不匹配时拒绝。当前静态核心没有这套动态 ID/Job 生命周期，只恢复经验证的冻结意图。checkpoint 之前的业务 source 身份仍用于去重；静态数据 owner token 只在确认全部旧进程退出后复用，不代表租约 fencing 或脏进程自动接管。原活动目录始终保留供核对。

Kafka恢复必须显式seek manifest next_offset，即使consumer group commit更靠后也不能跳过checkpoint后输入；seek不回退broker已确认值。若指定offset已不在保留日志中，恢复明确失败。MemMock跨进程重启测试必须由固定、持久外部测试事件日志重建adapter；进程内内存snapshot不等于跨KV进程持久化恢复。

确定性对照测试固定输入顺序和虚拟时钟，checkpoint后重放保持逻辑输出一致。线上只保证既有接纳顺序与持久状态恢复，不承诺仅靠原始Kafka记录精确复现跨源merge order、处理时间、异步完成时序或外部副作用。没有新增相应记录/协议前，不能声称这些外部结果端到端exactly-once。RocksDB内部WAL继续保留。

## 10. 验收用例与完成门槛

适配器共享契约与真实进程集成分别记录，开发/测试使用 Clang 和 ASan/UBSan/LSan，Release 单独验证。下表保留完整产品验收，其中静态核心已经覆盖部分路径；每条的全部边界仍需对应证据，不能把整表当作已通过或全部未实施。

| ID | 可观察轨迹与断言 |
| --- | --- |
| MC01 | MetaStore内存/持久适配运行相同操作幂等、内容冲突、revision、UNKNOWN契约；持久适配实际重启恢复Schema/config/operation记录；CLI默认值不能覆盖元数据 |
| MC02 | 1 Coordinator + 1 KV + 至少2 Compute进程；SDK仅配置Meta endpoint发现两节点；不同业务key确实发给不同PID，完成DDL→输入→级联MV→Trigger→请求函数→查询 |
| MC03 | 一个worker安装失败、另一个成功：新对象不READY、普通查询/输入被屏障挡住；Coordinator重启后仍保留repair阶段，同operation修复不重复backfill，全部worker成功才READY |
| MC04 | prepare/install/activate/publish各阶段丢响应；同operation_id原内容重试收敛，改内容冲突；发布UNKNOWN不开放准入；空shard也在目标确认集合 |
| MC05 | SDK缓存旧route_version/deployment_epoch；刷新保留原身份不双算；unregister与restore后旧部署请求拒绝，不能自动变成新Job输入 |
| MC06 | 两Job相同本地关系名/业务key/source offset：对象、dedup、Timer、Request互不污染；同物理shard只有一个Engine/owner，跨Job可变依赖和重复可写归属明确拒绝 |
| MC07 | stop时输入/Timer/函数完成并发、KV UNKNOWN：保留完成通道，确认后STOPPED；停止后不计算，另一Job仍运行；重复stop/unregister幂等；默认保留结果，实际Executor/订阅/Timer/cache资源释放 |
| MC08 | resume先执行停机期间到期Timer，再接纳新输入；恢复原source前缀；UNREGISTERED拒绝resume，旧epoch失效；保留引用的Schema/函数/数据不允许误删 |
| MC09 | 接纳Request v1后申请v2：v1按原输入/Schema/实现完成或显式取消；晚v1/旧attempt不改v2 latest；引用在任务真正退出前保留 |
| MC10 | 配置新默认值、非法字段、Schema不兼容、配额缩至不可释放占用以下：明确拒绝或保持暂停；节点所有cache预算之和受限，UNKNOWN cache命中不能推进 |
| MC11 | 同源10投A未完成、12投B已完成：不能越过10确认；10完成后前缀到13。覆盖合法offset空洞、adapter预取、ack UNKNOWN、重启token变化及原payload冲突 |
| MC12 | watermark未知分区、idle/全idle、重新活跃、late边界与恢复无回退；未提交输入不进入可发布checkpoint水位 |
| MC13 | checkpoint同时有跨shard输入、到期Timer和函数回包：切面一致；FROZEN后注入写被拒；KV snapshot回执与manifest逐shard相等；缺manifest快照不能恢复 |
| MC14 | 全三类进程受控停止/重启与显式checkpoint restore；Rocks新目录恢复，保存原活动目录；broker group位置领先仍从manifest seek；日志缺失/dirty-run/图不符拒绝普通resume |
| MC15 | 1/2/4 Compute独立部署相同固定日志/虚拟时钟，比较逻辑状态/请求结果/触发原因，不比较PID、commit_id或物理完成顺序 |
| MC16 | 真实KafkaMessageSource保留独立协议contract；主功能后脚本启动固定apache/kafka:4.3.1单节点KRaft完成真实broker测试，证据与MemMock/协议mock分别陈述 |
| MC17 | KV物理N+1与Engine逻辑N核对，业务hash永不到control shard、Compute owner写control被拒；checkpoint期间control阶段可推进而业务FROZEN不变；restore 验证 snapshot 内的冻结控制意图，之后发布的完成标志不要求已经包含在 snapshot 内 |
| MC18 | 运行旧Job后追加独立新Job：全局drain/lease释放、显式rebind后原source/FSM/Timer/dedup完整保留；旧archive按原generation读取，新index namespace不覆盖旧结果；任意旧对象改变拒绝，部分迁移pause+repair。仅启动前DDL切片不能代替本验收 |
| MC19 | 通过 Meta 注册/读取 Schema、配置和内部函数定义；登记成功但未激活时不改变 Compute；部署引用缺失或内嵌 Schema/实现身份不符拒绝安装；active 读取返回同一 image，切换中的旧 query 拒绝后重新 bind；历史结果只按原 Schema 解码，未实现的扩展 family 明确拒绝激活 |

## 11. 当前状态、证据与后续实现

静态控制面及真实集群链路已实现，最终全局审查已并行完成一轮。三项发现已修复：新投递确认前 Clock 不越过其固定 dispatch time；不完整 checkpoint 意图冻结期间禁止其它控制修改；Compute 初始化前校验全局 shard_count。当前最终源码门禁由[核心交付](core-delivery.md#验证证据与剩余门槛)统一维护，不再记录已经失效的“CLI 只有 KV”或待合并任务表。

[Catalog codec 审查](reviews/M5-catalog-codec-review.md)保留 17 项限定证据：typed Schema/BoundExpr/SQL/Function/Candidate/RequestStatus/Latest、NULL/-0/Vector/Timestamp、未知/篡改字段和精确预算。完整 codec 限制 bytes、relations、fields、expression nodes/depth 及累计 provenance；隐藏字段/aggregate 数组上限统一到 max_fields_per_relation，默认 4096。该组件证据不等于动态元数据/Job API 已实现。

IStateStore::limits 与 `/v1/kv/limits` 提供真实能力，Cached 透明转发，Compute/Meta 已接入有效配置报告。后端限额与 canonical/wire 约束分别计算，缓存不能解除 UNKNOWN。

后续工作保留：

1. 实现独立版本化 Schema/Config/Source/Extension/Job 记录、统一 ID allocator、引用管理与操作 journal；现有整体 Meta 记录不能直接推导为完整多版本模型。
2. 按 §6.4 增加单 owner 下的多 Job 生命周期和 append-only rebind，完整保留旧进度、状态、历史解码及晚回包 fencing。std::function 不经 JSON 序列化，部署依靠明确 recipe/实现身份匹配。
3. 静态 Store owner 尚无动态授权更新；变更物理 owner 需要先实现并验证受控更新协议，不能仅提升 Meta epoch 宣称旧节点已被 KV 隔离。
4. 多 Job 首先隔离可写子图；若开放共享可变关系，须定义停止/依赖方、backfill、源 fan-out 确认和共享 Timer 生命周期，并保留数据清理依赖保护。
5. 当前 checkpoint 使用排空后静止切面。保留活动物理任务跨 checkpoint、clean-run 自动普通重启或多代恢复 bundle 都需要单独实际协议和恢复测试，不能从 Request 组件存在推断支持。
6. UDF/UDTF/UDAF 实际执行/ABI/分发、Kafka Compose 真实 broker、1/2/4 Compute 等价与深度性能按产品待办继续；Process Function 不在当前完成条件。

每个后续 milestone 一轮独立审查并记录合理发现的修复/验证。现有多个全局报告保留在[审查目录](reviews/)，不因为这次文档整理再开新轮，也不把文档或测试通过替代目标覆盖审查。
