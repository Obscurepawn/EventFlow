# EventFlow 产品范围与工程约束

Status: deferred（当前核心交付完成；完整产品后续范围保留）
Owner: root
Updated: 2026-09-14

## 目标与范围

实现一个以物化视图表达特征计算依赖、以有状态 FSM/有界 E-NFA 控制触发的 C++20 数据库。原始题面保留在根目录私有 PDF；产品解释见[架构文档](../docs/feature_computation_architecture.md)。实现必须包含真实共享 KV、多个 Compute 进程和公开 SDK/DDL 入口，不能以纯内存示例或成功占位接口代替。

用户在 2026-09-14 将当前工作收敛为[五小时核心交付](core-delivery.md)，窗口为 04:08:34-09:08:34 UTC。先完成核心，再运行条件、Clock、checkpoint 恢复三个端到端示例；SQL 与独立特征计算另有两个本地示例。完整产品验收项保留，但超出当前核心的部分不假装完成，也不自动加入本轮完成门槛。

完整产品包括有限 SQL SELECT/DDL、nullable scalar/string/vector Schema、级联投影/过滤/SUM/COUNT/AVG/同 key lookup join、Latest、窗口/撤回、可读状态的灵活 Trigger、固定输入异步请求、共享祖先、版本缓存、持久终态和受控恢复。SQL 与 C++ 共用一个 Catalog，不维护第二份可独立编辑的业务 DAG。通用引擎不硬编码 ticker、应用字段或模型。

不包含 MVCC/历史 as-of、跨 key shuffle/全局事务、完整 PostgreSQL 或 Flink CEP 方言、自动 HA、在线无停顿迁移或真实模型训练。共享 KV 不改变同 shard 单 owner 写入顺序。

## 不变量

I1: 同一 shard 单 owner；同一逻辑 key 全部状态转移有序；Executor 是对象，不是进程。
I2: 源轮staging覆盖base/MV/FSM/Timer/Trigger与源进度并原子提交；后续请求受理读取已确认当前R，R+1同批写入固定输入/Manifest、occurrence消费和once。claim/completion各自原子发布对应请求/结果/Latest；不把整个dispatch或source+capture强称一个commit。UNKNOWN保留原批与身份，确认后才公开/派发。
I3: transport delivery_id 与业务 PK/version、commit_id、occurrence_id、request_id、attempt_id 独立；运输重复先于 MV/FSM 过滤。
I4: 已受理 Request 的输入不可变；旧 attempt 或旧候选不能覆盖当前终态/latest；故障可重复执行，逻辑结果仅接受一次。
I5: Request 容量不足不能在 dispatch 内阻塞等待；完成/超时/取消有独立保留容量，源输入可背压。
I6: 每轮固定 processing time；到期先于该时刻数据执行；窗口 (now-W, now]，截止点不再接受新匹配。虚拟与在线时钟采用同一转换规则。
I7: 分片关系 PK 包含 partition key，不能普通更新迁移 key；Table TTL 是逻辑删除并传播撤回。
I8: 基表/MV/内部状态按稳定 key 编码；不含 Worker ID 或历史 revision 后缀；同 shard 完成捕获，无 MVCC。
I9: 函数/Schema/计划版本固定；未知语法/类型/不支持的分布属性明确拒绝。
I10: MemOnly 及真实 adapters 共用公开契约；集成测试不能全部以 mocks 替代实际进程与协议。

## 已确认的工程要求

| 方面 | 约束 |
| --- | --- |
| 依赖与构建 | 第三方仅发布头文件、预编译静态/动态库及许可证/清单，集中 `cpp3rdlib/`；不引入第三方实现源码或构建下载。项目只使用 CMake/Makefile，普通构建不需要 Java/ANTLR 生成器。当前预编译包面向 Ubuntu 24.04 / Linux x86_64，基础 glibc/C++ ABI 要求必须写明。 |
| 外部设施 | storage/source/time/function/transport 通过接口隔离，提供 MemOnly/MemMock/Simple 实现；适用时与真实 adapter 运行相同契约。内存和协议 mock 不替代真实进程证据。 |
| 测试 | GoogleTest 头文件与库来自 `cpp3rdlib/`，仅 `BUILD_TESTING=ON` 的测试目标链接。默认端到端使用确定性输入，不强制启动 Kafka。 |
| 工具链 | Clang C++20；开发/测试带 ASan/UBSan/LSan 且诊断失败即停。Release 使用 `-O3`、NDEBUG、可用 LTO，不使用 sanitizer 或 fast-math。历史 GCC smoke 不是最终质量门禁。 |
| 错误与命名 | 自有生产函数 `noexcept`，正常失败返回 Status/StatusOr，初始化使用 open/create 工厂；不用 throw/catch 处理业务错误，不专门包装第三方异常。第三方返回错误转 Status。函数/变量 snake_case，类型 PascalCase，必要生成签名和 override 保留。 |
| 格式与语言 | 全部自有 C++ 最终 clang-format，并有必要英文注释解释所有权、提交、时间和恢复。代码、测试、注释、日志和可执行代码块使用英文 ASCII；有意 Unicode 测试数据采用转义。设计文档与示例 README 使用中文，不修改第三方或只读参考仓库。 |
| 编辑器与示例 | Bear 捕获标准 `compile_commands.json`；clangd 实际检查代表性代码。五个示例各自有独立目录和 `run.sh`，脚本封装按需构建与运行，运行包使用随包二进制。 |
| 审查 | 每个完成 milestone 一轮独立 fresh-context review；实现者修复合理发现并定向验证，不自动反复开轮。最终多个独立全局 reviewer/challenger 并行一轮，记录发现、处置、证据与缺口。 |
| 发布 | 不自动上传或发布私有题面、参考仓库、制品；不能以文件存在声称镜像运行、性能或未执行测试通过。 |

Status 自身以固定缓冲报告错误。`noexcept` 不承诺把标准库分配失败或第三方未报告的异常转换为可恢复业务错误；GoogleTest 生成测试体及第三方/ANTLR 生成签名不强行改写。

## 部署、输入、缓存与恢复

- 三类自有进程为 Coordinator/Meta、KV Service、Compute；N 个 Compute 对应 N+2 个进程。FunctionWorker 是 Compute 内的执行能力。首版 HTTP/1.1 + JSON RPC 复用 cpp-httplib，Kafka adapter 使用 librdkafka 原生协议；传输变化不能改变提交语义。
- Meta 是 Schema、Catalog、配置、源、assignment、部署和 checkpoint 元数据的权威。完整产品还要管理 Job 与 UDF/UDTF/UDAF 的签名、版本、制品身份/位置、激活进度和引用。当前 SDK 通过 Meta 代理；直接路由缓存刷新属于后续协议。
- Compute 缓存必须有节点级预算，覆盖跨轮状态、编译计划和版本函数结果。状态缓存受 owner/assignment epoch/确认 revision 约束，确认后写穿，UNKNOWN 不能靠缓存继续推进；交接或不兼容图变化清除相应缓存。
- `IMessageSource` 提供 poll、delivery token、连续已交付前缀 ack、pause/resume/seek，业务 Schema 解析独立于 broker。MemMock 与真实 librdkafka adapter 均保留；关闭 Kafka 自动提交，只有计算持久提交后才确认。HTTP 超时不证明写入未发生，重试保留原身份与 payload。
- Watermark 和一致 checkpoint 是核心要求。KV 提供 `create_snapshot` 与恢复；Meta 先协调暂停和冻结，再绑定 Catalog、源位置、各 shard revision、Timer/FSM/请求及控制状态。恢复从 manifest 位置重放，不能使用更靠后的 broker group commit 跳过记录。
- 可靠输入日志加完整 checkpoint 协议可以承担应用重放，不再另造一份业务 redo；它不能代替跨源顺序、时钟、异步结果和外部副作用的恢复定义。现有 RocksDB WAL 保留，Memory snapshot 不提供跨 KV 进程持久性。
- 集成基线必须真实部署 1 Coordinator、1 KV、至少 2 Compute，证明不同 key 到达不同节点，并覆盖 DDL、级联 MV、条件/Clock、异步结果、SDK 查询和整体恢复。1/2/4 Compute 逻辑等价是保留的完整产品扩展验收。

## 后续产品能力

| 能力 | 保留的具体要求与当前边界 |
| --- | --- |
| UDF/UDTF/UDAF | 用户 C++ 模块独立用 CMake 编译成 `.so`，经 Meta 注册/分发并由 Compute 加载；只需重编插件，不需重编整个系统。Schema/ABI/制品内容身份固定，在途请求和历史解码保留原版本。当前尚无远程用户代码上传，内置函数不证明扩展已实现。 |
| UDTF/UDAF 语义 | UDTF 有界多行输出需稳定身份及替换/删除撤回；UDAF 需 add/retract/merge/finalize 和状态 codec。均进入同一 Catalog，分别提供真实模块与测试，不因此开放跨 key 计算。 |
| Process Function | 用户已暂缓；实现和专门调研均移出当前交付，不能用 FSM 回调换名代替。未来恢复范围时另定受控状态/Timer/输出/checkpoint 契约。 |
| 常驻 Job | unbounded Job 不等于一次 Request，也不表示资源无限。完整产品提供 job_id/version、stop/resume/unregister、路由失效和资源释放；当前只有静态单部署 deploy/pause/resume/checkpoint。 |
| Job 停止/注销 | 先停止准入、确认 UNKNOWN，再排空或明确取消在途请求并保存进度。unregister 释放 Executor/订阅/Timer/cache/路由引用，不退出整个 Compute；重复操作幂等。默认保留状态与结果，数据清理需独立显式操作及依赖/版本检查。 |
| 多 Job 与图版本 | 每物理 shard 仍只有一个 Engine；命名空间及可写关系归属隔离。后续追加独立 Job 需要受控 append-only Catalog rebind，保留旧 source/FSM/Timer/dedup，并按原 generation 读取历史；不能以新空 namespace 伪造迁移。任意已有图/Schema 演化仍需独立协议。 |
| 通用观察 host | 选择同 owner 轮主动读取声明的同 key MV，同步推进规则状态/Trigger；不建立通用 CDC、持久 witness 队列或独立逻辑观察通道。Catalog/纯程序/Engine 观察基础已有，完整 host 绑定后置；可合并 readiness 重检查不等于任意 crossing 订阅。 |
| Kafka Compose | 独立脚本使用 Apache 官方 `apache/kafka:4.3.1` 单节点 KRaft，兼任 broker/controller，无 ZooKeeper；负责就绪、建 topic、测试、日志和清理。不提交 Kafka 发行包或镜像，不把协议 mock 当真实 broker 证据。该集成按用户优先级后置。 |
| 性能与打包 | 深度 benchmark、镜像运行、在线迁移与复杂运维后置。已有运行包/脚本只按实际平台及执行证据声明可用，不造吞吐或跨平台承诺。 |

有界 E-NFA 已纳入核心并完成单 owner 语义验收，保留简单 FSM。guard 可读路径历史/typed registers 和预声明的同 key 唯一 MV；本轮读集在 MV 稳定后冻结，试探分支不能修改共享业务状态，不能任意查询 SQL 或读取其他 key。详细合同见[CEP](cep-enfa.md)和[同轮观察](change-observation.md)。

## 完整产品验收清单

本节保留原始验收ID和未完成项，不因五小时收敛而伪装勾选。当前核心完成门槛及证据见[core-delivery.md](core-delivery.md)；AC12插件、AC20复杂Job/直连路由刷新、AC22 Kafka Compose、AC14/17深度性能与打包等后续项不阻塞本轮核心，其他AC的超出核心部分也保留为完整产品要求。

- [ ] AC01: 原题六字段新闻适配、明确扩展输入、结果时间戳与请求来源。
- [ ] AC02: SQL/C++ 两入口建立同一 Catalog/算子图，非法列、NULL/聚合/跨 key 计划被拒绝。
- [ ] AC03: 类型与有序 key 编码正确；Schema 不硬编码；PK/partition 检查。
- [ ] AC04: 连续级联 MV 对插入/替换/删除/TTL 与独立重算一致；必要窗口/latest/lookup join 可用。
- [ ] AC05: 简单FSM的clock/guard/edge/strict-next/sequence/within/reset/Ready可组合；核心CEP另有有界E-NFA、多候选/ε/历史状态/skip/持久化与恢复的实际测试。
- [ ] AC06: 文本编码/首次预测/条件确认/周期预测应用契约完整运行；修订、过期、未就绪和候选替换有精确轨迹。
- [ ] AC07: 冻结输入、正常共享祖先一次、retry/attempt/终态、latest 不倒退。
- [ ] AC08: MemOnly 与 RocksDB 通过相同 KV 合约测试；真实 RemoteStateStore/服务进程通过读写/扫描/原子/重复/未知提交测试。
- [ ] AC09: 外部输入去重与源连续确认前缀正确；受控重启恢复保持明确语义。
- [ ] AC10: 容量 1 的请求队列无释放死锁；输入/结果/批字节限制和错误行为可观察。
- [ ] AC11: 实际部署1个Coordinator、1个KV Service和至少2个Compute，全部通过公开HTTP RPC接口完成DDL、输入、MV传播、FSM触发、函数计算、查询和checkpoint重启恢复；1/2/4 Compute的确定性回放逻辑结果等价。FunctionWorker是Compute内部能力。
- [ ] AC12: 独立CMake构建用户.so插件，经Coordinator注册/分发到Compute，版本/ABI/输入输出Schema校验；UDF/UDTF/UDAF三类均有实际算子执行及单元/集成证据（Process Function已移出本次交付）；MemOnly/simple设施可替换。
- [ ] AC13: 仅 CMake/Makefile 构建，cpp3rdlib 无第三方 .cpp/源码包；不需联网下载依赖即可正常编译。
- [ ] AC14: README 命令、样例启动、故障与性能脚本可运行；保存环境/结果及局限，不造吞吐数字。
- [ ] AC15: 持久化结果、受控重启、模型在途请求和 Timer 重建有测试证据。
- [ ] AC23: Watermark（分区进度/idle/late策略）和一致checkpoint/restore实现，KV snapshot flush及消息重放边界通过单元/真实集成。
- [ ] AC21: Compute有界跨轮状态/计划缓存及版本函数缓存命中失效正确，UNKNOWN/交接不读错状态。
- [ ] AC22: IMessageSource支持MemMock与Kafka，手动连续前缀ack/背压/重放通过共享契约和Kafka协议mock测试；默认端对端使用MemMock；独立脚本使用Docker Compose运行apache/kafka:4.3.1单节点KRaft集成。
- [ ] AC20: MetaService/SDK节点发现、DDL生命周期、Job提交/stop/resume/unregister、路由版本刷新及原身份重试通过真实进程集成；单实例无HA。
- [ ] AC19: 最终成果由多位新fresh-context reviewer分别审核目标实际覆盖、架构/接口语义、工程/测试证据；多位reviewer在一轮中并行工作，合理问题由实现者修复复测、记录闭环后才能交付。
- [ ] AC16: 每个 milestone 的一轮独立 fresh-context review 与修复/验证闭环留档；最终完整覆盖审计。
- [ ] AC18: Bear 实际捕获 compile_commands.json，clangd --check 代表性代码与外部头文件通过；普通构建不强依赖 Bear。
- [ ] AC17: 容器/运行包交付尽量减少环境假设，明确实际验证的运行方式；发布不自动上传外部平台。

## 当前状态与下一步

核心 SQL、事件生命周期、有界 CEP、请求/Latest、共享 KV、静态控制面和多 Compute 已实现。具名初始化调整后的最终 Clang Debug/Bear、Release、45/45 CTest（414.61s）及六个 Release 示例通过；新闻示例保留原题六字段并展示嵌入式自定义 Trigger/API，远程集群仍使用已编译配置。

最终多个独立 reviewer 已并行完成一轮，合理问题已修复并纳入最终回归。源码清单、二进制身份、详细日志和范围映射统一见[最终验证](verification/final-validation.md)；运行包已在独立解压目录通过六个示例验证，当前核心状态已登记完成。以上不表示本文件完整产品 AC 的所有扩展都已完成，原验收编号及未完成项保持不变。
