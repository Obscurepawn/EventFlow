# 实现契约与验收轨迹

Status: active
Owner: root
Updated: 2026-09-13

本规格冻结当前提交目标的接口与行为，细化产品架构；不引入 MVCC、HA 或跨 key 计算。合理的实现调整必须记录原因和测试。

## C1. 输入身份、顺序与确认

Envelope 包含 source_id、source_partition、offset（非负单调序号）、relation、operation、row、processing_time_us。delivery identity 是前三者的组合，独立于关系 PK、业务版本和 commit_id。文件适配器以固定文件身份与原始行号产生该身份，不以每次进程启动的新 UUID 作为 source_id；无 news_id 的原题输入以来源行身份生成新闻业务 ID，不猜测两条正文是否修订。

Router 在每个源分区维护已提交的连续前缀；乱序完成允许记录但不能跨过空洞确认。每个 shard 在同一个状态批次记录其已消费 delivery identity/源进度；重投在任何 MV、窗口或 FSM 推进前识别。同身份不同 payload 拒绝；同业务 PK 的新版本是业务修改，正常执行 old/new。

source partition 内按 offset 发往各 shard；对每个目的 shard 保序，一个 key 的跨源次序为 owner 已接纳次序。首版离线 replay 顺序控制所有输入及虚拟 Timer，线上只保证实际接纳顺序，不声称源 event-time 总序。

## C2. 时间、Timer 与过期

每次 dispatch 只读取一次 clock，得到单调不回退的 processing_time_us；本轮所有算子共享它。线上时间域为持久化 Unix 微秒进度，进程内使用 monotonic elapsed 推动并以恢复时墙钟锚定，输出审计时另记实际 wall time。虚拟时钟可显式 advance，拒绝回退。

推进到 t 时，先处理所有 deadline <= t 的到期 Timer/窗口删除，再处理该时刻数据；延迟处理的回调与新相对Timer均使用当前处理时间t，原deadline只作为scheduled_time_us审计值；进入新状态的相对等待从t开始，不回填到已错过的过去时刻；CEP 有效区间为 [armed_at, deadline)，恰好 deadline 到达的新闻不再匹配。排队数据使用 dispatch 时间，截止前入队但截止后执行会过期，不回溯入队时间。处理时间窗口为 (t-W,t]。

Timer 身份为 rule/key/candidate_version/scheduled_time/generation。取消与重置使旧 generation 失效。重启从持久化 deadline 恢复，停机时间计入期限；启动后即处理到期任务，无需等待新业务输入。周期默认合并到最新应到时隙并记录 skipped_slots；补发模式受明确容量限制。

## C3. 资源与错误

输入 mailbox、PendingTrigger、已受理 Request、inflight function task 分别有数量/字节上限。等待额度不能阻塞当前 dispatch；额度不足保存有界 PendingTrigger 或明确拒绝新的触发受理，并记录状态。本轮数据关系仍按其已接受输入语义提交，不静默丢原始数据。

外部输入准入背压与内部完成/取消/超时分开；后者有预留通道，必须能释放额度。PendingTrigger 满时停止新增外部输入，但继续处理完成与必要 Timer。已受理 Request 不合并、更换输入或静默消失。

默认上限由部署配置提供，并验证单事件、单结果、单批 mutations/bytes 以及每 key 候选/窗口预算。纯 C++ 同步函数不支持安全强制抢占，必须满足有界执行契约；不可信或昂贵函数通过进程隔离。

Schema/分区错误在准入阶段明确拒绝。已开始 dispatch 的永久计算异常 fail-stop 当前 shard：不确认输入、不公开 staging，暴露 FAILED 状态与错误；其他 shard 可继续。KV UNKNOWN 暂停该 shard，保持同一个 commit 和完整 mutations，先确认结果，不丢弃并重算。

## C4. KV 与外部接口

IStateStore 是唯一状态接口；MemoryStateStore、RocksStateStore、RemoteStateStore 实现同一 contract。KV service 支持 --backend memory|rocks，并使用相同 HTTP RPC。Memory adapter 具备原子批、revision、owner 检查和提交去重，不是只用 unordered_map 的弱语义替身。

API:
- get_many(shard, keys) -> revision + optional byte values
- scan(shard, prefix, after, limit) -> revision + ordered key/value page
- commit(CommitBatch) -> COMMITTED|REJECTED|UNKNOWN + revision + duplicate/reason
- status(shard) -> current revision + last receipt
- limits() -> 实际open配置的max_batch_bytes/max_mutations/max_read_keys/max_read_bytes/max_scan_rows；默认未报告返回FailedPrecondition，不推断默认值。max_batch_bytes包含完整canonical header、身份和逐mutation长度前缀。Remote原16MiB wire限制独立存在，使用者还需匹配传输预算。

CommitBatch: shard、owner token、expected_revision、commit_id、mutations。commit_id 固定为 make_commit_id(owner_token, shard, expected_revision) 的规范编码，服务端验证此绑定；owner token 包含受控部署的 assignment epoch。更换内容不能复用同一已提交身份，也不能给历史身份附加新的 expected_revision 绕过检查。服务端从完整规范内容计算/比对指纹，不盲信客户端给定 hash；重复 id 不同内容拒绝。检查顺序：owner authorization、最近 receipt identity/content、expected_revision、mutation keys/bounds；同 id 正常重试即使 expected_revision 已落后仍返回原 receipt。

一批所有数据变更、new_revision 与 receipt 必须原子写入。每 shard 同时仅一个未决提交；receipt 可以仅保存最后一份。更旧重复返回 stale/conflict且不得修改；client 必须在完成确认前不推进下一 commit。线上持久后端采用 WAL + sync=true 作为成功 ACK 基线。

逻辑 data key 固定以 shard 分区；元数据 key 由 store 私有 namespace 保存，客户端不能覆盖 revision/receipt。单次 get_many/scan 与同 shard commit 互斥，无 MVCC；跨页一致捕获只能由暂停该 shard 写入的 owner 完成。

IClock 提供虚拟与线上实现；IFunctionExecutor 提供 deterministic/simple 与远程实现；ISourceAdapter/ResultSink 边界保存身份与确认规则。内存和真实组件都运行公开契约测试。

## C5. 关系、Schema 与有限 SQL

类型：Bool、Int64、Float64、String、TimestampMicros、FloatVector；nullable 有独立 NULL。内部 Row 为按 Schema 排列的 Value vector；入口 JSON object 转换后拒绝未知字段、类型错配和缺少必填字段。整数溢出与非有限浮点数拒绝；整数到 double 只按显式规则允许。PK/partition 只允许非空 Bool/Int64/String/Timestamp，PK 包含 partition 列。

持久化关系保存完整当前行；Upsert 为 delete-old + insert-new 的单次逻辑变更，Delete 和 Table TTL 产生撤回。删除后的 MV 必须等价于当前基表重算。全生命周期累计只能来自明确 append-only Source；有限 Table 的后台清理不能保留已经不属于关系的数据贡献。

SQL 必须支持 CREATE TABLE（字段、NULL、PK，分区配置可用 WITH 或 SDK）、CREATE MATERIALIZED VIEW AS SELECT、SELECT expressions FROM relation WHERE predicates、别名、AND/OR/NOT、比较/算术、GROUP BY 分区键及 SUM/COUNT/AVG；COUNT(*)、COUNT(column)、NULL 的 SQL 三值语义显式实现。拒绝不支持的语法，不能成功解析后静默忽略。

受限 Join 固定为按相同业务分区键的 inner one-to-one lookup join，左右输入在该 key 上最多一行，任一侧的 insert/update/delete 都传播；缺侧不输出。其他 Join/聚合跨 key/递归/任意子查询明确拒绝。Latest 和有界处理时间窗口可先用 SDK 声明，仍进入相同 Catalog/DAG，并可由 SQL 查询/继续派生。

CREATE MV 启动前可创建；有现存数据时使用暂停后的有限全量初始化，初始化完成前 CREATING，不可查询/触发。无法获取所需历史则明确拒绝；不做在线 backfill。

key 编码使用 typed stable binary ordering；string 处理 NUL 和复合边界，signed int 保序。state/result/query 都使用同一 Codec。普通行 key 不携带历史 revision 或 Worker ID。

## C6. FSM 子集

通用 FSM 定义具有命名状态、事件类型匹配、声明的视图读集/guard、状态寄存器变更、emit 和 Timer 操作。状态转换是 step(state,event,reads)->changes/outputs/timers，不允许从 callback 绕过 owned state 或直接产生外部副作用。

必须提供构造器：OnEvent、LevelPredicate、Crossing、Periodic、StrictNext(event_type)、FollowedBy、Within、Once、Reset、当前 Ready 的 AND/OR。简单FSM继续支持已确定子集；按2026-09-14用户最新决定，核心CEP另外实现有界E-NFA多候选及ε转换，首个模式/skip范围见cep-enfa.md，不宣称完整Flink语法兼容。

StrictNext：跨阈值 Armed；Trade/Quote 不消费；第一个同 key News 无论情绪都消费；正面 Accepted emit一次，其他 Rejected；任意状态 reset 取消旧 Timer/寄存器并回到初始态。guard 在本轮声明 MV 稳定后运行，运输重复事件不得进入 step。

FSM emit 到 Capture 在 staging 本地传播；FSM、Timer、PendingTrigger/Request 和待派发记录同批提交。成功提交后才派发远程任务；不宣称网络与 KV 原子。

## C7. Request / node / attempt

Request: ACCEPTED -> RUNNING -> SUCCEEDED|FAILED|CANCELLED（不可逆终态）。受理创建时冻结输入及图/函数/Schema版本。PendingTrigger 不是已受理 Request，可等待 readiness/额度且有期限。retry 是同一 Request 的同一节点、同一输入的新 attempt，不是新请求。

Node: WAITING_DEPENDENCIES -> READY -> RUNNING(attempt n) -> SUCCEEDED 或 RETRYABLE -> RUNNING(n+1) 或 FAILED。只接受当前 attempt 的首个终态结果；旧 attempt、重复回包、已取消/终态请求的结果无副作用。超时提升 attempt 后即 fencing 旧 attempt，即使旧成功后来到达也忽略。最大次数/期限之后失败。

祖先永久失败使所有依赖它的未完成下游 SKIPPED/FAILED，并使 Request FAILED；取消与失败释放额度。在同一批保存节点结果/状态与下游可执行记录。正常无失败路径共享祖先仅派发一次；故障允许物理重复执行，逻辑发布仅一次。

B/C once 标记在 Request 成功受理时消耗；后续失败不隐式发新请求，重试属于原请求；D 将来仍可发新的时间请求。LatestSuccess 按候选版本及 request admission_seq，旧完成不得覆盖新值；LatestRequestState 与最近成功结果分开。

## C8. A/B/C/D 演示契约

每 ticker 最多一个当前新闻候选。不同 news_id 的新新闻替换旧候选；相同 news_id 的更高 text_version 替换旧版本。旧/重复版本不重新激活；旧版本任务可完成并留历史，但不能改当前候选/latest。原题模式不假装有真实成交量，Trade/Quote 是明确扩展。

A：每个合格新候选创建 Embedding 请求。固定 key 内文本/编码器版本共享结果；不承诺跨 key 全局只算一次。

市场 Ready：当前 Quote 有 bid/ask、bid>0、ask>=bid、观测年龄<=2s；历史至少2个有效 price样本；Embedding匹配当前候选版本。示例阈值/窗口作为app配置，不写死在engine。

B：上述输入首次同时Ready，受理一次初始预测。候选从接纳时间起TTL=300s，缺Ready最多等待2s，过期记录原因。C/D 的预测受理要求B已受理（不要求B已成功），避免首次路径被跳过。

C：在新闻接纳后的30s内，quote mid 相对新闻参考价上涨>=0.1%，且最近10s成交量>=100时确认一次。B尚未受理时可记录有界Pending C，必须在其期限内满足前置；每候选最多受理一次。

D：每5s检查当前候选；只在B已受理且所需输入Ready时受理，未Ready的时隙记skipped，不在未来补出大量旧请求。过期/修订停止旧候选的后续周期。预测使用最近120s历史，收益/风险是确定性演示函数，不声称投资有效性。

验收轨迹覆盖 A后市场Ready、C先于Embedding完成、D多次miss、修订时旧结果回流、同key新候选替换、无输入过期。所有阈值/函数/规则声明都在示例配置与SDK层。

## C9. 重启与失效范围

P1必须支持受控 quiesce：停止外部准入，完成本轮commit，处理/排空已受理任务或明确取消并持久化终态，保存Catalog fingerprint、状态/身份/Timer/progress，写clean-stop标志再退出。新owner只在旧进程已停止的受控流程接管，共享存储不需要搬状态文件。

持久化KV重启必须保持已ACK批次与回执；Compute非正常退出后不自动接管/续算。启动发现dirty-run拒绝普通resume，要求人工recovery命令执行状态/任务审核或重新回放；不能静默丢失已受理请求。此后若实现人工 crash resume，另加专门故障测试，不按现有文档推定已支持。

owner token 为部署静态assignment身份，KV始终校验；在线租约选举与自动fencing后置。更换owner要求受控停机、未决提交已确认，并更新持久assignment epoch。已ACK持久化基线为WAL+sync；MemOnly服务重启按定义不保留状态，测试必须明确使用哪个backend。

## C10. Build / packaging / tooling

第三方仅 cpp3rdlib/include、lib、licenses 与版本/校验清单；不纳入第三方 .cpp、源码包或在线构建下载。头文件模板/inline允许。默认 CMake + Unix Makefiles，根Makefile仅包装cmake/make；ANTLR自有grammar的生成C++代码随项目提供，常规build不依赖Java。

动态依赖闭包尽量随包，ELF相对RPATH指向包内lib；glibc/C++ ABI与架构必须说明，并用匹配镜像减少reviewer环境要求。没有容器运行证据时不能宣称镜像已测试。

本地 Bear 捕获真实构建产生 compile_commands.json（clangd标准名，不是compiler_commands.json）；make compdb执行一次实际编译，并用clangd --check验证代表性源码与依赖解析。CMake自身也导出build/compile_commands.json作为不安装Bear时的辅助，但本机交付证据必须含Bear实际输出。


## GoogleTest 构建约束

单元测试使用 gtest，头文件及预编译 gtest/gtest_main 库集中 cpp3rdlib。仅 BUILD_TESTING=ON 的测试target链接；核心库和主程序不能传递依赖gtest。

## Status / noexcept 编程规范

项目自有生产函数统一noexcept；可失败操作返回Status/StatusOr<T>，初始化改为open/create工厂，正常错误不使用throw/catch。第三方正常返回错误转Status，不加库异常包装；输入解析优先非抛API并先验证类型/范围/UTF-8。没有将std分配或第三方未报告异常变成可恢复业务错误的承诺。Status自身使用固定消息缓冲，报告错误不分配内存。GoogleTest生成的测试体及第三方/ANTLR生成签名不强行修改。M1已通过的旧异常接口测试需要按新契约重新执行，不能沿用旧绿灯。

## M1 已固定的接口细节

StatusOr外层表示调用参数/读取错误；CommitResult内层保留Committed/Rejected/Unknown，不能把UNKNOWN当成可随意重算的失败。读取预算max_read_bytes累计原始key/value，远程协议保留相同ErrorCode。真实RocksDB写返回不确定错误后实例fail-closed，需关闭重开再确认；不依据memtable读取推断未提交。KvServer::start拥有并启动监听线程，就绪才返回；stop关闭并join，避免start/stop分散造成竞态。
