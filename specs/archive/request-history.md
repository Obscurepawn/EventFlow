# 请求与函数视图：决策和验证历史

本归档合并原函数视图提案及 Catalog、Engine bridge、Executor、RequestMachine、
RequestRuntime 五份实现交接，保留关键决定、反例和证据入口。它不是新审查或当前完成声明。
现行语义见[运行时契约](../runtime-contracts.md)、[请求规格](../request-functions.md)；
最终组合证据见[验证报告](../verification/final-validation.md)。旧组件数量/日志只对应当时版本。

<a id="function-design"></a>
## 一个 Catalog，受控写入与固定输入

函数结果是一种 materialized relation；依赖统一来自 Catalog::dependencies()。
Request/node/task 只记录该图的执行状态，不保存第二份可编辑业务 DAG。函数池只收固定
typed 输入、返回 payload，没有 Engine/KV 写权限；结果必须回到唯一 shard owner 审核。

函数多输入按完整 `(business_key..., request_id)` 对齐，不能把 request_id 加入 shard key，
也不能放宽原 same-key one-to-one JOIN 冒充请求 join。同类型的 news_id/request_id 不能
互换，故引入 request identity domain。SQL 已完成但无行与尚未完成分开；可选成功缺值为
nullopt，失败祖先不能冒充可选缺值。Latest 是 Catalog 受控 selector，不是用户可写基表。

原提案建议的公开 install_function 不作为兼容 API 承诺；落地采用私有 runtime bridge，
没有公开 allow_mv_write 或可伪造“已验证”标志。触发与受理分开：源变化形成 occurrence，
之后在已确认 revision R 捕获，R+1 同批保存 Request/capture/once/occurrence 消费。
完整致因 witness 与后来 capture 不是同一时间切面。

文本编码请求的输出先进入当前候选 LatestEmbedding，首次/条件/周期预测各自在自己的
request identity 下捕获该值；不把两个请求的行直接 zip。once 在成功受理时消耗。
LatestSuccess 只选当前 candidate epoch、成功请求，再按 admission_seq 比较；较晚完成
的旧请求不能覆盖新受理结果。完成/取消/超时必须保留释放额度的通道。

UDF/UDTF/UDAF 制品上传及动态执行后置，Process Function 已移出当前交付；内置 registry
不证明这些扩展已实现。独立函数池通过也不证明 Compute 已接入，集群证据须另验。

<a id="function-catalog"></a>
## Catalog 元数据与身份域

显式 RelationKind 与 BaseWriteRole 防止函数视图被误当作外部可写表。FunctionView 保存
函数 ID/version/implementation identity、精确 alias/required/Field 签名与 payload schema，
不保存 callback 地址。同函数版本的签名和实现身份必须一致；普通 Catalog 的默认字段
保持原 canonical shape，新增非默认元数据才改变 fingerprint，避免无关默认值破坏兼容。

RequestCapture 定义身份域；单输入 SQL 通过隐藏 key 继承完整域，即使 SELECT 隐藏了身份。
GROUP BY/JOIN 改变身份后清域。所有输入必须同域、同物理 identity/partition；宿主生成
输出 visible/hidden 身份，worker 不能指定写入关系或伪造 key。from_snapshot 逐项重建，
核验 kind/payload、前向引用、域/签名/provenance，并拥有独立深拷贝。

角色/域遗漏及同函数版本冲突先出现行为 RED。历史 function_catalog 17 + catalog 19，
36/36、Clang/Werror/三 sanitizer，证据
`/tmp/eventflow-function-catalog-tests/build/Testing/Temporary/LastTest.log`。
该证据仅证明元数据层，不证明 runtime 激活和执行。

<a id="engine-bridge"></a>
## Engine 私有提交桥接

EngineRuntimeAccess 激活前核对 Registry 签名；每 Engine 一个 RequestRuntime，并借用
相同存储/owner/config。prepare 在 R 读取稳定输入，受控 typed writes 后稳定 SQL，
finalize 只写 RuntimeStatus，再稳定派生关系；RoundContext revision 始终是 R。
RequestCapture/FunctionView 只允许新 identity 插入；公开 apply/emit/raw stage 不能绕过权限。

私有 prepublish 失败丢弃 overlay，不发布半轮；UNKNOWN 保留最终 batch，resolver 不重跑
prepare/finalize 或重捕获。公开生命周期 hook 的永久错误语义未因此全面放宽。
多阶段 touched rows 即使最终净零也保留内部传播记录，仅返回时过滤净零；aggregate 每阶段
从本轮 Before 加累计差分计算，避免重复累计。join 插入后撤回和聚合临时行也须正确清理。

round-local scan 只允许完整同 shard scope，逐页核对 revision/prefix/order/codec，Before
忽略 overlay、Staged 合并它；每次扫描和整轮累计读字节都计费，不提供 MVCC/as-of。
原三类多阶段反例在四 KV 上共 12 项 RED；历史新测试 69 + 回归 131 = 200/200，
6 suites、17.46 秒，日志 `/tmp/eventflow-engine-runtime-build/Testing/Temporary/LastTest.log`。
独立闭环见[执行桥接审查](../reviews/M4-execution-fresh-review.md)。

<a id="function-executor"></a>
## 执行器的所有权、取消与缓存

Simple 显式 run_ready，ThreadPool 使用固定 pthread worker、条件变量且锁外运行 callback。
submit 固定 signature/实现及 typed 输入；active 包含 queued/running/completed-unpolled。
受理同时预留输入和最大 completion 容量，因此结果产生不再等待空位。poll 移交结果所有权，
owner 未持久确认前必须保留它；执行器不能代替 durable TaskOutbox 或 attempt fencing。

排队取消不执行，运行取消等待有界 callback 返回并丢弃结果；不强杀线程。close 停止准入、
join worker，完成结果仍可 poll，不能销毁自己所在的 worker。任意 callback 时长/临时内存
不是本组件可强制保证的预算，不能将逻辑容量宣称为 RSS 硬上限。

同 task ID 比较完整规范内容；active 幂等，poll 后有限 receipt 重放原 completion，超窗口
可物理重执行。pure 结果 cache 使用函数/版本/实现与全部实际 typed 输入，区分 NULL、
缺值、signed-zero；错误/取消不缓存。request_id 若仍是可见输入，不能偷偷从 cache key
删除；需上游 SQL 投影隐藏它。不同 task 的同时 miss 不承诺 single-flight。

历史 38 项共享契约/真实 worker/latch 同步测试通过，证据
`/tmp/eventflow-function-executor-build/Testing/Temporary/LastTest.log`；随后执行桥接
独立审查补充容量整改及更新结果，见上述独立报告。最终 Compute 线程池证据另见验证报告。

<a id="request-machine"></a>
## 纯请求状态机

RequestMachine 从可信 Catalog targets 推导祖先，不写 KV 或调用函数。refresh 在 SQL
稳定后读取完整 identity；Ready 首次固定输入，retry 保持它。claim 产生严格递增 attempt
及 durable task 候选；完成只接受当前 RUNNING 的完整 task/function/version/implementation。
owner now 决定 deadline，等号先超时，不依据 worker completed_at 授权。旧 attempt、
取消/终态后回包没有写入；永久失败跳过未完成后继并释放任务，共享祖先正常只 claim 一次。

Manifest 分开保存 capture/admission R/R+1，纯 fixture 才允许 (0,0)。task ID 使用小型
规范 tuple，完整 fingerprint 和 frozen-input witness 独立保存；在 begin 预验 4,096 字节
task 身份上限，不放宽 Executor 限额。codec 校验可信 expected_manifest、状态/节点/期限/
任务身份/输入存在性，并要求重新编码字节一致；它不能替代可信 owner key 的绑定。

独立挑战发现 Accepted 装得下但终态诊断装不下，以及首次 Ready 可携带永久错误。
整改 begin/active decode 预留最大终态编码（最大 attempt/time、384 字节最坏转义 Status），
严格校验 attempt/error 组合。两个仓库回归先 RED 后 GREEN；独立 reviewer 复验关闭。
仓库 21/21；`/tmp/eventflow-request-machine-review-probes/build` 含 3 探针共 24/24，
二分最小预算覆盖取消、最大错误与期限，日志为该目录 `Testing/Temporary/LastTest.log`。
当时 request_machine.cpp SHA-256 为
`482986f12596a913db20808ae29948b90a93b2cb9242a2adfab7924fa11c2672`，测试为
`1e23deb14eddc84ce1881e15e87e2527409e4185baa021ff4e15dce1a416be56`。
这仍仅是纯状态机证据，不证明 KV 原子 owner。

<a id="request-runtime"></a>
## RequestRuntime 持久 owner 与工具历史

同 scope/request ID 重试比较 exact admission 编码（类型/signed-zero），不重捕获；
not-ready 不受理、不消耗 sequence、不推进时间。claim 先提交，确认后派发；完成输出、
SQL/节点刷新和请求记录同批。active book 有界，终态移入按需读取的 KV archive。
UNKNOWN 保留原 batch、候选 book/result，任务与 effects 确认前不可见。

独立 owner 审查复现 resolver 被公开入口抢走、companion 覆盖私有 index、detach 后 DDL
孤立持久请求，以及按当前编码计费使已受理请求无法 claim。整改为 owner 专属确认入口、
逐次私有 state 写权限、持久请求 Catalog 迁移限制，以及每 active machine 最大状态与
envelope 预留；恢复也验证。原报告保留冻结哈希和 58 仓库 + 11 独立探针共 69 项证据：
[Request owner 审查](../reviews/M4-request-owner-fresh-review.md)。

历史 RED：`/tmp/eventflow-request-admission-identity-red.log`、
`/tmp/eventflow-request-catalog-red.log`、`/tmp/eventflow-request-namespace-red.log`。
早期组合 `/tmp/eventflow-m4-bridge-owner-format-test.log` 后又有 namespace 修改，不能
冒称最终版本；后续主工程 26 suites 为 `/tmp/eventflow-m4-main-current-test.log`、131.12 秒。
当时 Bear 数据库 47 项只是工具历史，最终数据库应重新生成。

clangd 20.1 的默认 ExtractFunction 自检曾报内部 break/continue probe 错误：
`/tmp/eventflow-request-runtime-clangd.log`；同源 `--tweaks=DefineInline` 完整 parse/index
为 0 errors，`/tmp/eventflow-request-runtime-clangd-index.log`。没有修改业务语义掩盖工具错误，
也不把受限检查声称为全部重构功能通过。最终工具与构建证据以当前验证报告为准。
