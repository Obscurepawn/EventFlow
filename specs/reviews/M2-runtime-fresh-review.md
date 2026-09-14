# M2 运行时独立审查

日期：2026-09-13。Reviewer：`m2_fresh_runtime_review`（独立上下文）。结论：**FAIL，5 项 must-fix 待整改复核**。

本次只审 M2：增量 old/new、聚合、lookup join/菱形传播、初始化、预算、UNKNOWN 确认、公开 hook/query 接口及持久状态复用。M3 的窗口/FSM/Timer、M4 模型及 M5 多 Compute/受控恢复不是本 gate 缺陷。依据为 `AGENTS.md`、`specs/sql-runtime.md`、`specs/runtime-contracts.md` 与当前源码；不以历史审查结论替代本次核验。

## 审查证据

- 完整阅读 `include/eventflow/engine.hpp`、`src/runtime/engine.cpp`、`tests/engine_contract_test.cpp`，并交叉检查 Catalog/表达式、StateStore 接口及基本 Engine 测试。
- 独立运行当前 `build/bin/engine_test`：2/2 通过；`build/bin/engine_contract_test`：48/48 通过，涵盖 Memory、Rocks、RemoteMemory。
- 在 `/tmp/eventflow-m2-runtime-review/` 建立独立 CMake 实验，直接编译当前 `src/runtime/engine.cpp`，链接现有组件库；没有修改生产源码或主 build。命令：`cmake -S /tmp/eventflow-m2-runtime-review -B /tmp/eventflow-m2-runtime-review/build`、`cmake --build /tmp/eventflow-m2-runtime-review/build -j2`、`/tmp/eventflow-m2-runtime-review/build/repro`。
- 实验所用 `engine.cpp` SHA256：`637756b597eb7131f99d67f621e7696baa953b96c53fb18fd30aaf9535ef7b7a`；`engine.hpp`：`fd7fd618d326c81fb75229f168bbfbf77cede4e6777b94efbd7363c9a1596d08`。以下行号对应这一审查快照。
- 主任务报告的全套 CTest 9/9、231 顶层测试及 SQL demo 不作为本 reviewer 亲自执行的证据。已有 50 项 Engine 测试通过，没有覆盖下面五条路径。

## R1：P1，公开 hook 可重入 Engine 并以旧计算结果覆盖新 revision

位置：`src/runtime/engine.cpp:373-378`、`:397-400`、`:650-653`、`:688-690`。

`ready()` 只检查 FAILED 和 pending，执行 round 时没有 busy 标志。hook 可以在同一个线程中调用同一 Engine 的 `apply`。内层提交推进 `state_->revision` 后，外层 `publish` 使用此新 revision 构造批次，却提交在旧 revision 上计算的 overlay，Store 无法检测该读写不一致。

复现：定义 `f(k STRING,id INT64,v INT64,PRIMARY KEY(k,id)) PARTITION BY(k)` 与 `totals = SELECT k,SUM(v) AS s FROM f GROUP BY k`。外层插入 `(a,1,1)`，hook 内调用同一 Engine 插入 `(a,2,2)`。两次均成功 Committed；对基表重算 SUM 为 **3**，持久 MV SUM 为 **1**。

修复建议：在 Engine 入口增加贯穿整个 round 生命周期的重入保护，拒绝 hook 内再次启动 apply/install/确认等操作；提交身份与 expected_revision 固定使用 round 的 revision，并在发布前验证 Engine 状态未前进。补充重入拒绝且无额外提交的回归测试。

处置：OPEN。验收：以上轨迹拒绝重入或得到一致结果，不能提交陈旧覆盖。

## R2：P1，hook emit 与原 DAG 汇合后可留下下游中间结果

位置：`src/runtime/engine.cpp:346-358`、`:581-595`、`:691-699`。

第二次 stabilize 仅禁止 emitted relation 是原始输入 root，未验证它的后继与第一次执行图互不相交。当第二次计算使汇合 MV 恢复为 round-before 图像时，`put_row` 删除其 changes，`stabilize` 因该 changes 为空而不把汇合节点加入 affected，第一次计算过的下游无法撤回中间结果。

复现：`a(k,v)` 和 `b(k,v)` 都以 k 为 PK/partition；`j = SELECT a.k,a.v*b.v AS product FROM a JOIN b ON a.k=b.k`；`d = SELECT k,product AS p FROM j`。初值 a=1、b=4，j=d=4。一次 apply 把 a 改为2，在 hook 中 emit 把 b 改为2。提交成功后 **j=4，d=8**。回调还已经观察过第一次的 j=8，不能称其为稳定的最终关系图。

修复建议：若此 hook 只用于独立请求子图，必须验证 emitted root 的可达图与已稳定连续图无交集并明确拒绝重叠；若要允许汇合，需要重新设计统一稳定点和传播，使变化回到 before 的节点仍清除其下游暂态，再交付最终 changes。不能只扩大 roots.contains 检查。

处置：OPEN。验收：该轨迹整体拒绝且不提交，或所有物化结果均为4且无中间变化外露。

## R3：P1，raw stage 可以绕过关系维护直接改写基表/MV/内部累计器

位置：`src/runtime/engine.cpp:206-220`、`:339-340`。

公开 `RoundContext::stage` 只检查 key 属于当前 shard，可以直接写 `'T'` 基表/MV key，也可以覆盖已分配的聚合 state key。它不生成 RowChange、不验证关系行、不运行依赖节点；这绕过了 `emit` 的禁止直接写 MV 等检查。

复现：定义 `f(k,v)` 与 `totals = SUM(v)`；apply 插入 `(a,1)`，hook 调用 `round.stage`，用合法 codec 将同一基表物理 key 写成 `(a,100)`。提交成功，**基表 v=100，MV SUM=1**，公开 changes 仍携带值1。

修复建议：公开 stage 应只允许扩展所拥有的 state namespace/object IDs，保护关系 `'T'` 域、Engine 元数据和已分配的聚合 state 域。关系变更走 `emit`/typed TableChange；如确需支持 raw 关系 mutation，应转为受验证的同轮关系变更并维护 DAG。内部执行器仍可调用私有 RoundState staging。

处置：OPEN。验收：raw 写受保护关系或引擎状态被拒绝且无持久副作用；现有合法自定义状态 stage 合约继续通过。

## R4：P2，query 接受其他 Catalog 的同 version 计划并返回错误关系

位置：`src/runtime/engine.cpp:759-778`；`include/eventflow/catalog.hpp:36-53`。

`BoundPlan.catalog_version` 只是定义序号；不同 Catalog 可以有相同版本与 RelationId。Engine::query 只比较该数字，不验证计划来源 fingerprint、依赖关系/Schema/布局。即使调用者只使用合法 Catalog binder，不手工篡改 BoundPlan，也能在另一 Engine 上执行指向不同关系的计划。

复现：Engine Catalog version=1 定义 `prices(k STRING,price INT64,PRIMARY KEY(k)) PARTITION BY(k)`，写入 `(a,123)`。另一个 Catalog version=1 定义 `credits(k STRING,credit INT64,PRIMARY KEY(k)) PARTITION BY(k)` 并 bind `SELECT credit FROM credits WHERE k='a'`。把此合法计划传给第一个 Engine 的 query，成功返回字段 **credit=123**，实际读取 prices。

修复建议：BoundPlan 携带可验证的 Catalog 身份/fingerprint 或完整依赖版本/布局标识，并在 public query 执行前核验；由于 BoundPlan 是公开可写结构，还应在边界验证其完整合法性，或把可执行计划收敛为只能由 binder 产生的不可变对象。补充不同 Catalog 同 version 的拒绝用例。

处置：OPEN。验收：此轨迹返回 Conflict/InvalidArgument，不能把不存在的 credits 解释为 prices。

## R5：P2，hook emit 绕过 EngineConfig.max_rows 的准入预算

位置：`src/runtime/engine.cpp:346-358`、`:656-657`。

`max_rows` 只检查 `apply_many` 的初始 inputs.size()；hook 的追加源关系 emit 没有计入同轮行数。虽有独立 mutations/bytes 上限，不能替代该行预算。M2 规格要求 RoundContext 的有界追加和输入/中间/输出预算统一由 Engine/RoundContext 强制。

复现：`EngineConfig.max_rows=1`，一个空输入 round 的 hook 连续 emit 两个不同 PK 的合法基表行。API 返回 Committed，并发布 **2 条 RowChange**。相同两行通过 apply_many 输入会 ResourceExhausted，生命周期入口的预算行为不一致。

修复建议：定义清晰的同轮行/追加源事件预算计数，将外部 inputs 和 hook emit 纳入同一预算，分配/stage 前检查；同时核对中间、输出预算的契约与实现。补充边界1、重复同 identity 和失败原子性的回归。

处置：OPEN。验收：该轨迹 ResourceExhausted，revision 不增加，没有部分行或变化公开。

## 已核实部分与剩余边界

正常 apply 路径按 Catalog 拓扑顺序维护 before/final-after，完整 old/new 测试、双向 join、菱形汇合、精确撤回以及有限初始化通过现有三 adapter 测试；UNKNOWN 保存原 batch/changes，确认成功交付原 changes 并拒绝重复 resolve 的现有测试通过。以上正向证据不消除 public hook/query 的五个反例。

本记录没有把 M3/M4/M5 未实现能力列为 M2 缺陷，也没有提出第三方异常包装。需逐项修复、添加相应回归并由独立 reviewer 复核后，才可将此门禁标为 PASS。最终完整交付仍需另行多位 fresh-context 全局审查。
