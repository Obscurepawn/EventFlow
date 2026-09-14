# 有限 SQL 与持续增量关系执行

Status: done（SQL 子系统基线；最终核心门禁独立记录）
Owner: root
Updated: 2026-09-14

## 目标与依据

把 SQL 与 C++ 定义编译到同一个 typed Catalog/关系计划，在唯一 shard owner 的同一个 staging round 内增量维护级联 MV，并通过现有 `IStateStore` 原子提交。本文记录已实现的 SQL 子系统接口和边界。

本规格从以下当前本地文件细化接口，不覆盖主规格的最终范围：

- [主规格](product-scope.md)：I1–I10、AC02–AC04、AC08、M2/M3 分工。
- [实现契约](runtime-contracts.md)：C3 资源/失败、C4 KV、C5 SQL/关系、C6 下游 FSM、C9 恢复。
- [架构说明](../docs/feature_computation_architecture.md)：§3 SQL/SDK 同路径、§5 单 key 计算、§6 状态与物化。
- 当前 [types.hpp](../include/eventflow/types.hpp)、[status.hpp](../include/eventflow/status.hpp)、[state_store.hpp](../include/eventflow/state_store.hpp)、[key_codec.hpp](../include/eventflow/key_codec.hpp)。

M2 实现 SELECT/DDL、typed binding、Catalog、filter/project、SUM/COUNT/AVG、同 key one-to-one inner lookup join、级联增量维护以及有限初始化。Latest、处理时间窗口/TTL 调度、Timer、运输身份去重、FSM 的完整生命周期由 M3 接上本文同一执行和状态接口；没有因此删除主规格里的这些能力。M4 的请求派生关系与版本化函数继续接入相同 Catalog。

## 可执行 SQL 子集

每次解析恰好一条语句，允许一个末尾分号，之后必须 EOF。关键字不区分 ASCII 大小写。普通标识符转换为 ASCII 小写，双引号标识符保留原文，内部 `""` 表示一个双引号；字符串用单引号，`''` 表示一个单引号。源码须为合法 UTF-8，支持空白与 `--` 单行注释。SDK 的普通名称使用同一规范化工具，显式精确名称具有与双引号名称相同的语义。

以下为语法范围说明，最终 `.g4` 负责完整优先级与 EOF；不能把此处的省略号当作宽松接受任意尾部文本。

```sql
CREATE TABLE fills (
  account STRING NOT NULL,
  fill_id INT64 NOT NULL,
  quantity INT64 NULL,
  price FLOAT64 NULL,
  accepted_at TIMESTAMP_MICROS NOT NULL,
  PRIMARY KEY (account, fill_id)
) PARTITION BY (account);

CREATE TABLE limits (
  account STRING NOT NULL,
  ceiling INT64 NOT NULL,
  PRIMARY KEY (account)
) PARTITION BY (account);

CREATE MATERIALIZED VIEW totals AS
SELECT account, SUM(quantity) AS volume, COUNT(*) AS rows_seen,
       COUNT(quantity) AS nonnull_rows, AVG(quantity) AS mean_quantity
FROM fills
GROUP BY account;

CREATE MATERIALIZED VIEW active AS
SELECT t.account, t.volume, l.ceiling
FROM totals AS t INNER JOIN limits AS l ON t.account = l.account
WHERE t.volume > l.ceiling AND NOT (l.ceiling < 0);

SELECT volume * 2 AS twice_volume, mean_quantity
FROM totals WHERE account = 'desk-a' AND volume IS NOT NULL;
```

| 构造 | M2 接受的形式及语义 |
| --- | --- |
| CREATE TABLE | 六种 `BOOL/INT64/FLOAT64/STRING/TIMESTAMP_MICROS/FLOAT_VECTOR`；字段 `NULL` 或 `NOT NULL`；一个表级复合 `PRIMARY KEY`；`PARTITION BY (...)` 为 EventFlow 扩展 |
| 缺省 nullable | 未写 NULL/NOT NULL 的普通列 nullable；PK 列未标注时隐式 NOT NULL；显式 `PK ... NULL` 拒绝；最终调用现有 Schema 校验 |
| 分区配置 | SQL 未写 PARTITION BY 时，由同次定义操作的 `DefinitionOptions.partition_key` 提供；两者都缺失或互相冲突时拒绝，不能猜测字段名或默认第一列 |
| CREATE MATERIALIZED VIEW | `CREATE MATERIALIZED VIEW name AS select`；只引用已存在的基表/MV；同名定义拒绝，不支持 CREATE OR REPLACE |
| SELECT | 表达式列表及 `AS alias`，普通列可无别名；FROM 一个关系及可选别名；`*`/`alias.*` 展开可见列；计算列要求 AS，重复输出列名拒绝 |
| 标量表达式 | 列、限定列、NULL/TRUE/FALSE/Int64/Float64/string 字面量、括号、单目 +/-、二元 + - * /；`CAST(expr AS FLOAT64)` 支持显式数值提升 |
| 条件 | =、<>、!=、<、<=、>、>=、IS NULL、IS NOT NULL、AND、OR、NOT；WHERE 的最终类型必须 Bool |
| 聚合 | SUM(numeric_expr)、AVG(numeric_expr)、COUNT(*)、COUNT(expr)；GROUP BY 为完整业务 partition key 的简单列集合，允许书写次序变化，内部按声明分区次序归一化 |
| Join | 每个 SELECT 至多一个 `[INNER] JOIN relation [AS alias] ON ...`；ON 是左右完整 partition key 对应列等值条件的 AND；两侧都必须能证明每 partition key 至多一行 |

GROUP BY 存在时 SELECT 中的非聚合列必须来自分组键，可对分组键和聚合结果使用已支持的标量表达式；聚合不能出现在 WHERE、JOIN ON 或另一个聚合参数中。无 GROUP BY 的聚合会跨 key，明确拒绝；即使查询上下文恰好限定了一个 key，首版也要求写出 GROUP BY，保持两种执行生命周期的绑定规则一致。

M2 不实现的语法必须以 `InvalidArgument` 明确失败，包括 DISTINCT、HAVING、ORDER BY、LIMIT、UNION、子查询、CTE、递归、SQL 窗口语法、outer/cross/many-to-many join、SQL DML、未知函数与未知 WITH 选项。LIMIT 不承担安全预算职责：API 的捕获预算超限返回错误，不能悄悄截断结果。后续可增加语法，但不能让 parser 接受而执行器忽略。

输入变更通过 Table API/后续 source adapter 提交。FloatVector 可从现有 JSON/Row 入口写入并投影，不提供 SQL vector 字面量、排序或算术。TimestampMicros 使用独立类型绑定，不能因 Value 底层也是 int64 就自动允许与 Int64 运算。

## 类型、NULL 与错误

1. binder 将每个表达式解析为 `DataType + nullable + slot/operand`，运行期不再按字段字符串反复查找。两入口都必须走相同 binder；手写 C++ AST 不能绕过类型或分区校验。
2. 算术默认要求同一种 Int64 或 Float64 类型；不同数值类型必须显式 CAST。Int64→Float64 采用 M1 已有的精确可表示规则，丢精度时返回 InvalidArgument；不要静默把大整数四舍五入。Int64 `/` 向零截断，Float64 `/` 为浮点除法；除零、Int64 溢出及非有限结果失败。
3. 比较两侧必须是同类标量；Bool 只提供相等/不等，String 按 UTF-8 字节序比较，TimestampMicros 只同类型比较。FloatVector 仅支持 IS NULL/IS NOT NULL、COUNT 和投影。
4. 裸 NULL 为待定类型，由另一侧操作数/CAST/布尔上下文确定；`SELECT NULL AS x` 等无法推导具体输出类型的形式拒绝，`CAST(NULL AS FLOAT64)` 合法。COUNT(NULL) 只关心是否 NULL，可绑定并恒定忽略该贡献。
5. 任一比较操作数为 NULL，结果 UNKNOWN；算术遇 NULL 得 NULL。NOT UNKNOWN = UNKNOWN；FALSE AND UNKNOWN = FALSE；TRUE OR UNKNOWN = TRUE；其他组合采用 SQL 三值表。WHERE 只让 TRUE 的行通过。
6. 固定左到右求值；AND 左值 FALSE、OR 左值 TRUE 时短路，其他情况计算右侧后组合三值结果。未执行的分支不会产生除零等错误。此规则对查询和持续计算相同。
7. 项目自有生产函数/访问器/回调均 `noexcept`；可失败构造使用 `StatusOr<std::unique_ptr<T>> create/open`。复用当前 ErrorCode，未知列/类型/SQL 为 InvalidArgument，缺关系为 NotFound，同名/版本冲突为 Conflict，运行状态不允许操作为 FailedPrecondition，预算超限为 ResourceExhausted。
8. 语句有语法或绑定错误时不修改 Catalog。准入前可检查的数据错误直接拒绝；开始 dispatch 后的永久计算错误按 C3 fail-stop 当前 shard，staging 不提交、不发布，不继续消费下一事件。查询求值错误只使本次查询失败。

数值字面量和 CAST 使用非抛解析/检查，不通过异常实现业务控制流。标准分配失败与第三方未报告的异常仍遵循主规格既有边界，不新增 catch 包装。

## AST、BoundPlan 与 Catalog

### 与 parser 解耦的数据结构

AST 自有全部字符串与子节点，不保留 ANTLR parse tree、Token 或 input stream 指针。可以用 `std::variant` 和 move-only 节点表达：

- `ExprAst = ColumnRef | Literal | Unary | Binary | IsNull | Cast | AggregateCall`；每项保留 source span，SDK 可使用空 span。
- `SelectAst { projections, from, optional_join, optional_where, group_by }`。
- `StatementAst = CreateTableAst | CreateMaterializedViewAst | SelectAst`。
- `CreateTableAst`/SDK `TableDefinition` 共享字段和主键/分区定义；DDL 不能走独立的弱校验路径。

BoundPlan 只含稳定 ID/slot 和类型化表达式，至少记录：

- `catalog_version` 与所有依赖的 relation/schema/plan 版本。
- 输入 RelationId、输出可见字段、内部物化布局、完整 partition key 类型与列 lineage。
- 节点 `ReadRelation → [LookupJoin] → [Filter] → [Aggregate] → Project` 及拓扑依赖。
- `UniquePerPartition`/`ManyPerPartition` 基数证明、输出行身份生成规则。
- 聚合 slot、参数类型、返回类型、内部状态编码版本与稳定 state object ID。
- 节点状态读集/写集及单轮预算；explain 输出算子、输入关系、分区属性与是否有内部状态。

绑定只读取不可变 Catalog 快照；先构造完整候选，再一次发布。禁止绑定期间注册半个 MV，禁止接受未解析列并在执行时猜测。Catalog 是对象，运行的每个 Engine 持有它的只读版本；部署阶段分配稳定 IDs，禁止以 unordered_map 遍历顺序分配持久 IDs。

### 查询 Schema 与持久化 Schema 分开

当前 `Schema::validate_definition()` 要求非空 PK/partition key，不能把无 key 的普通 `SELECT volume AS v` 输出假装成合法持久 Schema。

`ResultSchema` 仅包含 `vector<Field>`，用于查询/表达式输出；`RelationDescriptor` 同时持有可见 ResultSchema、供当前 `encode_row/decode_row` 使用的 storage Schema，以及 visible-to-storage slot 映射和 RowIdentity 布局。基表两套列通常一致；MV 可附加 binder 生成的隐藏身份/partition 字段，使合法投影不必暴露原始 PK。

隐藏字段不参与 `SELECT *`，不允许用户引用，命名使用保留前缀 `__ef_` 并拒绝用户定义该前缀。字段名和 id 由通用布局生成，不识别 account/ticker 等业务字段。非聚合投影继承输入行的完整 PK 身份，所以两条不同输入投影成相同可见值仍是两条关系行；不能意外变成 DISTINCT。分组输出以完整 group key 为行身份；one-to-one join 以归一化 partition tuple 为身份。NULL/FloatVector 等可见投影列不承担隐藏 PK。

基表 PK 的物理 key 编码按 partition 列在前、剩余 PK 列保持声明相对次序排列，在 descriptor 中固定该布局；这允许用同 key 前缀扫描，且不要求用户把 PRIMARY KEY 的书写顺序改掉。编码仍使用现有 typed key codec，不拼接带分隔符的字符串。

### 建议公开接口

以下是待实现的接口形状，省略类型定义和私有实现；不表示这些头文件已经存在。`StatusOr<T>` 保持当前 move-only/no-throw-move 约束，查询返回类型不能包含裸引用。

```cpp
StatusOr<StatementAst> parse_sql(std::string_view sql) noexcept;

class Binder {
public:
  static StatusOr<BoundStatement> Bind(
      const StatementAst& ast, const CatalogSnapshot& catalog,
      const DefinitionOptions& options) noexcept;
};

class Catalog {
public:
  static StatusOr<std::unique_ptr<Catalog>> create() noexcept;
  StatusOr<CatalogSnapshot> snapshot() const noexcept;
  StatusOr<DefinitionResult> define(
      const StatementAst& ast, const DefinitionOptions& options) noexcept;
  StatusOr<DefinitionResult> define_sql(
      std::string_view sql, const DefinitionOptions& options) noexcept;
  StatusOr<BoundPlan> bind_query(const SelectAst& ast) const noexcept;
  StatusOr<BoundPlan> bind_query_sql(std::string_view sql) const noexcept;
  StatusOr<std::string> explain(RelationId relation) const noexcept;
};
```

`define_sql` 只做 parse 后调用 `define`；C++ builder 生成同一个 StatementAst。查询两入口最终都调用 Binder。`DefinitionResult` 包含新 relation ID、版本和 CREATING/READY 状态。Table/MV 的实际运行激活由 Engine 在暂停点验证/初始化完成；不可通过单独调用 Catalog.define 将一个未初始化 MV 暴露为 READY。

## Engine、staging 与状态所有权

### 可复用的单轮接口

`Engine` 是一个 shard 的单线程 owner 对象，持有 store shared_ptr、owner token、revision、不可变 Catalog 快照和执行图。调度线程调用它；executor/node 不是进程，不创建自己的 owner 或独立 commit。一个进程以后可以调度多个 Engine，某个 shard 的任意时刻只允许一个未完成 round。

```cpp
struct RowChange {
  RelationId relation;
  Row identity;
  std::optional<Row> old_row;
  std::optional<Row> new_row;
};

enum class StateImage { Before, Staged };

class RoundContext {
public:
  std::int64_t processing_time_us() const noexcept;
  StatusOr<std::optional<std::string>> read(
      const std::string& key, StateImage image = StateImage::Staged) noexcept;
  Status stage(Mutation mutation) noexcept;
  StatusOr<std::optional<Row>> read_relation(
      RelationId relation, const Row& identity,
      StateImage image = StateImage::Staged) noexcept;
  Status emit(RowChange change) noexcept;
};

class Engine {
public:
  static StatusOr<std::unique_ptr<Engine>> open(
      std::shared_ptr<IStateStore> store, CatalogSnapshot catalog,
      const EngineConfig& config) noexcept;
  StatusOr<DefinitionResult> install(
      const StatementAst& definition, const DefinitionOptions& options) noexcept;
  StatusOr<DispatchResult> apply(const TableChange& change,
                                 const DispatchContext& context) noexcept;
  StatusOr<DispatchResult> resolve_pending_commit() noexcept;
  StatusOr<QueryResult> query(const BoundPlan& plan,
                              const QueryContext& context) noexcept;
  StatusOr<QueryResult> query_sql(std::string_view sql,
                                  const QueryContext& context) noexcept;
};
```

`TableChange` 包含基表 relation ID、Upsert/Delete、明确的 PK identity 与可选完整新 Row；Upsert 时验证新行 PK 与 identity 相同。更换 partition key 不作为同一行普通更新接受。Delete 缺行是无关系变化的成功；相同内容 Upsert 不向 MV 重复贡献。运输去重仍是后续 envelope 身份协议，不能把相同业务行内容当成 delivery identity。

`DispatchContext` 至少携带由调度器固定的 processing_time_us，以及 M3 后续使用的 accepted event/identity 上下文；运行期不在每个算子里读取墙钟。`DispatchResult` 明确区分已提交、未决提交和失败，不把接受准入当成已提交。外层 Status 表示调用/计算错误，提交返回的 Unknown 必须保留为未决状态。

RoundContext 不暴露可供算子直接 commit 的 IStateStore。默认 `read(..., Staged)` 先查本轮 overlay，再查固定 revision 的 store；`read(..., Before)` 只观察轮次开始前的值。第一次读到的 committed bytes 缓存为 before image，后续 stage 只改 after image。所有读取必须与 round 的 expected_revision 相同，发现不一致时停止当前 shard，不能用不同 revision 拼接快照。

M3 的窗口删除、FSM state、Timer intents、身份/源进度、PendingTrigger/Request、待派发记录都调用同一个 `stage`。调度顺序预留为 `到期输入 → 基表/窗口变更 → 连续 DAG 稳定 → FSM/捕获 → seal/commit → 发布/派发`；M2 不以每条 MV change 各自 commit。M3 若需要一时刻多个逻辑事件，可建立有序 rounds，不能在一个已密封批次上继续修改。

### 持久 key 与提交协议

复用 `row_key(shard, 'T', relation_id, identity)` 作为基表/MV 行；内部聚合等状态使用 `'S'` 及 Catalog 分配的稳定 state object ID；必要索引用 `'I'`。这些均为 store 允许的 `D + shard` 数据域，不使用 store 私有 revision/receipt 元数据域。不在 key 尾部追加当前 revision/worker ID，不用业务字段名称确定 namespace。

同一物理 key 本轮多次写合并为一项最终 Mutation。所有可恢复的内部状态和物化结果都是 durable bytes；进程内 cache 是加速，关闭后不能丢失维护下一轮所需的累计器或行身份。Catalog fingerprint/布局版本在部署和恢复时固定，读取不匹配状态必须拒绝，不能照旧 bytes 猜测新 Schema。

完整提交步骤：

1. 接受一轮后暂存 base old/new；执行 affected DAG；所有变更只进入 overlay。
2. 检查输入/读集/中间 row/输出/累计 mutations/bytes 预算，冻结有序 mutations。使用当前 `make_commit_id(owner, shard, expected_revision)`；staging 不携带自创的另一套提交身份。
3. `IStateStore::commit` 返回 Committed 后才推进本地 revision、安装 after cache、发布最终变更并允许下一轮；同一回执 duplicate 也只完成同一个 pending round 一次。
4. Rejected 不公开 staging；owner/revision conflict 进入停止状态并报告，不擅自重新计算。外层调用错误也不能凭空假定批次已提交。
5. Unknown 保留原始 CommitBatch 的身份、字节内容及待发布变化；暂停该 shard 的后续事件和查询。调用 `resolve_pending_commit` 只能查询 receipt/重试同一完整批次，不能重新执行算子或换 commit_id。RocksDB fail-closed 必须按 M1 契约重开后再确认。

远程服务可能已提交但响应丢失，因此 Unknown 时禁止从 store 的新值刷新 cache 并继续。查询经 owner 进入，不能通过绕过 owner 的直接 remote read 暴露未确认轮次为引擎已发布结果。

## 连续维护算法

### 变化传播与汇合

一次逻辑 Upsert 读取旧行并构成 `{old,new}`，Delete 构成 `{old,nullopt}`；插入为 `{nullopt,new}`。同一个 relation/identity 的本轮多次变化归并为“轮次开始之前 → 本轮最终结果”。没有可见变化时不发送下游 row delta，但上游事件身份和后续 FSM OnEvent 仍可处理其事件语义。

以拓扑顺序执行受影响节点：一个节点等待本轮所有上游处理完成，读取它需要的 committed before 与 overlay after，各输入身份只处理最终 old/new。MV 节点共享，不因为多个下游订阅而运行多次。特别是同一个基表分叉后又 join 的菱形图，不能在左支到达时先用旧右支发布一个瞬态结果，再当作两个业务事件让 FSM 观察。

filter/project 对 old/new 独立求值：old 不通过、新通过是 insert；old 通过、新不通过是 delete；都通过且值/身份相同是 no-op；其余为 replacement。投影不丢失内部 RowIdentity。对外 ChangeBatch 在整图稳定后提供，不包含先删后插的临时中间状态。

维护复杂度以受影响 row/group/join key 与图边数为界。常规 apply 禁止扫描整表、全 shard 或所有 group；聚合不能逐输入调用 SELECT 重算。SELECT 捕获和明确的暂停初始化可以扫描相应有限范围。用测试 IStateStore 计数器证明稳态无全表 scan，不能只以“输出对了”证明是增量算法。

### 聚合状态及 NULL

每个 group 保存总输入行数 `row_count`，每个 COUNT(expr)/SUM/AVG 参数保存独立的非 NULL 计数，需要时保存可撤回的和；多个表达式可以共享等价参数状态，但必须显式确定等价计划。COUNT 输出 Int64 非 NULL，SUM/AVG 输出 nullable。

| 当前 group | COUNT(*) | COUNT(expr) | SUM(expr) | AVG(expr) |
| --- | --- | --- | --- | --- |
| 至少一行，expr 全 NULL | row_count | 0 | NULL | NULL |
| 至少一个非 NULL expr | row_count | nonnull_count | 非 NULL 值的和 | 和 / nonnull_count |
| 删除最后一行 | 删除整个 group 输出与内部状态 | 同左 | 同左 | 同左 |

SUM(Int64) 输出 Int64；SUM(Float64) 输出 Float64；AVG(Int64/Float64) 输出 Float64。NULL 在输入表达式求值完成后决定是否贡献，COUNT(expr) 不要求 expr 数值类型。COUNT 溢出、最终 Int64 SUM 越界或最终 Float64 非有限均是计算错误；禁止 wraparound、NaN、Infinity 或以 NULL 代替错误。

对于每个 old/non-NULL 参数撤回一次，对每个 new/non-NULL 参数加入一次；同一 group 的所有输入 delta 先合并再检查最终输出，不把同轮算子调度顺序引起的临时溢出当作关系结果。内部 Int64 和至少使用足够宽的有符号中间表示，明确编码格式与界限；row_count/non-null count 不能降为负数，遇到不一致为 Internal/坏状态。

Float64 的可撤回和不能只保存一个普通 double。`1e16 + 1` 后删掉 `1e16` 必须留下 `1`，不能因早期舍入变成 `0`。实现采用可正负撤回的精确二进制累计器或有同等证明的确定性表示；一个可行方案是把有限 binary64 化为以 `2^-1074` 为单位的有符号整数并用固定 limbs 累加，再只在输出边界舍入。根据 Int64 最大行数界限，34 个 64-bit limbs 可覆盖累计绝对值范围；符号单独保存。AVG 在精确 numerator/count 上完成输出舍入，不能先把大和转换成 Infinity 再除回有限均值。

这一数值状态是算法选择，不需要第三方实现源码。若实现选择其他算法，必须补充可撤回误差边界和删除/抵消测试，并明确它怎样满足当前关系重算契约；不得只放宽一个通用浮点 tolerance 来隐藏丢失的小量。测试 reference 可以用独立排序/高精度实现；不能调用生产增量累计器重新包装为“独立重算”。

### One-to-one inner lookup join

binder 根据 PK/唯一性 lineage 证明左右关系每 partition tuple 至多一行：PK 恰好等于 partition tuple 的基表、GROUP BY 完整 partition 的聚合 MV、其 filter/project 后继可满足；PK 还含 fill_id 的明细表不满足。两侧 partition tuple 长度、顺序归一化后的类型以及 shard 配置必须一致；ON 对应所有 key 列，其他 join 条件放入 WHERE。不能因数据样本暂时只有一行而接受潜在 one-to-many。

join 为输入任一侧的变更注册订阅。对受影响 key，取左右 before pair 生成 old joined row，取左右 after pair 生成 new joined row。缺任一侧无输出；右侧更新/删除必须传播到左侧驱动的既有输出，不允许仅监听左侧。最多定点读取左右当前行，不扫描其他 keys。两侧在同轮都变化仍输出一个合并后的 old/new。

## 查询、初始化与 parser 构建

`QueryContext` 可以给出完整业务 partition tuple 与行/字节预算。没有显式tuple时，Binder可从已绑定WHERE的AND确定项中抽取完整partition key等于同类型常量的约束，从而支持现有设计中的SELECT ... WHERE ticker='AAPL'直接路由；不是对SQL字符串做正则解析。缺少完整唯一约束、OR涉及不同key或不能静态确定时明确要求调用方提供scope，不进行跨key查询。多输入计划使用相同 tuple。Engine 暂停该 shard 写入，按固定 revision 捕获该 key 的有界行/依赖，释放调度入口后运行相同 BoundExpr/关系语义；结果包含 ResultSchema、rows、catalog_version 与 revision。没有 ORDER BY 的 SQL 不承诺业务顺序，测试按内部身份规范化后比较。不存在分组行时 GROUP BY 查询返回零行。

所有分页读取必须在 owner 的捕获暂停范围内、相同 revision；超过预算整体失败，不能返回貌似完整的前几行。不提供跨 key 全局原子快照。已物化关系 get/query 不隐式触发外部模型。

启动前创建表/MV是首个运行路径。已有有限数据上的 MV 安装必须进入受控暂停：新定义状态 CREATING，按依赖拓扑读取完整所需有限输入、建立初始内部状态/物化行，全部初始化成功后才 READY 并恢复输入。初始化可分预算批次写入不可查询的新 object IDs，失败保留 CREATING/FAILED 及明确清理/重试状态；不得在批次中途开放查询/FSM。多 owner 部署由后续控制面协调全体暂停与激活，M2 的本地 install 不宣称跨进程在线 DDL。

缺少历史、无法完成有界捕获或布局版本不兼容时明确拒绝。初始化允许全量计算，常规输入继续使用上述增量路径；不得把每条输入都当作“初始化”。初始化和后续恢复共用稳定 Catalog/object IDs，M5 还需完成 clean-stop/dirty-run 与持久 Catalog fingerprint 的控制面验收。

SQL grammar 建议位于 `src/sql/EventFlowSql.g4`，自有 grammar 生成的 C++ 文件放 `src/sql/generated/`。当前 generator 位于临时路径 `/tmp/eventflow-cpp3rdlib.uriwoV/antlr-4.10-complete.jar`，与 `cpp3rdlib` ANTLR4 runtime 4.10 配对；该路径仅是本次生成工具位置，不是运行/普通构建依赖。生成代码随项目纳入，正常 CMake/Make 不下载、不调用 Java，不引入 ANTLR 第三方实现源码。

parser 自有适配层使用错误 listener 记录词法/语法错误，要求消费 EOF，并在错误出现后禁止构造/发布有效 StatementAst。使用 ANTLR 正常错误报告机制；不要用 BailErrorStrategy + 业务 catch 来绕开 noexcept 约定。第三方及 ANTLR 生成签名保持原样，自有 AST visitor/adapter 遵循项目函数规范。

## 行为验收（六组）

这些是 M2 待实现的验收，不是当前通过的测试。使用 cpp3rdlib GoogleTest，仅测试 target 链接。关系合约套件至少运行 MemOnly、RocksDB 和真实 HTTP RemoteStateStore；定点读取/增量复杂度与 UNKNOWN 注入可另加可观察测试 adapter，但不能取代真实后端。

1. **SQL/SDK 等价与拒绝**：以上 fills/totals/active 用 SQL 与 C++ AST 建立两个 Catalog；规范计划、Schema/隐藏键、依赖和输出一致。用另一套非市场字段和复合分区验证非硬编码。覆盖引号/alias/EOF、未知列、模糊列、非法数值提升、PK NULL、缺分区、缺 group key、重复名称、潜在 one-to-many、outer join/子查询/尾部垃圾，失败后 Catalog 不变。
2. **NULL/类型与物化身份**：NULL/0/负数/浮点/字符串/向量投影；三值真值表与短路；COUNT(*)/COUNT(column)/SUM/AVG 的 all-null 和 empty-group 区别；SELECT 可不输出 key；两个不同 PK 投影为同值仍保留两行。覆盖 Int64 溢出/除零/Float64 非有限和大整数 CAST 拒绝，未发布任何半轮结果。
3. **级联替换/删除等价**：有多个业务 key 的明细→filter/project→aggregate→threshold MV，用固定种子生成 insert、同 PK replacement、显式 delete、重复相同行和缺行 delete。每个成功提交后与独立当前基表全量 reference 比较所有物化结果及 key 数量；覆盖 NULL→值→NULL、筛选穿越、最后一行删除及 Float64 `[1e16,1]` 撤回大数。验证只改变目标 key，稳态扫描计数为零，读取/mutation 数不随无关历史行数增长。
4. **join 双向和菱形图**：缺左/缺右时为空；先左后右及先右后左相同；每侧更新/删除/重插都精确输出 old/new；同一祖先分叉汇合同时改变两侧时仅看到最终 pair，共享祖先仅运行一次。与 reference join 比较，非法 many-per-key 关系在 binder 阶段拒绝。
5. **原子提交与 UNKNOWN**：base、aggregate state、MV/cascade 同批可见；提交拒绝时全不公开。真实 HTTP server 在已提交后丢响应，Engine 保留完全相同 batch，阻止后续 apply/query；重试只确认原 receipt，不重复 delta、不增加第二次 revision，之后才处理下一输入。预算超限/定点读取失败不能留下部分物化状态；新 Engine 用同一版本布局读取 durable state 后可继续维护（不以此代替 M5 受控恢复协议）。
6. **初始化/查询/M3 接口**：先写有限数据再暂停 CREATE MV，CREATING 期间不可读，READY 时输出等于 reference，后续增量正确；初始化失败不激活半成品。单 key 查询分页 revision 一致，行/字节超预算不截断，旧 BoundPlan 版本拒绝。用显式删除模拟未来窗口撤回、用固定 DispatchContext 时间验证整轮一致；只证明接口可接，真实 Timer/TTL/FSM 仍为 M3 待验收。

## 实现与审查结论

实际接口位于 [sql_ast.hpp](../include/eventflow/sql_ast.hpp)、[catalog.hpp](../include/eventflow/catalog.hpp)、[engine.hpp](../include/eventflow/engine.hpp)，实现位于 `src/sql/` 和 `src/runtime/engine.cpp`；[SQL 示例](../examples/sql_query_demo/README.md)给出用户入口。

- Catalog 是不可变元数据定义构建器，不提供业务数据 MVCC。Engine 负责安装，InstallResult 携带实际 CommitResult；初始化使用单个有界原子批，超预算整体拒绝。
- 查询可从完整常量等值谓词推导 scope。绑定关系的 Schema 来自同一 CatalogSnapshot；公开 BoundPlan 验证结构及 catalog_fingerprint，内部查询复用已验证不可变 Catalog 与有界计划缓存，不逐次 deep clone。
- 隐藏物化身份、ResultSchema 分离、拓扑 before/最终 after、精确可撤回累计器已经采用。ExactSum 使用固定 34 limbs、280 字节 EFS1 编码，支持 double/int64 撤回及直接平均值舍入；128-bit 中间整数是当前 Linux/Clang 平台选择，不增加第三方源码。
- UNKNOWN 首次确认返回原 DispatchResult/changes，不能清空后丢失待发布变化；重复 resolve 明确失败。EngineConfig 为全局预算，QueryContext 只能收紧。HTTP RPC 使用 TCP_NODELAY。
- 数据操作拒绝 round 内重入，publish 固定 round.revision。普通 raw hook.stage 仅可写非保留扩展 S/I 状态；T、S0 和已有聚合状态不能旁路修改。hook.emit 与主输入合计受 max_rows 限制，其可达 SQL 图不能与已经稳定的主输入图相交；跨图反馈必须进入下一次 dispatch。
- 自有生成解析代码也执行 clang-format，重新生成时应用同一格式规则；普通构建不运行 Java，第三方头文件不改。

[阶段完成记录](reviews/M2-review.md)和[运行时整改复验](reviews/M2-runtime-remediation-review.md)保留五项问题的修复及独立测试。阶段 Clang 三 sanitizer 10/10 CTest、289 项及命名重构后的验证见 `/tmp/eventflow-snake-clang-test.log`；随后收尾 17/17 和内部查询成本 28 项复验以完成报告为准。ExactSum 的独立 Fraction/整数 oracle 31,230 例是数值组件证据，不替代 Engine 集成。

## 当前状态与下一步

SQL 基线已完成，不再保留“实现中”或等待合并的旧交接。窗口、生命周期、请求与真实集群已在同一接口上继续实现；各自保证范围见相应规格。最终源码的全量门禁统一记录在[核心交付](core-delivery.md#验证证据与剩余门槛)，历史 SQL 测试数字不作最终集群通过声明。
