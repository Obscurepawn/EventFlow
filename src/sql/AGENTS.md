# SQL 与 Catalog 模块

本目录继承上层 `AGENTS.md`；修改前阅读 [规格索引](../../specs/INDEX.md)和 [SQL 合同](../../specs/sql-runtime.md)。
规格中的早期接口草图仅供追溯，实际签名以 [catalog.hpp](../../include/eventflow/catalog.hpp)和 [sql_ast.hpp](../../include/eventflow/sql_ast.hpp)为准。

## 职责与入口

- `EventFlowSql.g4`：项目 SQL 子集的唯一 grammar；`generated/` 保存对应 ANTLR lexer/parser 产物。
- `sql_parser.cpp`：`parse_sql`、词法/语法错误转换、自有 AST 构造及 AST builder helpers。
- `binder.cpp` / `binder_internal.hpp`：列与类型绑定、分区证明、隐藏身份布局、聚合和 lookup join 计划。
- `expression.cpp`：类型化表达式求值、SQL NULL/短路、可见行与持久行之间的转换。
- `catalog.cpp`：SQL/SDK 统一定义、Function/Candidate/RequestStatus/Latest/Observation 元数据及快照验证。
- `catalog_codec.cpp`：Catalog 与计划的规范编码、解码及恢复一致性检查。
- CMake 中 `ef_sql` 链接 bundled ANTLR runtime；`ef_catalog_codec` 依赖 `ef_sql`。
- 运行时激活、持续维护和提交属于 `src/runtime/`；Catalog 定义成功只表示元数据成立。

## 解析与绑定约束

- 每次只接受一条语句、可选末尾分号和 EOF；未知或未支持的语法必须明确拒绝，不能接受后忽略。
- 保留合法 UTF-8 检查、NUL 拒绝、SQL 字节/token/嵌套预算；语法错误后不得发布有效 AST。
- 普通名称按 ASCII 小写规范化，双引号名称保留精确文本；AST 自有字符串与节点，不借用 ANTLR 对象。
- SQL 与手写 AST 必须进入同一 binder；C++ 入口不能绕过列、类型、NULL、分区或身份校验。
- 查询 `ResultSchema` 与持久化 `Schema` 分开；隐藏 `__ef_` 身份列不得进入 `SELECT *` 或用户引用。
- 投影保留完整行身份；相同可见值的不同输入仍是不同关系行，不能意外获得 DISTINCT 语义。
- GROUP BY 覆盖完整业务 partition key；lookup join 必须证明完整对应分区键和两侧 `unique_per_partition`。
- 不隐式提升 Int64/Float64；显式转换须精确可表示，溢出、除零、非有限数值返回错误。
- 保留 SQL 三值逻辑及从左到右的决定性短路；WHERE 只有 TRUE 通过，未执行分支不产生求值错误。
- Catalog 先验证完整候选再发布；稳定 IDs、关系 provenance、版本和 fingerprint 必须能通过 snapshot 重建验证。
- RequestCapture、FunctionView、RuntimeStatus、Latest 保持受控写角色与 identity domain；不能新增独立业务 DAG。
- 修改静态函数或自动请求关系时联查 [Trigger 最新授权](../../specs/trigger-api-ergonomics.md#10-当前状态与交接)和 `TriggerSet::bind` 的原子装配。

## 修改与验证

grammar 变化必须依 [generated/AGENTS.md](generated/AGENTS.md)用匹配版本再生成，并同步自有 AST adapter、binder 与行为测试。
不要编辑生成签名以满足项目命名或 `noexcept`；业务错误转换写在自有适配层，避免异常控制流。
从仓库根目录对已由 Clang 配置、构建的 Debug 测试目录执行相关组：

```sh
ctest --test-dir build --output-on-failure -R '^(sql_parser|catalog|expression)_test$'
ctest --test-dir build --output-on-failure -R '^(catalog_codec|catalog_observation|function_catalog|latest_catalog)_test$'
ctest --test-dir build --output-on-failure -R '^engine.*_test$'
```

解析变化覆盖拒绝尾随语句、转义/引号、优先级和预算；绑定变化覆盖 SQL/SDK 等价、分区/身份伪造拒绝及失败不改变 Catalog。
计划或表达式变化应验证实际增量执行与恢复，不能只比较 AST；这里只列验证入口，不宣称执行结果。

## 规格导航

- [SQL 与增量关系](../../specs/sql-runtime.md)、[运行时合同](../../specs/runtime-contracts.md)。
- [请求与函数](../../specs/request-functions.md)、[Latest](../../specs/latest-selector.md)、[同轮观察](../../specs/change-observation.md)。
