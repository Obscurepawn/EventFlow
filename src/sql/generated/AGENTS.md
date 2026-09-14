# SQL 生成代码

本目录继承 [SQL 模块规则](../AGENTS.md)及上层规则；仅保存项目自有 grammar 的生成产物。
源文件是 [EventFlowSql.g4](../EventFlowSql.g4)，当前 lexer/parser 文件头标记 `ANTLR 4.10`。

## 文件职责

- `EventFlowSqlLexer.cpp` / `.h`：token 识别与 lexer ATN。
- `EventFlowSqlParser.cpp` / `.h`：语法规则、上下文类型与 parser ATN。
- `*.tokens` / `*.interp`：同一生成批次的 token 和解释元数据，需与 C++ 产物保持一致。
- 自有 AST 转换和错误到 Status 的适配位于 [sql_parser.cpp](../sql_parser.cpp)。
- CMake 的 `ef_sql` 直接编译现有两个 `.cpp`；常规构建不运行 Java 或生成器。

## 再生成约束

- 必须从项目 grammar 使用 **ANTLR 4.10** 再生成，与 `cpp3rdlib` 中 bundled runtime 4.10 配对。
- 生成器版本、校验摘要和依赖来源见 [依赖说明](../../../cpp3rdlib/README.md)；不要使用机器上未核实版本的 `antlr4`。
- JAR 只作为开发时生成工具保留在项目外；不要提交 JAR、第三方实现源码或添加构建时下载。
- 不手改 token 编号、serialized ATN、生成逻辑或 generated signatures；业务修改回到 grammar 或自有 adapter。
- 保留生成器要求的函数命名、异常签名和 framework API，不批量添加 `noexcept` 或重命名为 snake_case。
- 再生成后可使用项目既有格式工具作机械排版；格式调整不能混入语义修补。
- 保持当前仅 lexer/parser 的产物范围；增加 listener/visitor 需有实际调用方和构建说明。

以下命令在 `src/sql/` 执行；先将 `ANTLR4_10_JAR` 指向已验证的 4.10 JAR，普通构建无需此变量：

```sh
java -jar "$ANTLR4_10_JAR" -Dlanguage=Cpp -no-listener -no-visitor -o generated EventFlowSql.g4
```

## 再生成后的验证

核对生成文件头版本、全部产物 diff、parser rule 与自有 adapter 的对应关系，并遵守根目录格式门禁。
随后从仓库根目录用 Clang/CMake 构建相关测试，对已构建的 Debug 目录执行：

```sh
ctest --test-dir build --output-on-failure -R '^(sql_parser|catalog|expression)_test$'
```

重点检查错误输入不产生有效 AST、EOF、运算优先级、字符串与引号名称、UTF-8 以及 parser 预算。
新增语法还须覆盖 binder 拒绝路径和实际执行语义；生成成功不能证明 SQL 能力实现完成。
本文件列出后续工作要求，不记录已经再生成或通过测试的结论。

详见 [SQL 实施合同](../../../specs/sql-runtime.md)与 [规格索引](../../../specs/INDEX.md)。
