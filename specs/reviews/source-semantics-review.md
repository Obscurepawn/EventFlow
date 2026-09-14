# 源码可读性维护：独立语义审查

日期：2026-09-14。Reviewer：`source_semantics_review`，独立 fresh-context reviewer；本次是同一轮并行全局审查中的第二份报告。仅修改本报告，没有修改生产源码、公开头文件或测试。

## 结论与发现

本次授权范围内未发现需要整改的 P0/P1/P2 问题，也未发现新增注释与当前实现相矛盾的地方。36 个手写 C++ 文件的差异仅包含 2630 个空行和 40 行英文 ASCII 独立注释。独立 Clang raw-token 比较未发现非注释 token 类型或拼写变化。

新增说明集中解释原批重试、捕获时点、固定输入、完成截止时间、依赖稳定顺序、成功结果选择、源确认与一致 checkpoint；没有把当前静态函数扩展为动态 UDF 家族，也没有将存储 snapshot 写成完整 checkpoint。允许本次注释与排版维护交付；这不是新一轮产品运行验收。

| 编号 | 严重性 | 发现与处置 |
| --- | --- | --- |
| S-01 | 信息，非阻断 | 修改前快照覆盖 `src/` 和 `.clang-format`，因此独立逐字节结论覆盖这些对象；没有 `cpp3rdlib/`、`.reference-repos/`、`include/`、`tests/` 的本轮修改前快照。不能仅凭此差异文件，额外证明这些目录的完整前后字节身份。报告保留此证据范围，未发现本次新增注释需要访问或改写它们。 |
| S-02 | 信息，非阻断 | 本轮没有编译、链接、运行测试、sanitizer 或真实 Kafka 执行证据。以下现有测试只做源码与断言核对，不记作重新运行通过。纯空行和注释修改的验收使用全文件差异、词法比较及语义审查；无需据此扩大为系统重测。 |

## 需求、合同与检查对象

已读根、`src/`、各生产模块、公开头文件、测试及 `specs/` 的适用 `AGENTS.md`，以及 [规格索引](../INDEX.md)、[本次授权](../source-spacing.md)。重点核对 [运行时合同](../runtime-contracts.md)、[SQL 合同](../sql-runtime.md)、[请求合同](../request-functions.md)、[事件请求接入](../event-request-integration.md)、[Trigger 最新授权](../trigger-api-ergonomics.md) 与当前头文件。

修改前对象为 `/tmp/eventflow-source-spacing-h_oifo0k/before/`；审查输入还包括同目录的 `source.patch` 和 `verification.json`。未将 Git HEAD 当作本轮基线。审查时再次读取实际工作树，没有只接受实施者提供的 PASS 字段。

## 独立静态证据

对修改前快照的 56 个文件逐一比较，并枚举工作树 `src/` 文件集：

- `src/` 文件集相同，没有增加或删除源文件；36 个修改文件均为手写 `.cpp`/`.hpp`。
- 使用 Python `difflib.SequenceMatcher(..., autojunk=False)` 对保留换行符的完整行序列比较：所有差异都是插入，没有替换或删除。插入仅为 2630 个空行、40 行 ASCII `//` 注释。
- 对 36 个修改文件的前后版本分别执行 `clang++ -x c++ -std=c++20 -fsyntax-only -Xclang -dump-raw-tokens <file>`，全部命令退出 0。去掉 raw whitespace/comment 与位置、行首标记后，保留 token 类型及拼写的序列完全相同。此命令只做原始词法处理，不解析 include，也不构成构建通过声明。
- 没有插入点紧跟反斜杠续行；搜索手写源码中的 `__LINE__`、`__builtin_LINE`、`source_location` 和 `#line` 无匹配，未发现空行改变行号敏感行为的使用点。
- 9 个 `src/sql/generated/` 文件（含目录指导文件）、SQL grammar 及 `.clang-format` 逐字节不变。

独立结果保存在 `/tmp/eventflow-source-spacing-h_oifo0k/semantics-review-verification.json`，`issues` 为空。对按路径排序的所有 `src/` 文件，以相对路径、NUL 和原始字节连续计算的审查对象 SHA-256 为：

```text
e5837acff053aa3726f8184d1854410def4bb9b68723bc57429d0e9ce0ac21d8
```

实施者的长期逐文件清单及 formatter 结果另见 [本轮验证](../verification/source-readability-validation.md) 和 [源码 SHA-256](../verification/source-readability-sources.sha256)。独立审查没有把实施者的 formatter 运行记作 reviewer 自己执行。

## 新注释的语义核对

以下文件行号对应审查对象；测试列表示已检查的现有测试源码。

| 说明 | 实现与接口证据 | 现有测试证据 |
| --- | --- | --- |
| UNKNOWN 保留原批及输出，确认时不重新计算；清缓存也不能解除屏障 | `engine.cpp:1036` 保存 batch/changes/catalog/time；`engine.cpp:1713` 重提同一 pending batch。`cached_store.cpp:373` 仅清 entries；`request_runtime.cpp:564` 暂存 book/result，`995` 确认后才发布。 | `engine_contract_test.cpp:651` 断言 canonical batch 相同、hook 仅一次、未确认读写受阻；`cached_store_test.cpp:201` 覆盖 invalidate 后 pending 仍存在；`request_runtime_test.cpp:335` 覆盖 claim/completion 不提前公开任务与结果。 |
| capture_match 在匹配轮生成参数，等待受理保留该 occurrence 的输入 | `event_processor.cpp:936` 调用 `make_admission` 并将 admission 保存在 intent；契约明确 arguments 按值随 intent 持久化。此处不是实际受理时重新选择事件。 | `computation_test.cpp:348` 在丢回复、确认、snapshot 重开和后续 99.0 输入之后，仍断言投影一次且结果来自原 2.0/4.0 匹配。 |
| capture_current 读取确认版本 R，受理状态与捕获根写入 R+1 | `request_runtime.cpp:821` 在受理 frame 调用 capture；`855` 和 `856` 分别记录 `context.revision()` 与 `+1`；随后同 frame 保存 request 状态。`request_machine.hpp:29` 有同一公开说明。 | `request_runtime_test.cpp:244` 直接断言两种 revision、捕获值及受理时没有任务执行；NotReady 与 UNKNOWN 回归分别位于同文件 `417`、`301`。 |
| 依赖完成后冻结节点参数，重试不会改用后来数据；deadline 等号排除成功 | `request_machine.cpp:994` 在依赖完成及必要输入检查后保存 `frozen_inputs`；`1088` 在接受匹配 attempt 的 completion 前调用 `advance`。 | `request_machine_test.cpp:287` 更改源值后断言重试 inputs 相同、旧 attempt 被忽略；`323` 断言 owner 时钟到 deadline 时拒绝 worker 的成功。 |
| UNKNOWN 恢复保存的阶段及固定处理时间；同刻数据先处理到期 | `event_processor.cpp:3799` 使用 `operation.target_us`，各 phase 传递同一 `at`；`3959` 只在存在 pending commit 时保存 operation；`4000` 在 `due <= target` 时先走 timeout round。它不承诺无 pending 的普通后续 dispatch 永远沿用旧时间。 | `event_request_test.cpp:272`、`514`、`539` 覆盖受理/source/ACK 的原批确认；`trigger_pattern_test.cpp:163` 有排除式 deadline 恢复测试。 |
| Catalog 依赖先定义、按顺序稳定，菱形后继只看到最终两支结果 | `catalog.cpp:1482` 校验定义顺序，并使用已重建前缀校验各 plan；`1559` 先对现有 Catalog bind 后 append。`engine.cpp:1364` 按此顺序处理受影响关系。 | `engine_contract_test.cpp:498` 在同轮两次源变化后断言 joined 只发布一次最终 `(6,10)`，不出现左右支中间组合。 |
| Latest 按当前 candidate epoch 内的 admission sequence 选择成功结果 | `engine.cpp:1332` 先检查 epoch 和 `SUCCEEDED`，`1339` 拒绝更小 sequence；相同 sequence 绑定不同输出返回冲突。 | `request_latest_test.cpp:257` 先完成新请求、后完成旧请求，断言 winner 仍为 sequence 2，随后取消更晚请求也不擦除最后成功值；`277` 区分单函数成功和整个请求成功。 |
| Broker 只推进已完成前缀；ACK 不确定时保留同一 token | `message_source.cpp:158` 遇未完成记录停止前缀；commit 失败后保存 `pending_ack` 并保留 inflight。`message_source.hpp:76` 要求计算持久确认后 ACK，并明确同 token 重试。 | `message_source_test.cpp:156` 覆盖乱序完成前缀；`352` 注入 Kafka protocol commit 错误，断言其他 ACK/seek 受阻并重试原 token。此处是 librdkafka mock，未声称真实 broker 集成。 |
| Coordinator 保留原投递和分配时间，源进度依赖 input_committed 而非 HTTP/整轮成功 | `coordinator_service.cpp:476` 先 save pending delivery/time 再转发；`503` 明确读取 `input_committed`/duplicate，再更新 progress。 | `coordinator_service_test.cpp:315`、`461`、`503` 分别保留原输入重试、原时间和未确认输入时钟屏障的回归。 |
| checkpoint 先 quiesce 和核对源切面，再冻结 manifest 和创建 KV snapshot | `coordinator_service.cpp:630` 的 fanout/verify/save 顺序与注释一致；`566` 的 `finish_checkpoint` 核对 snapshot 中 control revision、worker revision 和 receipt，之后才保存 complete 标志。 | `coordinator_service_test.cpp:340` 检查丢回复重试、冻结 revision 与恢复源前缀；`core_cluster_test.cpp:410` 定义真实 Rocks 全进程恢复，并断言 source progress、水位、Catalog 与 worker revision。 |

其余新增注释也已逐处核对：`key_codec.cpp:55` 的符号位翻转与大端排序一致；`exact_sum.cpp:21` 的二进制最小单位与整数移位、精确撤回及最终舍入一致；`catalog.cpp:262` 保留投影隐藏身份；`expression.cpp:102` 符合左操作数决定性短路与 NULL 三值逻辑；`computation.cpp:348` 的私有 Catalog/Registry 副本在失败时不发布；`state_store.cpp:516`、`525` 将数据与 receipt 放在同一 RocksDB WriteBatch 并启用 WAL sync。对应测试入口包括 `exact_sum_test.cpp:38`、`engine_contract_test.cpp:349`、`expression_test.cpp:191` 和 `computation_test.cpp:236`。

## 剩余边界与交接

没有待修复的语义发现。实施者可将本报告与同轮可读性 reviewer 的报告合并记录，并完成本次规格与验证状态更新。若之后再修改注释或空行，应针对实际最终对象刷新差异/词法/清单检查；无需仅因本报告完成而发起第二轮 fresh-context 审查。

本报告不重新验收产品功能、动态扩展、线上接管、真实 Kafka 或性能，也不把“存在测试”改写成“本轮测试通过”。
