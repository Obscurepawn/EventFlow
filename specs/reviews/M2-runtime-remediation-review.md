# M2 运行时整改独立复核

日期：2026-09-14。Reviewer：`m2_runtime_gate_close`（独立上下文）。结论：**PASS：原运行时审查 R1–R5 已闭合，本次未发现剩余运行时 must-fix**。

本记录复核 [M2-runtime-fresh-review.md](M2-runtime-fresh-review.md) 的五个反例，依据当前 `AGENTS.md`、`specs/sql-runtime.md` 和运行时/查询入口源码。该 PASS 仅关闭原五项运行时整改，不代替 M2 其他领域或最终交付的全局审查。M3 的缓存、MessageSource/Kafka 和 M5 MetaService 不属于本 gate 缺陷。

## 逐项处置

| 原问题 | 当前实现与验收证据 | 处置 |
| --- | --- | --- |
| R1：hook 重入导致陈旧 round 覆盖新 revision | `src/runtime/engine.cpp:410` 的 `ready()` 拒绝 active；apply_many、install、query、resolve 各入口的 `OperationGuard` 覆盖操作全程。`:436` 的 publish 检查 round/Engine revision 相同，commit_id 与 expected_revision 固定取 round.revision。回归覆盖内层 apply 拒绝、外层一次一致提交、向外传播错误时整体不提交，以及查询/安装/确认入口重入。 | CLOSED |
| R2：hook 图与原 DAG 汇合留下下游中间结果 | `src/runtime/engine.cpp:738` 比较原 inputs 根与 emitted 根的完整可达关系图，交集导致整轮失败且不调用 commit。原 roots 在输入是 no-op 时仍保留。回归覆盖 a/b→j→d 原反例、原 root 重发、原输入无 changes 但后继相交，以及独立请求子图的正常单批提交。 | CLOSED |
| R3：raw stage 旁路改写关系/引擎状态 | `src/runtime/engine.cpp:359` 仅接受当前 shard 的 S/I 扩展域，拒绝所有 T 行、object 0 和当前 Catalog 的聚合 state_id；内部执行器仍使用私有 RoundState::stage。回归覆盖基表覆盖、MV 删除、S0 engine/其他键和聚合状态覆盖均无持久副作用，同时合法扩展 S/I 状态继续提交。 | CLOSED |
| R4：同 version 的其他 Catalog 计划被误执行 | `src/runtime/engine.cpp:814` 在读取数据前调用完整 plan 校验；`src/sql/catalog.cpp:258` 同时检查 version 与完整规范定义 fingerprint。结构验证同时检查表达式、聚合参数、布局、输出及 scope。回归验证 prices/credits 原反例拒绝且健康查询继续可用，也覆盖移除必要 aggregate argument 的计划。另一次独立只读交叉核查未发现普通合法 API 的正确性/UB 缺陷。 | CLOSED |
| R5：hook emit 绕过 max_rows | `src/runtime/engine.cpp:723` 把原 inputs 数记入 round；`:379` 在 emit 写入前检查同一计数，成功 emit 后计数递增。回归覆盖空输入追加两行、重复同 identity、原 inputs 加 emit 共用上限、精确边界一行成功；拒绝路径确认 revision、持久 rows 和提交次数均不变。 | CLOSED |

## 本 reviewer 实际验证

- 确认主构建为 Clang 18.1.3、Debug；ef_engine 的编译选项包含 `-fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`。相关库和测试可执行文件时间晚于对应源码。
- 亲自执行 `build/bin/engine_review_test`，显式设置 `ASAN_OPTIONS=detect_leaks=1:halt_on_error=1`、`UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`、`LSAN_OPTIONS=exitcode=23`：**54/54 通过，退出码 0**，覆盖 Memory、Rocks、真实 HTTP RemoteMemory，未出现 sanitizer 诊断。
- 亲自执行既有 `build/bin/engine_contract_test`，使用同样 sanitizer 环境：**48/48 通过，退出码 0**，用时 132.455 秒，未出现 sanitizer 诊断；涵盖三 adapter 的随机撤回 reference、join/菱形、UNKNOWN、预算和初始化。主任务报告的 CTest 10/10 不冒充本 reviewer 亲自执行的证据。
- 本次没有修改生产源码或主构建，没有使用 GCC；未新增实验代码。

审查快照 SHA256：

```text
src/runtime/engine.cpp       7e78a336e8cb1585fc7535c109c6030f8f9fd06f6fb88566d6697f1bea866cc9
tests/engine_review_test.cpp 48e40462b9af5e7d99d7b0cecb18301ebbf8da2b8afc323fd4f7cddd71491027
src/sql/catalog.cpp          9ef59075015555f6d45e20d448ec74b657be954cb2b8041d78d00a8d4428e9e3
src/sql/binder.cpp           fdda1ce0fc101d376d4e4468a1694ca1ff7a3574caa9901cec8508a04f6acc23
```

## 非阻断观察与边界

当前 `validate_bound_plan`（`src/sql/catalog.cpp:391`）每次查询会调用 `Catalog::FromSnapshot` 完整重校验并深拷贝 Catalog；`query_sql` 的 `bind_select` 路径（`src/sql/binder.cpp:543`）还会先做一次。这与规格 §M2实现细化中“不逐次深克隆全部Catalog”的成本约定有偏差。已告知 root；可在内部保留已经验证的 Catalog 路径，同时让公开接口继续检查不受信任的 Snapshot/Plan。此处没有以未测量的性能推断新增正确性 must-fix，也没有把五项修复的 PASS 扩大为所有资源/性能约定已完成。

本 gate 只证实扩展 raw stage 不能覆盖当前运行时保留状态；后续扩展 object ID 的分配和跨组件治理仍须由后续规格统一。hook emission 的无交图约束已写入 M2 规格，因此本复核按整体拒绝相交图的既定语义验收，不承诺 hook 内通用反馈事务。
