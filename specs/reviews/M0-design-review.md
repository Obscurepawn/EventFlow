# M0 独立设计审查与处置

状态：M0 通过。双独立 challenge 后由 m0_gate_review（fresh context）复核；最后一项身份绑定修订已经落实。
审查对象：docs/feature_computation_architecture.md；原始 PDF；当前目标。
审查者：fresh_design_semantics、fresh_design_storage，均以 fork_turns=none 启动，未参与原设计。

## Findings 与决定

两份独立review共提出13条意见，合并为下列9个契约缺口。核心架构保留，不加入已排除的MVCC、HA或跨key执行。

| ID | 意见 | root判断与处理 | 对应契约 / 验收 |
| --- | --- | --- | --- |
| R01 | 请求容量满时可能阻塞完成事件并死锁 | 接受；外部准入与内部完成通道分离，dispatch不等待额度，Pending有界 | C3；额度1压力/完成/超时活性 |
| R02 | KV commit去重不能代替源输入去重 | 接受；delivery identity单独持久化，在MV/FSM之前去重；业务修订单独处理 | C1；重复Trade/News/新版本 |
| R03 | 一个源partition跨多个shard，offset确认可能跳过空洞 | 接受；Router只确认连续完成前缀，shard局部去重 | C1；AAPL阻塞、MSFT完成、重放 |
| R04 | 处理时间/截止点/恢复Timer语义不确定 | 接受；每轮固定时间，到期先于同时间数据，持久deadline，停机计时 | C2；截止前/等于/之后、重启过期 |
| R05 | attempt胜出、失败传播、once与终态不明；物理一次过强 | 接受；当前attempt首个终态有效，旧attemptfencing，祖先失败下传，逻辑一次发布 | C7；超时重试后旧结果/失败回包 |
| R06 | A/B/C/D缺少候选/Ready/先后与miss具体规则 | 接受；固定示例应用契约，单当前候选，B受理前置、C/D明确政策 | C8；可数occurrence/request轨迹 |
| R07 | 手动重启已承诺而恢复又称可选 | 接受；P1受控quiesce/resume必须，dirty-run禁止静默续算，非自动HA | C9；持久KV重启/clean-stop恢复 |
| R08 | Table TTL删除是否撤回未定，可能违背MV=Q(Table) | 接受；Table TTL是逻辑Delete，生命周期累计使用明确Source | C5；TTL与独立全量重算 |
| R09 | 同步UDF异常和永久错误处置不明 | 接受；准入错误拒绝，dispatch永久错误fail-stop shard，不确认输入 | C3；抛异常/超限与其他shard隔离 |

reviewer关于“有限SQL/Trigger子集要冻结”的建议同样接受，具体在C5/C6列出支持和明确拒绝的边界。独立审查没有发现需要替换统一MV/FSM/sharedKV主架构的理由。

## 新增用户约束

- cpp3rdlib集中预编译库与头文件；CMake/Makefile构建。
- 外部依赖抽象与MemOnly/simple组件；真实协议集成仍保留。
- 尽量提供完整运行依赖与镜像。
- Bear生成实际compile_commands.json并验证clangd。

这些要求已进入主spec与C4/C10，不当作可选文档建议。

## Gate

- [x] 两份独立fresh-context challenge完成。
- [x] 对每项合理意见记录判断和具体契约。
- [x] m0_gate_review 复核 R01–R09，提出单项 commit_id/历史回执问题；C4 增加 owner/shard/expected_revision 规范身份绑定。
- [x] 接口与后续milestone验收已明确；实现证据由相应milestone提供，不冒充M0已有代码。


## Fresh gate 补充发现 R10

只保存最后回执时，任意commit_id可与新revision组合复用，不能保证历史身份不复用。接受并将commit_id绑定owner/shard/expected_revision；服务端验证绑定，M1增加历史id复用测试。reviewer明确该修订后可进入M1。
