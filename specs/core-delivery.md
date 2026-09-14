# 核心交付与最终验收

Status: complete（核心、质量门禁、文档与运行包验证完成）
Owner: root
Start: 2026-09-14 04:08:34 UTC / 12:08:34 Asia/Shanghai
Core deadline: 2026-09-14 09:08:34 UTC / 17:08:34 Asia/Shanghai

## 交付边界

用户要求优先在五小时内完成真实核心，再完成端到端示例；独立工作可以并行。部署为一个 KV、一个 Coordinator/Meta 和多个 Compute，以业务 key 静态分片，状态放在共享 KV。无 HA、MVCC 或跨 key 计算。

当前使用明确版本的 `feature_computation` 配置，基表/MV、条件/Clock、固定输入请求和 Latest 共用 Catalog。稳定计划 ID 的业务含义为文本编码 A、首次就绪预测 B、条件确认 C、周期预测 D。内置函数不等于动态用户扩展。

| 核心要求 | 当前状态 |
| --- | --- |
| 真实三类进程，至少两个 Compute 分别处理业务 key；SDK 只配置 Meta 地址 | 已实现，真实进程基线和示例已运行 |
| Meta 持久管理核心 Schema/Catalog、配置、路由、源前缀、水位和 checkpoint | 已实现 |
| 启动前 SQL DDL 可追加表和级联 MV；全部 Compute 安装确认后开放输入 | 已实现；运行后在线 DDL 明确拒绝 |
| 原投递身份重试；源确认与后续计算背压分开 | 已实现；未确认原输入不能被 Clock 越过 |
| 输入到 MV、触发、固定输入异步函数、Latest、SDK 查询 | 已实现；Compute 使用真实线程函数池 |
| 暂停、一致 checkpoint、Rocks 全部进程停止后恢复并继续处理 | 已实现；恢复验证数据与 control shard 的冻结切面 |
| Clang 三 sanitizer、Release、格式、clangd、示例、最终独立审查闭环 | 45/45 全量测试、Debug/Release、六个示例和独立解压运行包全部通过 |

## 保留的后续产品范围

UDF/UDTF/UDAF 上传与算子执行、在线 Schema/图迁移、复杂多 Job/unregister、Kafka Compose、深度性能和完整镜像验证后置。Process Function 及专门调研已移出当前交付。

有限 E-NFA 与同轮 observer 基础已有实现；通用 host 绑定及完整观察输出预算调度后置。观察采用产生变化的 owner 轮内主动读取声明 MV、同步推进规则状态和 Trigger，不建设通用 CDC、持久 witness 队列或独立逻辑观察通道。现有简单条件和 Clock 路径必须真实运行。

完整目标与未完成验收保留在[产品范围](product-scope.md)。未实现能力明确拒绝或不暴露，不提供返回成功的占位方法。

## 核心接口与一致性

- 使用 HTTP/JSON；所有 64 位版本/revision/offset/time 在 API 中采用规范十进制字符串，shard/partition/count 为整数，业务 Row 复用 Value codec。成功返回裸 JSON，失败返回 `{error,error_code}`；ingest 另有 `{code,message}` 与独立的 `input_committed`、`duplicate`。
- Compute 提供 `/v1/compute/install|ingest|pump|query|request|pause|resume|quiesce|status|checkpoint`；Meta 提供 `/v1/meta/discover|catalog|ddl|deploy|ingest|pump|query|request|pause|resume|checkpoint|status`。SDK 的 `drain` 在固定时间重复已确认成功的有界 pump；不自动重发不确定控制操作。
- 逻辑数据 shard 数为 N，Meta control shard 为 N，KV 物理 shard 数为 N+1。worker 安装必须校验全局 shard_count、Catalog fingerprint、版本和 DDL。静态重启不表示在线再均衡或自动接管。
- 每个 shard 只有一个串行 owner。模型 callback 在线程池执行；owner 轮询有界并在 slice 之间释放锁。单次 Compute pump 保持一秒总软预算，未完成时返回 `work_remaining`，不以延长 owner 阻塞隐藏异步行为。
- 源输入先同批推进 base/MV、FSM 状态、Trigger/PendingIntent 与源进度。后续受理在已确认当前 R 捕获，R+1 同批保存固定输入、Manifest/请求状态、occurrence 消费和 once。Manifest 记录实际 capture/admission revision、时间和受理序号；SQL 查询 revision 是另一次当前读取版本。
- 原投递及其分配时间在 pending 时保留。未确认的新输入阻止 Clock 超过该固定时间；已确认旧投递的 replay 单独识别。完整原批 UNKNOWN 必须先确认，缓存不能绕过。
- checkpoint 冻结期间除完成同一 checkpoint 外禁止控制修改；快照必须匹配 worker revision/receipt 和冻结 control revision。Rocks restore 创建新活动目录，不能在旧 live 数据库上仅添加 restore 标志。WAL 保留。
- occurrence 被消费后尚未独立保存完整历史 TriggerWitness。当前审计可读请求身份、阶段、版本/时间和捕获输入，不承诺还原每个历史 guard 读值。

## 示例与开发入口

六个目录均有独立 `run.sh`，自动配置、按需编译并运行；新目录使用 Debug/sanitizers，已有 Release 配置保留，运行包缺少 CMakeLists 时使用随包二进制。支持 `EVENTFLOW_BUILD_DIR` 和 `EVENTFLOW_SKIP_BUILD=1`。示例 README 使用中文，代码、日志和代码块仍为英文 ASCII。

- [新闻特征](../examples/news_feature_demo/README.md)：原题六字段、嵌入式自定义 MV/条件/Clock/共享函数 DAG。
- [SQL 查询](../examples/sql_query_demo/README.md)：基表、聚合与级联过滤。
- [独立特征计算](../examples/feature_computation_demo/README.md)：依赖图、四条触发路径、候选修订/到期。
- [条件触发](../examples/condition_trigger_demo/README.md)：真实四进程、级联 MV 与一次性条件确认。
- [Clock](../examples/sdk_clock_polling_demo/README.md)：无新输入时触发、固定输入版本与源位置不变。
- [checkpoint 恢复](../examples/checkpoint_recovery_demo/README.md)：两个 Compute 的不同 key、两代真实进程与恢复后继续计算。

根目录 Bear 数据库实际捕获 81 个编译条目；clangd 使用 `.clangd` 指定的 `build/compile_commands.json`，避免改名后的旧条目推断错误 include 路径。完整解析/索引检查使用 `--tweaks=DefineInline`，不把已知 ExtractFunction 工具内部问题当作代码诊断。

## 验证证据与剩余门槛

最终证据集中在[验证记录](verification/final-validation.md)，不再重复保留旧构建手记。

- 具名初始化调整后的完整 Clang Debug/Bear 构建通过，81 个实际编译条目；Release 使用 O3/ThinLTO，无 sanitizer/GTest 链接。
- 最终 **45/45 个 CTest 目标通过，414.61s**，ASan/UBSan/LSan 开启；六个 Release run.sh 示例退出 0，SQL 脚本还通过空目录首次构建。
- AST 聚合初始化核对 344 处具名、58 处空初始化，0 剩余位置初始化；noexcept 声明/lambda 和 clangd 核对记录在验证目录。
- 134 文件源码清单与 Debug/Release 服务哈希已经保存；两个独立全局审查的三个去重严重发现及异步执行问题已整改并通过最终组合验证。
- 读者 README/实现导读、六个示例和三张详细 PNG 已落地；本地嵌入式新闻 API 与远程固定配置的能力边界明确区分。

运行包已经重新生成，清除源码路径覆盖后在独立解压目录运行六个示例全部成功，清单和动态库路径均验证通过。核心交付完成；完整产品后置项保持开放。后续高层Trigger API先独立设计，不改变当前运行时完成状态。
