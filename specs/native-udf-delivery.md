# 生产 C++ UDF 上传与首次部署

Status: complete
Owner: root
Updated: 2026-09-16

## 最新授权

用户要求修改生产代码，并通过 example 端到端验证。用户明确选择本轮场景：**服务启动后，通过 SDK 上传 UDF、提交定义并开始计算**。同一构建环境下 `.so` 可直接使用 EventFlow C++ 数据结构。

## 范围与设计决定

- 将示例接口/加载器提升为生产 `udf.hpp`/`udf.cpp`，使用 C++ FunctionSignature、FunctionInputs、StatusOr<Row>。固定 C linkage 仅定位入口。受信任原生函数使用现有 Compute 线程池；本轮不引入独立 Worker 或声称沙箱隔离。
- SDK 上传受限大小的 `.so`；Coordinator 在 STAGING 校验构建身份、签名和不可变版本，将函数元数据和原始制品在共享 KV 的 control shard 同批提交。Compute 从共享 KV 读取精确制品，校验内容身份后加载。checkpoint 自然包含制品，但还需测试恢复重新加载。
- 新增简单的结构化计算提交 API：DDL、输入关系、当前视图字段 capture、显式有限 session、source 条件/Clock 和 UDF 引用。复用 TriggerSet、单 Catalog、Request 固定输入和 owner 提交。首版条件为有类型的列/常量比较组合，不接入任意 SQL 标量函数、CEP DSL 或任意回调序列化。
- 动态定义只在首次部署前 STAGING 提交，提交原子替换 staging 定义；部署后拒绝修改。保留原静态 ComputationModule 和 SQL-only 路径。不存在“已运行计算图在线迁移”的隐含承诺。
- 同函数 ID/version 不允许换字节；相同上传和相同提交重试幂等，UNKNOWN 先确认原批。制品摘要参与函数 implementation identity 和 Catalog 恢复身份，不能只按路径定位。
- SDK 和服务按预算限制制品、定义、签名、字段、函数数和请求大小。使用 Clang、三 sanitizer 与 Release O3/LTO；不下载第三方实现。

## 验收

- [x] 生产加载器与动态绑定器有 GoogleTest，先验证未实现行为失败，再实现。
- [x] SDK 连接已启动的通用 eventflow 服务，上传 `.so`、提交表/MV/Trigger/请求定义、deploy，无业务函数静态链接到 Server。
- [x] 真实 Coordinator、共享 Rocks KV、两个 Compute 和消息源进程；客户端只发布消息/部署/查询，后台主动消费并执行 UDF，两个 shard 精确结果通过。
- [x] 检查重复注册、版本冲突、缺失函数、错误 capture Schema、部署后拒绝改图、库生命周期、固定输入及无新消息 Clock。
- [x] 原静态和 SQL-only 路径相关回归；一致 checkpoint 与基于其制品重新加载的恢复验证。
- [x] Debug/Release 构建、相关回归、真实 example、安装运行、格式与静态约束有 fresh evidence。
- [x] 一轮并行多位独立 fresh-context 全局审查，处置后定向验证，记录 specs/reviews。

## 工作状态

| 工作 | 状态 |
| --- | --- |
| 加载器、制品身份与合同测试 | 完成；RED/GREEN 与错误路径验证通过 |
| 结构化定义绑定、SDK 与服务注册 | 已实现；真实 HTTP 注册/绑定/部署定向测试通过 |
| 多进程 example、恢复、回归 | 完成；65/65、Debug/Release 与安装目录端到端恢复通过 |
| 独立审查与交付证据 | 完成；同轮两位 reviewer，发现与自查项已定向处置 |

## 当前状态与交接

工作树已有上一轮 cpp_udf 本地示例，保留其历史记录并迁移到生产接口。此前 specs 中“动态 UDF 后置”由本次授权扩展为以上有限切片；UDTF/UDAF、Process Function、任意运行中图迁移仍后置。

生产变更包括 include/eventflow/{udf,submitted_computation,cluster_client,trigger,compute_service,coordinator_service}.hpp、生成 ABI 标识模板、src/runtime/{udf,submitted_computation,trigger}.cpp、src/client/cluster_client.cpp、三个服务入口、根 CMake 和对应模块指南。测试增加 udf_test、submitted_computation_test、无效插件 fixture 与已有 Coordinator UNKNOWN 测试。新增 examples/native_udf 的定义、验证客户端和五进程/恢复脚本；原 cpp_udf 示例复用生产加载器。

首轮证据：`/tmp/eventflow-native-red.log` 确认加载器/摘要未实现失败；`/tmp/eventflow-native-green.log` 确认加载器与本地示例通过。绑定测试首次未配置运行库路径，修正 RPATH 后 `/tmp/eventflow-native-bind-red.log` 确认未实现绑定失败。中间修复 candidate 字段 ID 冲突、输出 ticker 与 host 身份字段冲突、缺少 key_codec/udf 头文件及 const Json 返回值；这些失败不作为已通过证据。服务 RED 为 `/tmp/eventflow-native-service-red.log` 的未知注册字段，GREEN 为 `/tmp/eventflow-native-service-green.log`。

首轮真实 Debug 示例 `/tmp/eventflow-native-e2e.log` 与 `/tmp/eventflow-native-udf-PFnt09/`：首次计算 51.5/128、周期 53、固定输入保持；checkpoint 后四个数据库进程重启，从 KV 重载 UDF，新增输入得到 54.75，旧阈值仍 51.5，全部进程退出 0。恢复客户端传入不存在的 UDF 路径且不注册。该证据仍需由最终构建与回归刷新；下一步完成全量回归、Release/安装验证及同轮独立审查。


## 最终交付证据

| 门禁 | 最终结果 |
| --- | --- |
| Clang Debug 构建 | 全项目构建 exit=0，address/undefined/leak 和 failing diagnostics；最后宿主刷新 `/tmp/eventflow-native-host-final-build.log` 无 warning/error |
| 全量回归 | `/tmp/eventflow-native-full-ctest.log`，65/65 通过，exit=0，806.50 秒；包含原静态集群、SQL-only、Compute、Coordinator、UNKNOWN、原业务与新增原生集群恢复 |
| 新增集群例 | 全量 CTest 中 native_udf_example 47.49 秒通过；首轮及最终流程均是五个真实进程与四个数据库进程 checkpoint 重启 |
| 定向边界 | `/tmp/eventflow-native-codec-final.log`，加载器、提交定义、本地例 3/3 通过；`/tmp/eventflow-native-unknown.log` 单独确认原子注册 UNKNOWN 重试通过 |
| Release | `/tmp/eventflow-native-release-delivery-build.log` 全构建 exit=0；O3/NDEBUG/ThinLTO，无 sanitizer；`/tmp/eventflow-native-release-final-e2e.log` 首次部署与恢复通过 |
| 安装目录最终验证 | `/tmp/eventflow-native-install-final.log`：安装至 `/tmp/eventflow-native-udf-install`，独立 run.sh 首次结果 51.5/128、周期 53、恢复后 54.75，旧阈值 51.5，全部服务退出 0；运行数据 `/tmp/eventflow-native-udf-5BUOpK` |
| 格式与静态检查 | 全项目 clang-format 21.1.2 检查、git diff --check、两份 UDF shell 语法、25 个变更代码/脚本 ASCII、新文档链接和围栏通过 |
| clangd 语法/索引 | 两个新增生产 cpp 在 `--tweaks=none --check` 下均 0 errors；默认 check 的编辑器重构动作自测分别出现 5/6 个 tweak 失败，AST 编译无诊断，不将这部分工具功能宣称通过 |
| 独立审查 | [合同](reviews/NATIVE-UDF-contract-review.md)、[交付](reviews/NATIVE-UDF-delivery-review.md)，同轮并行 fresh-context；无新增阻断实现问题，一项过时测试指南已同步 |

本轮额外自查有真实 RED/GREEN：非法 UTF-8 序列化原先导致终止；UInt64 最大函数版本被有符号解析拒绝；低 mutation 预算原先使 Coordinator 进入 failed。分别修复 UTF-8/decoder 处理、精确 UInt64 和提交前容量检查，扩展测试均通过。原审查报告保留检查快照与未独立运行项，并追加实施者处置；不再发起审查轮次。

最终实现范围是“启动后的首次部署”：原生上传默认关闭，启用后接收受信任同 ABI 的 <=1 MiB 库；SDK 提交结构化定义，Compute 线程池执行，KV 保存及恢复原制品。没有独立 Worker、任意依赖分发、运行中图变更、SQL 标量 UDF、UDTF/UDAF 或 Process Function；消息服务在恢复时保持运行，不宣称其宕机持久恢复。

完整源文件与 Release 二进制身份记录在 `/tmp/eventflow-native-delivery-sha256.txt`。所有本轮必要工作已完成；后续扩展需新的授权与规格，不以本地原型或本次有限切片代替完整 UDF 家族验收。

身份清单 SHA-256：`131f212aa97f42e44bb16af4fb8be5a9c5a92f9be344112af72f06bb59856c52`。Release Server：`ad1e49d8d86ab159df73acb3b6fc4b1abf49ccff15ed415e577bb3b290811942`；验证客户端：`30f223ce3b87cc990a852a38564ba209bf371fcb56c461f065a314756ea193c5`；UDF：`777a3eb4746a108bc112a82477052fdc591576e1d45d390dcfe191c2c9214619`。
