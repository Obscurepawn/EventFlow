# 模块指南工程可用性独立审查

Date: 2026-09-14
Reviewer: guidance_usability_review
Round: 本次文档维护唯一一轮并行独立审查
Result: 未发现需要整改的文档问题

## 独立性与范围

本 reviewer 以 fresh context 启动，只接收原始任务 `/init 请创建各个模块的AGENTS.md`、审查范围和外部并行变动提示，未参与指南编写。完整阅读根 `AGENTS.md`、`specs/module-agent-guidance.md`、`specs/INDEX.md` 及全部新增 23 份指南后，直接核对当前文件。作者后续发送的自查结果未作为独立通过依据；未另启 reviewer 或重复审查轮次。

检查根导航、七个生产模块及 `src/` 上层、SQL generated、公共头文件、测试、七个示例及示例上层、CMake、第三方依赖、docs/specs 的继承、职责、路径、命令、接口和证据边界。生产与测试源码仅作引用依据，不开展新产品验收。已知 `CMakeLists.txt`、README、Trigger spec 和 `feature_computation_demo_test.cpp` 有外部并行改动，未将其归因于本次文档作者，也未回滚这些文件。

## 实际验证方法与结果

| 检查 | 实际结果与文件依据 |
| --- | --- |
| 目录覆盖和继承 | 根加 23 份指南全部存在；`src/` 七个直接模块和 `examples/` 七个示例均有本地指南。根 `AGENTS.md:47-57` 提供导航并声明根规则继续适用；子层未豁免错误处理、工具链或质量要求。 |
| 本地引用与围栏 | 独立 Python 静态检查 24 份指南中的 146 个 Markdown 本地链接，目标均存在；带 fragment 的链接匹配目标标题，围栏均闭合。此计数不包含 spec/index 文本自身的其他链接。 |
| CTest 命令 | 从当前 `CMakeLists.txt:146-228` 的显式 `add_test` 与测试配对表提取注册名，逐条匹配指南内 19 个 `-R` 正则，全部非空命中。没有执行 CTest；注册存在不代表相应测试通过。 |
| 构建入口 | `tests/AGENTS.md:10-18` 和 `cmake/AGENTS.md:17-28` 与 `CMakeLists.txt:1-29,49-51,146-156,225-227` 一致：Clang、Debug 三 sanitizer、失败诊断、Release 禁用测试和 sanitizer、受支持时 LTO。`Makefile:17-20` 证明 `make compdb` 确实 clean 后重编，指南已说明副作用。 |
| 格式与依赖范围 | `cmake/Format.cmake:7-19` 仅收集项目 C++ 路径，包括 SQL generated，没有收集 `cpp3rdlib/` 或 `.reference-repos/`。`cmake/AGENTS.md:9-13`、`cpp3rdlib/AGENTS.md:5-10` 保留预编译依赖和生成签名边界，没有添加下载或 vendor 改写要求。 |
| 七个脚本与环境覆盖 | 逐个读取 `examples/*/run.sh`，确认首参、`EVENTFLOW_BUILD_DIR`、`EVENTFLOW_SKIP_BUILD`、通用及各例专用 binary 变量、配置/构建目标和运行包分支有实际实现；七个脚本均有可执行位，逐个 `bash -n` 返回 0。未启动脚本或服务。 |
| 生成版本与资产 | lexer/parser 文件头均为 ANTLR 4.10；`cpp3rdlib/include/antlr4-runtime/Version.h:30-32` 为 4/10/0；`cpp3rdlib/README.md:79` 记录匹配 JAR 与摘要。`src/sql/generated/AGENTS.md:16-27` 使用相同版本和仅 lexer/parser 的命令，普通 CMake 直接编译既有产物。独立用 Python SHA256 核对 `MANIFEST.sha256` 345 个既有条目，全部一致；未修改任何发布资产或 manifest。 |
| 实现和证据层级 | `tests/store_contract_test.cpp:74-93,481`、`tests/compute_service_test.cpp:119-150,542` 支持四种 KV 适配合同；`tests/message_source_test.cpp:55-104,473` 明确使用 librdkafka mock；`tests/core_cluster_test.cpp:353-410` 区分 Memory/Rocks 多进程和 Rocks 恢复。指南没有把这些源码入口描述为本轮已运行或真实 Kafka 已集成。 |

独立检查快照中，按指南相对路径排序并拼接 `path + NUL + file_bytes + NUL` 的 SHA256 为 `377cde191d41c4ea11e895dcb70a2dc374fa3228a0f8cdb9a399f2aa45f2ddd8`，覆盖根与新增的全部 24 份指南。工作树可能继续变化，该摘要只绑定本次检查对象。

## 契约与示例抽查

- `include/eventflow/computation.hpp:9-26` 支持 `TriggerSet::bind -> BoundComputation` 和私有装配；`trigger.hpp`、`typed_trigger.hpp`、`static_function.hpp` 与当前 typed Pattern、session、capture、静态函数导航一致。`specs/trigger-api-ergonomics.md:454-466` 的最新授权优先于其前文历史建议；指南没有把动态扩展或 Process Function 加回完成门槛。
- `CMakeLists.txt:110-116` 确认公共特征定义确实被 Compute 与 Coordinator 链接。`examples/feature_computation_demo/AGENTS.md:7-13` 保留这条生产路径；相关测试和集群示例入口被同时列出。
- 七个示例分别核对了 `main.cpp` 中的关键输入、查询、状态或输出标记：SQL 的 90/30 插入与撤回；condition 的 `[110,true]`、220 和单次确认；Clock 的 4.9s 固定报价、source progress 不变及实际 capture/admission revision；checkpoint 的跨 owner key、原 offset 去重和 110 到 135；news 的终态及固定 revision；typed Pattern 的 gate、40.6/94.5、D 后固定结果及三个回调。相关指南没有将纯打印 SQL 示例夸大为自身完整断言测试。
- `src/client/cluster_client.cpp:393-440` 的 ingest 原字节重试与 `input_committed` 处理，以及 `src/sql/binder.cpp:413-437` 的 lookup join 唯一性约束，与对应指南的描述一致。
- `src/service/AGENTS.md:13-23` 明确 1 个共享 KV 与 S+1 个物理 shard 的区别、control shard 路由边界、UNKNOWN 与一致 checkpoint。`docs/AGENTS.md:7-8`、`tests/AGENTS.md:30-35` 及根末段明确历史核心验证不能证明后续重构通过。

## 分级发现与处置

| 级别 | 发现 | 处置 |
| --- | --- | --- |
| P0/P1 | 未发现错误授权、根规则削弱、私有材料发布或第三方资产破坏指令。 | 无整改项。 |
| P2 | 未发现失效路径/anchor、空匹配测试正则、不存在的构建目标或脚本覆盖变量、当前 API 与历史授权混淆。 | 无整改项。 |
| P3 | 未发现足以妨碍后续 Agent 使用的具体低优先级问题。 | 无整改项。 |

## 结论与局限

在本次静态检查范围内，指南满足模块覆盖、继承、工程入口与证据边界要求，可以作为本次文档维护的独立审查通过意见。实施者仍需汇总同轮另一份独立报告并更新维护 spec 状态；此意见不替代该汇总。

未运行 CMake 配置、构建、CTest、示例、服务、ANTLR 再生成、真实 Kafka 或恢复探针，也未穷尽核验每条复杂运行时不变量的实现正确性。hash 一致仅证明现有清单内容匹配，不证明 ABI 或行为。此审查不宣布任何新产品 milestone、Trigger 重构或系统质量门禁完成。本 reviewer 唯一写入为本报告。
