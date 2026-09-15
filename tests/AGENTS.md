# 测试开发约定

本目录继承根目录约束。修改测试前，阅读 [规格索引](../specs/INDEX.md)及被测模块的当前合同；Trigger/Pattern 以[最新授权记录](../specs/trigger-api-ergonomics.md#10-当前状态与交接)为准。

## 测试入口

- 测试目标统一在根 `CMakeLists.txt` 的 `BUILD_TESTING` 分支注册；新增目标同时配置 CTest、超时和失败即退出的 sanitizer 环境。
- 使用 `cpp3rdlib` 提供的 GoogleTest；只给测试目标链接 `GTest::gtest_main`，不引入构建下载或另一套测试框架。
- 手写辅助函数、fixture 方法和 lambda 遵守 `noexcept`；保留 GoogleTest 生成的测试体签名。正常失败用 `Status/StatusOr<T>`。
- 从仓库根目录配置、构建并运行；已有构建缓存若使用其他编译器，应另建目录：

```sh
cmake -S . -B build -G "Unix Makefiles" -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build --parallel 4
ctest --test-dir build --output-on-failure
```

Debug 由 CMake 强制启用 ASan、UBSan、LSan 和不可恢复诊断；CTest 已设置泄漏检测与失败退出码。Release 是关闭测试及 sanitizer 的交付构建，不代替测试门禁。

## 定向验证与合同

```sh
ctest --test-dir build --output-on-failure -R '^(store_contract_test|engine_contract_test|event_processor_test)$'
ctest --test-dir build --output-on-failure -R '^(trigger_test|trigger_pattern_test|typed_trigger_test|typed_input_domain_test|computation_test|streaming_database_example)$'
ctest --test-dir build --output-on-failure -R '^(core_cluster_test|remote_store_test|message_source_test|http_message_source_test|source_poller_test|sql_module_service_test)$'
```

- 选择实际受影响的目标；需要单个 GoogleTest 用例时运行 `build/bin/<test_target> --gtest_filter=Suite.Case`，并显式设置与 CTest 相同的 sanitizer 环境。
- `store_contract_test` 在 Memory、Rocks 及各自的 HTTP 适配器上复用合同；其他参数化合同按其 fixture 支持的后端扩展，不只验证 simple 实现。
- `RemoteHarnessStore` 是进程内真实 HTTP 服务/客户端；`remote_store_test` 使用真实 KV 子进程；`core_cluster_test` 启动真实 KV、Coordinator 和两个 Compute，覆盖 Memory/Rocks 及 Rocks checkpoint 恢复。这些证据层级应分别表述。
- 多进程测试使用 `core_process_harness.hpp`/`kv_process.hpp`，保留动态端口、READY 超时、日志、子进程退出状态及失败路径清理；不要以同进程对象替代端到端部署。
- 消息源快速测试使用 MemMock；`message_source_test` 的 Kafka 适配器合同使用 librdkafka protocol mock，不依赖外部 broker，也不证明真实 Kafka Compose 集成。后者仍为后置范围。
- 断言可观察的不变量：UNKNOWN 确认原批后再 ACK、无半批状态、epoch/owner 失效、预算边界、deadline 排除边界、固定捕获输入、恢复身份及连续源位置；不要只断言调用次数或成功状态。
- 强类型静态 `Function` 已有实现与测试。[生产 UDF 规格](../specs/native-udf-delivery.md)新增原生 `.so` 加载、SDK 上传、首次部署及恢复制品的验收，以 udf_test、submitted_computation_test、Coordinator UNKNOWN 用例和 streaming_database_example 验证。完整 UDTF/UDAF、Process Function 与运行中图迁移仍后置。
- 记录实际运行命令、退出结果、源码状态及未覆盖边界；不要引用旧测试总数代替当前验证。全局审查和规格证据要求继承根目录约束。

旧业务回归只保留在 `tests/support/legacy_feature_fixture.*`，相关服务测试显式注入测试模块；生产服务和新示例不得链接这个 fixture。本轮不新增专门故障注入测试。

`sql_module_service_test` 使用默认 SQL 模块验证自动消费、级联 MV 和暂停/checkpoint/resume，并覆盖不带函数请求的普通 Trigger；不要仅用带 request 的业务模块证明通用服务可用。
