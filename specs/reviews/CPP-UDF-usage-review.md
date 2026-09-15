# C++ 动态函数示例：独立用法与交付审查

日期：2026-09-16。Reviewer：udf_usage_review，独立 fresh-context，同轮双 reviewer 之一。

## 范围与结论

依据用户要求“让 `.so` 直接使用 EventFlow C++ 类型，并增加展示用法的 example”，读取根目录、examples、cpp_udf、specs 的 AGENTS.md、规格索引及 [本轮规格](../cpp-udf-example.md)，检查新增示例、根 CMake/README 接线、现有执行器接口和实现、构建运行日志。审查期间未修改实现。

**在本地、受控、同一进程的示例范围内，没有发现阻断交付的实现或需求匹配问题。** `.so` 是真实 MODULE；入口返回 C++ 描述，调用实际传递 FunctionInputs/Row/StatusOr；Simple 与 ThreadPool 使用现有执行器。README 明确没有 SDK 上传、在线注册、独立 Worker、KV 持久化和恢复，没有把示例接口包装为生产 SDK。

## 发现与处置

### U1：低优先级验证缺口——缺失描述入口未被现有负例单独触达

- 证据：`/tmp/eventflow-cpp-udf-negative.log` 中 `missing_entry` 的实际诊断是 `missing eventflow_udf_build_id symbol`。它覆盖构建标识入口缺失，未证明“构建标识匹配但缺少 eventflow_udf_entry”分支已执行。
- 代码中的第二次 dlsym 及失败返回本身存在，未发现实现错误；不能把第一入口缺失的结果当成第二入口分支的运行证据。
- 建议：通过临时 CMake MODULE 导出匹配的构建标识、故意省略描述入口，断言宿主以 1 退出并报告 `missing eventflow_udf_entry symbol`。
- 已通知实施者；由实施者在本节追加定向验证处置即可，不需要新一轮审查。

## 实际证据

1. 独立运行 `EVENTFLOW_SKIP_BUILD=1 bash examples/cpp_udf/run.sh build`，退出 0；Simple/ThreadPool 均得到 ACME 51.5、拒绝零计数并验证固定输入。宿主代码同时覆盖错误输入类型、未知版本和 registry 生命周期持有。
2. 检查本轮 Debug/Release 日志，均构建真实插件和宿主并运行成功：`/tmp/eventflow-cpp-udf-debug.log`、`/tmp/eventflow-cpp-udf-release.log`。实际 flags.make 显示 Debug 使用 `-fsanitize=address,undefined,leak -fno-sanitize-recover=all`；Release 使用 `-O3 -DNDEBUG -flto=thin`，没有 sanitizer flags，编译器为 `/usr/bin/clang++`。
3. 检查 `/tmp/eventflow-cpp-udf-ctest.log`：`cpp_udf_example`、`function_executor_test`，2/2 通过。负例日志显示不存在的库、构建标识入口缺失、构建不匹配均退出 1。
4. 检查实施者安装日志 `/tmp/eventflow-cpp-udf-install.log`：完整 Release 安装包含 `bin/eventflow_cpp_udf_demo`、`lib/libeventflow_news_udf.so` 和示例目录；`/tmp/eventflow-cpp-udf-install-8YYNuS` 中脚本运行成功。run.sh 的无 CMakeLists 分支由脚本位置推导安装根路径，与这些安装位置一致；两个产物覆盖变量均保留引号。
5. 独立执行 `bash -n examples/cpp_udf/run.sh`，并检查中文 README 全部相对文件链接和代码围栏配对，通过。
6. `git diff -- examples/streaming_database` 为空；独立执行 `EVENTFLOW_SKIP_BUILD=1 bash examples/streaming_database/run.sh build`，退出 0。日志 `/tmp/eventflow-cpp-udf-streaming-review.log` 显示 `STREAMING_DATABASE_OK`、51.5/128/53、固定输入、正常 checkpoint，Coordinator/KV/两个 Compute/消息服务五个子进程均退出 0。保留数据与子进程日志位于 `/tmp/eventflow-streaming-zyzv04`。

审查代码快照 SHA256：main.cpp `f28ea5047c5ebe2f8fe4bf21b95a391e9de1d081503bb5635afb58221ce262ef`；news_impact.cpp `f4f3aeb08cda918e7473351c9bd73f0cbfe68c1da8069f05b10b7f5bfa93157e`；udf_library.hpp `3ccdaad7df0c1fa02eb61250569f3f616ad7d1ebc048e18ecfce3ac3458cd001`。

## 未验证边界

- 本 reviewer 复用现有 Debug 程序运行两个示例，未独立重新构建全部项目；Debug/Release 构建、CTest 与安装证据来自本轮实施者日志，不能称为 reviewer 独立重新执行。
- 没有跨编译器、跨标准库或跨 SDK 版本兼容性证明；构建标识仅为受控示例的拒绝保护，没有代码真实性和隔离保证。
- 未验收动态上传、线上部署、独立 Worker、SQL UDF、恢复制品管理或完整 UDF/UDTF/UDAF 产品；这些均在本轮明确范围之外。
- 原 streaming 检查验证本轮正常启动、消息处理和 checkpoint 创建，不代表故障恢复或 Kafka 集成验收。

## 实施者处置记录

U1 与代码审查 R1 是同一证据缺口，已补验：临时 CMake MODULE `/tmp/eventflow-udf-missing-entry-38vcpj1l/` 导出匹配构建标识但不导出描述入口，宿主明确返回 `missing eventflow_udf_entry symbol`、exit=1。最终 `/tmp/eventflow-cpp-udf-negative.log` 分开记录缺库、缺 build ID、缺业务入口与构建标识不匹配，四项均符合预期且无 sanitizer 诊断。未修改实现逻辑，不再启动独立审查轮次；完整证据见 [本轮规格](../cpp-udf-example.md)。
