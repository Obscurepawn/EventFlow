# C++ 动态函数示例

Status: complete
Owner: root
Updated: 2026-09-16

后续状态：用户已授权 [生产 UDF 接入](native-udf-delivery.md)，本地示例随后复用生产接口。下文保留本地示例完成时的验收快照，不作为当前生产上传/部署/恢复的证据。

## 授权与范围

用户选择让 `.so` 直接使用 EventFlow C++ 数据结构，并要求在 examples 增加用法示例。本轮增加可运行的本地插件机制示例：独立编译新闻评分函数，运行时加载，通过现有 FunctionRegistry 和执行器传递 FunctionInputs、Row 与 StatusOr。

这不是完整动态 UDF 产品交付：不实现 SDK 上传、Coordinator 注册、在线 Catalog 安装、独立 Worker、SQL 标量 UDF 或 checkpoint 制品管理。示例接口保留在 examples/cpp_udf，不提前发布为生产 API；streaming_database 仍是完整集群示例。

## 接口与约束

- 宿主与插件使用同一次受控 Clang/CMake 构建的 SDK、标准库、配置；构建标识在读取 C++ 描述对象前检查，不宣称任意 ABI 兼容或不可信代码隔离。
- 固定入口符号只用于查找；签名、输入、结果均使用 C++ 类型。无正常路径异常控制流。
- Registry callback 持有动态库引用；执行器及所有插件相关对象销毁后才能卸载。输入在 submit 成功后固定。
- 插件必须检查输入形状、类型、有效值与输出有限性，返回 Status。
- 所有新代码 ASCII、snake_case、noexcept、具名 aggregate 初始化；不改第三方和现有生产行为。

## 验收

- [x] 一条 run.sh 构建并运行真实 `.so`，同一程序验证 Simple/ThreadPool 两种执行器。
- [x] 输入 news_count=2、sentiment_price_sum=103 得到 51.5；宿主释放本地动态库引用后仍能执行；提交后更改调用方输入不改变结果。
- [x] 非法计数通过 UDF 返回错误；类型错误、缺失版本被执行器拒绝；缺失库、缺失入口或构建标识不符返回失败。
- [x] Clang Debug 三 sanitizer、Release、定向执行器回归、安装目录独立运行与格式/脚本验证。
- [x] 同轮两位 fresh-context reviewer 独立审查并记录处置。

## 工作状态

| 工作 | 状态 |
| --- | --- |
| 示例、构建接线、中文说明 | 完成 |
| 定向验证 | 完成 |
| 同轮独立审查与记录 | 完成；一项去重证据缺口已补验 |

## 变更文件与当前进度

已修改 examples/cpp_udf/（接口、构建标识模板、插件、加载器、宿主、脚本、README、AGENTS）、根 CMakeLists.txt、README.md、examples/AGENTS.md、specs/INDEX.md 和本规格。生产 src/include 未修改，原 streaming_database 示例源码未修改。

## 本轮验证证据

| 验证 | 结果及日志 |
| --- | --- |
| Debug 构建与运行 | `bash examples/cpp_udf/run.sh build`，exit=0，两种执行器 51.5/错误/固定输入均通过；`/tmp/eventflow-cpp-udf-debug.log` |
| 定向回归 | 刷新 function_executor_test 后执行 `ctest --test-dir build --output-on-failure -R '^(cpp_udf_example\|function_executor_test)$'`，2/2 通过；`/tmp/eventflow-cpp-udf-ctest.log` |
| Release 构建与运行 | `bash examples/cpp_udf/run.sh build-release`，exit=0；-O3、NDEBUG、ThinLTO，无 sanitizer；`/tmp/eventflow-cpp-udf-release.log` |
| 独立安装 | `cmake --install build-release --prefix /tmp/eventflow-cpp-udf-install-8YYNuS` 后执行安装目录 run.sh，exit=0；`/tmp/eventflow-cpp-udf-install.log` |
| 错误路径 | 缺库、缺 build ID 符号、匹配 build ID 但缺函数入口、混用 Release 插件/Debug 宿主，均 exit=1 且符合精确诊断；`/tmp/eventflow-cpp-udf-negative.log` |
| 既有集群示例 | usage reviewer 复用 Debug 产物运行，STREAMING_DATABASE_OK、51.5/128/53、checkpoint complete、五进程 exit=0；`/tmp/eventflow-cpp-udf-streaming-review.log` |
| 静态检查 | clang-format 21.1.2 对五个新增 C++/模板文件检查通过；bash -n、git diff --check、新代码 ASCII、示例/规格本地链接和围栏检查通过 |

所有新构建日志无编译 warning/error 和 sanitizer 诊断。未重跑全量 CTest；原集群验证复用现有程序，本轮未宣称动态 UDF 已接入集群。

缺入口探针最初使用 RocksDB 库，但因其传递依赖未定位而在 dlopen 阶段失败，不能作为入口验证证据。改用 libc 验证缺 build ID，再用临时 CMake MODULE `/tmp/eventflow-udf-missing-entry-38vcpj1l/` 验证匹配标识但缺业务入口；最终四项独立诊断均通过。

## 审查与处置

同一轮并行 fresh-context 审查：[代码](reviews/CPP-UDF-code-review.md)、[用法](reviews/CPP-UDF-usage-review.md)。两位指出的缺业务入口覆盖是同一项证据缺口，已按上表补验；保留原发现并追加处置，不重新发起审查。

Release 产物 SHA-256：宿主 `aaa336125a5501d5fdb4e1177ab5ab70957445730f9318d843ee60b1aeef3da7`；插件 `97748557a5e68f37a1453406247a6306c857180888e12edc5024dc85be2be0ef`。后续实现动态上传或生产插件接口时另立规格，不能沿用本例结果作为完整产品验收。
