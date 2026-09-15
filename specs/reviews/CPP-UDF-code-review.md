# C++ UDF 示例独立代码审查

日期：2026-09-16。Reviewer：`udf_code_review`，独立 fresh-context；本报告属于同一轮并行双审查，不是新增审查轮次。

## 范围与结论

依据根目录、examples、cpp_udf、specs 的 AGENTS.md、[当前规格](../cpp-udf-example.md)及用户授权，审查 `examples/cpp_udf/`、根 CMake 接线和相关说明。范围限定为受控 C++ ABI 的可运行本地插件示例。

未发现阻塞交付的代码正确性问题。发现一项低优先级验证证据缺口，见下文；不将此示例解释为动态 UDF 产品能力交付。

## 代码与合同核对

- `udf.hpp` 的两个固定符号仅使用 C linkage 固定名字；`UdfDefinition`、函数指针、`FunctionInputs`、`StatusOr<Row>` 确实跨共享库边界传递 C++ 对象。`nm -D` 确认 Release 模块导出 `eventflow_udf_build_id` 和 `eventflow_udf_entry`。
- `UdfLibrary::open` 在解释插件 C++ 描述前核对构建字符串；所有打开后的失败分支由 shared_ptr 析构关闭句柄。该检查并不建立任意 ABI 兼容或隔离保证，README 已明确限制同一受控构建与受信任代码。
- Registry 回调捕获 shared_ptr；Registry 克隆共享不可变 BuiltinFunction，执行器 TaskRecord 同样持有该对象。执行器状态持有 Registry，线程池 close 会 join；本例先销毁 completion，再关闭并销毁执行器，最后释放 Registry。未发现调用途中或插件返回对象销毁前卸载的路径。
- 用户函数检查输入数量、optional、列数、类型、非空 ticker、正计数和有限值；输出拥有字符串和值。现有执行器在 submit 校验签名/输入，在执行后校验输出 Schema 和预算，并复制为宿主拥有的结果。本轮没有绕过 Registry 和执行器的既有约束。
- Simple 实例在 run_ready 前改动调用方输入，确定性验证提交复制；线程池实例也验证相同最终结果。零计数由插件返回错误，错误类型和版本由执行器拒绝，职责边界清晰。
- MODULE 与宿主共用 ef_cpp_udf_options/ef_options；宿主目标依赖 MODULE，脚本只构建示例所需目标。源码运行与安装目录运行路径均接通；无需将第三方实现引入示例。

## 发现

### R1：缺失入口日志只覆盖了构建标识入口（低优先级）

审查时 `/tmp/eventflow-cpp-udf-negative.log` 的 `missing_entry` 项实际输出为 `missing eventflow_udf_build_id symbol`。该证据覆盖 `udf_library.hpp` 的第一次 dlsym 失败，尚不能证明“构建标识相同但缺少 eventflow_udf_entry”分支已经执行。

代码本身具备第二次 dlsym 的显式错误处理，未发现实现缺陷。建议补充一个只导出匹配 build ID 的小型共享库探针，或将现有证据准确标注为缺少 build ID，并明确剩余分支未验证。实施者可在本报告追加定向处置，不需要重新发起审查轮次。

## 实际验证证据

本 reviewer 独立执行：

```sh
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
LSAN_OPTIONS=exitcode=23 \
build/bin/eventflow_cpp_udf_demo "$PWD/build/lib/libeventflow_news_udf.so"
ctest --test-dir build -R '^(cpp_udf_example|function_executor_test)$' --output-on-failure
bash /tmp/eventflow-cpp-udf-install-8YYNuS/examples/cpp_udf/run.sh
bash -n examples/cpp_udf/run.sh
git diff --check
```

结果：Debug 两种执行器均返回 ACME 51.5、非法计数拒绝、固定输入验证通过；CTest 2/2 通过；已安装 Release 示例独立运行两种执行器通过；脚本语法与 tracked diff 空白检查通过。

另外读取实施者 fresh logs `/tmp/eventflow-cpp-udf-debug.log`、`/tmp/eventflow-cpp-udf-release.log`、`/tmp/eventflow-cpp-udf-ctest.log`、`/tmp/eventflow-cpp-udf-negative.log`。核对生成 flags：Debug MODULE 含 address/undefined/leak 与不恢复诊断；Release MODULE 和宿主链接含 `-O3 -DNDEBUG -flto=thin`，没有 sanitizer。没有将读取日志记为 reviewer 自己重新构建。

## 未验证边界

没有重新执行全项目构建/全部 CTest、跨编译器或标准库混用、任意恶意插件、插件长时间不返回、独立进程隔离、SDK 上传、在线图安装或 checkpoint 制品恢复。这些均不属于本轮局部示例交付；当前版本不宣称支持这些能力。构建标识是受控环境的误配检查，不是完整 ABI 证明。

## 实施者处置

R1 已定向补验：用临时 CMake MODULE `/tmp/eventflow-udf-missing-entry-38vcpj1l/` 构建只导出匹配 build ID 的库，Debug 宿主返回 exit=1 与 `missing eventflow_udf_entry symbol`。最终 `/tmp/eventflow-cpp-udf-negative.log` 分别记录 missing_library、missing_build_id、missing_entry、build_mismatch 四项；全部按预期返回 exit=1 且无 sanitizer 诊断。只修正验证覆盖，不修改执行逻辑，不开启新审查轮次。
