# C++ 动态函数示例

本例展示：将用户计算函数单独编译成 `.so`，宿主运行时加载它，直接传递 EventFlow 的 C++ 数据结构，并通过已有函数执行器取得结果。

这是**同一进程内的加载器示例**，已迁移到生产 UDF 接口。服务启动后通过 SDK 上传、提交定义、真实集群计算与恢复的完整用法见 [streaming_database](../streaming_database/README.md)；静态定义的数据库流程见 [static_streaming](../static_streaming/README.md)。本例本身仍只运行本地执行器。

## 运行

在仓库根目录执行，需要项目现有 Clang/CMake/Make 和 cpp3rdlib：

```sh
bash examples/cpp_udf/run.sh
```

默认使用 `build/`，新目录配置 Debug 并启用 ASan、UBSan、LSan；已有目录保留原构建类型。也可以指定目录：

```sh
bash examples/cpp_udf/run.sh build
EVENTFLOW_SKIP_BUILD=1 bash examples/cpp_udf/run.sh build
```

预期输出：

```text
simple: ticker=ACME impact_score=51.5; invalid_count=rejected; frozen_input=verified
thread_pool: ticker=ACME impact_score=51.5; invalid_count=rejected; frozen_input=verified
C++ UDF example passed.
```

输入为 `ticker="ACME"`、`news_count=2`、`sentiment_price_sum=103.0`。用户函数计算 `103.0 / 2 = 51.5`，并在计数为零时返回 `InvalidArgument`。示例还检查错误输入类型、未知函数版本和提交后的输入固定语义。

## 按什么顺序阅读

| 文件 | 作用 |
| --- | --- |
| [生产 udf.hpp](../../include/eventflow/udf.hpp) | 插件接口，使用现有 FunctionSignature、FunctionInputs、Row、StatusOr |
| [news_impact.cpp](news_impact.cpp) | 用户计算函数、输入输出 Schema 和导出入口 |
| [生产 udf.cpp](../../src/runtime/udf.cpp) | 从库字节加载、核对构建标识、注册并持有动态库 |
| [main.cpp](main.cpp) | 构造固定输入，分别通过 Simple/ThreadPool 执行并验证结果 |
| [run.sh](run.sh) | 构建与运行入口 |

调用链：

```text
read file -> sealed memory file -> dlopen -> check build ID -> read UdfDefinition -> FunctionRegistry
       -> submit(FunctionTask) -> invoke(FunctionInputs) -> StatusOr<Row>
       -> poll(FunctionCompletion)
```

`extern "C"` 只固定两个入口的符号名，方便 dlsym 查找。业务函数及描述仍使用 C++ 类型，没有改成 C 字节缓冲区接口。FunctionInputs 包含 optional<Row>，Row 包含 variant、string、vector 等真实 C++ 对象。

## 如何替换成自己的函数

1. 按 `StatusOr<Row>(const FunctionInputs&) noexcept` 编写计算函数，校验输入，正常失败返回 Status。
2. 在 `UdfDefinition` 中填写精确的输入输出 Schema、函数 ID、版本和 implementation identity，并设置 invoke 指针。
3. 同步宿主的任务参数和预期输出，重新构建宿主与插件。

本例为了让 `.so` 边界清晰，直接使用 Row。上传接口和 FunctionRef 已在 [生产集群示例](../streaming_database/README.md) 使用；这里专注 Row 与共享库边界。

## 编译与生命周期约定

- **只支持同一受控构建。** 宿主和插件由同一份根 CMake 构建，共用 Clang、标准库、配置及 SDK。示例核对构建信息、相关头文件摘要和标准库 ABI 标识；这不是任意 C++ ABI 兼容性证明。不要混用 Debug/Release 插件或其他环境的库。
- **只加载受信任代码。** dlopen 会执行库初始化代码，构建标识检查不提供沙箱或代码真实性保证。本例没有独立 Worker，插件故障会影响宿主。
- **回调持有动态库。** 本例在注册后释放宿主局部引用，Registry 中的回调继续持有库，执行器结束后才销毁回调和库。实例中所有 completion 也在此之前销毁；不得让插件实现的对象、析构器或回调在卸载后继续使用。
- **输入不外借。** 插件不得保存输入引用供返回后使用；结果拥有自己的字符串和数值。示例从 submit 后改动调用方输入，验证任务仍返回 51.5。
- **改实现就改版本和身份。** 同名同版本不覆盖已有 Registry 项。本例中的 task_id 只是本地执行器身份，不表示数据库持久 Request。

## Release 和安装目录

```sh
cmake -S . -B build-release -G "Unix Makefiles" \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
bash examples/cpp_udf/run.sh build-release
```

项目安装会包含 `bin/eventflow_cpp_udf_demo`、`lib/libeventflow_news_udf.so` 与本示例目录。进入完整安装目录后仍可运行：

```sh
bash examples/cpp_udf/run.sh
```

`EVENTFLOW_UDF_HOST` 与 `EVENTFLOW_UDF_LIBRARY` 可分别指定宿主和库文件路径；自定义库路径建议使用绝对路径。
