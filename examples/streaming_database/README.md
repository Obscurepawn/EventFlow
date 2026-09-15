# 流式数据库：SDK 上传 C++ UDF 并部署计算

本例使用**通用 `eventflow` Server**：先启动服务，再通过 SDK 上传 `.so`、提交表/MV/Trigger 定义并部署。业务函数和计算定义都没有静态链接到 Server。

## 一条命令运行

在仓库根目录执行：

```sh
bash examples/streaming_database/run.sh
```

脚本使用 Clang/CMake/Make，默认 Debug 开启 ASan、UBSan、LSan。它会启动独立 HTTP 消息服务、Rocks KV、两个 Compute 和 Coordinator，运行首次部署验证，再停止并从 checkpoint 重启四个数据库进程，消息服务保留原日志。

```text
NATIVE_UDF_DATABASE_OK shards=2 news_counts=3,2 threshold_impacts=51.5,128 periodic_impact=53 source_next_offsets=3,2
NATIVE_UDF_RESTORE_OK impact=54.75 frozen_impact=51.5 no_reupload=true
```

源码构建和已安装目录均支持 run.sh。指定构建目录或复用产物：

```sh
bash examples/streaming_database/run.sh build
EVENTFLOW_SKIP_BUILD=1 bash examples/streaming_database/run.sh build
```

Release 先配置，再运行：

```sh
cmake -S . -B build-release -G "Unix Makefiles" \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
bash examples/streaming_database/run.sh build-release
```

## 用户需要写什么

### 1. 一个普通 C++ 函数与导出描述

实际函数见 [news_impact.cpp](../cpp_udf/news_impact.cpp)，接口来自生产 [udf.hpp](../../include/eventflow/udf.hpp)。它接收包含 ticker、news_count、sentiment_price_sum 的 `FunctionInputs`，返回包含 scored_ticker 和 impact_score 的 `StatusOr<Row>`。

```cpp
StatusOr<Row> calculate_news_impact(const FunctionInputs& inputs) noexcept;
```

`scored_ticker` 是函数输出字段；计算关系自己的 `ticker` 属于 host 管理的业务身份，输出不能覆盖它。计数为零等正常失败返回 Status。函数声明必须包含输入输出 Schema、ID、版本与 implementation identity。

本例由根 CMake 的 `eventflow_news_udf` 目标生成 `.so`，用户不需要重新编译 Server 业务代码。当前 SDK 接口上传已编译库，尚不接收任意 C++ 源码或自动下载编译依赖。

### 2. 一份可提交的计算定义

[definition.cpp](definition.cpp) 是主要使用入口，只依赖公开 API。它声明：

- 六字段 `news_updates` 表和按 ticker 维护的 `news_stats -> news_features` 两级 MV。
- 每个 ticker 的首条新闻开启 30 分钟 session；到期后下一条源消息开启新 epoch，累计 MV 不自动清空。
- 至少两条新闻且平均情绪不低于 0.5 时，受理一次阈值请求。
- 每 200 ms 通过 Clock 尝试受理周期请求。
- 从 `news_features` 的声明字段提取固定输入，调用注册的新闻影响函数。

当前结构化 API 包括 `SubmittedComputation`、`SubmittedTrigger`、`ComputationCondition` 和 `FunctionRef`，见 [submitted_computation.hpp](../../include/eventflow/submitted_computation.hpp)。条件列表按 AND 组合，支持有类型的列与常量比较。

### 3. 上传、提交、部署

客户端真实调用见 [verify.cpp](verify.cpp)：

```cpp
EF_ASSIGN_OR_RETURN(auto client, ClusterClient::open(meta_endpoint));
EF_ASSIGN_OR_RETURN(auto function, client->register_udf(library_path));
auto definition = computation_definition(function);
EF_ASSIGN_OR_RETURN(auto staged, client->submit_computation(definition));
EF_ASSIGN_OR_RETURN(auto deployed, client->deploy());
```

`register_udf` 读取客户端文件并上传内容。Coordinator 验证库、持久化元数据与原始字节，返回不可变 `FunctionRef`。相同 ID/version 和字节重复上传返回相同引用；不同字节不能覆盖已有版本。

`submit_computation` 在 STAGING 中校验完整定义，返回绑定解释；成功提交替换尚未部署的提交定义。错误提交不发布部分 Catalog。`deploy` 将精确定义发送给 Compute，各 Compute 从共享 KV 读取原始 `.so` 并绑定自己的执行器。

### 4. UDF 如何到达 Compute 节点

上传后的 `.so` 保存在共享 KV 中。部署时，Coordinator 告诉各个 Compute 要使用哪个函数版本，Compute 再从 KV 读取对应的文件，检查 SHA-256 和 C++ ABI，加载后用于计算。

运行时使用已经加载的代码，不会每处理一条消息就重新下载。数据库从 checkpoint 恢复后也会从 KV 读取 `.so`，不用客户端重新上传。目前部署检查和每个分片初始化时可能分别读取，还没有单独做节点级文件缓存。

相关生产实现：[Coordinator 注册](../../src/service/coordinator_service.cpp)、[Compute 安装](../../src/service/compute_service.cpp)、[UDF 文件读取与绑定](../../src/runtime/submitted_computation.cpp)、[UdfLibrary](../../src/runtime/udf.cpp)。

只在服务已启动、计算尚未部署的阶段开放注册和提交。部署后返回 FailedPrecondition；本轮没有实现已有运行中计算图的追加或替换。

## 验证时间线

1. 五个服务全部启动，Coordinator 仍为 STAGING。
2. SDK 上传 UDF 两次，确认引用相同；提交业务定义。
3. 部署前向两个消息 partition 写四条新闻，确认尚未 ACK。
4. deploy 后数据库主动消费，两个不同 shard 的阈值结果分别是 51.5、128。
5. 第一 ticker 追加第三条新闻，周期结果变为 53；阈值请求的固定输入和结果 51.5 不变。
6. 无新消息时，数据库 Clock 继续产生周期请求。客户端不调用 ingest/pump/drain 推动数据库。
7. 暂停、恢复消费并创建一致 checkpoint，保存 Catalog、源位置、运行时状态和 control shard 中的 UDF 字节。
8. 停止四个数据库进程，从 Rocks snapshot 创建新的活动目录后重启，保持相同逻辑 node/shard 身份。
9. 恢复调用故意传入不存在的本地 UDF 路径，而且不调用注册/提交；恢复后的计算必须从 checkpoint 中加载UDF 文件。追加第四条新闻得到 54.75，旧阈值结果保持 51.5。

消息服务是有界内存 HTTP 设施，本例恢复时保持该进程运行；这不是消息服务宕机恢复或真实 Kafka 的验证。

## 执行与部署边界

- **受控 C++ ABI。** 使用同一 SDK、Clang、标准库、构建配置和目标平台。库与 Server 比较构建标识；原始字节的 SHA-256 进入函数实现身份。入口的 C linkage 只固定符号名，参数及返回值仍是 C++ 对象。
- **明确启用。** Coordinator 和 Compute 配置均需 `"allow_native_udfs": true`；脚本已经配置。默认关闭。只加载受信任、确定性、有界函数，当前直接在进程中加载和执行，不提供独立 Worker 或不可信代码沙箱。
- **资源界限。** 每个 `.so` 不超过 1 MiB，最多注册 16 个函数版本；提交定义不超过 256 KiB、64 条 DDL、16 个 Trigger。实际 KV 预算可能进一步限制上传。Server 启用后设置上传请求容量；SDK 有独立的 max_udf_request_bytes 上限。
- **依赖与卸载。** 示例库使用宿主兼容的标准库；没有任意依赖打包与分发协议。不要依赖原 `.so` 所在目录的相对资源。库从封存的内存文件加载，Registry/绑定回调持有其生命周期，调用相关对象销毁后才卸载。
- **计算语义。** guard 读取本轮更新后的 MV；capture_current 在受理时读取所声明的当前字段，缺行或必需字段为 NULL 时 NotReady。Clock 不隐式开启 session。已受理输入固定，任务可以重试，不承诺物理函数只执行一次。
- **持久化。** 注册元数据与库字节在同一 control shard 批次提交；UNKNOWN 先确认原批。checkpoint 保存原始UDF 文件，Compute 加载时重新验证摘要与 ABI。动态UDF 文件删除、运行中图迁移、SQL 标量 UDF、UDTF/UDAF 和 Process Function 不属于本次接口。

日志、配置、原始与恢复后的 Rocks 目录保留在脚本打印的临时路径。`EVENTFLOW_DATABASE_BINARY`、`EVENTFLOW_MESSAGE_BINARY`、`EVENTFLOW_DEMO_BINARY`、`EVENTFLOW_UDF_LIBRARY` 可覆盖对应产物路径。
