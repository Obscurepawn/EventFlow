# 示例开发约定

本目录继承[根约定](../AGENTS.md)。默认完整使用入口是 [streaming_database](streaming_database/README.md)，通过 SDK 上传 UDF、提交计算定义并部署。修改前读其 [AGENTS.md](streaming_database/AGENTS.md)及[本轮规格](../specs/streaming-udf-default.md)。[static_streaming](static_streaming/README.md)保留静态模块参考，[cpp_udf](cpp_udf/README.md)展示本地生产加载器，[native_udf](native_udf/README.md)只保留转发兼容入口。

## 默认示例职责

- `definition.cpp` 返回可提交的 SubmittedComputation，声明表、级联 MV、session、条件/Clock 与 UDF 引用；业务计算在独立 `.so` 中。
- 通用 eventflow Server 不链接示例业务。SDK 连接已经启动的 Coordinator，上传、提交后 deploy。
- `message_server.cpp` 是两个完整示例共用的 HTTP 消息设施。producer 只 publish，Coordinator 托管 SourcePoller；验证器不能调用 ingest/pump/drain 代替数据库运行。
- 真实共享 Rocks KV、Coordinator 和至少两个 Compute。验证不同 shard、51.5/128/53、固定输入，以及数据库进程 checkpoint 重启后无需重新上传即得到 54.75。
- `static_streaming` 保留原有独立静态模块流程；`native_udf` 不增加重复业务实现或重复端到端 CTest。

## 修改与验证

- 中文 README，代码/注释/命令/日志英文 ASCII，函数 noexcept、aggregate 指定字段，遵守根约定。
- run.sh 从源码或安装目录运行，支持构建目录参数/环境变量、Clang/CMake/Make 和已有构建配置；不下载依赖。
- 进程脚本检查 READY、客户端和所有子进程退出，保留 sanitizer 失败与日志、数据路径。更新例子时同步 README、CMake/安装与当前导航。
- UDF 上传/提交仍限首次部署前 STAGING。恢复时 HTTP 消息服务保持运行，不把此例描述为消息服务持久恢复、HA 或真实 Kafka 验证。
- 正常运行与相应合同测试验证后，按根约定做一轮独立审查，记录本轮证据，不沿用历史测试数量。
