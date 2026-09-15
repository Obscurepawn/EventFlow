# SDK 上传 UDF 的流式数据库示例

继承根目录与 examples 约定。当前调整见 specs/streaming-udf-default.md，生产接口见 specs/native-udf-delivery.md。

- definition.cpp 返回 SubmittedComputation，业务函数由 ../cpp_udf/news_impact.cpp 单独编译为 .so。
- 使用通用 eventflow Server，启动后 SDK 上传、提交、deploy；服务端不静态链接新闻业务。
- verify.cpp 只生产 HTTP 消息、控制部署、查询；不调用 ingest/pump/drain 代替数据库计算。
- run.sh 启动消息服务、共享 Rocks KV、Coordinator 与两个 Compute，再从 checkpoint 重启数据库进程；恢复不重新上传 UDF。
- 保留 51.5/128/53、恢复后 54.75 和旧固定结果 51.5；消息服务在恢复期间保持存活。
- 静态模块完整示例保留在 ../static_streaming，../native_udf 只是兼容转发入口。
