# 原生 UDF 示例兼容入口

完整上传示例已统一到 [streaming_database](../streaming_database/README.md)：定义、SDK 上传、部署、SQL 验证与 checkpoint 恢复都由该目录维护。

旧运行命令继续转发到同一实现：

```sh
bash examples/native_udf/run.sh
```

推荐入口：

```sh
bash examples/streaming_database/run.sh
```

仅需要静态 ComputationModule 方式时，参阅 [static_streaming](../static_streaming/README.md)。
