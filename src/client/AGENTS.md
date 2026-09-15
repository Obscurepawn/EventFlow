# 集群客户端

本目录继承[根规则](../../AGENTS.md)，提供应用使用的同步集群 SDK。

## 入口与职责

- [cluster_client.cpp](cluster_client.cpp) 实现端点与协议校验、Meta 调用、结果解码及有限重试；公开接口是 [cluster_client.hpp](../../include/eventflow/cluster_client.hpp)。
- `ClusterClient::open` 只配置 Meta 地址。Coordinator 决定路由和准入；当前客户端没有直连 Compute 的路由缓存或自动刷新协议。
- 修改公共接口时同步检查示例与 Coordinator/Compute 端协议，不能只让客户端接受一种服务端尚未实现的能力。

## 必须保持的契约

- `register_udf` 上传客户端 `.so` 并核对响应内容摘要；`submit_computation` 提交结构化定义，`deploy` 安装已提交定义。前两者是首次部署前控制操作，无自动重试；UDF 请求有独立字节预算，不绕过普通请求上限。参见 [生产 UDF 规格](../../specs/native-udf-delivery.md)。

- 64 位 version/revision/offset/time 在 Compute/Meta 元数据中使用规范十进制字符串，拒绝舍入后的 JSON number；shard/partition/count 按整数校验。
- 业务 Row 使用 Value codec，查询结果按返回字段验证类型和宽度，保留 Int64 精度；不要把元数据字符串规则强加到所有业务数值。
- 输入是否可确认取决于 `ClusterIngestResult::input_committed`；`operation_status` 可能报告提交后失败，不能因其失败抹去已提交事实，也不能因 HTTP 200 直接 ACK。
- 仅 ingest 可按显式 `ingest_attempts` 重试暂时性失败；每次发送完全相同的字节，保留原 source identity、payload 和时间。
- 控制变更只发送一次；网络失败不证明没有生效。禁止自动重试 DDL、deploy、pause/resume、checkpoint 或不确定 pump。
- `drain` 在固定逻辑时间重复已确认成功的有界 pump；遇到不确定失败或未确认输入时钟屏障立即返回，不自旋、不推进时间掩盖未决输入。
- 校验 endpoint、源身份、响应字节上限、重复 JSON 字段、完整路由和结构化错误；畸形响应不能被补默认值解释为成功。
- checkpoint 方法调用的是集群一致性协议，不是单独 KV flush；具体暂停、恢复流程遵循 Coordinator 合同。

## 验证入口

- [cluster_client_test.cpp](../../tests/cluster_client_test.cpp) 覆盖 Meta-only、精确身份、提交后错误、字节一致重试、控制调用一次及有界 drain。
- [coordinator_service_test.cpp](../../tests/coordinator_service_test.cpp) 验证真实 Meta/SDK 协作；[core_cluster_test.cpp](../../tests/core_cluster_test.cpp) 验证多进程数据链和恢复。
- 从仓库根目录对已刷新构建的 Debug 测试目录执行：

```sh
ctest --test-dir build --output-on-failure -R '^(cluster_client|coordinator_service|core_cluster)_test$'
```

规格：[核心交付](../../specs/core-delivery.md)的协议与路由章节；[控制面与集群](../../specs/control-plane.md)。
