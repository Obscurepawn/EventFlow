# 生产源码协作约定

本目录继承[根约定](../AGENTS.md)。修改前阅读 [specs 索引](../specs/INDEX.md)及对应模块的 `AGENTS.md`；跨模块变更同时检查公开头文件和调用方。

## 模块与入口

| 目录 | 职责与本层约定 |
| --- | --- |
| [core](core/AGENTS.md) | Value、Row、类型校验与稳定 key 编码 |
| [storage](storage/AGENTS.md) | IStateStore、Memory/Rocks/Remote 适配、确认状态缓存 |
| [sql](sql/AGENTS.md) | grammar、AST、binder、表达式、唯一 Catalog 与 codec |
| [runtime](runtime/AGENTS.md) | 增量 Engine、FSM/CEP、Trigger、请求与函数执行 |
| [ingest](ingest/AGENTS.md) | broker-neutral 输入投递、确认前缀与重放 |
| [service](service/AGENTS.md) | KV、Compute、Coordinator 的 HTTP 服务及一致 checkpoint |
| [client](client/AGENTS.md) | 只配置 Meta 地址的集群 SDK |

`main.cpp` 调用公开 `database_main`；`service/database_main.cpp` 负责 `kv`、`compute`、`coordinator` 子命令、配置校验和进程生命周期。应用通过 `ComputationModule.bind` 注入完整静态计算定义，通用服务不安装示例业务。进程角色不等同于 CMake library 划分，例如 `ef_transport` 同时包含 RemoteStateStore 和 KvServer。

## 修改边界

- 公共 API 放在 [include/eventflow](../include/eventflow/AGENTS.md)；实现私有头文件留在模块内部，避免调用方依赖 `engine_internal.hpp` 或 `binder_internal.hpp`。
- 各层使用 `Status`/`StatusOr<T>` 和 `noexcept`；输入解析或第三方返回错误在边界转为 Status，不用异常作为控制流。
- 外部依赖通过已有接口接入，保持确定性替身可用；底层 core 不引入服务、运行时或测试依赖。
- 启动配置必须经过校验与状态返回的 factory。保留服务停止、线程回收和 ready 消息的实际含义。
- 变更由根 `CMakeLists.txt` 接线；不要引入独立构建系统或构建时下载。
- `runtime/computation_runtime.cpp` 负责通用 owner/执行器推进；`ingest/source_poller.cpp` 负责保留投递并在输入确认后 ACK，Coordinator 托管持续消费。示例定义只位于应用目录，测试 fixture 不能成为生产默认业务依赖。

## 验证

构建方法和定向测试流程见[测试约定](../tests/AGENTS.md)。修改 `service/database_main.cpp` 或应用入口的启动、配置或停止路径时，至少核对 `core_cluster_test`、`remote_store_test` 和受影响的真实进程示例；仅构造服务对象不能证明进程入口可用。按影响范围完成根约定要求的质量门禁，不沿用历史测试数量作证据。
