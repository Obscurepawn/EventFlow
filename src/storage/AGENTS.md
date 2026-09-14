# 状态存储与缓存

本目录继承[根规则](../../AGENTS.md)，负责统一状态接口、本地持久化、远程适配和已确认状态缓存。

## 入口与职责

- [state_store.cpp](state_store.cpp) 实现 `MemoryStateStore`、`RocksStateStore`、规范批次身份与 snapshot；接口见 [state_store.hpp](../../include/eventflow/state_store.hpp)。
- [remote_store.cpp](remote_store.cpp) 实现 `RemoteStateStore`；配套服务在 [kv_server.cpp](../service/kv_server.cpp)，共享协议在 [kv_protocol.hpp](../../include/eventflow/kv_protocol.hpp)。
- [cached_store.cpp](cached_store.cpp) 实现 `CachedStateStore`；[cached_store.hpp](../../include/eventflow/cached_store.hpp) 定义容量、epoch 和未决提交接口。

## 必须保持的契约

- 所有适配器遵守 `IStateStore` 的原子批、owner 检查、revision 与去重语义；Memory 实现不能退化为语义较弱的测试字典。
- `commit_id` 绑定 owner、shard、expected_revision；服务端校验完整规范内容。同一身份不同内容必须拒绝，重试不得重命名或改写原批。
- mutations、revision、receipt 同批发布。最近一次相同提交可重试；旧身份不能绑定新 revision。`nullopt` 是删除，空字符串是有效值。
- `StatusOr` 外层调用错误与 `CommitCode::Committed/Rejected/Unknown` 分开。UNKNOWN 保留原身份和完整内容，在确认前不得推进下一提交或公开未确认状态。
- RocksDB 成功确认保留 WAL 且 `sync=true`；不确定写错误后实例拒绝读写，关闭重开后确认，不能以 memtable 可见性推断持久成功。
- cache 容量受节点分配预算约束；owner/epoch、revision 与确认状态变化触发相应失效。即使禁用条目缓存或执行 invalidate，也保留 UNKNOWN 阻挡与原重试身份。
- 命中缓存不能绕过后端读取数量/字节限制；`limits()` 报告适配器实际可用预算，远程线缆开销另外计入，不猜默认配置。
- 每次读取与同 shard 提交互斥，但没有 MVCC；跨页一致读取要求 owner 暂停该 shard 写入。
- `StoreSnapshot` 仅是存储边界；一致 checkpoint 由 Coordinator 绑定源位置、Catalog 和 runtime。已发布 snapshot 不可原地写入，恢复创建新目录。
- 当前 KV RPC 的 revision 使用 JSON 整数并校验范围；Compute/Meta 元数据使用十进制字符串。协议变更需同时检查客户端、服务端与契约测试。

## 验证入口

- [store_contract_test.cpp](../../tests/store_contract_test.cpp) 覆盖 Memory/Rocks 及远程适配；[remote_store_test.cpp](../../tests/remote_store_test.cpp) 包含真实 KV 进程与丢 ACK。
- [cached_store_test.cpp](../../tests/cached_store_test.cpp)、[store_limits_test.cpp](../../tests/store_limits_test.cpp)、[store_snapshot_test.cpp](../../tests/store_snapshot_test.cpp) 分别验证失效/UNKNOWN、预算和恢复边界。
- 从仓库根目录对已刷新构建的 Debug 测试目录执行：

```sh
ctest --test-dir build --output-on-failure -R '^(store_contract|store_limits|store_snapshot|cached_store|remote_store)_test$'
```

规格：[运行时契约](../../specs/runtime-contracts.md)的 KV/恢复章节；[事件生命周期](../../specs/event-lifecycle.md)；[核心交付](../../specs/core-delivery.md)。
