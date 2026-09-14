# 基础类型与键编码

本目录继承[根规则](../../AGENTS.md)，负责所有模块共用的数据表示与持久键语义。

## 入口与职责

- [types.cpp](types.cpp) 实现 Schema 校验、Value/Row 转换、精确 JSON 解析和行序列化；公开接口在 [types.hpp](../../include/eventflow/types.hpp)。
- [key_codec.cpp](key_codec.cpp) 实现分区、二进制键、前缀范围和十六进制编解码；公开接口在 [key_codec.hpp](../../include/eventflow/key_codec.hpp)。
- [status.hpp](../../include/eventflow/status.hpp) 是公共错误边界。此层不引入 SQL、调度、网络或存储适配器依赖。

## 必须保持的契约

- Row 按 Schema 字段顺序排列；JSON object 入口拒绝未知字段、缺少必填字段、错误类型和无效 UTF-8。内部 Row 同样需要校验。
- NULL 与空字符串、零值分别表示；PK/partition 仅允许非空 Bool、Int64、TimestampMicros、String，PK 必须包含 partition 列。
- 整数词法溢出必须返回错误，不能回退成已舍入的 double；整数转 Float64 仅接受可精确表示的值，拒绝非有限标量和向量元素。
- `stable_hash` 的常量与字节顺序、类型标签、有符号整数排序和字符串 NUL 转义都是持久格式，修改必须明确兼容与恢复方案。
- 复合键边界不能碰撞；扫描保留稳定二进制排序和半开前缀范围，普通数据键不得混入 worker 身份或历史 revision。
- `shard_for` 使用业务 shard 数；Coordinator 的额外 control shard 不参与业务 key 路由。
- 业务 Value/Row codec 与服务元数据协议分别维护；不要把 Compute/Meta 的十进制字符串规则推广成所有业务整数的编码规则。

## 验证入口

- 直接测试：[types_test.cpp](../../tests/types_test.cpp)，覆盖 Schema、JSON 整数边界、Row codec、稳定 hash、复合键和前缀范围。
- 改持久键或行格式时，同时检查 [store_contract_test.cpp](../../tests/store_contract_test.cpp) 与 [engine_contract_test.cpp](../../tests/engine_contract_test.cpp) 的调用边界。
- 以下命令从仓库根目录执行，使用已配置并构建的 Debug 测试目录；有生产改动时先按根构建要求刷新目标。

```sh
ctest --test-dir build --output-on-failure -R '^types_test$'
```

规格：[运行时契约](../../specs/runtime-contracts.md)的类型、键和错误章节；[SQL 与增量关系](../../specs/sql-runtime.md)。
