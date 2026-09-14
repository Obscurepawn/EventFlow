# 存储、缓存与预算：实现决策和验证历史

本归档合并原 KV cache、query cache、snapshot 和 StoreLimits 的四份实现交接。
它保留容易丢失的边界与验证入口，删除旧分工、重复 API 清单及已被当前系统接入取代的待办。
现行契约见[运行时契约](../runtime-contracts.md)，当前交付证据见[验证报告](../verification/final-validation.md)。
以下测试和指纹属于历史版本，不能代替最终产物门禁。

<a id="kv-cache"></a>
## 确认状态缓存

CachedStateStore 由串行 owner 使用。读取命中前验证 backing revision；部分命中时整批
回源，不能拼接旧缓存与新 revision。只有 expected_revision 的驻留条目能随确认 commit
推进 revision；未触及热点保留，已驻留 mutation 按最终值写穿。增长超过旧读取证明或
cache budget 时只淘汰该条目；写入不自动创建条目，也不扩大已证明的读取额度。

UNKNOWN/Unavailable 清空条目并保留完整 canonical pending identity，阻止读/scan/新
snapshot/异批提交。status 观察不能解封，原批 Rejected 也不能证明先前 UNKNOWN 未提交；
只有同批有效 Committed 才解除 fence。owner epoch/显式 invalidation 不得丢弃未决提交。

LRU entries/bytes 是 wrapper 全实例预算，pending identity 另有 max_pending_bytes。
负值、空字符串、二进制及重复 key 区分；预算计保留数据与固定元数据成本，不等于 RSS。
原实现记录曾写“存储没有能力查询”；后来新增 limits()，该旧状态描述已作废，不能再照搬。

历史证据：`/tmp/eventflow-cache-tests.BrI1tk/build/Testing/Temporary/LastTest.log`，
51/51（Memory/Rocks）；跨 round 改进的 10 个 RED 先证明只写 B 会不断淘汰热点 A。
[原独立审查](../reviews/M3-cache-component-review.md)保留初版发现及后续复验。
增量独立探针 `/tmp/eventflow-m3-independent-review/cache_write_through_review.cpp`
与仓库测试共 59/59，包含连续 50 轮驻留写穿及每后端 1,500 步固定种子混合提交序列。
该复验沿用审查上下文，不能冒称新 fresh milestone review。对应冻结指纹：

```text
cached_store.cpp              4b96895369dd5431c8a6fe94d44160b8a93e1c195ad4f4d436dfa5da85ecf3ef
cache_write_through_review.cpp 9c5f6d18021aa6f6505a767053f8dc356cee9ba648550890675f65a14bef14a1
```

<a id="query-cache"></a>
## SQL 计划缓存

Engine 保存已深度验证的可信 Catalog；内部 query_sql 使用精确 SQL 文本键的有界 LRU。
公开 query(BoundPlan) 仍完整验证 provenance/表达式结构，外部可变计划不能进入可信缓存。
scope、当前行、预算和 revision 不缓存；命中也重做准入和当前数据读取。

Catalog 成功激活时失效旧 binder/cache；UNKNOWN DDL 保留旧对象但阻止查询，只有原批
确认后切换。普通 apply 不失效计划，UNKNOWN apply 仍须先解封。初始默认 128 项/4 MiB
按 Engine 计费；Compute 必须再分配节点预算，不能把 per-Engine 默认值称为节点总限制。

TDD 先保留旧查询实现，两个 Memory/Rocks 命中用例真实失败，再实现缓存。
历史 22/22 在 `/tmp/eventflow-query-cache-tests/build/Testing/Temporary/LastTest.log`；
当时 engine.cpp SHA-256 为 `889554f22510019d4a7140449c4ba2b71ec49b77fb7f1566be242a2be87ddd2f`。
[独立查询缓存审查](../reviews/M3-query-cache-component-review.md)另有探针和证据边界。

<a id="snapshots"></a>
## KV snapshot 与恢复

snapshot ID 是有限 ASCII 名称，同 ID 固定首次成功边界，不随源库后续提交更新。
固定顺序持全部 shard 锁，原子捕获数据、assignment 和 receipts；Rocks 等待 Flush，
再调用 Checkpoint，继续使用同步 WAL。Memory snapshot 只在源进程内保存。

恢复先验证 owners/shard count，复制到不存在的新活动目录，不覆盖已有目录或把快照
硬链接成可写目标。文件与父目录 fsync；失败保留目录供检查。Remote 只暴露创建/查询
ID，不接受任意路径或替换活跃服务。默认有限快照配额防止无限名称增长；没有声称实现 GC。
KV snapshot 本身不绑定 source/Catalog/runtime，不能称为完整分布式 checkpoint；
后续控制面接入状态见当前核心规格。

早期 Memory stub 和 Rocks/RPC 未实现行为均先真实 RED。非法 UTF-8 snapshot ID 曾
在 Remote JSON 编码前触发第三方异常；将共享 validate_snapshot_id 前置后修复。
历史 snapshot 17/17 + StoreContract 61/61 在 `/tmp/eventflow-m3-snapshot-build`，
源工程 `/tmp/eventflow-m3-snapshot-build-src/CMakeLists.txt`，Clang 三 sanitizer。
不包含掉电/文件系统损坏测试；独立存储/时间审查见
[M3-io-time-component-review.md](../reviews/M3-io-time-component-review.md)。

<a id="store-limits"></a>
## 实际存储能力与有效预算

limits() 报告 max_batch_bytes、max_mutations、max_read_keys、max_read_bytes、max_scan_rows；
未实现适配器返回 FailedPrecondition，不能猜默认值。canonical batch 预算包含全部身份、
header 和 mutation 编码；Rocks 恢复使用本次实际打开配置，不能套用旧快照资源限制。
Cached 转发能力查询而不改变 UNKNOWN fence。Remote 的只读响应要求完整严格 schema、
正整数字段、单个合法 Content-Length、无重复 Content-Encoding，limits 响应硬限 4,096 字节。

原八种组合（四 KV 及 Cached）新增 32 项，连同 145 项回归共 177/177、3.66 秒；
历史证据 `/tmp/eventflow-store-limits-build/eventflow/Testing/Temporary/LastTest.log`。
重复 Content-Encoding 的两条 RED 后增加三种排列回归；
[独立能力查询审查](../reviews/M4-store-limits-fresh-review.md)保留 13 响应帧/8 请求帧探针关闭证据。

后续 Remote 报告收敛为传输可用预算，不能继续把上述原始报告测试当作新版本验收。
当时采用 `W=16 MiB,H=128`：`C=min(backend_batch,W/6)`、`M=min(backend_mutations,C/9)`；
`B=min(backend_read_bytes,(W-H)/4)`，K/S 分别按 `(W-H-2B)/5`、`/8` 收敛。
Engine 再扣最大 revision 身份 header 与每 mutation 17 字节，从剩余额度推导实际配置。
完整操作响应的资源限制不能与仅 limits 响应的 4 KiB framing 检查混为一谈。

Request/EventProcessor 必须沿同一有效配置验证准入和恢复的未来终态/控制提交能力。
[有效预算独立审查](../reviews/M4-effective-budget-review.md)保留恢复 mutation 与无状态表
请求增长预留两项后续 P1 的关闭证据；这些保证仍不是 OS 物理内存预留或吞吐基准。
