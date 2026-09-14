# M3 bounded confirmed-state cache 独立组件审查

日期：2026-09-14。审查者：fresh-context agent `m3_io_time_review`，未参与缓存实现。

本文前半部分保留初始版本的独立审查；该版本在成功提交后清除整个 shard 缓存。当前实现已经增强跨 round 保留与 resident 写穿，最新结论见文末“2026-09-14 跨 round 优化复验补记”。初始实现描述不作为当前行为说明。

依据 `AGENTS.md`、`specs/INDEX.md`、`specs/event-lifecycle.md`，本次审查范围为 `include/eventflow/cached_store.hpp`、`src/storage/cached_store.cpp`、`tests/cached_store_test.cpp`。读取实现交接 `specs/archive/storage-history.md#kv-cache` 以了解组件边界，但下述结论来自当前代码与独立构建/运行。

## 结论

在该组件声明的单 owner 串行调用、后端遵守 `IStateStore` revision/提交契约的范围内，未发现可复现的混合 revision、缓存预算越界、隐藏读取预算绕过或 UNKNOWN 解封缺陷。独立 Clang ASan/UBSan/LSan 构建运行 39 项测试全部通过，其中 35 项为仓库现有测试，4 项为审查者添加在 `/tmp` 的参数化探针。

这是组件审查结论，不代表 Engine/Compute 接入、全节点预算分配或 M3 里程碑完成。没有修改生产源码或仓库测试；新增本文档与 `/tmp` 探针。

## 契约检查与证据

| 契约 | 实现位置 | 审查结果 |
| --- | --- | --- |
| 命中前确认 revision | `src/storage/cached_store.cpp:146` | 缓存开启时先读后端 `status(shard)`，删除不同 revision 的条目；status 失败直接返回，不能继续返回未验证命中。全命中读可在线性化点上解释为该 status 所观察的 revision。 |
| 部分命中不拼接版本 | `src/storage/cached_store.cpp:187` | 任一 key 缺失或组合额度未获准入时，原 keys 整批回源；只缓存完整返回的同一 ReadResult。现有竞态测试在 status 与回源之间提交改写，结果全部来自新 revision。 |
| 读取数量/字节额度 | `src/storage/cached_store.cpp:157` | 每个 entry 保存成功后端整批读取的 keys 数与 raw bytes，作为静态后端限额的保守准入证据。新组合必须同时满足各 entry 证据并逐项计费，重复 key 按出现次数收费；无法证明则整批回源。 |
| 全实例条目预算 | `src/storage/cached_store.cpp:76` | 所有 shard 共用 map/LRU 与 entries/bytes 限制。每条计入两份键、可选值和固定 metadata charge；负缓存也收费。单条不可容纳时不插入；能容纳时先淘汰至两项限制均满足。没有按访问次数增长的独立 shard/request 表。 |
| owner/context 失效 | `src/storage/cached_store.cpp:272` | `invalidate` 清理条目；不同 owner epoch 清理条目；同 epoch 不清理。存在 pending 时不允许切换 epoch。 |
| UNKNOWN 保留原身份 | `src/storage/cached_store.cpp:104`、`:221` | 提交前有界编码完整 canonical identity。UNKNOWN、Unavailable 或无效成功 revision 清空条目并保留身份；不同内容的提交不交给后端。只有相同身份重试获得有效 Committed 才解除 pending。 |
| UNKNOWN 不被旁路解除 | `src/storage/cached_store.cpp:148`、`:212`、`:254` | get/scan/create_snapshot 全部被阻止；status/snapshot_status 只供观察；invalidate、LRU 淘汰或关闭条目缓存都不清除 pending。原请求重试被 Rejected 后仍保持阻断。 |
| 成功/拒绝提交 | `src/storage/cached_store.cpp:232` | Committed 需严格满足 `expected_revision + 1`；随后仅失效该 shard。普通已知 Rejected 不发布 staged 值，也不无故删除已确认值。 |
| snapshot 透传 | `src/storage/cached_store.cpp:254` | 正常请求透传实际后端；未解决 UNKNOWN 禁止创建新边界。快照本身的实现问题由独立 IO/time review 跟踪。 |

## 独立运行

目录：`/tmp/eventflow-m3-independent-review`。`cache_probe.cpp` 在同一翻译单元包含当前仓库的全部 `cached_store_test.cpp`，另定义两项 `TEST_P`；每项分别运行 Memory/Rocks 参数。因此总数为 `35 + 2 × 2 = 39`。

构建直接编译当前 `src/storage/cached_store.cpp`，链接主任务已更新的 `build/libef_storage.a`、`build/libef_core.a` 及 `cpp3rdlib` 预打包 RocksDB/GoogleTest。编译器 Clang 18.1.3，C++20，启用 `-Wall -Wextra -Wpedantic -fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`；运行开启 leak 检查与失败退出。

```sh
cmake -S /tmp/eventflow-m3-independent-review -B /tmp/eventflow-m3-independent-review/build -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
cmake --build /tmp/eventflow-m3-independent-review/build --target cache_probe -j 3
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 /tmp/eventflow-m3-independent-review/build/cache_probe
```

实际结果：39 / 39 通过，退出码 0，无 sanitizer 诊断。

独立补充测试：

1. `ReviewRejectedRetryCannotResolveUnknownEvenWithCacheDisabled`：`max_entries=0`；一次提交已经应用但返回 UNKNOWN；绕过 wrapper 的另一笔真实后端提交推进 receipt；原请求再次提交被 Rejected。确认 pending 仍存在，其他 shard 的 get/scan 也被阻止，owner epoch 不能变化，status 观察到 revision 2 也不解除阻断。Memory/Rocks 均通过。
2. `ReviewRandomReadsMatchBackendWithBudgetsAndExternalWrites`：固定随机种子 213，每个后端 1000 次读取/外部写入序列，包含两个 shard、重复 key、缺失值、删除和不同长度值。后端限制为 4 keys/72 raw bytes，缓存限制为 7 entries/1600 charge bytes。每次直接后端结果与缓存结果的成功状态、错误码、revision 和 values 相同，缓存 entries/bytes 始终不超过预算。Memory/Rocks 均通过。

版本 SHA256：

```text
a0ad3012972f267801056a71b96c28269caeffe7c93eab5399b069776507c9bb  include/eventflow/cached_store.hpp
6fc6d78ee19a32a5de9d06b3ea08eb35e82ab41e02a133a84750d4f3b2b93c2e  src/storage/cached_store.cpp
5cd2b3f45f1c8d6ab24e135926bc6f6a247e8759156969809678c1b42f131816  tests/cached_store_test.cpp
c8e3d020b9fccc6ace3095d73a33dfdec3d989d5384e3fe0be868728e3ca7589  /tmp/eventflow-m3-independent-review/cache_probe.cpp
```

## 集成边界与下一步

- 主任务仍须把 wrapper 接入 Engine/Compute，并验证 round revision、持久提交前不可见、UNKNOWN 后原批次重试、owner assignment 变化的实际调用链。本审查没有将参数化组件测试当作进程部署或 E2E 证据。
- `max_bytes` 是文档约定的保留逻辑成本，不是 RSS 硬上限。Compute 必须统一分配各缓存的预算，并单独计入 pending identity、后端、在途读写与调用者缓冲；不能给每个实例各分配一整份节点预算。
- 准入证据依赖后端实例的读取限制保持有效。若部署替换/恢复后端或改变其读取限额，上层应清理或重建 wrapper，并同步 owner/context epoch；当前组件没有从 RemoteStateStore 自动检测配置变化的协议。
- 本轮使用真实 Memory/Rocks 后端，未把缓存 wrapper 再包在 HTTP 远程后端上运行共享测试；RemoteStateStore 自身和快照 HTTP 路径已有独立证据，但 wrapper+remote 组合仍属于后续集成范围。
- 计划缓存、函数结果缓存与生命周期输入处理不在本组件范围。根规格与 M3 完成状态由 root 维护。

## 2026-09-14 跨 round 优化复验补记

root 随后指出初始版本的成功提交会清空整个 shard，对连续计算的跨 round 复用有限。实现者新增 `apply_confirmed_commit`，本审查者没有参与代码实现；先独立推导边界，再只读检查当前源码与新增测试并补充 `/tmp` 探针。当前优化版本未发现新的可复现契约缺陷。

当前成功提交行为为：只将 `entry.revision == batch.expected_revision` 的该 shard 条目提升到确认 revision，保留未修改的 resident 值；其他 revision 条目失效。对原先已驻留的 mutation key，使用本批最后一次 mutation 的最终值写穿；新 key 不由写入直接加入缓存。原 `admitted_keys`/`admitted_bytes` 不增加；最终值超过旧读取证明或全局 cache budget 时只淘汰受影响条目。删除保留为负缓存，空值与缺失值保持区别。UNKNOWN、Unavailable、无效成功 revision 与精确重试的阻断行为保持原契约。

| 关键风险 | 实际复核 |
| --- | --- |
| 外部提交造成旧 revision 缺口 | `apply_confirmed_commit` 先按 expected revision 过滤；更旧条目不能被无条件升版，避免保留外部已经改过的 untouched 值。 |
| duplicate receipt | 后端确认相同 canonical receipt 后，expected revision 条目可安全提升；已缓存 confirmed revision 的重复调用目前保守失效，不用旧批次覆盖其他 revision。 |
| negative→positive、增长/缩水 | raw key+最终 value 必须仍满足原读取准入证明；写入不会扩大 proof。缩水先销毁旧字符串再保留新值，bytes 账本同步扣减/增加。 |
| 组合读取预算 | 单条写穿成功不代表组合查询可命中；`get_many` 仍验证新组合总字节与各 entry 的既有 proof，超限时整批回源获得后端错误。 |
| 同 key 多次 mutation | 存储契约允许重复 mutation，按顺序最后值生效。临时表只借用本批最后一项，避免先增长再删除/缩小造成不必要淘汰；不会保留该临时表。 |
| cache budget 与其他 shard | 写穿空间不足仅淘汰 touched 条目，不挤出 untouched/其他 shard 值；全实例 entries/bytes 上限保持。 |
| UNKNOWN 及重试 | 当前全部原 UNKNOWN 回归仍通过；随机序列还覆盖成功写入但返回 UNKNOWN 后的精确 duplicate retry，未发生提前升版或读穿。 |

独立构建目标：`/tmp/eventflow-m3-independent-review/build/cache_write_through_review`。它直接编译当前 `src/storage/cached_store.cpp`，包含当前仓库 51 项测试、初始审查 4 项独立探针以及本次 4 项参数化探针，共 **59 / 59 通过**，退出码 0，无 ASan/UBSan/LSan 诊断。编译器、C++20、失败诊断设置与前述独立 CMake 配置相同。没有修改生产源码或仓库测试，没有扩大到 Engine/多进程审查。

新增独立探针：

1. `ReviewFixedSizeResidentWritesStayHotAcrossFiftyRounds`：每个 Memory/Rocks 后端先读取两个键，之后连续 50 round 更新其中一个固定长度值，每轮同时读取 modified/untouched 键；revision和值全部正确，整个过程 backend get 始终只有最初的 1 次。随后空 mutation 提交继续提升 revision，也没有回源。
2. `ReviewMixedRoundWritesAndDuplicateMutationsMatchBackend`：固定种子 3124，每个 Memory/Rocks 后端 1500 步随机序列，混合经 wrapper 和直接后端的提交、重复 mutation、值增长/缩水/删除、UNKNOWN 后精确重试及重复 receipt。所有读取的成功状态/错误码/revision/values 均与直接后端一致；entries 始终不超过 7，charge bytes 不超过 1600。

```sh
cmake --build /tmp/eventflow-m3-independent-review/build --target cache_write_through_review -j 2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 /tmp/eventflow-m3-independent-review/build/cache_write_through_review --gtest_brief=1
```

优化复验版本 SHA256：

```text
fb1a058c173f7809c88d93e08113e0ea3bea02b05011098f943f842785810f43  include/eventflow/cached_store.hpp
4b96895369dd5431c8a6fe94d44160b8a93e1c195ad4f4d436dfa5da85ecf3ef  src/storage/cached_store.cpp
c75b21d96dd5f4281b80953d5d9993491a9eca25e1387f6c53a7b5f1a159b70b  tests/cached_store_test.cpp
9c5f6d18021aa6f6505a767053f8dc356cee9ba648550890675f65a14bef14a1  /tmp/eventflow-m3-independent-review/cache_write_through_review.cpp
```

前述 Engine/Compute 接入、节点预算统筹、backend/context 变更失效与多进程证据边界继续有效；此次只确认当前组件优化的正确性及实际回源次数改善。
