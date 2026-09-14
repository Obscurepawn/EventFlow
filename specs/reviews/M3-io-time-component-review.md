# M3 输入、时间与 KV 快照组件独立审查

日期：2026-09-14。审查者：独立 fresh-context agent `m3_io_time_review`。

本次只审查 `IMessageSource` 的 MemMock/Kafka 实现、clock/timer/watermark，以及 Memory/Rocks/Remote KV snapshot/restore。依据为 `AGENTS.md`、`specs/INDEX.md`、`specs/event-lifecycle.md` 和总规格中 AC22/AC23。没有修改生产代码和仓库测试；仅新增本文档及 `/tmp` 下的独立探针。M3 的 Engine/FSM/window/cache 集成及最终全局审查仍由后续工作承担。

## 结论与处置

本次累计发现 2 项 P2，均已由 root 整改并由本审查者复验关闭：公开 `open` 工厂可修改命名快照，以及发布目录 fsync 失败后的重试跳过同步。当前审查范围内没有遗留的已确认组件缺陷。具体原始复现、修复与验证证据保留如下。

输入与时间组件未发现可复现的契约缺陷。此结论限于下述源码路径、现有测试和独立探针，不代表完整 M3 生命周期已完成。

## P2-1：公开 open 工厂允许修改已有命名快照（已修复并复验）

**需求：** `create_snapshot(snapshot_id)` 冻结全部 KV 数据及 shard receipt/revision；既有 ID 重试必须返回原 snapshot。`from_snapshot` 创建新 store，不能写开原 snapshot。

**触发条件：** 客户端使用同一项目提供的 `RocksStateStore::open`，把路径配置为已存在的 `source_path/snapshots/<id>`。无需直接编辑 RocksDB 文件，也无需篡改内部数据格式。

**原因与位置：** 首次审查版本 `src/storage/state_store.cpp:257` 的 `StoreEngine::open` 在校验路径非空后，直接调用可写的 `rocksdb::DB::Open`；未识别快照目录或持久化的不可变标记。`src/storage/state_store.cpp:483` 的同 ID 重试只重新读取该路径；`from_snapshot` 同样信任该路径中的当前数据。因此快照目录被项目公开 API 写入后，同 ID 不再代表原版本。

最小公共 API 序列：

```cpp
auto source = RocksStateStore::open(db_path, config);
source->commit(batch_revision_0_with_value_before); // revision 1
source->create_snapshot("fixed");
auto writable = RocksStateStore::open(db_path + "/snapshots/fixed", config);
writable->commit(batch_revision_1_with_value_after); // succeeds
writable.reset();
source->create_snapshot("fixed"); // revision 2, not the original revision 1
RocksStateStore::from_snapshot(db_path, "fixed", fresh_path, config);
// Restored key value is "after" instead of "before".
```

独立探针：`/tmp/eventflow-m3-independent-review/review_probe.cpp`，通过独立 CMake 目标链接本仓库当前构建库。Clang 18.1.3 + ASan/UBSan/LSan，退出码 0，实际输出：

```text
snapshot_revision_before=1
open_snapshot_writable=1
same_id_retry_revision=2
same_id_restored_value=after
```

**影响：** 若部署或恢复操作将快照路径误传给正常工厂，后续同 ID 查询和恢复会静默得到另一个存储版本。已经绑定原版本的上层 checkpoint manifest 因而不能再信任 snapshot ID。这个触发依赖具体的公开 API 路径使用，故评级为 P2。

**建议整改：** 在调用可写 RocksDB open 之前识别并拒绝不可变快照目录，标识应持久化并覆盖路径别名；合法恢复只在新目标目录内移除或转换该标识。加入公开 API 回归：拒绝直接写开快照、原 ID 的数据/receipt/revision 不变、`from_snapshot` 的新实例仍可正常提交。

**整改复验：** root 增加持久化 `EVENTFLOW_IMMUTABLE_SNAPSHOT_V1` 标记；写开前检查标记，创建 snapshot 经 staging 目录写入标记后 rename 发布；`from_snapshot` 只对新目录省略该标记。审查者重新链接未修改的原 `review_probe.cpp`，实际输出 `snapshot_revision_before=1`、`open_snapshot_writable=0`，退出码 0。独立运行更新后的 `store_snapshot_test`，18 / 18 通过；新增回归同时验证路径别名拒绝、恢复实例可继续写入、原 ID 的 revision/value 不受影响。P2-1 关闭。

## P2-2：发布目录 fsync 失败后，重试会跳过未完成的同步（已修复并复验）

复验版本 `src/storage/state_store.cpp:530` 将 staging rename 为命名 snapshot，`:533` 再 `sync_path(root, true)`。当该目录 fsync 返回 EIO 时，首次 create 正确返回错误；但目标已在内存中的文件系统命名空间存在，重试进入 `:513` 的 existing-ID 分支，直接读出 snapshot 信息并返回成功，没有再次同步目录。读取存在的目录不足以证明先前失败的 rename 持久化已经完成。

独立故障探针为 `/tmp/eventflow-m3-independent-review/snapshot_fsync_probe.cpp`，使用链接器 `--wrap=fsync`，仅对本次临时库 `db/snapshots` 的第一次目录 fsync 返回 EIO；其余文件/目录同步都调用真实 fsync。它没有模拟 RocksDB 内容，也没有修改生产文件。Clang 三类 sanitizer，退出码 0，输出：

```text
first_create_ok=0 root_fsync_calls=1
retry_create_ok=1 root_fsync_calls=1
```

这证明重试成功路径没有重新完成失败的目录同步；没有据此声称已经实测断电丢失。影响是上层得到成功 snapshot 后可能依赖尚未确认持久化的发布边界。建议在 existing-ID 成功返回前重新同步 snapshot 根目录，持续同步失败时继续返回错误。

**整改复验：** root 在 `create_snapshot` 的 existing-ID 分支和 `snapshot_status` 成功返回前重新同步根目录。审查者只重链接当前存储库，未修改原 `snapshot_fsync_probe.cpp`，得到以下输出，退出码 0，无 sanitizer 诊断：

```text
first_create_ok=0 root_fsync_calls=1
retry_create_ok=1 root_fsync_calls=2
```

独立运行仓库新回归 `StoreSnapshot.RetryCompletesFailedSnapshotPublicationSync`，1 / 1 通过；该测试连续注入两次目录同步 EIO，验证 snapshot_status 同样保持错误，后续 create 重试才在第三次同步成功后返回。P2-2 关闭。本复验没有重复运行完整 19 项套件；完整套件结果由 root 自行记录，不能把本次单项结果扩写为本审查者独立运行了全套。

## 已核实的组件契约

| 组件 | 源码证据 | 独立核查结果 |
| --- | --- | --- |
| Source delivery/ack | `src/ingest/message_source.cpp:115` 的公共 acknowledge 状态机 | 按已交付顺序形成完成前缀；允许数字 offset 间隙；不同实例、伪造与撤销 token 被拒绝；重放不会降低确认位置。 |
| Kafka manual commit | `src/ingest/message_source.cpp:361` 配置及 `:531` ack | 禁用 auto commit 和 auto offset store，使用显式 commit；UNKNOWN 后必须重试原 token，不能 seek 越过；暂停不妨碍完成已有输入。 |
| Source bounds/replay | MemMock append/poll 与 Kafka poll/seek | key+payload、inflight count/bytes、有限 MemMock log 均有预算；byte-limited poll 不丢后续输入；超大 Kafka 记录停止交付并要求显式处理。 |
| Clock | `src/runtime/time.cpp:39`、`:54` | ManualClock 拒绝回退；SystemClock 使用恢复下界与 wall-time 锚定，之后以 steady elapsed 推进，并检查整数上溢。 |
| TimerQueue | `src/runtime/time.cpp:78`、`:105`、`:170` | generation 控制替换/取消；同 deadline 以 ID 排序；周期 tick 合并到最新 due slot；所有预算/溢出检查先于发布，decode 保留当前队列时间和下一 deadline。 |
| WatermarkTracker | `src/runtime/time.cpp:211`、`:226`、`:269` | 活跃未知分区阻塞；显式 idle 能解除阻塞；全 idle 保留水位；恢复活跃不能回退；使用记录到达前 watermark 判断 late。 |
| Whole-KV snapshot | `src/storage/state_store.cpp:470` | 固定顺序同时持有所有 shard 锁；Memory 同时复制 rows/receipt，Rocks 在该边界 flush/checkpoint，数据和 receipt 属于同一库版本。 |
| Snapshot restore/HTTP | `src/storage/state_store.cpp:665`（首次版本），`src/storage/remote_store.cpp:65`、`:207`，`src/service/kv_server.cpp:136` | 校验 assignment；拒绝覆盖已有目标；恢复复制而不创建可写硬链接；HTTP response 校验 ID、各 shard assignment 和 receipt 关系；丢失确认后同 ID 重试维持原版本。P2-1/P2-2 均已复验关闭。 |

## 本轮运行证据

主构建目录 `build` 的 `CMAKE_CXX_COMPILER=clang++`、`CMAKE_BUILD_TYPE=Debug`、`EVENTFLOW_SANITIZERS=ON` 已检查；目标链接含 `-fsanitize=address,undefined,leak -fno-sanitize-recover=all`。本审查没有 clean 或全量重建，也没有使用 GCC 参考目录。

| 执行 | 结果 |
| --- | --- |
| `build/bin/time_test` | 18 / 18 通过，退出码 0。 |
| `build/bin/message_source_test` | 30 通过、4 跳过、0 失败，退出码 0。4 个 skip 是 Kafka 特有测试在 MemMock 参数上的显式跳过。 |
| `build/bin/store_snapshot_test` | 17 / 17 通过，退出码 0；包含 Memory、Rocks、RemoteMemory、RemoteRocks 共享快照契约。 |
| `/tmp/eventflow-m3-independent-review/build/review_probe` | 3000 次 watermark 随机 observe/idle/encode/decode 循环保持可恢复性；one-shot 与溢出周期 timer 混合时失败不改变队列；同时复现上述快照 P2。退出码 0。 |
| `/tmp/eventflow-m3-independent-review/build/source_probe --gtest_filter='*Review*'` | 3 通过、1 跳过，退出码 0。对 48 条输入随机打乱完成顺序，逐步与独立完成前缀模型比较；4 次向后 seek 后确认位置维持 48；Kafka 某分区 UNKNOWN 时其他分区仍可 ack/poll。跳过项仅为 MemMock 不进行 Kafka 错误注入。 |

以上运行均未出现 sanitizer 诊断。Kafka 测试仅使用 librdkafka 协议 mock，没有启动或要求真实 MQ 集群；这些结果不被表述为真实 Kafka broker 部署证据。

复现独立探针的命令：

```sh
cmake -S /tmp/eventflow-m3-independent-review -B /tmp/eventflow-m3-independent-review/build -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
cmake --build /tmp/eventflow-m3-independent-review/build --target review_probe -j 2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 /tmp/eventflow-m3-independent-review/build/review_probe
```

首次审查版本 SHA256：

```text
08ce23b404419f1d4b7e1e1dd79b59c14bffda8036ef6ab4dd2d1702e5062c87  src/ingest/message_source.cpp
dd565351f3a468e3e853142c1f36fe2a219173e229e174d8957b758e92d63ace  src/runtime/time.cpp
008b7665bdf4941cac48168ba4309ede6b835226876f6020d57ed522c10c1aca  src/storage/state_store.cpp
c9cb83bff37a79f5e6ca900f67a27c75164777edfa6ef7609d600faa7b87ea4a  src/storage/remote_store.cpp
134757adfa3985b10feae1ef4b09995e874e9fa178fab7aa68a2ee823f0fd88e  src/service/kv_server.cpp
5b0e7c01d12d5c6903d04565c0d61a9856ba9889e8428887ae208cf354e59c18  /tmp/eventflow-m3-independent-review/review_probe.cpp
```

P2-1 复验版本 `state_store.cpp` SHA256 为 `b3de9d163347fa2f7340e93073ac33deb73060650c8d5717fb67e242c6db4dbd`。P2-2 关闭时的源码/探针 SHA256：

```text
0c55bc5d63a0045cf4e6d0466e339b0995e75c72d869f9af8394b7d441fa9282  src/storage/state_store.cpp
5c37cfdc2b6f0caf5a304eb6e4c7ab67ed3ca540f64424901b73d49a1e3297fd  tests/store_snapshot_test.cpp
de49047da6401ae3d763171cc084f69e82ef5b8341b645f604b60645207707d1  /tmp/eventflow-m3-independent-review/snapshot_fsync_probe.cpp
```

## 剩余边界与交接

- P2-1/P2-2 均已独立复验关闭。本审查不替代修复实现者的完整回归运行。
- 本轮定点注入了 snapshot 父目录 fsync 的 EIO；没有注入 RocksDB flush/checkpoint 崩溃，也没有独立验证断电恢复。正常 reopen/restore 和同步错误分支的证据不能扩展成真实断电恢复证明。
- 组件 snapshot 只是存储边界。Coordinator 仍需绑定 source positions、Catalog 和 Timer/FSM/request/runtime 状态，完成一致 checkpoint 协议与按该位置重放。
- `IMessageSource::ack` 不具备检查计算已持久化的能力；commit-after-compute 必须由后续 Engine/Coordinator 调用链和 MemMock 多进程 E2E 证明。watermark late 策略真正影响窗口、无输入时 timer 推进、同刻 timeout 顺序等也仍属于待审的 M3 集成。
- Root 维护 `specs/` 与 M3 状态；本次不把任何里程碑或最终交付标为完成。
