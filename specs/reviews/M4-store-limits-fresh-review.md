# M4 StoreLimits 独立范围评审

- 日期：2026-09-14。
- Reviewer：独立 fresh-context subagent `store_limits_review`。
- 范围：`AGENTS.md`、`specs/INDEX.md`、`specs/runtime-contracts.md` C3/C4，以及本次指定的两份 store 头文件、四份 storage/service 实现和 `tests/store_limits_test.cpp`。
- 结论：当前 StoreLimits 子任务未发现未解决的阻断问题。审查发现的 1 项 HTTP 严格解析问题已由实现者整改，并通过独立原始 HTTP 探针复验。该结论不表示 M4 预算准入或最终系统交付完成。

## 发现与处置

### R1 / P2：重复 Content-Encoding 可按头顺序绕过编码限制（已关闭）

- 原始定位：`src/storage/remote_store.cpp:147`，原实现仅检查 `get_header_value("Content-Encoding")` 返回的单个值，没有拒绝重复头。
- 复现：原始 HTTP 200 响应使用合法且精确的 Content-Length、合法五字段 JSON，并依次包含 `Content-Encoding: gzip`、`Content-Encoding: identity`。`RemoteStateStore::limits()` 返回成功；交换两个头的顺序则拒绝。
- 影响：响应允许互相冲突的编码声明，结果依赖重复头顺序，违背固定元数据端点的严格传输协议。未观察到状态修改或 UNKNOWN 绕过。
- 建议：直接拒绝多个 Content-Encoding；只有未声明编码或唯一的 identity 可通过。补充两个顺序以及重复 identity 的回归用例。
- 处置：实现者在 `src/storage/remote_store.cpp:147` 增加 `get_header_value_count("Content-Encoding") > 1` 拒绝条件，并在 `tests/store_limits_test.cpp:336` 加入两个混合顺序及重复 identity 回归。Reviewer 重链原始探针后 13/13 通过，正式 StoreLimits 测试 32/32 通过，R1 已关闭。

## 契约核对

| 要求 | 当前证据 |
| --- | --- |
| 五项正整数 limits，默认不支持显式失败，接口 noexcept | `include/eventflow/state_store.hpp:61`、`:87`；默认后端经 Cached/RPC 保留 FailedPrecondition 的测试通过 |
| max_batch_bytes 计完整规范批次字节 | `src/storage/state_store.cpp:46` 计 header/identity/framing；`:441` 校验 mutations/bytes；八种适配器组合覆盖恰好 256 bytes、257 bytes 及 mutation 上限 |
| 返回真实 open 配置，含 reopen/restore | `src/storage/state_store.cpp:666`、`:672`、`:679`、`:707`、`:714`、`:776`；Memory snapshot 继承源配置，Rocks reopen/restore 采用本次 validated 配置，测试通过 |
| 不读取或改变 shard 业务状态 | 两个本地 limits() 仅经 `config()` 读配置（`:496`）；server route 仅调用 limits() 并序列化；八种组合重复查询后 row/revision/receipt 与 duplicate receipt 均保持 |
| Cached 透明转发且不解除 UNKNOWN | `src/storage/cached_store.cpp:183` 仅转发；`:201`、`:268`、`:276` 仍执行既有 fence；`CachedMetadataQueryPreservesUnknownCommitFence` 独立运行通过 |
| 远端严格 JSON 字段与范围 | `src/storage/remote_store.cpp:98`、`:105`、`:168`；缺字段、未知字段、重复字段、零、负数、浮点、字符串、bool、null、数组、uint64 溢出均被测试拒绝；size_t 最大正整数接受 |
| 元数据响应有限且完整 | `src/storage/remote_store.cpp:134`：4096 bytes 上限，同时校验 Content-Length 并限制接收累积；4096 边界接受、超限拒绝；额外 raw HTTP 探针见下文，R1 已整改 |
| 服务请求严格且有界 | `src/service/kv_server.cpp:47`、`:51`、`:86`：现有 RPC 16 MiB 请求上限与 Content-Length/Transfer-Encoding 预路由检查；limits 仅接受空 object；非空/畸形请求返回 InvalidArgument，revision 保持 0 |

## 独立验证证据

- 直接运行实现者冻结源码对应的 `/tmp/eventflow-store-limits-build/bin/store_limits_test`：修复前 31 tests / 3 suites 通过；修复后独立再次运行 32 tests / 3 suites 通过，exit 0。
- 运行环境：`LD_LIBRARY_PATH=/home/obscure/EventFlow/cpp3rdlib/lib`，`ASAN_OPTIONS=detect_leaks=1:halt_on_error=1`，`UBSAN_OPTIONS=halt_on_error=1`；未出现 ASan/UBSan/LSan 诊断。
- 独立探针源：`/tmp/eventflow-store-limits-review-src/probe.cpp`；独立 CMake 构建目录：`/tmp/eventflow-store-limits-review-build`。使用 Clang 18.1.3、C++20、`-Wall -Wextra -Wpedantic -Werror`、ASan/UBSan/LSan、`-fno-sanitize-recover=all`。只读链接实现者冻结的 `ef_transport`、`ef_cache`、`ef_storage`、`ef_core` 库，没有修改其构建目录。
- 13 种原始 HTTP 响应：正常/唯一 identity 接受；缺失、重复、冲突、负数、溢出、4097 的 Content-Length，Transfer-Encoding + Content-Length，截断响应，单个 gzip 均拒绝。修复前两个重复 Content-Encoding 顺序中有一个错误接受，原始探针 exit 1；修复后重链同一探针，两个顺序均拒绝，13/13 通过，exit 0，无 sanitizer 诊断。
- 服务端原始请求探针：`/tmp/eventflow-store-limits-review-src/request_probe.cpp`，使用同一独立 CMake 构建和 sanitizer 配置；正常 `{}` 返回 200；缺失/重复/冲突/负数/溢出 Content-Length 与 Transfer-Encoding + Content-Length 返回 400；16 MiB + 1 返回 413；revision=0 且 receipt 为空，8/8 通过，exit 0。探针最初主动半关闭发送方向导致所有响应为空；移除这个不必要的 fixture 动作后，才将真实响应计入上述证据。
- 首次探针因动态依赖路径未设置未能启动；补充上述 LD_LIBRARY_PATH 后才得到实际运行证据，不将启动失败计作协议结果。
- 最终 SHA256：`src/storage/remote_store.cpp` = `d146309101620c072da142c00777b4e3fb5819ae2bb1507ffb3ae2a6f92835da`；`tests/store_limits_test.cpp` = `0806d92db7ed3680b33d31f5f821f73c838ac376f4868412437f846113bb7107`。本 reviewer 未修改 production/test 文件。

## 证据边界与下一步

- StoreLimits 报告后端 StoreConfig；已有 commit RPC 的 16 MiB JSON wire 上限是额外约束。本评审不承诺所有不超过 canonical max_batch_bytes 的批次都能通过远端 wire 大小限制。
- 本次没有评审 Engine 的预算消费、准入、PendingTrigger/Request 资源回收或全系统 M4 完成性，也没有扩展用户函数族。
- 正式交付的多轮全局 fresh-context review、真实多进程与 Kafka 集成证据，仍由总体规格和 owner 收敛；本报告不能替代这些门槛。
- 下一步：owner 将本子任务的接口与上述证据并入总体规格，继续完成 Engine 实际预算消费与 M4 全局验证；不得仅凭 StoreLimits 接口和本报告关闭总体预算任务。
