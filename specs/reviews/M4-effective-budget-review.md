# M4 Store→runtime 有效预算独立审查

- 日期：2026-09-14。
- Reviewer：fresh-context 独立 subagent `effective_budget_review`。
- 依据：`AGENTS.md`、`specs/INDEX.md`、`specs/request-functions.md` 的「Store→runtime有效预算接入」要求，以及实际生产源码、接口、测试和独立失败探针。
- 结论：本次发现的两项 RequestRuntime 预算 P1 已由实现者整改，并经原探针独立复验关闭。本报告仅关闭 StoreLimits 到 Engine/请求宿主的本轮配置匹配审查，未发现该范围内仍未处置的阻断问题；不表示 M4 或完整系统交付完成。

## 审查范围与隔离

检查 `include/eventflow/{engine,state_store,event_processor}.hpp`、`src/storage/remote_store.cpp`、`src/runtime/{engine,event_processor,request_runtime}.cpp`、`src/runtime/engine_internal.hpp`、`examples/feature_demo.cpp`，以及预算、运行时和 Demo 契约测试。额外读取实际 canonical codec、KV JSON 协议、服务端请求上限，确认计算对应真实编码。

独立副本：`/tmp/eventflow-effective-budget-review-9v4pezjs`。`build/` 为首轮冻结版本，`build-fixed/` 为整改后重新配置、完整编译的独立构建；未使用 root 的 `build/` 产物。只复用仓库 `cpp3rdlib/` 中的既有依赖。Reviewer 没有修改主树生产或测试文件，只写本审查报告。初始版本的有关源文件保存在副本 `initial-source/`，独立探针为 `tests/review_effective_probe.cpp`。

## 发现、影响与处置

### R1 / P1：活跃 RequestRuntime 恢复没有重验有效 mutation 预算（已关闭）

原 `RequestRuntime::open` 验证记录、索引及 active bytes，但没有重新执行 transition reservation。EventProcessor 自己补做的宿主预留不能代替直接使用 RequestRuntime 时的责任。

独立复现：先正常受理 `r1`，销毁 runtime/Engine，再通过实际只允许两项 mutation 的适配器恢复。Engine 的有效 `max_mutations` 正确变成 2，RequestRuntime 却返回 open 成功。随后两次取消都返回 `ResourceExhausted: round mutation count exceeded`；revision 不变，未抵达 commit。记录、索引和 Engine metadata 至少需要三项 mutation，不涉及用户回调输出。

整改：`src/runtime/request_runtime.cpp:577` 对每条恢复中的活跃记录调用 `reserve_transition`，在加入 active book、对外成功返回前检查。本次原探针 `RestoredActiveRequestMustRejectUnusableEffectiveMutationLimit` 已从失败转为通过，恢复现在明确拒绝不可用的容量。

### R2 / P1：没有 RequestStatus 的请求未预留自身记录/索引字节增长（已关闭）

原 `reserve_status_transition` 在缺少 `request_status_relation` 时提前返回，因此连 RequestRuntime 自有记录、索引及其 key 的字节预算也未验证。具有 RequestStatus 时，原计算还没有明确包含 Engine 必写 metadata。

独立复现：正常固定 fixture 的受理批次 raw mutation 为 18,224 bytes，首次 claim 为 18,664 bytes。让适配器报告 `max_read_bytes=18,224`，Engine 正确得到该有效 raw 上限；没有 RequestStatus 的请求仍返回 accepted，首次 claim 随即返回 `ResourceExhausted: round mutation bytes exceeded`。这个差额来自正常任务元数据增长，无需超出任何回调输出约束。

整改：`src/runtime/request_runtime.cpp:213` 的 `reserve_transition` 对所有 plan 统一保留完整 record/index 上限、key 和 Engine metadata，再按需追加 RequestStatus；新受理与恢复复用同一检查。`src/runtime/engine.cpp:1481` 的私有 `metadata_bytes` 调用与真实写入相同的 metadata 编码路径，并使用 `INT64_MAX` 的最大 processing-time 宽度，避免复制 JSON 布局后逐渐失配。原探针 `OwnRecordBytesMustBeReservedWithoutStatusRelation` 已转为通过：不足预算在受理前被拒绝。

## 契约核对

| 问题 | 独立结论与证据 |
| --- | --- |
| canonical header 是否覆盖最大身份宽度 | Engine 以配置 owner、真实 shard 和 `UINT64_MAX` 生成真实 commit ID，再调用实际 `canonical_batch` 获取空 batch header。mutation 数量字段本身固定为 uint64 宽度，因此空 batch 计算不会漏掉 count 位宽增长。最大 revision 预留保守覆盖可提交 revision。 |
| `header + 17N + raw` 是否为可同时使用的矩形 | put 每项为 key length 8、presence 1、value length 8，共 17 bytes；delete 只需 9。按有效总 mutation 数预留 17N，包含 Engine metadata 自己的 mutation；乘法前使用除法比较，无法留下正 raw 空间则拒绝。四种 KV 下 exact raw 可提交、再加 1 byte 不触达 Store commit；最大 identity header 下 canonical 恰好等于 8,192 bytes。 |
| 数量、读与 scan 上限 | max_mutations、page_rows 分别取实际能力与请求值的较小值；max_bytes 同时受剩余 canonical 和 max_read_bytes 限制。max_rows 仍表示轮内总行数，单点 get 只需 max_read_keys 至少为 1。scan 用有效 page_rows 分页；新增 read-key 与 scan-prefix 检查在请求进入 Store 前约束输入。cursor 来自已经检查过的读取 key。 |
| 能力缺失、无效或无空间 | 未报告能力、任何零能力、无法容纳最大身份及 mutation framing 均在 Engine 读取/修改 shard 状态前失败；巨大的 mutation 请求不能导致 reservation 乘法溢出。resolve 对已经有效的配置保持幂等。 |
| Remote 的 `6C` | protocol 对 owner/commit ID 的 JSON escaping 最多六倍，二进制 key/value 仅 hex 两倍；canonical 自带的 header 与每 mutation framing 足以覆盖对应 JSON 语法。独立 60 组 owner/revision/count 组合同时测 put/delete，包括 400,000 个控制字符的合法 owner、引号/反斜杠、UTF-8 和最大整数。全部满足实际 `dump().size() <= 6 * canonical.size()`。 |
| Remote read/scan 公式 | 令 W=16MiB、固定预留 F=128，先取 B≤(W−F)/4，再令 K≤(W−F−2B)/5、S≤(W−F−2B)/8。实际 get 请求/响应分别被 F+2B+5K 覆盖，scan 响应被 F+2B+8S 覆盖；prefix、after 分别≤B 时 scan 请求被 F+4B 覆盖。独立实际 JSON 探针核对 null、空值、hex 数据和整数宽度。 |
| Remote 真实 HTTP 边界 | 在大容量 Memory 后端上，Remote 报告 canonical 2,796,202 bytes、read 4,194,272 bytes。独立探针提交恰好 canonical 上限的二进制批次，随后 get/scan 成功；prefix 与 after 各恰好 4,194,272 bytes 的空 scan 也实际通过服务端。 |
| 宿主是否使用最终配置 | EventProcessor 在 bind 前 resolve，并在 Engine open 后保存 `engine->config()`；control/terminal 预留读取此有效配置。RequestRuntime 从相同 Engine 取得有效配置。宿主恢复解码会重新做 control reservation，随后对 active requests 逐条做 publication reservation；runtime 自身恢复经 R1 修复后也检查自身责任。 |
| 默认 Processor 与 Demo | Processor lifecycle 默认改为 1MiB，Demo 明确使用 512KiB，Demo 不再要求 Engine 16MiB。默认 owner/shard 0、10,000 mutations 下 Local effective raw 为 4,024,189 bytes，Remote 为 2,626,087 bytes，宿主发布必须在这个更小值内证明。完整 Demo 70 项及修复后 12 项关键路径均通过四种 KV。 |
| 预算不足的请求是否被伪装成已受理 | actual 1MiB Store 的四种 KV 测试在 capture 前拒绝创建 request，保留原 occurrence/once 状态，返回 Deferred 并附 `ResourceExhausted` 原因。该结果没有 accepted record。永久配置不匹配的具体原因由操作结果暴露。 |

## 新鲜验证证据

构建使用 Clang 18.1.3、C++20、Debug，仓库 CMake 强制 ASan/UBSan/LSan、`-fno-sanitize-recover=all`；运行设置 `ASAN_OPTIONS=detect_leaks=1:halt_on_error=1`、`UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`、`LSAN_OPTIONS=exitcode=23`。下列通过项未出现 sanitizer 诊断。

- 首轮冻结：StoreLimits 32、Engine runtime 69、Engine limits 3、RequestRuntime 58、完整 Demo 70，共 232 tests 通过。CTest 五个 target 共 289.48 秒，Demo 本身 281.77 秒。另运行 EventRequest 84 项通过；明确排除了当时依赖旧 2MiB 默认值的四个 TerminalBudget fixture，未把这些排除项算作通过。主实现者随后将其容量写成显式 2MiB 并补安全断言。
- 失败证据：`initial-probe.log` 记录 R1 的成功恢复及连续两次取消失败；`own-record-probe.log` 记录 R2 的 18,224 / 18,664 bytes 差异及 accepted 后 claim 失败。
- 整改后全新 `build-fixed/`：StoreLimits 32、Engine limits 16、Engine runtime 69、RequestRuntime 66、EventRequest 96，共 279 tests / 五个 target 全部通过，CTest 74.89 秒；日志 `fixed-contract.log` 与 `build-fixed/Testing/Temporary/LastTest.log`。
- 整改后 Demo：正常 embedding→prediction、durable tasks 恢复、第一项 UNKNOWN 后保留已 poll completions 三条轨迹各跑四种 KV，共 12/12，通过 52.90 秒；日志 `fixed-demo.log`。生产变动仅在预算检查与共享 metadata 编码，不重复完整 70 项慢 Demo。
- 最终主树新增的 Deferred reason 断言另行编译并独立复验四种 KV，4/4 通过；日志 `final-reason.log`。
- 最终独立 probe：5/5 通过，16.825 秒；日志 `fixed-probe-final.log`。包含两个原失败探针、60 组 canonical/wire 组合、真实 HTTP 边界及 read/scan JSON framing。
- HTTP 边界探针首次用默认 5 秒客户端超时，在构造/解析接近 16MiB 的 scan 请求时返回 transport failure；测试客户端改为 30 秒后相同大小实际通过。本报告证明编码/容量边界，不把该耗时当作吞吐或 5 秒延迟承诺。

## 源码一致性

副本 `review_hashes.json` 保留初始 SHA256，`review_fixed_hashes.json` 保留实际整改编译版本，`main_final_hashes.json` 保留审查结束的主树值。修复后的生产 `.cpp` 与内部头文件和主树逐字节相同；公开 Engine 头文件差异只是一处缩进，并使用实现者相同 clang-format 21.1.2 格式化后逐字节比对一致。

| 最终主树文件 | SHA256 |
| --- | --- |
| `src/runtime/engine.cpp` | `21b322027bc7d36e41e5a8cc7da7b43378578e8c6cb1bcedfeffdfa2747d07b9` |
| `src/runtime/request_runtime.cpp` | `9931d0567e0c23c6de21a021e583fc6222cc70a2c04da786a49dd7bae55da7dd` |
| `src/runtime/engine_internal.hpp` | `04257ff91e655b689ef8427484838827541650665e6af6af389c721c56e54f52` |
| `src/runtime/event_processor.cpp` | `dd82f11f58b02b42ef08325063e6aa76b8dd2dfe9c0878e4f2941d9573e8b86d` |
| `src/storage/remote_store.cpp` | `4a71d3627ae13c98906305016fa8ad3b783ad52d44b25c371a4eac1b9441e210` |
| `examples/feature_demo.cpp` | `028db6ccf140cd613f2fb0195d5cbe598e3fdc2c9c550063f408b2a736f0cec1` |

## 证据边界与下一步

- 这是固定 adapter 能力与 runtime budget 的匹配审查。Engine 不会把满足 raw 矩形的写入送入一个必然因 canonical framing 或本地 16MiB JSON 请求上限而失败的 Store；本次两种 accepted/恢复后无法完成的自身预算反例已消除。
- 不把未知用户回调输出、任意 SQL 下游或任意 companion 写入视为无限，也不替其他宿主证明这些预算；它们仍须遵守已有声明的 callback/closure envelope。无 RequestStatus 的 Runtime 自有预留成立，不等于其任意应用下游已获预算证明。
- Remote 报告的是有效操作 profile；通用响应接收器仍不提供针对恶意服务端的 16MiB 硬累积上限。scan 输入 prefix/after 的额外边界明确保留，真实 HTTP 探针不是 hostile-response 测试。
- 本次没有证明 M5 控制面、真实 Coordinator/KV/多 Compute 部署、Kafka、全局 watermark/checkpoint、通用 completion/view-change 订阅或最终多轮全局 review 完成。未实现的范围继续由规格管理，不能用本报告关闭总体里程碑。
- Owner 将本报告和当前 head 验证证据更新到有效预算规格，随后继续核心系统的剩余范围。


路径说明：示例后来按用户要求移入 `examples/feature_computation_demo/` 并重命名；本报告的日志和SHA对应当时受审版本，源码链接指向当前位置。
