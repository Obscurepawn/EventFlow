# M4 A/B/C/D 独立产品轨迹审查

Reviewer: `m4_app_fresh_review`（独立 fresh context，无实现职责）  
Date: 2026-09-14  
Disposition: **REQUEST CHANGES**；发现两个可复现的 driver 正确性阻碍，不能关闭本轮 M4。

## 范围与依据

审查对象为尚未合并的 `/tmp/eventflow-event-request-work`，重点为 `examples/feature_demo.hpp`、`examples/feature_demo.cpp`、`examples/feature_demo_main.cpp`、`tests/feature_demo_test.cpp`，追踪 EventProcessor、RequestRuntime、RequestMachine、Catalog 与 Latest 的实际接口。依据主树 `AGENTS.md`、`specs/INDEX.md`、`specs/runtime-contracts.md` C1–C10、`specs/event-request-integration.md` 及原题 PDF、`docs/research.md`。PDF 仅在本地提取查看，没有发布或上传。

使用 develop-with-specs 与 verification-before-completion 的证据流程；本 reviewer 不改源码、既有测试、spec 或参考仓库。实现者 handoff 的 58/58 仅作为导航；以下证据来自自己的 Clang 构建、测试进程和独立对抗程序。

本轮可证明的是单 owner 的确定性 SDK/driver 和 Memory、RocksDB、HTTP→Memory、HTTP→RocksDB 四种 KV 适配器。HTTP 测试的 KvServer 在同一测试进程中启动；不能称为 Coordinator/KV/多 Compute 的多进程部署验收。M5、多进程恢复/consistent checkpoint、通用 completion/view-change 订阅、UDF/UDTF/UDAF/Process Function、Release/最终交付与多轮全局审查仍未完成。A→B level 重查不能替代完整通用 view-change 事件语义。

## Must-fix F1：UNKNOWN 后 driver 无确认路径，直接使用底层 resolver 又使输入 offset 失同步

严重度：P1。位置：副本 `examples/feature_demo.cpp:530`、`:553`，以及 `src/runtime/event_processor.cpp:359`、`:3457`。

`FeatureDemo::ingest()` 仅在当前 `process()` 返回 `input_committed` 时递增 `next_offset_`；`pump()` 直接调用 `EventProcessor::pump()`，后者的 `ready()` 对任何未决 KV commit 返回 `FailedPrecondition`。driver 没有调用 `resolve_pending_commit()` 的路径，无法使一次正常的 lost ACK 收敛。

独立 `LostReplyStore` 先执行真实 MemoryStateStore commit，再把 ACK 替换为 UNKNOWN；没有改写实现或伪造持久状态。轨迹：

1. `news(v1, t=1)` 已写入存储，但 `ingest` 返回 `UnknownCommit`。
2. `pump(2)` 返回 `event processor has an unresolved commit`，`has_pending_commit()` 仍为 true。
3. 另一测试显式调用公开 `processor().resolve_pending_commit()`，确认 `input_committed=true`，再 `pump(2)`。
4. `news(v2, t=3)` 仍用旧 offset，返回 `same durable identity has different payload`。

因此不能以“底层已有 resolver”或“调用方可以通过 processor() 自行确认”证明 demo 恢复正确；confirmed rounds、source position、active request/task 路由必须一起进入 driver 的消费路径。`consume` 是 private，目前调用方无法把底层确认结果安全交回它。

最小修复方向：driver 保留并解析 pending operation，在继续 source/pump/claim/completion 前确认原批；只消费 resolver 新返回的 confirmed rounds，并从已确认 source position 同步输入身份。保持原操作 fixed now，不重新 capture 原批，不重复提交或重复发布已确认子轮。不得把本地日志当恢复依据。

证据：`/tmp/M4app-probe/probe.cpp` 的 `SourceUnknownMustBeRecoverableThroughDriver` 与 `RawProcessorResolveMustNotDesynchronizeDriverOffset`；`/tmp/M4app-probe/result.log` 两条实际失败。截止本次报告尚未修复、尚未复验。

## Must-fix F2：批量 poll 后首条完成遇 UNKNOWN，其余完成被丢弃

严重度：P1。位置：副本 `examples/feature_demo.cpp:565`–`:588`，对照 `include/eventflow/function_executor.hpp` 的 poll 所有权契约。

`run_functions()` 先 `poll(limit)`，executor 此时已经把整个结果数组的所有权转交调用者并释放 reservation。随后逐条 `complete_request()`；任何一条返回错误（包括合法 UNKNOWN）都会提前 return，局部 `results` 数组中尚未提交的完成随之销毁。相关 `submitted_tasks_` 仍保留，于是 durable outbox 的任务也不会重新 submit。这是可恢复结果丢失，不是纯粹的展示日志缺项。

独立轨迹：

1. 同 ticker 建立两个新闻版本，两个 A task 均已经 claim/submit。
2. `run_functions(2)` 一次取得两条完成；第一条 owner completion commit 的 ACK 被注入 UNKNOWN。
3. 确认 pending kind 为 `CompleteRequest`，然后使用底层 resolver 确认第一条原批，以隔离 F1。
4. 再 `pump(3)`、`run_functions(3)`，返回执行完成数为 0；第二条 Request 的持久状态仍是 RUNNING。
5. executor 的 `active_tasks=0`，`callback_calls=1`、`cache_hits=1`，证明两个任务确实都产出了完成；第二条不是尚未执行。旧版本第一条可以正常写历史，当前版本第二条却只会等待超时。

最小修复方向：把已 poll 的完成保存在有界 driver 队列中，逐项确认 owner 接收后移除；UNKNOWN 确认期间保留当前及后续结果，清理路由/提交标记与确认结果一致。修复 F1 时仍需独立覆盖此轨迹，单纯在 `pump` 增加 resolver 不足以恢复已经被销毁的结果。

证据：`/tmp/M4app-probe/probe.cpp` 的 `CompletionUnknownMustRetainOtherPolledResults`；`/tmp/M4app-probe/completion-unknown.log` 实际失败（期望后续完成数 1，实际 0；期望 Success，实际 Running）。截止本次报告尚未修复、尚未复验。

## 产品语义与权威状态核对

| 需求 | 实现与当前证据 | 判断 |
| --- | --- | --- |
| A 每个合格候选；文本/编码器按 ticker 共享 | `candidate.on_source` 按 news_id/text_version 检查；A 从受控 Candidate 捕获文本；函数节点来自 Catalog；同内容缓存不绕过 candidate epoch | 普通路径已实现；UNKNOWN driver 阻断见 F1/F2 |
| B Ready、2s 等待且 once | Quote 必须有效且年龄 ≤2s，至少两个有效 history price，Latest embedding 对应当前 epoch；level binding 从接纳时起计 2s；accepted binding 与 Request 同批 | 现有四 KV 测试覆盖缺失/无输入过期、age 等号、非法值；不把回调执行计为 B accepted |
| C 30s、涨 ≥0.1%、10s 成交 ≥100、先 B accepted | wildcard guard 读稳定 Candidate/Quote/trade_volume；TTL 撤回 10s trade；C absolute deadline；binding prerequisite=b | 四 KV 测试覆盖 C 先于 embedding、B 受理后且未成功即可 C 受理、阈值配置、30s 等号过期 |
| D 5s、当前 slot skip、不补旧槽 | PeriodicPolicy + TryOnceAtFiring；miss reason/计数属于 durable candidate progress；coalesced slots 另由持久 periodic position 记录 | 独立轨迹先跳至 19s、新 Quote 不补旧请求、20s 当前槽受理通过 |
| 120s history / 300s candidate TTL | history 有 TTL，并按 observed_us 做 `(now-120s, now]` 有效样本过滤；expiry fence 先删除并 typed deactivate；Candidate/Latest 同边界失效 | 独立 120s 前后 D capture 与现有无输入 300s 过期通过 |
| 旧版本正文 TTL 后 high water 仍有效 | raw S key 与 source/Candidate 同批，key 是 ticker/news_id；正文 TTL 不删除 high water | 现有四 KV 测试在 reopen 后验证 n1v2/v3 不复活、v4 可激活 |
| 新 news_id 替换、旧完成只留历史 | Candidate epoch 推进；Latest 要求当前 active epoch、整 Request 成功且选较大 admission_seq | 独立查询 `embedding`：旧完成后历史 1 行但 Latest 0 行；新完成后历史 2 行、Latest 1 行 |
| 共享祖先、Fixed R/R+1 | RequestMachine 按 Catalog dependencies 建唯一节点；RequestRuntime 捕获 `capture_revision=R`、`admission_revision=R+1`，受控 Capture 写入与 Request 同批 | 独立 B 已受理后把 Quote 100→200、history 100→150；持久 quote_capture 仍为100、features.change=0，总 callback=4（A + features/prediction/risk） |
| 无新 input 的 A→B | completion companion 持久设置 level_dirty，后续 pump 受理 B；active book / task outbox 为恢复依据 | 普通与对象重开路径已有四 KV 覆盖；不是通用 completion/view-change 发布能力 |
| 满 active queue 仍释放 | RequestRuntime 有界 active，未受理 occurrence 留存；完成释放后继续 admission | 独立 16 个 active A + 第17候选：A17 留 pending；一次 run_functions 最终完成17个，持久历史17行，active归零 |
| 取消/timeout | EventProcessor 控制入口独立于外部 source；driver 即使 run_ready=0 仍 poll 已取消完成 | 现有四 KV timeout 测试验证 callback=0、executor active=0；不代表所有错误分支均正确（F2） |
| 重启 | FeatureDemo open 读取 processed_positions、bounded active_requests、pending_tasks；不扫描全 request history；日志 deque 有界且只作展示 | 验证限于 owner 对象重开/存储适配器；未执行 crash/dirty-run/multiprocess recovery，不得宣称 C9 完整关闭 |

两个 Request targets 为 prediction 与 risk；features 是真正共享祖先。app 没有自建第二张可执行依赖图。但它仍必须正确消费已确认的 owner 状态，F1/F2 正是这条边界上的缺陷。

## 独立构建与执行证据

构建目录 `/tmp/M4app-review`；Clang 18.1.3、C++20、`-Werror -Wall -Wextra -Wpedantic`、`-fsanitize=address,undefined,leak`、`-fno-sanitize-recover=all`。GoogleTest 只属于测试目标。

```sh
cmake -S /tmp/eventflow-event-request-work -B /tmp/M4app-review \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS=-Werror -DBUILD_TESTING=ON
cmake --build /tmp/M4app-review \
  --target feature_demo_test eventflow_feature_demo eventflow -j4
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
ctest --test-dir /tmp/M4app-review -R '^feature_demo_test$' \
  --output-on-failure --timeout 600
```

- 新构建成功，退出 0；配置/构建日志：`/tmp/M4app-review-config.log`、`/tmp/M4app-review-build.log`。
- `eventflow_feature_demo` 独立运行退出 0，日志 `/tmp/M4app-review-main.log`；展示成功不抵消 F1/F2。
- 58 项现有测试独立完整执行：58/58 PASS，CTest 退出 0，耗时 227.38s；`/tmp/M4app-review-tests.log` 与 `/tmp/M4app-review/Testing/Temporary/LastTest.log`。这是本 reviewer 新运行，未使用 handoff 的通过作为证据。
- 独立 CMake probe：`/tmp/M4app-probe/CMakeLists.txt`、`probe.cpp`，同一组 Clang/Werror/sanitizer 编译要求。第一轮 6 项：4 PASS、2 FAIL，退出 1；第二轮独立 completion UNKNOWN 1项：1 FAIL、退出1。失败均为上述逻辑断言，无 sanitizer 报错。
- 测试进程环境启用 ASan/UBSan failing diagnostics、LeakSanitizer `exitcode=23`。没有 release、容器、Kafka 或多进程 E2E 执行证据。

## 审查代码快照

下列四个冻结文件与本轮独立构建一致；完整关联源码 hash 在 `/tmp/M4app-review-source.sha256`。修复后必须追加新证据，不得把这些冻结 hash 的 GREEN 挪作新代码验收。

```text
fa4d9537a37f077c95db8115439d430be30f741ea72a8b435483a4e7c8bb5246  examples/feature_demo.hpp
90cca46fee2d9e9f16712ccca5f6b716a674aa9c4291a4b97d112ca2562868e2  examples/feature_demo.cpp
57da84e2c072ec4da8fc1eb1a0e3d0852b574e820794a27fc8ae6b62101d4379  examples/feature_demo_main.cpp
5f690feab0ac7999460ca652789da5f9bdef26fc15df8abf88bb1d2b4e71ccad  tests/feature_demo_test.cpp
```

后续：先修复 F1/F2 并补四 KV fault-injection regression，再由独立 reviewer 重跑三条故障轨迹、普通 C8 合同与主树整合验证；本记录不关闭 M4 或最终全局 review。


路径说明：示例后来按用户要求移入 `examples/feature_computation_demo/` 并重命名；本报告的日志和SHA对应当时受审版本，源码链接指向当前位置。
