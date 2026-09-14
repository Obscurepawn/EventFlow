# 完整 Reviewer 镜像全局审查：构建与运行完整性

Date: 2026-09-14  
Reviewer: reviewer_product_build_review（独立 fresh-context）  
Status: complete（本 reviewer 审查通过；一个 P2 已修复并定向复验）  
Scope: 本次完整源码镜像里程碑的一轮双 reviewer 并行全局审查；不是此前环境镜像审查的重开。

## 审查依据与检查对象

用户要求 reviewer 用简单命令编译、运行最新 EventFlow；先前独立交付环境镜像后，用户已确认源码稳定、文档仍在编辑，授权完成最终 bundled-source 镜像。依据根约定、`specs/AGENTS.md`、[索引](../INDEX.md)、[镜像规格](../reviewer-image.md)、[核心边界](../core-delivery.md)与[运行时合同](../runtime-contracts.md)检查。

本轮冻结目录为 `/tmp/eventflow-reviewer-context-hy1mq45d`；基线 `/tmp/eventflow-reviewer-build-inputs.json` 记录 518 项编译输入，创建时间为 `2026-09-14T13:58:22.295285+00:00`。独立 Python SHA256 核对冻结目录和主树均为 **518/518 一致**。这证明本次检查与冻结源码相符；尚不替代镜像内 manifest、二进制及实际运行证据。

本次 reviewer 只编辑本报告；没有修改生产、测试、示例或构建输入，也没有启动与交付构建争用资源的重型构建。

## 静态初查记录

此表保留实际镜像运行结果到达之前的初查范围；表中的待验证事项均已在下方处置表和同轮运行复核中闭环。

| 检查项 | 已取得证据与结论 |
| --- | --- |
| 构建政策 | `CMakeLists.txt:1-29,43-52` 强制 Clang；Release 为 `-O3 -DNDEBUG`、检测受支持的 IPO 并禁止测试；Debug 强制 ASan/UBSan/LSan 与不可恢复诊断。`docker/entrypoint.sh:43-55` 显式选择 Clang、Unix Makefiles 和 LLD，Debug/Release 使用独立目录。最终 flags 仍需容器内检查。 |
| 默认产物与重编译 | `Dockerfile:43-54` 在拷入生产/示例/测试源码后执行 Release 构建，只清除构建目录中的非 `bin` 内容。`entrypoint.sh:107-119` 的 `review` 使用 `--clean-first`，`test` 构建 Debug 后以 CTest 真实退出码退出，不仅运行随包二进制。 |
| 链接和 ABI | `CMakeLists.txt:53-60,73-89` 使用随包动态库，设置相对 RPATH 并禁用 new dtags；`Dockerfile:4-25` 选择 amd64、GNU C++ 开发库及 LLVM archiver。该配置具备完整链接路径，实际 `ldd`、ELF 标记和镜像 ABI 仍待检查。 |
| 测试隔离 | `CMakeLists.txt:149-266` 中 GoogleTest imported targets 和链接仅存在于 `BUILD_TESTING` 分支，CTest 为全部已注册目标设置 sanitizer 环境。Release 不带 GTest 的实际链接证据仍待检查。 |
| 项目依赖约束 | 冻结目录 `cpp3rdlib` 共 347 个普通文件/有效链接，未发现 `.c/.cc/.cpp/.cxx` 第三方实现；12 个库链接全部指向同目录随包文件。独立运行 `sha256sum --check --quiet MANIFEST.sha256`，退出 0。Dockerfile 安装系统工具链后直接使用随包依赖，没有 C++ 依赖构建下载。 |
| 示例覆盖 | 冻结源码存在八个 `examples/*_demo/run.sh`：checkpoint_recovery、condition_trigger、engine_clock_trigger、feature_computation、news_feature、sdk_clock_polling、sql_query、typed_pattern。`entrypoint.sh:92-105` 动态发现并运行，CMake 包含对应八个 executable。入口与八个脚本逐项 `bash -n`，9/9 通过。 |
| 真实进程和退出 | condition、sdk_clock_polling、checkpoint 脚本启动真实 KV、Coordinator 和两个 Compute；脚本使用 READY、子进程状态、反向停止及 `wait` 判断。`Dockerfile:40` 的 Tini 向进程组转发信号；`src/main.cpp:16-20,69-90,117-124,169-177` 将 SIGTERM/SIGINT 交给停止循环。实际容器 SIGTERM 及子进程退出仍待验证。 |
| checkpoint 不变量 | `examples/checkpoint_recovery_demo/run.sh:95-131` 使用两代真实进程，恢复至新的 RocksDB 目录；`main.cpp:93-169` 断言两 worker、源位置、水位、Latest 与 trigger_audit 恢复不变、旧投递去重及新输入继续推进。`tests/core_cluster_test.cpp:410-478` 还检查两代 PID、Catalog、冻结 revision 与源前缀。 |
| 控制面与存储边界 | `coordinator_service.cpp:496-570,808-835` 将 source/watermark/Catalog/worker receipt/control revision 绑定为冻结切面，拒绝把后续 live DB 当作 snapshot 恢复。`state_store.cpp:313,481` 保留 RocksDB WAL；`cached_store.cpp:284-324` 在 UNKNOWN 时保留原 commit 身份、阻止 snapshot 并仅在确认后应用缓存。镜像未修改这些路径。 |
| 范围保持 | 本镜像沿用现有简单输入与进程架构；没有引入 Kafka broker、HA、动态 UDF/UDTF/UDAF 或 Process Function。容器默认示例/协议 mock 通过不能升级为真实 Kafka 集成声明。 |

## 发现、处置与未完成门槛

静态初查未确认新增 P0/P1/P2 缺陷；随后真实信号探针确认了下方 PB-7 的 P2 包装入口清理问题。以下验收必须由本轮实际对象闭环，不能据静态配置或此前环境探针宣布完成。

| 编号 | 状态/影响 | 所需证据或处置 |
| --- | --- | --- |
| PB-1 | 通过 | 初始默认与 review 容器均 network none 退出 0；review 完整重编译 47 个 C++ object 后八示例通过。最终镜像保留完全相同的九个二进制，默认入口也再次在无挂载、network none 的容器中八示例通过并退出 0。 |
| PB-2 | 通过，保留两次运行区别 | 104 个 Debug C++ object 完整构建；首次全量其余 59 项通过，仅 coordinator 在原 180 秒 timeout 下失败。最终镜像按 600 秒配置完整复验 coordinator，CTest 1/1、GoogleTest 10/10 通过，158.06s。全部 60 个目标本轮完成验证；首次全量仍是退出 8、59/60。 |
| PB-3 | 独立检查通过 | 下方独立检查已核实 ELF/GTest 隔离、无 `LD_LIBRARY_PATH` 的动态库解析、非 root、518 项镜像身份与两个容器配置中的 Release/Debug flags、LLVM LTO；构建最终执行结果由 PB-1/PB-2 验收。 |
| PB-4 | 原失败已修复，最终通过 | 初始 SIGTERM 探针缺少 cleanup，已由 PB-7 整改。最终镜像真实四进程 TERM/INT 分别退出 143/130，四个 child 均正常退出；真实示例失败仍返回原非零状态。 |
| PB-5 | 通过 | 完整镜像归档 309,553,316 bytes，独立 SHA256 校验通过；已读实际 load 成功日志。导入后本 reviewer 独立检查 image ID 不变并默认运行八示例，6.091s、退出 0。 |
| PB-6 | 内容核对通过；owner 汇总收尾 | 初读的源码稳定状态/示例数量已修正，环境记录明确标为已完成历史阶段，根 README 和 Docker README 的编译/默认运行/测试/平台/日志命令相符。root 在汇总两位 reviewer 后同步 spec 与 INDEX 的最终交付状态，不需要重新开启审查轮次。 |
| PB-7 | P2；fixed，定向复验通过 | 初始 `docker/entrypoint.sh:65-68` 外层 Bash 未处理 SIGTERM/SIGINT，搭配 `Dockerfile:40` 的 Tini `-g`，外层可先退出，使内层清理未完成即结束容器。最终 `entrypoint.sh:66-72` 保留前台执行并捕获信号，已在实际 `e07c9842...` 镜像中验证四服务完整停止。 |

## 同轮独立镜像检查

镜像构建日志 `/tmp/eventflow-reviewer-image-build.log` 已完成源码 Release 构建和导出，实际镜像可运行。本 reviewer 独立执行 `docker image inspect` 及多个 `--network none` 容器检查，未启动重型构建。

- Image ID：`sha256:71b5663d8a2b4373601b85f75841fbb49d8bae2ed0f375ef690f6c3093a3fc7c`；Linux amd64、默认 `10001:10001`、Tini `-g`、`demo all`。
- `SOURCE_MANIFEST.sha256` 包含 551 项；清单自身 SHA256 为 `a65415b31a965473a10b82147aab3c512895f5371d083a018d5ea764d44bb197`。容器内逐项校验退出 0；映射 `/usr/local/bin/eventflow-review` 至基线 `docker/entrypoint.sh` 后，全部 **518 项编译输入与基线完全一致**。
- 对服务及八个示例分别执行 `env -u LD_LIBRARY_PATH ldd`，9/9 无缺库。服务 ELF 实际使用 `$ORIGIN/../../cpp3rdlib/lib` RPATH，依赖为随包 RocksDB/ANTLR 和系统 GNU C++/glibc 运行库；`nm -C` 未见 ASan/UBSan/LSan 或 `testing::` 符号。
- 镜像工具链实际为 Clang/LLVM/LLD 18.1.3、CMake 3.28.3、glibc 2.39、GNU libstdc++ 14.2。
- 未知命令、未知示例、`test extra`、零编译并行数均退出 2；以 `EVENTFLOW_DEMO_BINARY=/bin/false` 触发真实 typed_pattern 脚本失败，容器退出 1，五种场景均没有误报 `PASS:`。
- 只读检查 `eventflow-reviewer-rebuild-check` 中的 `compile_commands.json`：47 项全部使用 Clang、`-O3 -DNDEBUG -flto=thin`，无 sanitizer 或 fast-math；服务 link command 使用 LLD 且不链接 GTest。Cache 指定 LLVM 18 ar/ranlib、Release 与 `BUILD_TESTING=OFF`。
- 只读检查 `eventflow-reviewer-debug-check` 中的同一配置：104 项全部使用 Clang、ASan/UBSan/LSan、不可恢复诊断和 frame pointer；60 个已注册 CTest 目标全部设置三 sanitizer 失败环境。该配置证据不代替尚未结束的构建与测试结果。
- 已完整阅读 `/tmp/eventflow-reviewer-default.log`，包含全部八个示例成功标记；checkpoint 两代运行显示两个 worker、恢复后的 audit、旧输入去重与 110 -> 135 的继续推进，条件示例记录四个 child 的正常退出。默认命令的退出码留待实施者本轮终态记录绑定。
- 随后独立检查已停止的 `eventflow-reviewer-default-check`，network none、ExitCode 0、无 OOM，默认入口确为 `demo all`。独立检查 `eventflow-reviewer-rebuild-check` 同样退出 0；完整 review 日志有 47 次实际 C++ object 编译、八个 demo 成功，无 warning/error/sanitizer 诊断。记录 `/tmp/eventflow-product-build-review-rebuild.json`。

独立原始证据为 `/tmp/eventflow-product-build-review-independent.log`、`/tmp/eventflow-product-build-review-identity.json`、`/tmp/eventflow-product-build-review-errors.json`、`/tmp/eventflow-product-build-review-flags.json`。这些结果只关闭各自对应的门槛，完整测试与导入结论保持待验证。

## PB-7 信号问题原始证据与建议

两次探针都在真实条件示例完成业务断言后，用一个打印 `REVIEWER_CLIENT_WAITING` 的临时客户端包装脚本保持四个服务存活，再向容器发送 SIGTERM；未修改项目源文件。

- 默认包装入口 `eventflow-reviewer-signal-check`：退出 143，0.217s，`stopped_children=0`；日志停在 `REVIEWER_CLIENT_WAITING`，没有脚本的 child 停止或末尾日志路径输出。
- 直接 exec 原示例脚本 `eventflow-reviewer-direct-signal-check`：退出 143，0.732s，`stopped_children=4`；四个 child 均 `exit=0`，末尾 cleanup 日志完整。

本 reviewer 独立读取两容器 inspect、docker logs 和 `/tmp/eventflow-reviewer-signal.json`、`/tmp/eventflow-reviewer-direct-signal.json`，确认同一初始镜像、network none、真实四进程，差异仅在多出的一层入口 Bash。Bash 默认信号退出使 Tini 的直接子进程先结束；当前证据不能支持默认入口等待全部服务优雅清理完成。影响范围是 reviewer 容器的中断行为，可能在清理完成前结束进程 namespace。

建议在外层入口中处理 SIGTERM/SIGINT，保留向当前子进程传递信号并等待其清理结束的语义；修复后用同一默认入口探针确认四个 child 正常退出、清理日志完整、退出状态保留 143 且不需要强杀。保留这两次原始证据，在同一报告追加修复 hash、最终镜像身份及定向复验，不重开 fresh-context 轮次。

同轮第一次尝试把示例改为后台 child，再在外层 trap 中发 TERM 和 wait。独立读取当时 `/tmp/eventflow-reviewer-patched-signals.json`：TERM 退出 143、0.431s，但 `stopped_children=0`；INT 退出 130、7.054s、四 child 停止，但 `timed_out=true`。因此没有关闭 PB-7。外层重复 TERM 可打断示例已重置 trap 的 cleanup；后台 Bash 子进程还会改变 SIGINT 继承。已建议保留同步前台运行并由 Tini 负责一次组信号分发，外层仅捕获信号、等待前台清理后退出；最终结论仍以复验为准。

第二次修复在 `docker/entrypoint.sh:66-72` 保留同步前台，并加 INT/TERM trap。独立读取 `/tmp/eventflow-reviewer-final-signals.json` 与两份 `final-signal-*.log`：TERM 退出 143、0.373s，INT 退出 130、0.422s，两者均四个 child `exit=0`、无超时、cleanup 完整。挂载修复脚本的行为验证通过。

随后独立核对打包发现仍需同步固定上下文：主树入口 SHA256 已为 `c57ef717474ac876a3bc11563bf51a0cab69ba174a8aaf018d3c43f4636d4708`，而 `/tmp/eventflow-reviewer-context-hy1mq45d/docker/entrypoint.sh` 及当时 `eventflow:reviewer`（`6cc961...`）仍为旧 `71c023e478585164e04282ed397395df105b609dec01de98753b1ab5bc1d1b57`，`final-image-build.log` 全层缓存。因此此时 PB-7 只达到“修复本身通过”，尚未关闭最终产物门槛。已通知 root 把本次入口修复写入构建上下文，保留原基线并记录允许差异，再核对最终 image manifest 与实际信号运行。

root 随后说明固定上下文同步断言检测到并发的 CMake timeout 调整，因而停止同步；`6cc961...` 只是误触发的全缓存中间构建，不作为最终验收。独立重新计算 518 项基线，并从已结束的原 Release 容器读回 CMake 做 diff：当前恰有两项差异，入口修复和 `CMakeLists.txt:255` 为 `coordinator_service_test` 增加现有 600 秒 sanitizer timeout。其余 **516 项输入保持一致，所有 C++/测试源码未变**。CMake 没有改变编译、链接或断言，只调整这一 CTest 的超时；证据 `/tmp/eventflow-product-build-review-allowed-changes.json`。原 Debug 全量仍按初始 timeout 运行，后续应保留其原结果，并按新配置完整复验 coordinator 目标。

## 最终镜像身份的独立复核

实际最终镜像已重新构建为 `sha256:e07c9842cab9ab2fa4aca2a2049d39c0ed3c459da4c213469fb6d83156a26de4`，保留 Linux amd64、UID 10001 和原工具链。独立运行镜像内清单校验退出 0，551 项 `SOURCE_MANIFEST.sha256` 的自身 SHA256 为 `04a93339d798bbc2b686ccf41264d47ca709229fda0e6fc3d6504626186c78ab`。

镜像与当前主树 **518/518 编译输入完全一致**；相对初始基线仅有上方两个已核对的差异。入口 hash 已为修复后的 `c57ef717...`，CMake hash 为 `1d81834c0065c959dfa53620860ec4e40f7e12cbab2b654fd6f4d04c429d9904`。服务与八个示例的 SHA256 全部与最初 `/tmp/eventflow-reviewer-candidate-binaries.sha256` 相同，故初始版本的 ABI/二进制检查仍与最终交付对象一致。最终入口再次运行真实 typed_pattern 脚本的失败注入，保留退出码 1、没有误报 PASS。

独立证据：`/tmp/eventflow-product-build-review-final-identity.json`、`/tmp/eventflow-product-build-review-final-error.json`。随后分别核对了最终镜像的正常默认入口、实际信号退出与归档导入。

已进一步独立 inspect 并读取 `eventflow-reviewer-image-signal-term`、`eventflow-reviewer-image-signal-int`、`eventflow-reviewer-final-default` 的完整 docker logs：三者均实际使用最终 `e07c9842...`、network none、`Mounts=[]`，没有用挂载脚本替代修复。两信号容器分别退出 143/130，日志各含四个 child `exit=0` 和完整 cleanup；默认容器退出 0、八示例全部通过，均无 OOM。证据 `/tmp/eventflow-product-build-review-final-runtime.json`。PB-7 由此在同一轮关闭。

## 本轮 Debug 首次全量结果

独立检查 `eventflow-reviewer-debug-check` 的容器终态为退出 8、无 OOM、network none。`/tmp/eventflow-reviewer-test.log` 包含 104 次实际 C++ object 编译和全部 60 个 CTest 结果：59 通过，只有 `coordinator_service_test` 在旧 180 秒配置下以 180.13s 超时。总测试时间 1012.63s；`compute_service_test` 587.38s、`feature_computation_demo_test` 429.90s 均通过。没有构建 warning/error 或 sanitizer 诊断。

该日志 SHA256 为 `c4b2f486c9bdd62d002060c7afccb353e7a444a0e5909add0e3e3b0f8e42519b`；独立结构化核对为 `/tmp/eventflow-product-build-review-debug.json`。首次失败记录保留，按最终 CMake timeout 的完整目标复验见下一节。最终结果明确表达“首次全量其余 59 项 + 同源目标完整复验”，不改写原命令的退出状态。

## coordinator 的同轮完整定向复验

定向复验在实际最终 `e07c9842...` 镜像、network none 的 `eventflow-reviewer-coordinator-recheck` 内运行。该容器从最终 CMake 配置 Debug；独立读取生成的 `CTestTestfile.cmake`，确认整个 coordinator 目标的 `TIMEOUT=600` 和 ASan/UBSan/LSan 失败环境。不是选择单个 GoogleTest case，也未关闭 sanitizer。

timeout 调整不改变二进制，因此实施者复用本轮完整 Debug 构建的两个文件。本 reviewer 从原已结束的 Debug 容器逐字读取并计算 SHA256，又在最终 recheck 容器读取实际 hash，二者完全相同：

- `coordinator_service_test`：`3e9b7f590fac4ae0e4c8742192ec42037a65a2812972ed8da533b1913d87bd44`。
- `eventflow`：`7f795cfe91a8de0a58ea2469ecbde42eeaff3a1150b58210b1af7dd14e7be23a`。

`/tmp/eventflow-reviewer-coordinator-recheck.log` 为 CTest **1/1 通过，158.06s**；独立读取容器 `Testing/Temporary/LastTest.log`，确认 **10/10 个 GoogleTest case 通过**、`Test Passed.`，没有 sanitizer 诊断。证据为 `/tmp/eventflow-product-build-review-coordinator-config.json`、`/tmp/eventflow-product-build-review-original-debug-binaries.json`、`/tmp/eventflow-product-build-review-coordinator-result.json`。因此 PB-2 以本轮 59 个首次通过目标与这次完整目标复验闭环，不声称最终镜像又从零重跑过整套 60 个目标。

## 归档与导入后的独立运行

最终归档为 `artifacts/eventflow-reviewer-linux-amd64.docker.tar.gz`，309,553,316 bytes；SHA256 为 `89eeb6d14b5a5375ab66fd303b5948367258b8fa9312e6f44003bfd55d856699`。本 reviewer 独立执行其 `.sha256` 的 `sha256sum --check`，退出 0。

已审阅实施者 `/tmp/eventflow-reviewer-archive-audit.json` 的 19 层、607 个项目条目及无私有材料记录，读取实际 `/tmp/eventflow-reviewer-load.log` 的成功导入输出。导入之后，本 reviewer 独立 inspect 确认仍为最终 `e07c9842...`，再执行 `docker run --rm --network none eventflow:reviewer`：八个示例均通过、6.091s、退出 0，包含真实进程及 checkpoint 恢复。独立原始证据为 `/tmp/eventflow-product-build-review-after-load.log` 和 `/tmp/eventflow-product-build-review-after-load.json`。

交接前再次逐项核对 `/tmp/eventflow-reviewer-final-build-inputs.json` 与当前工作树，518/518 一致；用户后续文档编辑不改变已记录的镜像文档快照。

## 当前结论与下一步

本 reviewer 审查通过：没有未处置的 P0/P1/P2；发现的 P2 信号清理问题已修复并在实际最终镜像定向验证。Clang/ABI/同源、Release 重编译、全部 60 个 Debug 测试目标的本轮组合证据、默认八示例、失败与信号状态、归档校验/导入后运行均已闭环。root 可在汇总另一位同轮 reviewer 结论后同步规格与最终交付记录。

边界保持不变：这是 linux/amd64 reviewer 镜像的编译运行交付，不是原生 ARM、真实 Kafka broker、HA 或动态用户扩展的验收。没有对其他宿主平台或性能作额外声明。
