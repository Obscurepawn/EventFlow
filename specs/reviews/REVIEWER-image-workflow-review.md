# Reviewer 镜像使用流程独立审查

Reviewer: image_workflow_reviewer  
Date: 2026-09-14  
Round: 本次交付唯一一轮、两位 fresh-context reviewer 并行全局审查之一  
Status: 当前编译环境范围复核通过；归档待验证项已关闭，最终项目快照验收后置

## 最新范围调整

2026-09-14，实施者转达用户最新要求：源码仍在修改，先完成 Docker 环境，再处理最终项目快照。当前里程碑交付工具链与 `cpp3rdlib` 环境镜像、本地导入归档，以及挂载外部源码后的构建命令；通过项目外的小型 CMake 工程验证离线 Clang、Debug/Release、sanitizer 与依赖编译运行。最终源文件封装、七个项目示例、完整项目 CTest 和真实集群 cleanup 验证后置，不能成为当前环境里程碑的完成门槛，也不能由环境 smoke 冒充通过。

下文保留首次静态审查的发现及快照，供追溯。W1 已修复，镜像内保留 `docker/README.md` 原路径；W2 是用户进行中的示例重命名事项，不改用户拥有的 `examples/AGENTS.md`，最终源码稳定后再核对。所有环境实现和证据复核均在同一轮内完成。

## 当前环境实现复核

2026-09-14，本轮继续核对实际环境镜像 `eventflow:build-env`，ID 为 `sha256:720a928535bb08cd551d8f61179145934f78c6e65120004ffacb24a24965a175`。未新增 P0/P1/P2 问题。环境实现、工具链、离线依赖编译运行、基础入口，以及后来补充的归档导入证据均已复核；本 reviewer 对当前编译环境范围的结论为通过。此结论不包含仍在修改的 EventFlow 最终源码快照。

### 原发现处置

| 发现 | 处置与定向复核 |
| --- | --- |
| W1 / Docker 文档路径 | 已解决。当前 Dockerfile 第 34 行保留 `docker/README.md`，本 reviewer 在该镜像的断网容器中读取其 SHA256 为 `c43c1eff150592425c6c509fea453985254e88cd859eaff44e1fbff7c7121574`，与当前仓库文件相同。新说明第 77 行明确完整研究文档和 specs 留在源码仓库 |
| W2 / 示例旧名称 | 转为最终源码快照事项。当前环境 stage 不复制 examples，入口从挂载源码动态发现 `*_demo/run.sh`，不依赖历史七项名称；用户正在修改的示例指南未由 reviewer 改动，不阻断当前环境交付 |
| 挂载依赖和文件权限风险 | 已处理。说明第 20–28 行要求完整仓库及 `--user "$(id -u):$(id -g)"`，明确挂载遮蔽镜像同路径依赖，实际使用源码仓库的 `cpp3rdlib`。入口第 7–8 行使用独立 `build-docker-release` / `build-docker-debug`，避免复用普通本机构建缓存；临时 smoke 的两个构建目录实际 owner 为 1000:1000 |
| Tini 与应用 cleanup 边界 | 基础 shell 信号路径已由实施者验证；实际项目集群 cleanup 仍后置。没有用基础 shell 探针宣称三个集群脚本的信号回收通过 |

### 已核对证据

| 范围 | 证据与结论 |
| --- | --- |
| 实际镜像配置 | 本 reviewer 执行 `docker image inspect`：linux/amd64，User=10001:10001，Entrypoint 为 Tini `-g` 加 eventflow-review，Cmd 为 help；Config.Env 只有 PATH 与 EVENTFLOW_SOURCE_DIR，没有主机代理配置 |
| 环境和源码身份 | 本 reviewer 在 `--network none` 容器内读取入口、Docker 文档与依赖清单的哈希，均与当前仓库文件一致；入口哈希 `71c023e478585164e04282ed397395df105b609dec01de98753b1ab5bc1d1b57`，依赖清单哈希 `d94c2c52d64ef224867d55a032e6c293d352b0bc2f90527961a25e24c352851f`；容器内依赖 `sha256sum --check --quiet MANIFEST.sha256` 退出 0 |
| 工具链 | `/tmp/eventflow-environment-final-identity.log` 与实际镜像配置对应：Clang 18.1.3、CMake 3.28.3、glibc 2.39、g++ 13.3、libstdc++ 14.2，以及 lld/llvm 18；新增 LLVM archiver/ranlib 供 ThinLTO 静态库使用 |
| Release 编译 | 检查 `/tmp/eventflow-environment-release-final.log` 和临时工程实际 flags/link 文件：Clang 构建静态 `dependency_probe` 后链接可执行程序，`-O3 -DNDEBUG -flto=thin`、lld、llvm-ar/llvm-ranlib 生效，无 sanitizer 或 fast-math；该日志是实施者的 fresh 构建证据，本 reviewer 没有重复构建 |
| Debug 与 GoogleTest | 检查 `/tmp/eventflow-environment-debug-final.log`、实际 flags/link 和 CTestTestfile：ASan/UBSan/LSan、`-fno-sanitize-recover=all` 及失败环境变量生效；GoogleTest 仅在 BUILD_TESTING 分支链接，CTest 1/1 通过且无诊断。这个数量属于临时依赖工程 |
| smoke 实际内容 | 读取 `/tmp/eventflow-container-smoke-176awiko` 中 CMakeLists、probe.cpp、main.cpp、test.cpp：验证 C++20 span、ANTLR input、JSON 序列化、HTTP loopback 请求、RocksDB 打开/Put/Get/销毁、librdkafka producer 创建/销毁和线程退出。没有启动 Kafka broker，也没有编译用户生产源码 |
| 独立离线运行 | 本 reviewer 使用该镜像、`--network none`、UID/GID=1000:1000、挂载上述临时工程，清空 LD_LIBRARY_PATH 后运行其已构建 Release 探针，退出 0，输出 `DEPENDENCY_SMOKE_RESULT=0`；不重复重型构建。实施者的 `/tmp/eventflow-environment-release-runtime.log` 进一步表明非基础动态依赖均解析到随包 cpp3rdlib，无 not found |
| 入口行为 | 阅读 `/tmp/eventflow-environment-cli-checks.json`：默认 help 退出 0，未挂源码 build 退出 2；自动发现 alpha/beta 示例并通过；新增 fails 示例退出 7，没有打印成功；未知命令和无效并行数退出 2。动态发现避免固定的七/八示例列表随源码变化失效 |
| 基础信号 | 同一 CLI 记录中 shell 输出 SIGNAL_READY 后，经 docker stop 输出 SIGNAL_HANDLED，容器退出 0，耗时 0.184 秒。这证明 Tini 到 exec Bash 的基础 SIGTERM 转发，不证明实际项目集群 cleanup |

### 归档与重新导入定向复核

| 范围 | 证据与结论 |
| --- | --- |
| 本地交付文件 | `artifacts/eventflow-build-env-linux-amd64.docker.tar.gz` 实际为 301637853 字节，权限 0644，owner 1000:1000；文件名与 Docker 使用说明相同 |
| 归档摘要 | 本 reviewer 在 artifacts 中实际运行 `sha256sum --check eventflow-build-env-linux-amd64.docker.tar.gz.sha256`，返回 OK。摘要为 `225fcf58d46d315326792abdf4647ebe359eeb00ac6b391aae7ff97149756679`，与 `/tmp/eventflow-environment-export.json` 相同 |
| 层内容审计 | 核对实施者的 `/tmp/eventflow-environment-archive-audit.json`：读取归档的 10 个真实镜像层，370 个项目环境条目，architecture=amd64，environment_only 与 private_project_material_absent 均为 true。结合此前 Dockerfile/允许清单、镜像身份和哈希检查，没有将最终项目源码或私有题面混入环境交付；本 reviewer 未重复执行完整层扫描 |
| 重新导入 | `/tmp/eventflow-environment-load.log` 记录 `Loaded image: eventflow:build-env`；`/tmp/eventflow-environment-after-load.log` 记录相同 `720a9285...` 镜像 ID 与默认帮助。本 reviewer 再次 `docker image inspect eventflow:build-env`，确认完整 ID 未变、amd64、UID/GID=10001:10001、Cmd=help |
| 导入后运行 | `/tmp/eventflow-environment-after-load-smoke.log` 记录 Release `DEPENDENCY_SMOKE_RESULT=0` 与 Debug GoogleTest 1/1 通过；实施者提供的命令条件为只读挂载临时工程、UID/GID=1000:1000、无网络。只读挂载下运行既有探针验证导入结果可执行，不把它当作再次 fresh 编译或项目全量测试 |

原环境归档待验证项全部关闭。实施者应将现有日志摘要、镜像 ID、归档摘要和两份同轮审查结论汇总到验证记录，并同步规格中的环境任务状态；本 reviewer 未修改其他 owner 的规格或实现文件。

### 明确保留的后续范围

最终 EventFlow 源码封装、八个当前示例、完整项目 CTest、真实集群信号与恢复验证属于用户确认源码稳定后的工作，不是本轮环境完成门槛。本环境 probe 未验证 Kafka broker，也不宣称动态 UDF 家族、HA 或 Compose 完成。

## 首次静态审查目标与结论（保留历史）

原始要求是交付 Docker 镜像，使 project reviewer 能以简单命令编译并运行最新项目；用户明确授权安装本机 Docker。本审查按[镜像规格](../reviewer-image.md)核对完整 reviewer 路径，包括默认示例、离线重编译/测试、退出行为、文档、打包边界和可导入归档。

静态实现能够支持预期路径：镜像固定 Ubuntu 24.04 amd64，包含 Clang/CMake/Make、随仓库的第三方资产、源文件与预编译 Release 示例；入口不会在运行时安装工具或下载项目依赖。七个示例中的后三个实际启动 KV、Coordinator 和两个 Compute，checkpoint 脚本明确停止旧代后启动恢复代。未发现静态上阻断默认构建或示例执行的 P0/P1 问题。

当前发现两项 P3 文档问题。不能据静态检查宣布交付完成：本次观察时只有 `ubuntu:24.04` 基础镜像，工具链构建仍在下载系统包，默认运行、离线重编译、完整 Debug CTest、信号与归档导入尚无最终镜像证据。修复与后来补充证据继续记录在本报告中，不另开 fresh-review 轮次。

## 首次发现（处置见上文）

### W1 / P3：镜像内的 Docker 使用说明链接失效

- 位置：[Dockerfile](../../Dockerfile) 第 38–41 行；[根 README](../../README.md) 第 545 行。
- 证据：Dockerfile 将根 README 原样复制，但只把 `docker/README.md` 放到 `/opt/eventflow/DOCKER.md`。没有创建镜像内的 `docker/README.md`，而根 README 的详细使用入口仍指向该路径。
- 影响：reviewer 在 `shell` 中从根 README 继续查看测试、单例与日志命令时，实际相对路径不存在。其余 `docs/`、`specs/` 也有意未入镜像，但当前 README 没有说明镜像内文档集与源码仓库的区别。
- 建议：至少保留 `docker/README.md` 的原相对路径，并把该路径纳入 SOURCE_MANIFEST；如继续精简其他文档，清楚说明完整设计/历史证据从外部源码包阅读，无需扩大镜像到私有材料。
- 首次状态：待实施者处置和最终镜像内 `test -f docker/README.md` 定向核对。当前环境镜像内已修复并核对，见上文 W1 处置。

### W2 / P3：随镜像携带的示例指南仍使用旧目录名

- 位置：[examples/AGENTS.md](../../examples/AGENTS.md) 第 14 行。
- 证据：该表仍列 `clock_trigger_demo`，当前实际目录、CMake 目标和 Docker entrypoint 已改成 `sdk_clock_polling_demo`。Dockerfile 第 30 行会把这份指南随 examples 一起复制进镜像。
- 影响：按指南查找源码的 reviewer 或后续 agent 会查找不存在的目录。此项不影响当前 Docker 默认命令，因为入口和 CMake 的实际路径已经一致。
- 建议：同步表内目录名，沿用最新示例范围与业务描述。
- 首次状态：待实施者处置和字面搜索复核。当前按用户源码仍在修改的决定后置，不阻断环境交付，见上文 W2 处置。

## 已执行的独立检查

| 检查对象 | 实际方法及结果 |
| --- | --- |
| 项目约束 | 读取根 AGENTS、specs/AGENTS、specs/INDEX、reviewer-image，以及 cmake、cpp3rdlib、examples 和三个集群示例的适用指南 |
| 构建合同 | 阅读完整 CMakeLists：Clang 强制校验；Release `-O3 -DNDEBUG`、关闭测试和 sanitizer，并通过 `check_ipo_supported` 启用可用 LTO；Debug 通过接口选项启用 ASan/UBSan/LSan 和不可恢复诊断；GTest imported targets 和链接仅处于 BUILD_TESTING 条件内 |
| 离线可行性 | CMake 只使用随包头文件/库及系统 Threads，没有 FetchContent/ExternalProject/运行时下载；项目已有生成的 SQL parser，普通构建不执行 Java/ANTLR。此项只是静态可行性，不替代 `--network none` 实跑 |
| 入口覆盖与语法 | 从 `demo_names` 提取七个名称，逐一检查 `examples/<name>_demo/run.sh` 存在并执行 `bash -n`，七项均返回 0 |
| 错误参数 | 在本机对实际 entrypoint 执行 help、未知命令、未知 demo、未知 build mode、test 多余参数：分别退出 0、2、2、2、2 |
| 失败传播 | 用 `EVENTFLOW_DEMO_BINARY=/bin/false` 覆盖 typed_pattern 的实际运行程序，调用实际 entrypoint；退出 1，输出中没有 `PASS:`。没有构建或启动服务 |
| 命名一致性 | entrypoint、CMake、Clock run.sh/main.cpp 中实际命令均指向 `sdk_clock_polling_demo`；遗漏见 W2 |
| 项目入口语言 | Dockerfile 和 docker/entrypoint.sh 的字节均为 ASCII；读者 README 为中文 |
| 依赖身份 | 在 cpp3rdlib 中执行 `sha256sum --check --quiet MANIFEST.sha256`，退出 0；共享库符号链接均使用目录内相对目标 |
| 私有材料边界 | `.dockerignore` 先全部拒绝，再只允许构建目录，最后排除 PDF、Git、缓存和历史包；Dockerfile 不复制整个仓库。当前允许目录中未发现 PDF、deb、JAR、归档或 core 文件；未修改或读取参考仓库/私有题面 |
| 许可证资产 | 当前 cpp3rdlib/licenses 中保留逐包版权文件及适用许可证正文，Dockerfile 将整个依赖目录复制入镜像；最终镜像仍需核对实际文件。这里只核对资产保留，不宣称完成分发法律审核 |
| 运行架构 | 条件示例使用 Memory KV；Clock 和 checkpoint 使用 Rocks KV；三者均创建 Coordinator 与两个 Compute。checkpoint 按 prepare → stop_cluster → restore → verify 执行，不把 KV flush 当作一致 checkpoint |
| 当前 Docker 状态 | `sg docker -c 'docker image ls ...'` 可执行；观察时只有 Ubuntu 基础镜像。`/tmp/eventflow-docker-toolchain-build.log` 显示工具链下载进行中，因此没有把主机历史测试作为容器通过证据 |

没有另起项目构建、完整测试或导出任务，避免与实施者重复占用资源。独立检查中的脚本探针只运行帮助、错误分支及 `/bin/false`，不构成容器内示例通过证据。

## 原完整源码镜像的证据待核对（按最新授权后置）

| 项目 | 待补充内容与判据 |
| --- | --- |
| 最终源码身份 | 固定最终镜像 ID，核对 SOURCE_MANIFEST、TOOLCHAIN-VERSIONS 及当前宿主源文件；尤其确认并发重命名后的 Clock 目录已经进入该镜像 |
| 默认运行 | 同一最终镜像使用 `--network none` 默认命令退出 0，七个成功标记完整；核对三个集群及两代恢复日志，不能只统计 entrypoint 打印 |
| Release 重编译 | `--network none ... review` 实际编译源码并运行七个示例；检查 Release compile/link flags 和动态依赖，不能使用主机旧 build 目录或缓存代替 |
| Debug 测试 | `--network none ... test` 完成 Debug 全量构建与当前全部 CTest，保留退出码、测试数量和 sanitizer 诊断；历史的 45/55 个测试数字不能直接移用 |
| 非 root 和错误退出 | 最终镜像 `id` 符合 10001:10001；错误参数和真实 demo 失败保留非零退出；不需要 privileged 或 Docker socket |
| 信号与回收 | 对实际集群 demo 发送 SIGTERM，检查退出状态、耗时、cleanup 输出。当前 Tini 使用 `-g`，但 entrypoint 外层 Bash 没有 trap；需证据说明外层 shell 先退出时，示例回收不会被 PID 1 的退出截断。这是待验证风险，尚未认定为已复现缺陷 |
| 归档和再次导入 | 本地 docker save 归档、SHA256、docker load 成功输出、导入后同一镜像身份与默认离线运行。仓库内不上传归档、不上传私有材料 |
| 文档与资源 | 交付 README 的文件名/路径与实际归档一致；说明实际镜像大小、构建大致资源需求和降低并行数的方法；修复 W1/W2 后定向核对 |

## 静态快照身份

以下 SHA256 绑定本报告首次审查对象；之后发生的修复与新增运行证据应另行记录，不覆盖首次检查事实。

| 文件 | SHA256 |
| --- | --- |
| Dockerfile | `bd69936bd0350a1f23fb267bcb307620ca79cc4ddbab6d8f584e88e850aeb392` |
| .dockerignore | `fb2c2feddab1d497ab2557682ed2104434a0e9e97e82055a234f871be54f4ef7` |
| docker/entrypoint.sh | `f4883ce8f8d6b523e998189404f58549b2b9f37bd7f62d106139c970df23dcaf` |
| docker/README.md | `0932dd55f7865a055cb1580631edeb3c3cdb297d0cf375cc9cd402ebaa9d3308` |
| CMakeLists.txt | `7850566d75a823dc8d1dc8e755a57ba5df0f26afdc82cf7a1a651e0a1b55fd0b` |
| README.md | `982f4dd4a01f9a5e7eac84e5357e547bcfe8552aca09e45eeb9a14b0d42ef639` |
| examples/AGENTS.md | `3fdfe6afc137c16f7792dcec2db985734612a6492be92a3fce77f1a88d5ddfb9` |
| examples/sdk_clock_polling_demo/run.sh | `e629f671503dea0fdf4a04b76d0af83bed0bae5d49e275135dd6b92d64142763` |

## 交接结论

首次审查要求实施者补充运行、导入证据及两项文档处置，并在本轮内定向复核。用户调整范围后，当前环境对应的实现、整改与证据已经完成上述复核，可汇总交付 `eventflow:build-env`；完整源码镜像仍保持后置，不能把本结论直接写成最终项目镜像或项目全量测试通过。
