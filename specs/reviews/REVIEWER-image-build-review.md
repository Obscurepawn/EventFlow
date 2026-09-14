# Reviewer 镜像独立全局审查：构建与交付

Reviewer：`image_build_reviewer`。日期：2026-09-14。此报告属于本次 milestone 的单轮、并行、fresh-context 全局审查；审查者没有修改实现，没有执行重型构建，也没有修改镜像。

**最新范围：先交付不含应用源码快照的 Docker 编译环境。** 用户在本轮审查期间说明源码仍在修改，要求先完成 Docker 环境；实施者据此调整为工具链与固定 `cpp3rdlib` 的 environment 镜像、挂载当前源码的入口及临时独立 CMake smoke。打包应用源码的 reviewer 镜像、当前完整示例和项目全量 CTest 等待代码稳定后再执行，不是当前 environment milestone 的完成门槛。

**当前结论：本次 environment 范围审查通过，原 P2 已修复，归档及重新导入证据闭环。** 同一 reviewer 已检查实际环境镜像、最新输入指纹、Release/LTO 与 Debug/sanitizer 构建证据，独立重跑小型临时 ABI 探针并复算归档摘要，最后核对十层归档审计及导入后运行记录。没有遗留的环境阻断问题；这不表示仍在变化的应用已经完成构建或全量测试。Docker 文件以外的并发源码变化由用户继续开发产生，不归因于本次环境实施。

## 原始要求、范围与检查对象

本次用户要求是从最新 EventFlow 项目生成 Docker 镜像，让 project reviewer 能以简单命令编译、运行；另明确授权本机安装 Docker。[当前规格](../reviewer-image.md)进一步要求本地归档、无外部网络运行、容器内 Release 重编译、Debug 全量测试和独立审查。没有将 Kafka Compose、HA、原生 ARM、动态 UDF 家族或 Process Function 纳入本次验收。

已阅读根 `AGENTS.md`、`specs/AGENTS.md`、`specs/INDEX.md`、本次规格、Dockerfile、`.dockerignore`、入口脚本、Docker 文档和根 README；另外检查根 CMake/Makefile、构建/测试目录约定、依赖说明与清单，以及现有示例的构建、进程、退出和恢复路径。旧核心或 Trigger 测试数量仅作为历史说明，不用于证明本次容器通过。

本报告初审静态快照的 SHA256（后文另列最终环境快照）：

| 文件 | SHA256 |
| --- | --- |
| `Dockerfile` | `bd69936bd0350a1f23fb267bcb307620ca79cc4ddbab6d8f584e88e850aeb392` |
| `.dockerignore` | `fb2c2feddab1d497ab2557682ed2104434a0e9e97e82055a234f871be54f4ef7` |
| `docker/entrypoint.sh` | `f4883ce8f8d6b523e998189404f58549b2b9f37bd7f62d106139c970df23dcaf` |
| `docker/README.md` | `0932dd55f7865a055cb1580631edeb3c3cdb297d0cf375cc9cd402ebaa9d3308` |
| `README.md` | `982f4dd4a01f9a5e7eac84e5357e547bcfe8552aca09e45eeb9a14b0d42ef639` |
| `CMakeLists.txt` | `7850566d75a823dc8d1dc8e755a57ba5df0f26afdc82cf7a1a651e0a1b55fd0b` |
| `Makefile` | `7a5cf34b364a45d7e6da8d1177cd24c490009fde961c08ebdb83de6855ea69c3` |
| `cpp3rdlib/MANIFEST.sha256` | `d94c2c52d64ef224867d55a032e6c293d352b0bc2f90527961a25e24c352851f` |

工作树当前全部为未跟踪文件，不能以 Git commit 充当构建内容身份。Dockerfile 中的源清单可提供内容身份；最终仍需将它与实际镜像 ID 和归档 SHA256 绑定。

## 发现

### R1 / P2：镜像内的 Docker 使用文档路径与根 README 不一致

- 证据：[Dockerfile](../../Dockerfile)第 24–30、38–39 行只复制指定构建目录，并将 `docker/README.md` 改名为 `/opt/eventflow/DOCKER.md`；没有复制 `/opt/eventflow/docker/`。
- 对应入口：[根 README](../../README.md)第 545 行让 reviewer 通过 `docker/README.md` 查看单独示例、Debug 测试、shell 和日志保留用法；第 74 行也显示 `docker/` 目录。原文件被放入镜像后，这条链接不能解析。
- 影响：只拿到镜像、打开随包 README 的 reviewer 找不到被推荐的详细编译/测试说明。当前容器入口本身不受影响，因此为 P2。
- 建议：在镜像中保留 `docker/README.md` 原路径；如果需要同时保留 `DOCKER.md`，可作为额外入口。也可明确生成适用于镜像布局的 README。应核对镜像文件系统，而不只检查宿主机上的 Markdown 链接。
- 处置：**已修复并定向复验通过。** 调整后 Dockerfile 第 34 行保留 `docker/README.md` 原路径，reviewer stage 继承该文件；本 reviewer 在实际 `720a9285…65a175` 环境镜像中确认文件存在，并确认 SHA256 与宿主文件一致。完整应用 reviewer stage 尚未构建，不把继承关系的静态检查当成该阶段运行通过。

`docs/` 和 `specs/` 没有纳入镜像是当前允许清单的取舍，不在此要求自动打包所有文档；不过镜像说明宜明确这些阅读入口需要外部源码包，避免把随包 README 中全部链接都描述为容器内可用。

## 静态合同评估

| 检查面 | 证据与判断 |
| --- | --- |
| 当前源码构建 | Dockerfile 第 24–35 行先复制当前 `cmake/cpp3rdlib/include/src/tests/examples`，再调用入口构建 Release；第 36 行只保留生成的 `bin`。没有复制宿主机 `build` 或 `build-release`，没有将旧本机二进制作为镜像构建输入。 |
| CMake/Make 与 Clang | Dockerfile 第 7–12 行提供 Clang 18、Make、CMake、lld 和 GNU C++ 开发环境；入口第 48–53 行指定 Clang 与 Unix Makefiles。根 CMake 第 7–8 行拒绝非 Clang。没有引入第三方源码下载或另一构建系统。 |
| Release 质量合同 | 根 CMake 第 13–26 行关闭 sanitizer/测试、强制 `-O3 -DNDEBUG`，并按支持情况启用 IPO；入口第 50 行选择 lld。实际是否生成 LTO、最终链接参数和二进制依赖仍需容器证据。 |
| Debug 与 GoogleTest | 根 CMake 第 27–29、49–51 行启用三种 sanitizer 与不可恢复诊断；第 146–156、220–227 行将 GoogleTest 限于测试并配置 CTest 失败环境。入口第 103–106 行构建 Debug 后 `exec ctest`，静态上保留 CTest 退出状态。 |
| 平台与 ABI | Dockerfile 第 1、4、7–8 行固定 Ubuntu 24.04 digest、拒绝非 amd64 并安装 GNU C++ 开发/运行环境，契合随包依赖的 Ubuntu amd64 GNU ABI 范围。没有 `-march=native` 或 fast-math 构建选项。其他宿主机内核和 ARM 模拟性能没有验证；镜像内动态加载仍待实测。 |
| 默认运行和失败传播 | 入口第 56–76 行逐个调用原有七个 `run.sh`，设置 Release 目录和 `EVENTFLOW_SKIP_BUILD=1`；脚本启用 `set -euo pipefail`，成功信息位于子脚本成功返回之后。没有用忽略退出码的包装代替示例断言。 |
| 四进程与恢复语义 | 条件示例第 103–120 行启动 Memory KV、两个 Compute 和 Coordinator；Clock 示例第 234–268 行启动 Rocks KV、两个 Compute、Coordinator 和客户端；恢复示例第 95–131 行准备一致 checkpoint、停止原集群、以 `--restore-from`/`restore_checkpoint` 启动新集群再验证。入口复用这些路径，没有把 KV snapshot 或容器重启改称自动恢复。 |
| 用户与进程入口 | Dockerfile 第 13–21 行创建 UID/GID `10001:10001` 并赋予源码目录写权限；第 50 行以 Tini `-g` 启动入口。静态配置符合非 root 与子进程管理意图，实际用户、信号与退出状态待容器核验。 |
| 私有材料边界 | `.dockerignore` 第 2–22 行默认拒绝并放行指定目录；第 25–34 行再次排除 PDF、Git、缓存和若干输出；Dockerfile 也只显式复制允许的路径。未发现从 `.reference-repos/`、`specs/`、历史 `artifacts/` 或私有目录复制的指令。实际构建上下文与所有镜像层仍需核验。 |
| 源码与依赖身份 | Dockerfile 第 34 行校验依赖清单；第 40–45 行对纳入的源码、依赖和实际入口生成 `SOURCE_MANIFEST.sha256`，记录工具链版本。当前输入路径无空白字符，现有 `sort/xargs sha256sum` 对本快照可用；身份验收仍需与主机当前文件逐项比对。 |
| 读者流程与边界 | Docker 文档给出 load、默认 demo、review、test、shell 和日志提取流程；明确 linux/amd64、制作镜像联网而运行不联网、MemMock/真实 broker 的区别。R1 修复后仍需补充实测资源规模和最终归档证据。 |

## 本 reviewer 已实际运行的轻量验证

- `bash -n docker/entrypoint.sh`：退出 0。
- 在 `cpp3rdlib/` 执行 `sha256sum --check --quiet MANIFEST.sha256`：退出 0。
- 遍历七个允许目录中的 534 个普通文件/符号链接：没有发现 PDF、密钥容器、常见凭据文件名、日志、会话转储或归档候选；12 个符号链接均有效，最终目标均在仓库内，没有空白字符路径。此文件名检查不是对任意文件内容秘密的完备证明。
- 对真实入口设置 `EVENTFLOW_SOURCE_DIR` 指向当前仓库：`help` 退出 0；未知命令、未知 demo、`review` 多余参数、未知 build 模式、build 多余参数、test 多余参数、编译并行数 0、测试并行数 `abc`，八种错误调用均退出 2，未触发构建。
- `sg docker -c 'docker image ls --no-trunc'` 可读取 daemon；当时只有固定 digest 的 Ubuntu 基础镜像，没有完成的 reviewer 镜像。`/tmp/eventflow-docker-toolchain-build.log` 当时仍在安装容器工具链，未出现本次最终构建成功证据。

以上验证发生于宿主机；没有把它们表述为容器运行、真实 Kafka 或产品完整质量门禁的通过证据。

## 原 bundled reviewer 镜像后置验收

以下表格保留最初完整镜像的验收要求。按最新用户指示，它们在应用代码稳定后继续，不能将“当前有意后置”描述成环境镜像实施失败，也不能以环境 smoke 提前宣告通过。

| 验收项 | 当前状态与所需证据 |
| --- | --- |
| 实际镜像 | 待完成构建退出码、最终镜像 ID、平台、工具链版本；不能只看正在增长的构建日志。 |
| 离线七示例 | 待对同一镜像执行 `docker run --rm --network none eventflow:reviewer`，核对七个 PASS、四进程场景、恢复场景及退出 0。 |
| Release 重新编译 | 待 `review` 的完整容器日志证明实际编译当前源码、链接并运行所有示例；运行预编译 demo 不替代重编译。 |
| Debug 全量 CTest | 待 `test` 完整构建和 CTest 终态，核对目标总数、失败数、超时和 sanitizer 诊断；禁止用历史 45/55 数量代替实际结果。 |
| Flags 与加载器 | 待实际 Release/Debug flags、Release 非 sanitizer、LTO、GoogleTest 目标隔离；清空 `LD_LIBRARY_PATH` 后检查所有 Release 可执行文件 `ldd` 无缺库/符号错误，并确认随包间接依赖。 |
| 入口与身份 | 待实际镜像的非 root UID/GID、shell 命令、错误退出码、源码/依赖清单；必要时对失败/终止路径做短探针。 |
| 包装隐私 | 待实际构建上下文清单及镜像层清单，确认没有私有 PDF、参考仓库、Git、宿主构建缓存或会话材料；不能只以最终层目录为空作为删除过私密层的证明。 |
| 归档导入 | 待 `docker save` 可导入归档、最终 SHA256、重新 `docker load` 后镜像 ID 和运行结果；记录实际归档大小、合理 CPU/内存/磁盘需求。 |
| 最新文件绑定 | 若 Dockerfile、入口、README 或编译输入在本报告后变化，必须对最终镜像中的相应文件重新核验；不要将此静态快照哈希冒充最终交付哈希。 |

## 同轮处置与定向复验

本节记录同一 reviewer 对调整后的 environment 实现及实际证据的定向复核，保留 R1 原发现与首次失败，不构成另一轮审查。

### 范围调整后的验收合同

- environment 镜像仅包含工具链、入口和固定第三方依赖/许可证，无应用源码快照或预编译应用；无源码时默认帮助命令可用。
- 当前源码通过明确的 bind mount 提供；容器 UID、可写输出路径和 CMake 缓存位置可用，不误复用宿主机带绝对路径的旧构建目录，不因默认入口改写并发源码。
- 在临时项目中实际用镜像工具链构建并运行 Debug/Release 第三方 smoke；核对 ASan/UBSan/LSan、GoogleTest 测试隔离、Release flags/LTO 和动态依赖。这个结果仅证明环境，不等于应用全量验收。
- 环境归档可校验、重新导入和运行；文档明确 environment 与后置 bundled reviewer 两种镜像的命令、范围和资源需求。
- 本次 R1 已在 toolchain stage 保留 `docker/README.md` 原路径，并由 reviewer stage 继承；实际环境镜像已复验。

### 首次环境构建失败记录

初审时构建仍进行中，后续读取 `/tmp/eventflow-docker-toolchain-build.log` 确认该尝试退出 100。第 176–177 行显示 `libpsl5t64`、`clang-18` 的 APT 下载经本机代理返回 HTTP 502，第 178 行为 APT 失败终态。这是当前环境准备的实际失败证据，不能标成已完成；实施者已获通知，等待修复后的新构建结果。该日志内容不证明项目源码或 ABI 有缺陷。

### 修复后环境快照与构建证据

当前已独立检查的镜像为 `eventflow:build-env`，ID 为 `sha256:720a928535bb08cd551d8f61179145934f78c6e65120004ffacb24a24965a175`。`docker image inspect` 实测为 `linux/amd64`、UID/GID `10001:10001`，入口为 Tini `-g` 加 `eventflow-review`，默认命令为 `help`；镜像未压缩大小为 1,228,805,572 bytes。环境变量只有 `PATH` 与 `EVENTFLOW_SOURCE_DIR`，没有把主机代理作为运行环境变量写入。

最新输入指纹与 `/tmp/eventflow-environment-input-hashes.json` 逐项一致：

| 输入 | SHA256 |
| --- | --- |
| `Dockerfile` | `d7f9a7b60f73931ad3e3bdefce060f8a0fc2ee300b8d50d98bca2fec45c79cae` |
| `.dockerignore` | `fb2c2feddab1d497ab2557682ed2104434a0e9e97e82055a234f871be54f4ef7` |
| `docker/entrypoint.sh` | `71c023e478585164e04282ed397395df105b609dec01de98753b1ab5bc1d1b57` |
| `docker/README.md` | `c43c1eff150592425c6c509fea453985254e88cd859eaff44e1fbff7c7121574` |
| `cpp3rdlib/MANIFEST.sha256` | `d94c2c52d64ef224867d55a032e6c293d352b0bc2f90527961a25e24c352851f` |

本 reviewer 直接运行无挂载、无网络的上述镜像：UID 为 10001，`docker/README.md` 存在；`src/include/CMakeLists.txt/examples/specs/.git` 均不存在。镜像内入口、文档、依赖清单的指纹与上表一致；`sha256sum --check --quiet MANIFEST.sha256` 再次退出 0。

HTTP 502 之后，Dockerfile 第 6–8 行先安装 CA 再切换为 HTTPS。首次 Release smoke 又暴露 ThinLTO 缺少 LLVM archiver，`/tmp/eventflow-environment-release-smoke.log` 保留 `Expected LTO support` 配置失败。Dockerfile 第 20–25 行已安装 `llvm-18` 并提供 `llvm-ar/llvm-ranlib`。实际最终构建日志为 `/tmp/eventflow-docker-environment-lto-build.log`，末尾导出上述 `720a9285…65a175` 镜像并完成解包；名字含 `environment-final-build.log` 的较早文件导出的是另一旧镜像，不能误用于本快照。

### 临时项目和独立复跑

探针位于 `/tmp/eventflow-container-smoke-176awiko`，不是当前 EventFlow 源码。已阅读它的 `CMakeLists.txt`、`probe.cpp`、`main.cpp`、`test.cpp` 及实际 flags/link 文件，确认探针执行 C++20 `std::span`、ANTLR 输入流、JSON 序列化、真实 loopback HTTP 请求、RocksDB Put/Get/关闭，以及 librdkafka 创建/销毁；它没有连接 Kafka broker。

| 验证对象 | 实际证据与结论 |
| --- | --- |
| Release 构建 | `/tmp/eventflow-environment-release-final.log` 从编译器探测开始，构建静态 `dependency_probe` 和可执行文件均完成；实际 flags 是 `-O3 -DNDEBUG -flto=thin`，静态库使用 `/usr/bin/llvm-ar-18`、`llvm-ranlib-18`，最终链接使用 lld。没有 sanitizer、fast-math 或 GoogleTest 链接。 |
| Debug 构建与 CTest | `/tmp/eventflow-environment-debug-final.log` 从独立构建目录完成编译；CTest 1/1 通过。实际编译/链接包含 `address,undefined,leak` 和 `-fno-sanitize-recover=all`，CTest 失败环境完整；只有 `dependency_test` 链接 GoogleTest，`dependency_smoke` 未链接。 |
| 独立运行复验 | 本 reviewer 以 UID/GID `1000:1000`、`--network none` 将临时项目只读挂载到 `/opt/eventflow`，清除 `LD_LIBRARY_PATH`，运行既有 Release `dependency_smoke` 和 Debug `dependency_test`。Release 输出结果 0；GoogleTest 1/1 通过、42 ms，无 sanitizer 诊断；整个命令退出 0。完整命令与输出保存在 `/tmp/eventflow-environment-independent-build-review.json`。未重跑构建，也未写入用户应用源码。 |
| 动态依赖 | 独立复跑同一容器的 `ldd` 无 `not found` 或缺失符号；RocksDB、ANTLR、librdkafka 及 gflags/压缩/SASL/OpenSSL 间接依赖均解析到挂载的 `cpp3rdlib/lib`，系统只提供 glibc、GNU libstdc++、libgcc 与加载器等基础设施。 |
| 命令与信号 | 已逐项阅读 `/tmp/eventflow-environment-cli-checks.json`：默认帮助、非 root shell、缺失源码、动态发现两个临时示例、新示例失败退出 7、错误命令/并行数退出 2 均有记录；停止容器后输出 `SIGNAL_HANDLED`，容器退出 0，耗时 0.184 s。该信号探针证明入口基础转发，不证明所有应用进程的停止协议。 |
| 挂载与目录 | 入口第 7–8 行使用 `build-docker-release/debug`，避开常规宿主构建目录；Docker 文档第 20–28 行显式使用宿主 UID/GID，说明完整仓库必须带 `cpp3rdlib`，挂载会覆盖镜像内依赖副本。没有暗中向宿主源码补第三方链接。 |

### 归档与重新导入验收闭环

已看到 `/tmp/eventflow-environment-export.json`、归档成员清单及 `Loaded image: eventflow:build-env` 的 load 日志。本 reviewer 独立读取并复算 `artifacts/eventflow-build-env-linux-amd64.docker.tar.gz`，大小为 **301,637,853 bytes**，SHA256 为 **`225fcf58d46d315326792abdf4647ebe359eeb00ac6b391aae7ff97149756679`**，与 sidecar 和 export 记录一致；文件权限为 0644。

同一轮最后核验已完成：

- 已阅读 `/tmp/eventflow-environment-archive-audit.json`。该记录针对实际归档的 **10 个镜像层**，统计 370 个项目路径条目，范围只有 `cpp3rdlib`、`docker` 和工具链版本文件，无应用源码或私有/参考材料；配置 blob 为 `e864e82c…6f727`、架构为 amd64，与最终构建日志一致。这里引用实施者的实际归档层审计，不冒称 reviewer 重新执行了所有层扫描；前文 reviewer 对允许目录、Dockerfile COPY、实际镜像文件系统和输入指纹的独立检查与它一致。
- `/tmp/eventflow-environment-load.log` 记录成功载入 `eventflow:build-env`；`/tmp/eventflow-environment-after-load.log` 记录导入后镜像仍为 `sha256:720a928535bb08cd551d8f61179145934f78c6e65120004ffacb24a24965a175`，默认入口显示挂载源码说明和全部帮助命令。本 reviewer 随后再次直接 `docker image inspect`，得到相同 ID、linux/amd64 和默认 UID/GID `10001:10001`。
- `/tmp/eventflow-environment-after-load-smoke.log` 记录重新导入后，以 UID 1000、断网及临时项目只读挂载执行 Release 运行探针和 Debug GoogleTest；Release 输出 `DEPENDENCY_SMOKE_RESULT=0`，GoogleTest 1/1 通过、47 ms，无 sanitizer 诊断。该结果与前文 reviewer 的独立离线复跑一致。

因此关闭本报告的归档、导入后身份/运行及包装隐私待核对项，建议验收当前环境镜像。环境之外的应用源码镜像、八个当前示例和完整项目 CTest 继续按用户指示后置；当前示例数量以源码稳定时的实际清单为准，不能继续沿用最初的七示例常量。真实 Kafka broker、HA 和原生 ARM 没有由这些证据验证，也没有纳入本次环境完成结论。
