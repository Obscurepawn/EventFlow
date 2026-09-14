# Reviewer 完整源码镜像：独立全局交付审查

Status: passed（同轮问题已关闭，完整镜像交付门槛均有实际证据）  
Reviewer: reviewer_product_delivery_review  
Date: 2026-09-14

本报告属于完整 `eventflow:reviewer` 镜像里程碑的一轮双 reviewer 并行 fresh-context 全局审查。先前编译环境的独立审查不代替本轮项目验收；本 reviewer 没有修改生产实现、构建输入或其他文档，也没有重复启动重型构建与全量测试。后续补充是同一上下文的定向闭环。

## 检查范围与对象

- 原始授权：让 reviewer 用简短命令编译运行；允许只提交完整源码及 Dockerfile；用户已确认源码稳定，仅继续修改文档；镜像及私有材料不自动上传。
- 阅读根约定、`specs/INDEX.md`、`reviewer-image.md`、核心/完整产品范围、生产/公共头文件/存储/服务/测试/示例/依赖/构建的适用 `AGENTS.md`。
- 检查 Dockerfile、允许清单、入口、根及 Docker README、CMake 依赖与质量配置；追踪 KV/Compute/Coordinator、共同 checkpoint/restore、输入身份、缓存 UNKNOWN 边界、代表性共享契约与真实进程测试、全部示例脚本入口。
- 实际固定上下文：`/tmp/eventflow-reviewer-context-hy1mq45d`；清单：`/tmp/eventflow-reviewer-build-inputs.json`，创建时间 `2026-09-14T13:58:22.295285+00:00`，清单文件 SHA256 为 `36450e21366ee3f48a586ca96c96dfa4912ea6342b5d1c63510be8d28dfc61da`。
- 对清单的 518 项逐一重新计算 SHA256：固定上下文与审查时工作树均 0 缺失、0 改变。上下文共 551 个文件，差额包含未纳入编译输入清单的文档；12 个符号链接均指向上下文内部。

## 发现与处置

### D1 / P2：主要读者入口仍依赖未随源码交付的环境归档

最初审查时根 `README.md:606–618` 先执行 `docker load -i artifacts/eventflow-build-env-linux-amd64.docker.tar.gz`，并将完整 reviewer 镜像表述为“源码稳定后”再制作。用户允许只交付源码及 Dockerfile，该归档也不在本次 Docker 上下文；照主要入口操作会先遇到缺少归档。

`docker/README.md:7–31` 已给出完整的 `docker build`、默认断网运行、`review` 和 `test`，可用作主要入口。建议根 README 同步为源码即可执行的两条命令，再列可选归档/环境挂载。若最终只刷新工作树文档，必须如实区分镜像内的旧文档快照；更直接的修复是在编译输入不变的前提下刷新末尾文档层，并记录最终镜像身份。

处置：已关闭。最终镜像 `sha256:e07c9842cab9ab2fa4aca2a2049d39c0ed3c459da4c213469fb6d83156a26de4` 内已是源码 `docker build` + `docker run` 首选入口；独立读取镜像内 README，SHA256 为 `e36b624acdea742623a76d90a143cebe2feb287d5c7eb8b423074a972fb38140`，默认运行和重新编译均不要求主机归档或源码挂载。

### D2 / P2：镜像规格保留过期的数量与当前状态

最初 `specs/reviewer-image.md` 与 `INDEX.md` 仍多处写“等待源码稳定”“本轮未执行”“七个示例”；实际固定上下文有八个 `examples/*_demo/run.sh`，入口动态发现且支持新增的独立引擎 Clock 示例。规格中“恢复完整镜像交付”后紧接“最新授权：先完成编译环境”，容易误读当前范围。

处置：已关闭原数量及阶段冲突。验收/任务表、INDEX 已改为源码稳定和八个示例；旧环境阶段标题已明确为已完成的历史阶段。现在本轮全部验收已有实际证据，实施者可以将完成勾选、当前 60 个测试的分次验证事实及最终证据链接同步到规格和 INDEX；报告不把此前进行中状态改写为已提前完成。

### D3 / P2：入口收到信号过早退出，真实服务清理未完成

实施者在本轮信号验证中发现旧包装入口收到 TERM 后在示例 cleanup 完成前退出。原证据 `/tmp/eventflow-reviewer-signal.json` 记录容器退出 143，但 `stopped_children=0`；仅有标准信号退出码不能证明服务正常停止。

处置：已关闭。入口保持示例为前台进程，在其执行期间安装 INT/TERM 延迟 trap，允许 Tini 的一次进程组转发触达示例及服务，并等待示例完成清理。最终入口 SHA256 为 `c57ef717474ac876a3bc11563bf51a0cab69ba174a8aaf018d3c43f4636d4708`。独立检查最终镜像的两份原始信号日志及容器 inspect：TERM/INT 分别退出 143/130，约 0.304/0.339 秒，均记录四个真实服务子进程 `exit=0`，容器无主机挂载。探针运行原条件示例后暂留客户端，验证包装入口到真实服务的清理链；它不是每个示例、每个中断时刻的穷举测试。

## 已独立核实的静态证据

| 检查 | 实际证据与判断 |
| --- | --- |
| 构建来源 | Dockerfile 固定 Ubuntu 24.04 digest，只通过 APT 安装系统工具链；C++ 依赖来自 `cpp3rdlib/`，构建前执行清单校验。`CMakeLists.txt:1–59` 强制 Clang/C++20，使用项目 CMake/Unix Makefiles，无 FetchContent/构建下载。APT 包未固定完整版本，因此这是可重复执行的构建配方，不是逐字节相同镜像的保证。 |
| 依赖与许可材料 | 独立执行 `sha256sum --check --quiet MANIFEST.sha256` 返回 0；随包包含许可证目录、逐包版权与来源说明、Apache/GPL 全文以及 SASL attribution。未发现第三方 `.cpp/.cc/.c/.cxx`、`.deb` 或 `.jar`。本审查核对材料存在与镜像复制链，不作法律合规结论。 |
| 隐私与上下文 | 固定上下文的文件名/类型检查未发现 PDF、私钥扩展、`.env`、`.git`、`.reference-repos` 或压缩历史制品；项目目录均为预期代码、SQL、脚本、指南。Dockerfile 从允许目录显式 COPY，没有先复制整个仓库再删除的层。最终逐层检查见下文。 |
| 默认入口 | Dockerfile 为 `USER 10001:10001`，Tini `-g` 管理入口，默认 `demo all`。入口校验构建并行参数、命令参数和示例名；`review` 清理后重编译；`test` 构建 Debug 后 exec CTest；Release/Debug 构建目录独立。 |
| 示例覆盖 | 当前八例为 checkpoint_recovery、condition_trigger、engine_clock_trigger、feature_computation、news_feature、sdk_clock_polling、sql_query、typed_pattern。CMake 定义主服务及对应八个可执行程序。简单示例在进程内；condition、SDK Clock 和 checkpoint 脚本明确启动 KV、Coordinator 与两个 Compute。 |
| 一致恢复 | `src/service/coordinator_service.cpp:523–570` 拒绝未确认输入，暂停源准入、quiesce workers，绑定源位置、水位、Catalog、worker receipt/revision 与 control revision，再创建并核对存储快照；`:808–835` 拒绝错误切面和稍后的 live DB。`examples/checkpoint_recovery_demo/run.sh` 先停完旧进程再创建新活动目录与第二代集群。 |
| 恢复断言 | `examples/checkpoint_recovery_demo/main.cpp:91–169` 检查两个不同 Compute、确认 checkpoint、恢复行/水位/源位置/trigger audit、原 offset 0 去重且历史不重复、新 offset 10 将 volume 从 110 推至 135。示例不是仅检查文件存在或复用原数据库。 |
| 存储与缓存边界 | `include/eventflow/state_store.hpp` 区分 Committed/Rejected/Unknown 与外层 Status，区分 KV snapshot 和系统 checkpoint；`state_store.cpp:480–487` 保留 `sync=true`、WAL，写入不确定后要求 reopen。`cached_store.hpp:11–18,36–63` 明确预算、epoch 与 UNKNOWN 原重试身份不可失效丢弃。 |
| 测试真实性 | `store_contract_test.cpp` 参数化覆盖 Memory/Rocks/RemoteMemory/RemoteRocks；`remote_store_test.cpp` 含真实 KV 子进程及丢 ACK；`core_cluster_test.cpp` 含 Memory/Rocks 真实四进程与停机恢复；`message_source_test.cpp` 共用 MemMock/librdkafka protocol mock 合同。源码有测试不等于本次容器运行通过。 |
| 质量配置 | `CMakeLists.txt:13–28,49–52` 分离 Release O3/NDEBUG/受支持 LTO 与 Debug ASan/UBSan/LSan/不可恢复诊断；`:149–237` 仅 BUILD_TESTING 引入 GTest，CTest 设置泄漏检测、失败退出和超时。本轮实际 flags/ELF/CTest 核实见下文。 |
| 诚实边界 | 当前为静态集群和已编译业务配置；默认运行不需要真实 Kafka。Docker 说明明确常驻 Compose、真实 broker、原生 ARM、HA 后置；完整产品规格保留动态 UDF/UDTF/UDAF、在线迁移等未完成项，静态 Function 不替代这些能力。数据卷和容器重启未被表述成一致 checkpoint 或自动恢复。 |

## 已执行的本轮镜像检查

初次完整镜像构建日志 `/tmp/eventflow-reviewer-image-build.log` 已出现成功 export/unpack。独立通过 `sg docker -c` 查询 image inspect，得到 `sha256:71b5663d8a2b4373601b85f75841fbb49d8bae2ed0f375ef690f6c3093a3fc7c`、`linux/amd64`、用户 `10001:10001`、Tini `-g` 与默认 `demo all`。

独立启动该镜像的短时 `--network none` shell：`sha256sum --check --quiet SOURCE_MANIFEST.sha256` 返回 0，清单 551 项；工具链记录为 Clang 18.1.3、CMake 3.28.3、glibc 2.39。对主服务和八个示例执行 `ldd`，九个 ELF 均无 `not found`，整体命令退出 0。主服务的 `DT_RPATH` 含 `$ORIGIN/../../cpp3rdlib/lib` 与 `/opt/eventflow/cpp3rdlib/lib`，`DT_NEEDED` 包含随包 RocksDB/ANTLR 和基础 C++/glibc 库。

以上是初次 image ID 的证据，不把它冒充最终归档身份。

### 最终输入与实际项目验证

最终镜像为 `sha256:e07c9842cab9ab2fa4aca2a2049d39c0ed3c459da4c213469fb6d83156a26de4`。最终输入清单 `/tmp/eventflow-reviewer-final-build-inputs.json` 的 SHA256 为 `28d1ea486a82407c7a655195850d09f5291747230b01022bb60922fbbb09135b`，仍是 518 项；独立重算最终上下文及当前工作树均 0 缺失、0 改变。相对初始编译输入，仅 `docker/entrypoint.sh` 的信号修复和 `CMakeLists.txt` 的 coordinator 测试超时配置改变，所有 C++ 源码/头文件与依赖保持不变；README 文档另行刷新。

独立在最终镜像短时断网 shell 执行 551 项 `SOURCE_MANIFEST.sha256` 校验及九个 ELF 的 `ldd`，退出 0。最终 manifest SHA256 为 `04a93339d798bbc2b686ccf41264d47ca709229fda0e6fc3d6504626186c78ab`；CMake SHA256 为 `1d81834c0065c959dfa53620860ec4e40f7e12cbab2b654fd6f4d04c429d9904`。

| 验证 | 已查阅的本轮证据 |
| --- | --- |
| 最终默认运行 | `/tmp/eventflow-reviewer-final-default.log` 有八例 PASS、两次恢复 audit 行校验及 `RESTORE VERIFIED ... 110 -> 135`；独立 inspect 容器 `eventflow-reviewer-final-default` 确认最终 image ID、`network=none`、UID/GID 10001、无 mounts、退出 0。 |
| 清理重编译 Release | 初始镜像的 `eventflow-reviewer-rebuild-check` 以 `review` 在断网、无 mounts、非 root 环境中编译运行，退出 0；`/tmp/eventflow-reviewer-review.log` 有全八例 PASS。与最终输入的 C++/依赖身份一致；本轮入口和测试超时变化由最终默认运行/信号/定向测试覆盖。 |
| Release/Debug 实际 flags | 独立读取已运行容器的 `flags.make`/`link.txt`：主服务 Release 为 `-O3 -DNDEBUG -std=c++20 -flto=thin`，链接 lld、无 sanitizer/GTest；Debug coordinator 为 `-fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`。 |
| 完整 Debug 原始结果 | `/tmp/eventflow-reviewer-test.log` 完成 60 个 CTest 目标，59 通过、coordinator 在旧 180 秒限制超时，总耗时 1012.63 秒；对应容器 `test` 退出 8。保留这次失败，不记成一次 60/60 全绿。 |
| coordinator 整目标复验 | `/tmp/eventflow-reviewer-coordinator-recheck.log` 在最终镜像中通过 1/1、158.06 秒。独立读取实际 CTest 配置确认 TIMEOUT 600 与三 sanitizer 失败环境；未筛选单个 GoogleTest 用例。独立从原 Debug 容器与复验容器分别取两个 ELF 算 hash：`coordinator_service_test` 均为 `3e9b7f590fac4ae0e4c8742192ec42037a65a2812972ed8da533b1913d87bd44`，主服务均为 `7f795cfe91a8de0a58ea2469ecbde42eeaff3a1150b58210b1af7dd14e7be23a`。因此 60 个目标已由完整初跑和一个完整目标的定向复验完成覆盖。 |
| 信号 | `/tmp/eventflow-reviewer-image-signals.json` 与 `image-signal-term.log`/`image-signal-int.log`，以及独立 container inspect，支持 D3 的退出码、四个子进程正常清理与无外部挂载结论。 |
| 错误参数与失败传播 | 独立对最终镜像执行五个短时断网负向探针：未知 demo、多余参数、未知 build mode、EVENTFLOW_BUILD_JOBS=0 均退出 2；用 EVENTFLOW_DEMO_BINARY 指向不存在的文件执行 typed_pattern，退出 1。五者均没有错误打印 PASS，整体探针断言退出 0。 |

### 归档与所有层

独立重算 `artifacts/eventflow-reviewer-linux-amd64.docker.tar.gz`：309,553,316 bytes，SHA256 `89eeb6d14b5a5375ab66fd303b5948367258b8fa9312e6f44003bfd55d856699`，与 `/tmp/eventflow-reviewer-export.json` 一致。独立流式读取归档所有 tar 层：manifest 引用 19 层，实际 17 个唯一 tar payload，其中同一空层引用三次；项目路径计数 608（包括 `/usr/local/bin/eventflow-review`），18 个随包许可证文件，无私有路径或 whiteout。镜像 config 仅设置 PATH 与 EVENTFLOW_SOURCE_DIR，未夹带主机代理环境。

第一次独立审计脚本在完成扫描后把“唯一 payload 数”误当“manifest 层引用数”断言为 19 而退出 1；随后直接检查 manifest 确认 19 次引用对应 17 个唯一 payload，差额全来自重复空层。这是审计计数口径修正，不是镜像缺层；原扫描输出与修正过程保留。

`/tmp/eventflow-reviewer-archive-audit.json` 另记录实施者的 19 层/607 项目路径口径（不计上述入口路径）；`/tmp/eventflow-reviewer-load.log` 有 `Loaded image: eventflow:reviewer`。

导入后运行也已关闭。另一位独立 reviewer 执行 `docker run --rm --network none eventflow:reviewer`，本 reviewer 读取 `/tmp/eventflow-product-build-review-after-load.json` 与原始 `.log` 交叉核对：仍是最终 `e07c9842…` 镜像，6.091 秒、退出 0，八个示例全部 PASS，并包含两条恢复 audit 输出与 `RESTORE VERIFIED ... 110 -> 135`。没有用导入成功消息代替实际运行。

## 本轮结论与交接

本轮审查通过，无未关闭的 P0/P1/P2 发现。D1 读者入口、D2 原数量/阶段冲突、D3 信号清理均有处置；60 个测试目标按完整初跑加一个完整目标的定向复验完成，初跑超时没有被删除。最终默认运行、离线重编译、真实服务与恢复、信号和失败退出、源码身份、依赖、所有镜像层、归档校验及导入后实际运行均已核实。

本轮完整 reviewer 镜像交付门槛均有实际证据。实施者的最终记录路径已确认为 `specs/verification/reviewer-product-validation.md` 与 [reviewer-product-evidence.json](../verification/reviewer-product-evidence.json)；本 reviewer 已核对机器可读记录中的最终身份、归档、输入差异和导入后结果。实施者据此完成规格/INDEX 的最终状态同步即可交付，不需要再等待另一 reviewer 或再开 fresh-context 审查轮次。

本结论仅覆盖本文固定源码与最终镜像，保留 linux/amd64、静态部署、显式停机恢复等已说明边界；不扩展为真实 Kafka/Compose、动态 UDF 家族、HA、原生 ARM 或性能验收通过。
