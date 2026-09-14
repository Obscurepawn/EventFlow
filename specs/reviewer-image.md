# Reviewer Docker 镜像交付

当前 Dockerfile 的默认行为已按用户最新要求改为[源码与编译环境](reviewer-source-environment.md)：只准备环境和源码，默认显示帮助。下文保留此前预编译示例镜像及其验收记录，不代表当前默认行为。

Status: complete（编译环境与完整源码镜像均已交付并验收）
Owner: root  
Updated: 2026-09-14

## 授权与目标

用户要求基于当前已完成的源码生成 Docker 镜像，让 project reviewer 以简单命令编译并运行项目；随后明确授权在本机安装 Docker。交付本地可导入的镜像归档及使用入口，不自动上传镜像或私有材料。

## 恢复完整镜像交付

用户已确认源码稳定，目前仅修改文档。以本轮固定构建输入生成 `eventflow:reviewer`，在容器中验证实际项目 Release 重编译、Debug/完整 CTest、全部现有示例、信号退出和归档导入。文档可继续修改；记录镜像内文档快照及编译输入指纹，不用后续文档变动替代源码验证，也不覆盖用户正在编辑的其他内容。

本轮是完整源码镜像交付里程碑，将进行一次新的双 reviewer 并行 fresh-context 全局审查；此前环境镜像审查作为已完成环境阶段的证据保留。

## 已完成的环境阶段：先完成编译环境

用户随后确认源码仍在修改，要求「先完成镜像环境」。本轮交付调整为 `eventflow:build-env`：Clang/CMake/Make、sanitizer、第三方依赖、挂载源码的编译入口及本地镜像归档。环境默认显示帮助；挂载完整源码后才执行 build/review/test。完整 `eventflow:reviewer` 源码快照及项目全量验收等待用户确认源码稳定。

环境验收使用项目外的临时 CMake 工程，实际连接 RocksDB、ANTLR、JSON、HTTP、librdkafka 和 GoogleTest，验证 Release/LTO 与 Debug/sanitizer，以及 UID、离线网络、镜像导入和退出码。该探针不代表 EventFlow 全量测试或真实 Kafka broker 通过。

### 本轮环境验收

- [x] 环境镜像构建成功；本机 Docker 安装和服务可用。
- [x] 默认帮助、非 root shell、工具链版本和依赖清单验证通过。
- [x] 非 root 挂载临时源码，断网完成 Release/LTO 和 Debug/GoogleTest/sanitizer 编译运行。
- [x] 独立构建目录、动态示例发现、失败退出及基础信号转发验证通过。
- [x] 本地归档及 SHA256，重新导入与运行验证通过。
- [x] 两位同轮独立 reviewer 完成环境范围复核，原问题处置和后置项目门槛明确记录。

## 范围与约束

- Ubuntu 24.04 / linux/amd64；使用随仓库的 `cpp3rdlib/`，系统运行库满足其 glibc/GNU C++ ABI。安装容器工具链属于环境准备，C++ 依赖不在构建中下载或从第三方源码重建。
- Reviewer 镜像包含当前项目源码、Clang/CMake/Make、Debug sanitizer 运行库、GoogleTest 和可直接执行的 Release 示例。默认运行全部现有示例（本轮固定输入为八个）；另提供容器内重新编译并运行、Debug 测试及交互 shell。
- 保留现有示例脚本的真实 KV、Coordinator、双 Compute 进程、退出状态和 checkpoint 恢复行为；使用现有 MemMock 路径，不加入 Kafka broker 或常驻 Compose 集群。
- Release 遵守 O3/NDEBUG/受支持 LTO、无 sanitizer/fast-math；Debug 测试遵守 ASan/UBSan/LSan 失败诊断。镜像中保留依赖许可证。
- 构建上下文及镜像层排除私有 PDF、参考仓库、Git、历史制品、本机构建缓存和会话材料；只复制明确纳入的源码与依赖目录。
- 不修改生产 C++ 的语义或既有接口，不把历史测试记录当作容器运行证据。

## 完整源码镜像验收（已完成）

- [x] Docker 本机安装完成，daemon 可用，实际构建镜像成功。
- [x] 镜像默认入口在无外部网络的容器内通过全部八个示例，包含真实四进程与恢复场景。
- [x] 容器内从源码完成 Release 重新编译并运行示例；Debug 构建与完整 CTest 通过。
- [x] 检查 Release flags、动态依赖、非 root 用户、错误参数与退出状态；源码/依赖身份可核对。
- [x] 导出可 `docker load` 的本地镜像归档，生成 SHA256，重新导入后运行验证。
- [x] README 提供最短导入、运行、重新编译命令；详细说明包含平台、资源、日志、测试和边界。
- [x] 两位 fresh-context reviewer 在同一轮并行审查本次要求、实现和验证证据，合理问题整改并定向复验。

## 任务与当前状态

| 任务 | Owner | 状态 |
| --- | --- | --- |
| Docker 安装及本机 daemon | root | 已完成，Docker Engine 29.8.0；官方 Ubuntu 基础镜像拉取成功 |
| Dockerfile、构建上下文与容器命令入口 | root | 环境入口已完成；挂载权限、独立构建目录、动态示例发现和失败退出通过 |
| 环境镜像构建与离线验证 | root | 完成；临时工程 Release/ThinLTO、Debug/GTest/sanitizer、动态库及导入后运行通过 |
| 独立审查、归档导入及读者文档 | root | 完成；同轮两份环境审查通过，302 MB 本地归档、校验与使用说明齐备 |
| 完整源码镜像与项目全量验收 | root | 已完成；8 示例、60 测试目标（59 初跑 + Coordinator 完整复验）、信号清理、19 层审计、归档导入与双 reviewer 全局审查通过 |

## 决策

- 2026-09-14：本次明确交付可编译的 reviewer 镜像，保留工具链和源码；预编译 Release 示例提供快速运行入口。常驻 Compose 与 Kafka 集成继续后置。
- 2026-09-14：主机没有 Docker、无免密 sudo，但 WSL 支持以 root 运行管理命令；按用户授权从 Docker 官方 APT 仓库安装，不修改本机 glibc 或项目 C++ 依赖。
- 2026-09-14：daemon 直连 Docker Hub 超时，而 shell 通过既有本地代理可访问；为新 daemon 配置同一代理后拉取成功。代理配置只存在主机 `/etc/docker/daemon.json`，不写入项目或镜像运行环境。
- 2026-09-14：基础镜像固定到 `ubuntu:24.04@sha256:224a1869083a311ef3f13648a154ba79832fbef6364d31493642ca03082da254`。镜像保留非 root 用户和 Tini；默认七个预编译示例，`review` 清理重编译 Release 后运行示例，`test` 构建 Debug 后运行 CTest。
- 2026-09-14：用户要求先完成环境。环境默认 help，挂载采用当前宿主 UID/GID，容器构建目录改为 `build-docker-release` / `build-docker-debug`。入口自动发现 `examples/*_demo/run.sh`，适配并发的 Clock 重命名和新增示例。
- 2026-09-14：工具链 HTTP 下载先后遇到 APT 502；安装 CA 后切换官方源到 HTTPS，环境构建成功。两位 reviewer 接受调整后的环境里程碑，在同一轮报告中继续定向检查，不把应用全量验证标为通过。

## Changed files

已修改根 `Dockerfile`、`.dockerignore`、`docker/entrypoint.sh`、`docker/README.md`、根 `README.md`、本规格与 `INDEX.md`。环境阶段保留 `verification/reviewer-image-validation.md`、`reviewer-environment-evidence.json` 及两份 `reviews/REVIEWER-image-*-review.md`。完整阶段新增 `verification/reviewer-product-validation.md`、`reviewer-product-evidence.json`、`reviewer-product-inputs.sha256` 和两份 `reviews/REVIEWER-product-*-review.md`。本地 `artifacts/` 中分别提供环境与完整镜像归档、SHA256 和配套 README。

## 最终交付与后续

完整镜像 `eventflow:reviewer` 已交付，Image ID 为 `sha256:e07c9842cab9ab2fa4aca2a2049d39c0ed3c459da4c213469fb6d83156a26de4`。本地归档 `artifacts/eventflow-reviewer-linux-amd64.docker.tar.gz` 为 309,553,316 bytes，SHA256 为 `89eeb6d14b5a5375ab66fd303b5948367258b8fa9312e6f44003bfd55d856699`。

[完整验证](verification/reviewer-product-validation.md)记录 Release/Debug、8 示例、60 测试目标、信号、归档及本轮独立审查。首次全量 CTest 为 59/60，唯一 Coordinator 旧 180s 超时；按用户最终 CMake 的 600s 配置复验整个目标，10 个 GoogleTest 全部通过。保留原失败，不将结果伪写为首次全量一次通过。

提交完整项目和 Dockerfile 即可由 reviewer 构建，镜像二进制为可选附件。文档按打包时快照记录，后续编辑不覆盖本轮源码证据。源码变动后重新构建并运行对应验证；本轮没有修改业务 C++ 或测试源码，仅修复 Docker 信号等待并纳入已有测试时限调整。

[环境验证](verification/reviewer-image-validation.md)及 `eventflow:build-env` 归档继续保留。Kafka Compose、原生 ARM、HA 和其他产品后置范围保持不变。
