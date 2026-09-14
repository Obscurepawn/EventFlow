# Docker 编译环境与源码

`eventflow:reviewer` 包含项目源码、Clang 18/CMake/Make、sanitizer 工具和 `cpp3rdlib/` 第三方依赖。制作镜像只准备环境和文件，默认启动显示帮助；项目编译、示例及端对端测试均由使用者显式执行。

目标平台为 **linux/amd64**，容器使用 Ubuntu 24.04 的 glibc/GNU libstdc++。宿主机需要能运行 Linux 容器的 Docker；ARM 主机还需要可用的 amd64 模拟环境。基础镜像及系统工具链只在制作镜像时下载。

## 准备镜像并进入源码目录

在完整源码仓库根目录执行：

```sh
docker build --platform linux/amd64 -t eventflow:reviewer .
docker run --rm --network none eventflow:reviewer
docker run --rm -it --network none eventflow:reviewer shell
```

第二条命令显示帮助后退出；第三条进入 Bash，工作目录为 `/opt/eventflow`。容器中已准备好源码和第三方依赖，可以直接阅读、修改或执行以下命令：

```sh
sha256sum -c SOURCE_MANIFEST.sha256
eventflow-review build release
```

`build release` 只编译，不运行示例或测试。若需要检查示例，在**同一个容器**中继续执行：

```sh
eventflow-review demo all
```

退出时，`--rm` 会删除容器内的源码修改、构建结果和临时日志。下方挂载方式可保留源码修改与构建结果；临时日志还需要单独导出，或将 `TMPDIR` 指向已创建的可写挂载目录。单独启动另一个临时容器不会继承前一个容器的编译结果。

## 显式选择验证

以下操作只在手动指定时执行：

```sh
# Build Debug and run all CTest targets with sanitizers.
docker run --rm --network none eventflow:reviewer test

# Rebuild Release and run all demos in one container.
docker run --rm --network none eventflow:reviewer review
```

入口自动发现 `examples/*/run.sh`。`demo NAME` 支持指定其中一个示例名称，但需要先在同一容器或同一挂载源码目录中完成 Release 编译。示例名称以镜像实际包含的源码为准。

## 挂载本地源码并保留构建结果

`toolchain` 阶段可单独制作成 `eventflow:build-env`，只包含工具链和第三方依赖。它的默认命令同样显示帮助。构建项目时挂载完整源码：

```sh
docker build --platform linux/amd64 --target toolchain -t eventflow:build-env .
docker run --rm --network none \
  --user "$(id -u):$(id -g)" \
  --mount "type=bind,source=$PWD,target=/opt/eventflow" \
  eventflow:build-env build release
```

上述命令适用于 Linux、WSL、macOS 的 POSIX shell。`--user` 使生成文件归当前宿主用户所有。构建结果保存在仓库内的 `build-docker-release/` 或 `build-docker-debug/`；独立目录避免复用包含不同绝对路径的本机构建缓存。需要交互修改时，在 Docker 参数中增加 `-it` 并把 `build release` 换成 `shell`。

挂载仓库必须包含 `cpp3rdlib/`；挂载会覆盖容器同路径目录，实际构建从挂载的仓库读取依赖。`eventflow:reviewer` 也支持同样的挂载方式。

## 命令与环境

| 命令 | 行为 |
| --- | --- |
| `help` 或无参数 | 显示帮助并退出 |
| `build release` / `build debug` | 只执行对应构建；Debug 编译测试目标 |
| `demo all` / `demo NAME` | 使用已有 Release 构建运行示例 |
| `review` | 清理重编译 Release，然后运行全部示例 |
| `test` | 编译 Debug，然后执行全部 CTest 目标 |
| `shell` | 进入 Bash；交互使用时给 Docker 增加 `-it` |

Release 使用 O3、NDEBUG、受支持的 LTO，无 sanitizer 或 fast-math；Debug 测试启用 ASan/UBSan/LSan，诊断失败即停。GoogleTest 只链接测试目标。默认并行编译数为 4，CTest 并行数为 2；资源较少时可增加 `-e EVENTFLOW_BUILD_JOBS=2 -e EVENTFLOW_TEST_JOBS=1`。

镜像默认 UID/GID 为 `10001:10001`，Tini 负责信号转发和子进程回收。项目编译不需要外部网络或 Java；`--network none` 保留容器内 loopback，供显式启动的本地进程通信。真实 Kafka 需要另行准备 broker。

## 镜像内容与快照

制作镜像需要访问镜像仓库及 Ubuntu 软件源。Dockerfile 固定基础镜像 digest，通过签名软件源安装系统工具链，在安装 CA 后使用 HTTPS 下载。项目第三方依赖只来自 `cpp3rdlib/` 并校验 `MANIFEST.sha256`；项目使用 CMake/Unix Makefiles 构建。

`.dockerignore` 使用允许清单，Dockerfile 只复制指定目录。私有 PDF、参考仓库、Git、历史制品和本机构建缓存不进入镜像。`TOOLCHAIN-VERSIONS.txt` 记录工具链版本，`SOURCE_MANIFEST.sha256` 记录 reviewer 镜像的源码与随包文件指纹。本说明位于 `/opt/eventflow/docker/README.md`；完整研究文档和 specs 保留在源码仓库。

镜像内容对应制作时的源码快照；修改 Dockerfile 或源码后，需要重新执行 `docker build`。以前导出的预编译示例镜像仍保留当时的默认行为，不能用旧归档验证这次调整。历史镜像验证记录保留在仓库 `specs/verification/`。

数据卷不等于一致 checkpoint，容器重启不等于自动恢复。常驻 Compose、真实 Kafka broker、原生 ARM 镜像及 HA 保持各自的后续范围。
