# C++ 第三方依赖

本目录是 EventFlow 的固定版本依赖包，目标平台为 **Ubuntu 24.04 / Linux x86_64（amd64）**。只包含发布头文件、预编译静态/共享库、许可证与本说明及 SHA256 清单。头文件内的模板和 inline 实现保留；第三方 `.cpp`、源码树、源码压缩包、`.deb`、生成器 JAR、第三方 CMake 配置和构建中间文件均未纳入。

## 版本与布局

| 依赖 | 上游 / Ubuntu 包版本 | 本地内容 | 主要许可证 |
| --- | --- | --- | --- |
| RocksDB | 8.9.1 / `8.9.1-2` | `include/rocksdb/`、`lib/librocksdb.so*` | GPL-2.0 或 Apache-2.0；保留两种许可证全文 |
| ANTLR4 C++ runtime | 4.10 / `4.10+dfsg-1` | `include/antlr4-runtime/`、`lib/libantlr4-runtime.so*` | BSD-3-Clause |
| nlohmann/json | 3.12.0 | `include/nlohmann/json.hpp` | MIT |
| cpp-httplib | 0.20.1 | `include/httplib.h` | MIT |
| GoogleTest（仅测试） | 1.14.0 / `1.14.0-1` | `include/gtest/`、`lib/libgtest.a`、`lib/libgtest_main.a` | BSD-3-Clause |
| librdkafka C / C++ | 2.3.0 / `2.3.0-1build2` | `include/librdkafka/`、`lib/librdkafka.so*`、`lib/librdkafka++.so*` | BSD-2-Clause；附带组件许可见逐包声明 |
| Cyrus SASL（核心运行库） | 2.1.28 / `2.1.28+dfsg1-5ubuntu3.1` | `lib/libsasl2.so.2*` | BSD-3-Clause-Attribution 等，见逐包声明 |
| OpenSSL（SSL/crypto 运行库） | 3.0.13 / `3.0.13-0ubuntu3.15` | `lib/libssl.so.3`、`lib/libcrypto.so.3` | Apache-2.0 |
| gflags | 2.2.2 / `2.2.2-2build1` | `lib/libgflags.so.2.2*` | BSD-3-Clause |
| Snappy | 1.1.10 / `1.1.10-1build1` | `lib/libsnappy.so.1*` | BSD-3-Clause |
| LZ4 | 1.9.4 / `1.9.4-1build1.1` | `lib/liblz4.so.1*` | BSD-2-Clause（库） |
| Zstandard | 1.5.5 / `1.5.5+dfsg2-2build1.1` | `lib/libzstd.so.1*` | BSD-3-Clause 或 GPL-2.0；含 MIT 组件 |
| zlib | 1.3 / `1:1.3.dfsg-3.1ubuntu2.1` | `lib/libz.so.1*` | Zlib |
| bzip2 | 1.0.8 / `1.0.8-5.1build0.1` | `lib/libbz2.so.1.0*` | bzip2 BSD 变体 |

逐包版权声明位于 `licenses/*.copyright`；这些发行包原始声明也列出未随本目录分发的示例、测试或 Debian 打包文件。JSON 和 HTTP 库保留上游 MIT 全文；发行包引用的 Apache-2.0、GPL-2、GPL-3 完整文本也附在 `licenses/`。

RocksDB 和 ANTLR 的 Ubuntu 包声明要求 `libc6 >= 2.38`、`libstdc++6 >= 13.1`，本包使用 GNU C++ ABI。`libc`、动态加载器、`libm`、`libresolv`（Sanitizer smoke）、`libgcc_s`、`libstdc++` 和线程设施由目标平台工具链提供。其余 `DT_NEEDED` 运行库已放在本目录，不需要安装 RocksDB、ANTLR、librdkafka、SASL、OpenSSL、gflags 或压缩库开发包。

## 项目链接

CMake 使用 `cpp3rdlib/include` 和 `cpp3rdlib/include/antlr4-runtime` 作为 SYSTEM include 目录，直接链接 `cpp3rdlib/lib/librocksdb.so`、`cpp3rdlib/lib/libantlr4-runtime.so` 和 `Threads::Threads`。头文件库不需要单独链接；当前 HTTP 使用不启用 OpenSSL、压缩等可选功能宏。

Kafka 适配层使用 C API：`#include <librdkafka/rdkafka.h>`，include 根仍为 `cpp3rdlib/include`，直接链接 `cpp3rdlib/lib/librdkafka.so` 和线程设施。`librdkafka++.so` 作为可选 C++ 接口预编译库附带，本项目的 `IMessageSource` 不需要链接它。Kafka 的 SASL/SSL/crypto 以及已有 zlib/LZ4/Zstd 运行依赖都使用下文同样的传递 RPATH 规则。

GoogleTest **仅在 `BUILD_TESTING=ON` 时供测试目标使用**。项目自行声明预编译 imported targets：`GTest::gtest` 指向 `lib/libgtest.a`，传递 include 目录和 `Threads::Threads`；`GTest::gtest_main` 指向 `lib/libgtest_main.a`，传递 `GTest::gtest`。测试目标链接 `GTest::gtest_main`；核心库、主程序及其公开接口不得传递 GoogleTest 依赖。没有纳入 gmock、`usr/src` 源码、GoogleTest CMake 配置或其他构建材料。

运行时必须让 RocksDB 的间接依赖也能找到。GNU/Linux 可在可执行目标设置 `LINKER:--disable-new-dtags`，并将 `BUILD_RPATH` 指向本目录的绝对 `lib` 路径，得到可传递的 ELF `DT_RPATH`；安装包应按其可执行文件位置设置 `$ORIGIN` 相对路径。另一种运行方式是给进程设置指向本目录 `lib` 的 `LD_LIBRARY_PATH`。仅设置可执行文件的 `DT_RUNPATH` 不会传递给 RocksDB 的间接依赖。

## 获取来源与原始 SHA256

RocksDB、ANTLR、GoogleTest、librdkafka、Cyrus SASL、gflags、Snappy、LZ4 和 Zstandard 使用 `apt-get download` 下载固定版本 `.deb`，在项目外的 `/tmp` 用 `dpkg-deb --extract` 解包，只复制所需内容。除下文单独说明的 OpenSSL 官方仓库下载外，各下载包的 SHA256 均与本机 APT 索引中的 SHA256 比对一致。没有执行 `sudo`、系统包安装或系统包修改。

| 原始包 | 下载来源 | SHA256（`.deb` 文件） |
| --- | --- | --- |
| `libantlr4-runtime-dev=4.10+dfsg-1` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/universe/a/antlr4-cpp-runtime/libantlr4-runtime-dev_4.10+dfsg-1_amd64.deb) | `89ccb5f533f9d1d55438ce9d5f3d4460c6a17f4613b3c700cda2ef9a57e1ce5e` |
| `libantlr4-runtime4.10=4.10+dfsg-1` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/universe/a/antlr4-cpp-runtime/libantlr4-runtime4.10_4.10+dfsg-1_amd64.deb) | `e811281e56fe3bcdf9fb21d7f5a49b2fc7c6c440d6ac97d7134baaade3442cc2` |
| `libgtest-dev=1.14.0-1` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/universe/g/googletest/libgtest-dev_1.14.0-1_amd64.deb) | `c7dd1ecf6b816bf13ab6e1d8813d1ff0ae7a4fa09326a0cd467c3943d952117f` |
| `librdkafka++1=2.3.0-1build2` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/universe/libr/librdkafka/librdkafka++1_2.3.0-1build2_amd64.deb) | `d5b1ce37ec720345057017d4d3a33469cbc50a6f4bc730e9d13586f3b5e5c823` |
| `librdkafka-dev=2.3.0-1build2` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/universe/libr/librdkafka/librdkafka-dev_2.3.0-1build2_amd64.deb) | `ed1e3d3a689b83b9bc6aeedb4b83b01dfff77c464897a87d2d082a0c9069c49f` |
| `librdkafka1=2.3.0-1build2` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/universe/libr/librdkafka/librdkafka1_2.3.0-1build2_amd64.deb) | `0294972c1eb7229644ba09c97772b69d0587686b1e082d75ea8d77d615385a2b` |
| `libsasl2-2=2.1.28+dfsg1-5ubuntu3.1` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/main/c/cyrus-sasl2/libsasl2-2_2.1.28+dfsg1-5ubuntu3.1_amd64.deb) | `eda097f98dcb3a08b9ce157d6191d140e4885c1cba47b683c94b8ca45e88f458` |
| `libssl3t64=3.0.13-0ubuntu3.15` | [Ubuntu 官方安全仓库包](https://security.ubuntu.com/ubuntu/pool/main/o/openssl/libssl3t64_3.0.13-0ubuntu3.15_amd64.deb) | `3d0955bc049bbcca0f4c3e78a3a8b994593d96db7d84f4320217224433844534` |
| `libgflags2.2=2.2.2-2build1` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/universe/g/gflags/libgflags2.2_2.2.2-2build1_amd64.deb) | `93acc6837f4759de9f93b20224f94c438f4f1fcd2b710312236378adb023634e` |
| `liblz4-1=1.9.4-1build1.1` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/main/l/lz4/liblz4-1_1.9.4-1build1.1_amd64.deb) | `319331270d5cc52d5ebffe51c941d7b01b432bc402c2924b557209a64d4ecbad` |
| `librocksdb-dev=8.9.1-2` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/universe/r/rocksdb/librocksdb-dev_8.9.1-2_amd64.deb) | `f827f6fcd1bc6f9cbfe5379b95dbb75b61f90c8d659c1d4335b5163c5e14c333` |
| `librocksdb8.9=8.9.1-2` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/universe/r/rocksdb/librocksdb8.9_8.9.1-2_amd64.deb) | `2ad062e48199d5a604f680e31c357b4e98dea045b002c300b3f8cd48c55a5a69` |
| `libsnappy1v5=1.1.10-1build1` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/main/s/snappy/libsnappy1v5_1.1.10-1build1_amd64.deb) | `c44dfa3b8be7c2873efb770890cb282a36a7e59b3b1900cb50d99f92e6be2da5` |
| `libzstd1=1.5.5+dfsg2-2build1.1` | [Ubuntu 镜像包](https://mirrors.tuna.tsinghua.edu.cn/ubuntu/pool/main/libz/libzstd/libzstd1_1.5.5+dfsg2-2build1.1_amd64.deb) | `dfcf25061e07aad7efd3f4f880ba5ad4d4d09ebe7fc8cc77ab6b8a161d6d4727` |

Kafka 依赖补充时，本机 APT 索引中的 `libssl3t64=3.0.13-0ubuntu3.11` 在镜像及官方池均已返回 404。改用 [Ubuntu 官方安全仓库](https://security.ubuntu.com/ubuntu/dists/noble-security/) 的 `3.0.13-0ubuntu3.15`（[包信息](https://packages.ubuntu.com/noble-updates/libssl3t64)）。已使用 `/usr/share/keyrings/ubuntu-archive-keyring.gpg` 验证 `InRelease` 的 Ubuntu Archive Automatic Signing Key (2018) 签名，再比对其 `main/binary-amd64/Packages.xz` SHA256（`6b41cbfe4722b46bc5d12622e7f5cadc424f252f9bf41eecf8ae99feaa9e6034`），最后按该索引验证上表 OpenSSL deb 的 SHA256。索引、包和解包目录都仅在项目外 `/tmp` 保留，未修改系统 APT 状态或安装包。

以下两个已安装版本在所用镜像返回 404，因此从本机 Ubuntu 安装包直接复制运行库与 `/usr/share/doc/<包名>/copyright`。版本由 `dpkg-query` 确认，库内容没有修改；下表 SHA256 对应实际复制的 ELF 文件。

| Ubuntu 包 | 本机来源 | SHA256（ELF 文件） |
| --- | --- | --- |
| `zlib1g=1:1.3.dfsg-3.1ubuntu2.1` | `/usr/lib/x86_64-linux-gnu/libz.so.1.3` | `9b64150b28505a33d6bc3ecf709c279f6de97a1c184dbda65d06ee4537f6d286` |
| `libbz2-1.0=1.0.8-5.1build0.1` | `/usr/lib/x86_64-linux-gnu/libbz2.so.1.0.4` | `218c4abdabce31161f3c2778762986d1fe06633705c7eb004a740946a5ccdfff` |

两个单头文件直接来自上游固定标签，未修改其内容：

| 资产 | 官方来源 | SHA256 |
| --- | --- | --- |
| nlohmann/json 3.12.0 | [json.hpp](https://raw.githubusercontent.com/nlohmann/json/v3.12.0/single_include/nlohmann/json.hpp)；[发布页](https://github.com/nlohmann/json/releases/tag/v3.12.0) | `aaf127c04cb31c406e5b04a63f1ae89369fccde6d8fa7cdda1ed4f32dfc5de63` |
| cpp-httplib 0.20.1 | [httplib.h](https://raw.githubusercontent.com/yhirose/cpp-httplib/v0.20.1/httplib.h)；[发布页](https://github.com/yhirose/cpp-httplib/releases/tag/v0.20.1) | `7d3f813d05dad23b9c66eb6d9c1ff3d66f6acfaf6e5900ce960f7990e78079ea` |

JSON 的 SHA256 同时与上游发布页给出的 `json.hpp` 校验值比对一致。HTTP 头文件 SHA256 是本次下载文件的实测指纹。许可证来自相同标签的 [JSON LICENSE.MIT](https://raw.githubusercontent.com/nlohmann/json/v3.12.0/LICENSE.MIT) 和 [cpp-httplib LICENSE](https://raw.githubusercontent.com/yhirose/cpp-httplib/v0.20.1/LICENSE)。

**ANTLR 发行包元数据限制：** 原样发布的 `Version.h` 定义 `ANTLRCPP_VERSION_MAJOR/MINOR/PATCH`，而字符串拼接宏引用 `ANTLR4CPP_VERSION_*`；因此 `RuntimeMetaData::getRuntimeVersion()` 返回宏名字面量，不能用于显示或判断版本。本目录保留发行包原始内容，实际版本由 APT 固定版本、头文件数值宏（4/10/0）和 4.10 生成代码的实际解析验证确认。

ANTLR 自有 grammar 的生成工具必须匹配 **4.10**。使用 [官方 antlr-4.10-complete.jar](https://www.antlr.org/download/antlr-4.10-complete.jar)，实测 SHA256 为 `fb5cf682be74aafb6cf355b118bb5b2fa4a10abd4bc0a59fb7883f597ecc7201`；该 JAR 仅保留在准备机器的 `/tmp`，不在交付目录中。常规 CMake/Makefile 构建使用项目已生成的解析代码，无需下载或运行 JAR。

## 文件校验与已执行验证

`MANIFEST.sha256` 覆盖本目录全部头文件、静态/共享库、许可证和 README，不包含清单自身；符号链接按其目标文件的内容计算哈希。可在本目录执行 `sha256sum -c MANIFEST.sha256`。相对符号链接目标如下：

| 符号链接 | 目标 |
| --- | --- |
| `lib/libantlr4-runtime.so` | `libantlr4-runtime.so.4.10` |
| `lib/libbz2.so.1.0` | `libbz2.so.1.0.4` |
| `lib/libgflags.so.2.2` | `libgflags.so.2.2.2` |
| `lib/liblz4.so.1` | `liblz4.so.1.9.4` |
| `lib/librdkafka.so` | `librdkafka.so.1` |
| `lib/librdkafka++.so` | `librdkafka++.so.1` |
| `lib/libsasl2.so.2` | `libsasl2.so.2.0.25` |
| `lib/librocksdb.so` | `librocksdb.so.8.9` |
| `lib/librocksdb.so.8.9` | `librocksdb.so.8.9.1` |
| `lib/libsnappy.so.1` | `libsnappy.so.1.1.10` |
| `lib/libz.so.1` | `libz.so.1.3` |
| `lib/libzstd.so.1` | `libzstd.so.1.5.5` |

初始依赖准备的历史验证（GNU C++）：在 Ubuntu 24.04 x86_64 对当时 8 个 ELF 运行库执行 `ldd` 均无 `not found`，并确认 RocksDB 的 gflags 与 5 个压缩依赖均解析到本目录。另用项目外临时 CMake/Unix Makefiles 程序，以 GNU C++ 13.3 / C++20 编译所有主要依赖，运行 RocksDB 打开数据库及 Put/Get、ANTLR 输入流及由 4.10 工具生成的临时 grammar 解析、JSON 解析和 HTTP 路由注册；不设置 `LD_LIBRARY_PATH` 时依靠 ELF RPATH 成功执行。该临时验证工程和数据库未纳入项目。

GoogleTest 初始补充的历史验证（GNU C++）：使用项目外临时 CMake/Unix Makefiles 工程，以 GNU C++ 13.3 / C++20 声明同名 `GTest::gtest` 与 `GTest::gtest_main` imported targets 并静态链接，CTest 执行 1 个 GoogleTest 用例（断言及异常断言）全部通过。仅检查依赖能编译运行；项目测试与生产目标的依赖隔离由项目构建验收另行确认。


## Kafka 依赖验证与边界（2026-09-14）

- 头文件 `RD_KAFKA_VERSION` 与运行时 `rd_kafka_version()` 一致，为 `0x020300ff`；`rd_kafka_version_str()` 为 `2.3.0`。仅复制 3 个发布头文件、C/C++ 两个共享库及其链接、SASL 核心运行库、OpenSSL SSL/crypto 运行库与逐包许可证；没有复制 librdkafka 静态库、源码或构建材料。
- 在项目外通过 CMake + Unix Makefiles，以 Clang 18.1.3 / C++20、`-Wall -Wextra -Werror`、`-fsanitize=address,undefined,leak`、`-fno-sanitize-recover=all` 构建生命周期 smoke；CTest 1/1 通过。环境启用 `ASAN_OPTIONS=detect_leaks=1:halt_on_error=1:abort_on_error=1`、`UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`、`LSAN_OPTIONS=exitcode=23`，没有 Sanitizer 诊断。预编译第三方 `.so` 本身为发行包原始二进制，没有重新插桩；本次验证覆盖调用程序、可观察的进程内存错误和泄漏，不宣称完成库内部的 Sanitizer 全覆盖。
- smoke 不配置 broker；分别运行 Producer 和 Consumer 的 `rd_kafka_conf_new` / `rd_kafka_conf_set` / `rd_kafka_new` / `rd_kafka_destroy` / `rd_kafka_wait_destroyed(5000)`。Consumer 配置 `group.id`，关闭 `enable.auto.commit` 和 `enable.auto.offset.store`；销毁后等待后台线程结束成功。对配置错误和空返回值显式检查，没有第三方异常包装。
- 清空 `LD_LIBRARY_PATH` 后，smoke 依靠 `DT_RPATH` 成功执行，所有非基础 `DT_NEEDED` 依赖均解析到本目录。整个目录当前 13 个共享 ELF 库的 `ldd` 检查无 `not found`，全部非基础依赖均来自本目录。系统基础依赖仅为 glibc/动态加载器（含 math、resolver、线程等设施）、GNU `libstdc++` 与 `libgcc_s`；Kafka/OpenSSL 包要求 `libc6 >= 2.38`，C++ 接口包要求 `libstdc++6 >= 13.1`。
- `builtin.features` 报告 `gzip,snappy,ssl,sasl,regex,lz4,sasl_gssapi,sasl_plain,sasl_scram,plugins,zstd,sasl_oauthbearer`，这表示编译能力，不表示已验证对应认证与网络配置。打包范围是 ELF `DT_NEEDED` 闭包；没有纳入按配置动态加载的 Cyrus SASL 认证/数据库插件、Kerberos 凭据、OpenSSL 额外 provider/engine、TLS CA/证书或系统认证配置。若部署选择相关认证方式，需另行明确插件与配置，并运行真实认证测试。
- 真实 Kafka broker 的订阅、生产/消费、手动提交、pause/resume、重放与恢复尚未由此依赖 smoke 验证；`IMessageSource` 实现通过独立协议 mock 验证。单节点 Docker Compose 集成脚本及真实 broker 验证按用户的五小时核心交付决定后置；后续固定使用 `apache/kafka:4.3.1`，采用兼任 broker/controller 的 KRaft 模式，无 ZooKeeper，不提交服务端发行包或镜像文件。当前不宣称该集成已实现或通过。项目其余依赖的早期 GNU C++ smoke 仅作为历史准备证据，不能替代当前 Clang/Sanitizer 项目质量门禁。

Cyrus SASL attribution（原样保留）：This product includes software developed by Computing Services at Carnegie Mellon University (http://www.cmu.edu/computing/).
