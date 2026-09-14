# 预编译依赖协作约定

本目录继承[根约定](../AGENTS.md)。依赖来源、固定版本、ABI、许可证、RPATH 及验证边界以 [README](README.md) 和 `MANIFEST.sha256` 为准；这是封装的第三方资产目录，不是项目实现源码目录。

- 只保存所需发布头文件、预编译库、许可证及来源/校验说明。不得加入第三方实现源码、源码压缩包、构建目录、Kafka 服务端发行包或容器镜像。
- 保持第三方头文件原样，不应用项目命名、ASCII、`noexcept` 或 clang-format 改写；项目自有适配代码放入 `src/`。
- 正常 CMake/Make 构建离线使用本目录；依赖更新的准备工作在项目外完成，不添加构建时下载。
- 更新依赖时核对固定版本、头文件/库 ABI、许可证、相对符号链接和非基础动态依赖闭包，并更新 README 与相应 manifest 条目。不要无依据重新生成清单掩盖内容漂移。
- `MANIFEST.sha256` 覆盖发布头文件、库、许可证和 README；本文件是项目协作说明，不是第三方发布资产，不要求为本次初始化修改资产清单。
- GoogleTest 仅供开启 `BUILD_TESTING` 的测试目标；ANTLR 代码生成器版本须匹配本目录 runtime，当前 SQL 使用 4.10。生成器的 Java 依赖不属于 EventFlow 程序的运行依赖。
- Kafka client 库可用不等于真实 broker 集成完成。后续 Compose 的固定镜像和范围遵循根约定，不把生命周期 smoke 或协议 mock 升格成真实集成证据。

资产变更后在本目录运行以下校验，并按影响构建实际项目调用方、执行相关 adapter 合同测试；仅校验 hash 不能证明 ABI 或行为正确。

```sh
sha256sum -c MANIFEST.sha256
```
