# 构建辅助约定

本目录继承[根约定](../AGENTS.md)。项目目标和链接关系集中在根 `CMakeLists.txt`，常用入口由根 `Makefile` 提供；当前本目录包含格式化脚本 `Format.cmake`。

## 构建合同

- 只使用 CMake/Makefile；项目要求 C++20 和 Clang。已有 GCC 参考构建目录不代表可用的当前项目工具链。
- Debug/开发测试保留 ASan、UBSan、LSan 及失败诊断；Release 使用 `-O3`、`NDEBUG`、受支持的 LTO，关闭 sanitizer 和 `BUILD_TESTING`。保持无 fast-math 的数值语义。
- 依赖只从 [cpp3rdlib](../cpp3rdlib/AGENTS.md) 的头文件与预编译库接入；不要添加 FetchContent、ExternalProject、自动下载或第三方源码构建。
- GoogleTest imported targets 和测试链接必须位于 `BUILD_TESTING` 条件内，不能传递到核心库、服务或公开 API。
- 保留运行包的相对 RPATH 和间接依赖解析；验证构建目录与安装目录时分别检查，不能用开发机环境变量掩盖缺库。
- 格式化脚本只处理列出的项目 C++ 目录，不扩展到 `cpp3rdlib/` 或 `.reference-repos/`。当前列表包含 SQL 生成文件；重新生成后按项目格式流程检查，不修改生成语义或签名。
- Bear/clangd/clang-format 是开发工具，不能成为普通构建的下载或运行前提。新增源文件要纳入目标与编译数据库。

## 验证入口

以下命令从仓库根目录运行；已有不兼容 compiler/generator 的缓存应另选构建目录。

```sh
cmake -S . -B build -G "Unix Makefiles" -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build --parallel 4
ctest --test-dir build --output-on-failure
cmake -DFORMAT_CHECK=ON -P cmake/Format.cmake
cmake -S . -B build-release -G "Unix Makefiles" -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
cmake --build build-release --parallel 4
```

根据改动选择实际必要的验证。配置、链接或工具链变更需验证受影响构建及最终 flags/依赖；纯 Markdown 修改只需文档核对。`make compdb` 会 clean 后重编以供 Bear 捕获，不能当作只读查询命令。
