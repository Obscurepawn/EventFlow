# Docker 源码与编译环境

Status: done
Owner: root
Updated: 2026-09-14

## 最新授权与范围

用户要求 Dockerfile 只准备编译环境和源码，镜像默认运行不执行 example 端对端测试，并同步 README。该要求取代[此前镜像交付](reviewer-image.md)中预编译 Release 示例及默认执行全部示例的约定；历史镜像验证仍只描述其原有快照。

保留 Clang/CMake/Make、bundled 第三方依赖、非 root 用户、Tini、源码与清单。删除镜像构建期间的项目编译，默认命令显示帮助；用户可显式调用 build、test、review、demo 或 shell。不改变项目 C++、示例或系统测试。并行进行的[数据库示例重写](streaming-database-example.md)保持其范围与状态。

## 验收与状态

- [x] Dockerfile 没有项目编译或示例/测试运行步骤，默认 CMD 为 help。
- [x] 入口无参数也显示帮助；显式构建和测试命令保留。
- [x] 根 README 和 Docker README 描述源码环境及手动操作，避免不同临时容器之间误用已删除的构建结果。
- [x] 完成适用验证及一轮并行独立审查，记录实际证据和未验证边界。

## Changed files

`Dockerfile`、`docker/entrypoint.sh`、`README.md` 的 Docker 小节、`docker/README.md`、本规格、`specs/INDEX.md` 及旧镜像规格的历史说明；[验证记录](verification/reviewer-source-environment-validation.md)、[输入清单](verification/reviewer-source-environment-inputs.sha256)与[环境审查](reviews/docker-source-environment-review.md)、[文档审查](reviews/docker-source-docs-review.md)。

## 当前状态与下一步

已完成。`bash -n`、静态检查、默认 help、真实镜像构建与无网络容器检查均通过；源码、工具链、非 root shell、依赖/源码清单齐全，镜像内没有项目编译目录。Docker 文件与根 README 的 Docker 小节已和镜像内核对；根 README 引言的同期编辑予以保留，不宣称整份 README 与镜像相同。

一轮双 reviewer 独立审查完成；D1 日志保留说明已修正并定向复查，无剩余发现。通过现有 WSL root 执行 Docker 验证，未调整宿主权限或安装工具。验证标签为 `eventflow:reviewer-source-check`；原 reviewer 标签/归档仍是历史快照，按 README 重新构建后应用新默认行为。未运行 C++ 编译、示例或测试，不将并行项目重写视为已验收。
