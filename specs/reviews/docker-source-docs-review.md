# Docker 源码环境独立文档审查

Date: 2026-09-14  
Reviewer: docker_source_docs_review（独立 fresh-context，同一轮并行审查）  
Scope: [最新授权](../reviewer-source-environment.md)、Dockerfile、docker/entrypoint.sh、docker/README.md、根 README 的 Docker 小节及索引中的历史边界。

## 结论与发现

默认行为和手动编译流程符合本轮授权，发现一处文件保留说明不准确，建议交付前修正。

| 编号 | 等级 | 证据与影响 | 建议与处置 |
| --- | --- | --- | --- |
| D1 | P2 | `docker/README.md:30` 先列出源码修改、构建结果和临时日志，随后称挂载源码可以保留“这些文件”。但第 52–55 行只挂载 `/opt/eventflow`；入口帮助第 33 行列出独立的 TMPDIR，而现有 `examples/checkpoint_recovery_demo/run.sh:36`、`examples/condition_trigger_demo/run.sh:41` 将日志写到 `${TMPDIR:-/tmp}`。照文档仅挂载源码并使用 `--rm`，示例临时日志仍会丢失。 | 将挂载保证限定为源码修改和构建结果；若保留日志说明，则明确 TMPDIR 指向已存在的挂载目录，或保留容器后导出。待实施者处置。 |

## 实际检查证据

- 已读取根及 specs 的 AGENTS、specs/INDEX.md 和本轮规格；对照 `/tmp/eventflow-docker-source-env-_fc4pmza/change.patch` 检查授权内改动，未修改生产、测试或示例文件。
- `Dockerfile:43–62` 只复制源码、生成文件清单和记录工具链；原 `RUN eventflow-review build release` 已删除，两个阶段的 CMD 均为 help。工具链安装、第三方 manifest 校验、非 root 用户及 Tini 保留。
- `docker/entrypoint.sh:86–90` 无参数选择 help 并提前退出，不依赖源码存在或触发示例发现；build/test/review/demo 仍由显式命令分派。第 43–56 行构建函数只调用 CMake 配置和编译，符合 `build release` 不运行测试的描述。
- 根 `README.md:608–633` 与 `docker/README.md:11–30` 均先进入 shell，再在同一容器 build 与可选 demo；未出现先销毁编译容器、再从新容器运行旧构建的错误操作。挂载命令使用宿主 UID/GID，构建目录位于挂载仓库内。
- `docker/README.md:77–85` 明确制作时快照、重新构建及旧归档仍保留旧默认行为；`specs/INDEX.md:15–19` 与最新规格将历史预编译镜像和验证记录标为历史证据，不据此宣称本轮测试完成。
- 当前 CMake 的 Release 和 Debug 选项与命令表相符；只核对配置源码，不把这一静态检查当作实际编译运行通过。

审查文件指纹：

```text
2f629ab1d3f2405b91c9a9edc4aeaa934de3f5f36cb7745182f8b087ae09930f  Dockerfile
b5e45ab85789a3212a1cf15c249cde37ee4c4f465c70dd59f62c62592b2950fd  docker/entrypoint.sh
1431a67c6761786b02202160edff90767b129601c03a80fc20f6ee9b7a999451  docker/README.md
57ec95f5e87ed4f7dd44986b81f62f5a5540fa8c7045edda8a8540d9520b9fbc  README.md
```

## 未验证与交接

本 reviewer 未重复执行 Docker 构建、C++ 编译、示例或 CTest。查看本轮 `build.log` 时仍处于工具链安装阶段，实际镜像完成与默认容器运行证据由实施者补充到本轮验证记录；历史镜像绿灯不能替代。并行示例重写与根 README 其他章节不属于本次审查，也不新增全产品验收门槛。

修正 D1 后对文字和实际路径作定向核对即可，无需重新发起独立审查轮。

## D1 定向复查

实施者已修改 `docker/README.md:30`，将挂载保证限定为源码修改与构建结果，并明确临时日志需单独导出或将 TMPDIR 指向已创建的可写挂载目录。独立 reviewer 重新读取该段，说明已与入口和示例的实际路径一致；D1 关闭。本次只复核该处整改，未新增审查轮或扩大运行验证范围。

整改后 `docker/README.md` SHA256：`34082f03a9b074544c8f9e09961a19f0fbf2616cfa51d732d15c260c9676e4d2`。本轮文档审查无剩余发现；镜像实际验证边界仍以上述交接为准。
