# Docker 源码环境独立审查

日期：2026-09-14。Reviewer：`docker_source_env_review`，本轮并行独立审查之一。

## 范围与结论

依据[最新规格](../reviewer-source-environment.md)、[索引](../INDEX.md)及根目录/specs 协作约定，检查 Dockerfile、入口脚本、Docker README 和根 README 的 Docker 小节。基线为 `/tmp/eventflow-docker-source-env-_fc4pmza/` 及其中的 `change.patch`。未检查或修改并行进行的应用/示例重写。

**未发现本次改动范围内需要整改的问题。** 当前文本与入口行为满足“镜像只准备编译环境与源码，默认不编译、不运行示例或测试”的要求；实际镜像构建与容器验证仍由实施者补齐，不据此宣布整个产品通过验收。

## 实际证据

| 要求 | 独立检查结果 |
| --- | --- |
| 制作镜像不编译项目 | [Dockerfile](../../Dockerfile) 删除了 `eventflow-review build release` 的 RUN；剩余 RUN 只安装工具、设置权限、校验依赖、生成清单及记录版本，没有项目构建或测试调用。 |
| 镜像与入口默认帮助 | Dockerfile 的 toolchain/reviewer 两阶段均为 `CMD ["help"]`；[入口](../../docker/entrypoint.sh) 使用 `${1:-help}` 并先处理帮助。实际执行无参数、`help`、`-h`、`--help`，均退出 0，输出完全一致且 stderr 为空。检查刻意设置不存在的源码目录及非法 jobs，证明帮助不依赖源码或构建配置。`bash -n` 通过。 |
| 显式命令保留 | 将 `build_project`、demo 实现以及参数校验到最终 dispatch 与基线逐字比较，完全一致；`build`、`test`、`review`、`demo`、`shell` 仍可显式选择。本轮未运行这些命令的项目编译或执行链。 |
| 工具链、源码和权限保留 | toolchain 阶段与基线逐字一致，保留 Clang/LLVM、CMake/Make、sanitizer、Tini、UID/GID 10001、第三方清单校验；reviewer 的源码 COPY、chown、源码清单与工具版本记录保留。只读核对 `.dockerignore`，本机构建缓存与私有文件排除规则未改变。 |
| 使用说明一致 | [Docker README](../../docker/README.md) 与[根 README](../../README.md) 的 Docker 小节均先展示默认帮助，再明确手动构建；示例在同一容器执行，说明 `--rm` 清理结果及挂载保留方式。历史预编译归档与当前行为已区分。 |

检查对象 SHA-256：

```text
2f629ab1d3f2405b91c9a9edc4aeaa934de3f5f36cb7745182f8b087ae09930f Dockerfile
b5e45ab85789a3212a1cf15c249cde37ee4c4f465c70dd59f62c62592b2950fd docker/entrypoint.sh
1431a67c6761786b02202160edff90767b129601c03a80fc20f6ee9b7a999451 docker/README.md
8ec55641c9cc3ff44bd943c4c52033cc3026815dbdb0a962da30387ad6451837 README.md Docker section
```

根 README 指纹仅覆盖 `### Docker：` 起至下一同级标题之前，避免将并行文档修改纳入本次结论。

## 证据边界与交接

未重复启动镜像构建，未运行产品构建、示例、CTest 或 sanitizer。查看 `/tmp/eventflow-docker-source-env-_fc4pmza/build.log` 时，构建仍在安装 LLVM 工具链，尚无最终退出结果；本报告不声称真实镜像已构建或容器已运行成功。实施者需依据实际构建结果和容器默认行为检查更新规格及最终验证记录。本轮无整改项，不要求额外 fresh-context 审查轮次。
