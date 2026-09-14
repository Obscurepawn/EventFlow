# 模块 AGENTS.md 初始化

Status: done
Owner: root
Updated: 2026-09-14

## 目标与范围

用户通过 `/init` 授权为各模块创建 `AGENTS.md`。根据当前源码目录、接口、CMake、测试与已批准规格，建立可被后续 Agent 直接使用的分层职责、不变量和验证入口。

只新增模块协作说明，并为根约定增加导航、为 specs 增加本次维护记录与审查证据。不修改生产/测试/示例代码、构建配置、第三方资产或 `.reference-repos/`；不改变核心交付或 Trigger 重构的状态。

## 验收与任务

| 工作 | Owner | 状态 |
| --- | --- | --- |
| 根约定、目录、当前授权与构建测试入口核对 | root / module authors | 已核对 |
| 七个 src 模块、公共头文件及生成代码协作说明 | root / module authors | 完成 |
| 测试、七个示例、构建、依赖、docs/specs 说明及导航 | root / module authors | 完成 |
| 链接/围栏/覆盖/修改范围检查 | root | 通过；外部并行工作变动单独记录 |
| 一轮并行 fresh-context 全局文档审查与合理发现处置 | independent reviewers / root | 两位均通过，无需整改的发现 |

验收条件：现有七个生产模块和七个示例均有目录内指南；其余维护目录有适用说明；规则继承根约定；源码路径、接口/测试名和命令有当前文件依据；不误写当前授权或历史验证；链接有效；完成一次覆盖全部新增指南的并行独立审查并记录处置。

## 决策与当前状态

- 公共规则由根文件继承，子文件重点解释职责、契约与定向验证，避免每个目录复制完整规则。
- SQL generated 单设说明以保留生成器/runtime 版本与再生成边界；第三方目录只添加协作说明，保持所有既有发布资产与 hash 清单原样。
- `trigger-api-ergonomics.md` 文末最新授权与公开头文件共同确认当前 typed Pattern/静态 Function 范围；保留动态扩展与 Kafka Compose 后置决定。
- 本次为文档维护，验证以覆盖、链接、源码/CMake 对照和独立审查为主，不运行无关产品构建，不复用历史测试通过数。

## Changed files

根 `AGENTS.md`；`src/AGENTS.md` 及七个模块指南、`src/sql/generated/AGENTS.md`；`include/eventflow/AGENTS.md`；`tests/AGENTS.md`；`examples/AGENTS.md` 及七个示例指南；`cmake/AGENTS.md`、`cpp3rdlib/AGENTS.md`、`docs/AGENTS.md`、`specs/AGENTS.md`；`specs/INDEX.md`、本文及本轮 `specs/reviews/` 报告。

## 验证与下一步

编辑前已记录 627 个既有文件的 SHA256，供检查本次修改范围；临时基线位于 `/tmp/eventflow-agents-init-a7e31zah/baseline.json`，临时文件不作为长期证据。

首次静态检查：根文件加 23 份新增指南共 24 份；连同本文和 INDEX 检查 26 份 Markdown，170 处本地链接及 anchor、20 个 ASCII 代码块、围栏闭合和行尾空白检查通过。根目录既有规则保留为原字节前缀。

修改范围检查发现工作区存在外部并行变更：`examples/feature_computation_demo/feature_computation_demo_test.cpp`、`CMakeLists.txt`、根 `README.md`、`specs/trigger-api-ergonomics.md` 相对基线发生变化。这些文件不在本次作者修改范围，已保留并重新读取相关事实；因此不宣称整个工作区只有文档变化，也不回滚其他任务。

最终验证：24 份指南内部 146 处本地链接及 anchor 有效，20 个代码块均为 ASCII 且围栏闭合，37 行命令代码通过 `bash -n` 语法检查；19 条 CTest 正则全部命中当前 CMake 注册的目标，文档具名测试均能定位。这仅证明文档入口有效，没有执行其中的构建、CTest 或服务命令。

按相对路径排序并拼接 `path + NUL + file_bytes + NUL` 得到全部 24 份指南的 SHA256：`377cde191d41c4ea11e895dcb70a2dc374fa3228a0f8cdb9a399f2aa45f2ddd8`。作者最终核对与独立工程审查快照一致。七个源模块、七个示例和其余维护目录的覆盖要求均满足。

## 独立审查与处置

两位未参与编写的 reviewer 以 fresh context 在同一轮并行检查全部指南，只开展本次文档范围的全局审查。

| 报告 | 结果与处置 | 证据边界 |
| --- | --- | --- |
| [契约审查](reviews/MODULE-guidance-contract-review.md) | 通过；无需要整改的发现 | 核对根规则、最新授权、当前接口/实现、固定输入、UNKNOWN、checkpoint、范围和导航 |
| [工程可用性审查](reviews/MODULE-guidance-usability-review.md) | 通过；无需要整改的发现；报告内根导航行号已由 reviewer 定向修正 | 独立链接/目标/脚本核对，七个 run.sh 可执行且语法通过，第三方 manifest 的 345 项 hash 一致 |

作者自查曾将服务指南的“KV 总数”细化为“KV 物理 shard 总数”，消除把 S+1 误读为进程数量的歧义；两份审查均包含这一最终措辞。未追加审查轮次，也未把文档审查当作新产品验收。

## 交接

本次初始化完成，无剩余文档整改项。根 [AGENTS.md](../AGENTS.md#module-guidance) 提供总导航；后续修改对应 API、脚本或模块契约时同步该目录指南。

当前工作区仍有其他任务的并行代码变更，本任务未执行或声称 C++ 构建、CTest、sanitizer、真实 broker 或恢复验证通过。Trigger 重构与其他产品状态继续由其原规格维护，不由本次完成状态覆盖。
