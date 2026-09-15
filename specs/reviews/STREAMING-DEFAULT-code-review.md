# 默认上传示例：独立代码与交付审查

日期：2026-09-16。Reviewer：streaming_default_review，独立 fresh-context 全局审查，与另一 reviewer 同轮并行。

## 范围与方法

按根目录、examples 及三个示例目录、specs 的 AGENTS.md 和 [本轮规格](../streaming-udf-default.md)检查默认上传流程、静态参考保留、兼容入口、构建安装、README 与导航。采用 develop-with-specs 记录原发现及证据；不修改实施文件。

当前 Git 工作树还包含前轮生产 UDF 实现，因此使用 `/tmp/eventflow-streaming-reorganize-zql2hhwv/{streaming_database,native_udf}` 作为本轮迁移前基线，避免将前轮改动误判为此次迁移。另抽查当前生产加载路径，验证 README 对分发方式的描述。

## 原始发现

### R1 · P2：根 README 审计说明仍引用静态流程的对象和 Clock 名称

根 README 的“5.1 SELECT 当前特征和触发记录”仍让读者调用 `bound.explain()["trigger_audit"]["scope_columns"]`。新的完整上传流程只有 `submit_computation` 返回的 JSON（示例片段变量为 `staged`），并没有本地 `bound` 对象。其审计字段表还把周期事件名写为 `news_clock`；当前 `src/runtime/submitted_computation.cpp` 为 Clock 生成 `item.name + "_clock"`，实际为 `periodic_impact_clock`。

影响：读者照新的 SDK 主流程查解释或对照 SQL 审计行，会遇到未定义对象或不匹配的事件名。应使用 SDK 返回的 `explanation`，并将周期事件名与当前 submitted 定义一致。

处置：待实施者修正并定向文字/接口核对；不需要重跑端到端测试。

### R2 · P2：使用 skill 的默认选择仍引导静态编译

`.skills/use-eventflow/SKILL.md` 的完整应用选择表和 Build step 2/5 默认要求 `static_streaming` / 静态 `ComputationModule`；`references/recipes.md` 开头仍写“优先复用”，链接标签为 `streaming_database`，实际指向 `static_streaming`。其他 reference 也保留了相同的标签与目标不一致。

影响：用户已将上传 UDF 指定为主要用法，但 Coding Agent 通过该现有使用入口仍会生成静态 Server 业务模块；文末一句说明上传可用不足以改变前面的操作步骤。应在 skill 的入口和默认步骤采用上传流程，并明确静态专属 reference 的名称和用途。静态技术说明本身可以保留。

处置：待实施者同步默认入口与静态标签，并检查 reference 链接。

## 已核实的结果

- 原 native_udf 的 definition.hpp/definition.cpp/verify.cpp/run.sh 与迁移后的 streaming_database 完全相同；原消息服务源码保留在 streaming_database，两个完整示例共用。
- 原静态 definition.hpp/definition.cpp/main.cpp 完整保留。verify.cpp 仅调整 Usage 二进制名；run.sh 仅调整对应目标与二进制名。没有删减业务验证。
- CMake 的通用 `eventflow` 不链接新闻业务定义；上传验证器使用 streaming_database 源码，`.so` 仍由 `eventflow_news_udf` 单独生成。静态目标明确使用 static_streaming。
- `native_udf/run.sh` 通过 `exec bash .../streaming_database/run.sh "$@"` 保留参数、环境与退出码；没有重复端到端 CTest。安装包含默认、静态和转发脚本以及共享库。
- 独立执行三个 run.sh 的 `bash -n` 通过。检查根 README 和三个示例的 7 个 Markdown 文件，未发现本地链接目标缺失。
- 抽查生产代码确认：Coordinator 同批保存注册元数据和 UDF 字节；Compute 的绑定路径按摘要从 KV 读取并检查，随后使用 UdfLibrary 加载。默认例与恢复调用维持首次部署上传、恢复不重新上传的边界。
- 根 README 的主要函数实现、定义片段、完整运行流程和源码入口已使用上传版；除 R1 外未发现主流程残留静态函数编译步骤。

## 验证证据与限制

本 reviewer 阅读本轮实际日志，没有再次启动完整构建或重复 CTest：

- `/tmp/eventflow-streaming-default-ctest.log`：streaming_database_example、static_streaming_example、udf_test、submitted_computation_test 共 4/4 通过，50.12 秒。
- `/tmp/eventflow-streaming-default-install.log`：Release 安装及安装目录实跑，包含两个 shard 的 51.5/128、周期 53、恢复 54.75 和固定结果 51.5；最终五个子进程退出码均为 0。
- `/tmp/eventflow-streaming-default-alias.log`：兼容入口完成同一首次部署和 checkpoint 恢复，`no_reupload=true`，五个子进程正常退出。
- 直接检查 build/CMakeCache.txt 和 flags.make：Clang、Debug、address/undefined/leak、禁止 sanitizer 恢复。build-release 对应 Clang、Release、`-O3 -DNDEBUG -flto=thin`，无 sanitizer 或 fast-math。

本轮未独立重跑全核心测试、Kafka、Docker、恶意 UDF 或新恢复故障注入；这些不属于本次示例搬迁新增验证。图像视觉与图中文字由同轮另一 reviewer 检查。以上证据不扩大前轮生产接口或完整产品验收范围。

## 审查结论

示例代码迁移、构建/安装及兼容入口未发现阻塞运行的回归。交付前处理 R1、R2 并记录定向核对结果，即可收敛本轮；无需自动追加 fresh-review 轮次。

## 实施者处置

R1 已修正：根 README 使用 staged["explanation"] 中的审计映射，周期事件名称为 periodic_impact_clock。R2 已修正：use-eventflow 的默认选择与操作步骤改为 streaming_database 上传流程；recipes 先介绍上传版，原内容明确放在静态参考小节，相关链接标签同步为 static_streaming。skill quick_validate 通过，根 README 和引用路径定向检查通过。用户随后要求的 Coding Agent 章节也改为加载该 skill 后直接描述业务，不再附实现/验证清单。本轮不再发起独立审查。
