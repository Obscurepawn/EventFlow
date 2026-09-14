# 源码逻辑分段与注释独立审查

日期：2026-09-14。Reviewer：`source_readability_review`，独立 fresh-context；本次单轮并行全局审查中的可读性与注释审查。范围来自[源码整理规格](../source-spacing.md)，不重新验收产品功能。

## 结论与处置建议

未发现需要整改的阻塞项。新增空行改善了参数校验、局部准备、循环处理、状态发布和返回之间的分段；抽查未发现把表达式、初始化列表或紧密相关的字段赋值拆成逐行空行的情况。24 处、40 行新增注释均已逐项核对，解释了不易从局部语句看出的原因或不变量，没有发现与实现及当前合同相冲突的描述。建议实施者合并另一位 reviewer 的结果、完成规格与证据记录后交付；本报告不代替另一份审查。

## 实际检查与证据

- 阅读根、`src/`、七个源码模块及 `specs/` 的 `AGENTS.md`，并读取 `specs/INDEX.md` 和本任务规格；按 develop-with-specs 工作流将独立结论保存在此处。
- 基线为 `/tmp/eventflow-source-spacing-h_oifo0k/before`。独立使用 Python `difflib.SequenceMatcher(autojunk=False)` 逐文件对照：36 个手写 C++ 文件仅插入 2630 个空行和 40 行独立 `//` 注释，没有删除、替换或其他新增非空行。基线 manifest 共 56 个文件，另外 20 个保持字节一致；其中 generated、grammar、模块指导和 `.clang-format` 均未改变。
- 对 36 个文件均检查代表性新增分段；进一步阅读 Engine 提交/传播、EventProcessor 阶段恢复、RequestMachine 就绪/超时、RequestRuntime 受理/发布、Coordinator checkpoint 和缓存提交路径。短 getter、参数赋值、编解码构造仍保留紧凑语句组，较长路径可按逻辑段阅读。
- 执行 `sha256sum -c /tmp/eventflow-source-spacing-h_oifo0k/reviewed-sources.sha256`，36/36 文件匹配。查看同目录 `verification.json` 及已落盘的[验证记录](../verification/source-readability-validation.md)：实施者记录 token 与 insertion-only 检查通过，37 个手写文件通过 formatter，新增空行边界问题为空。长期源码身份见 [SHA-256 清单](../verification/source-readability-sources.sha256)。本审查的可读性结论另外基于实际源码阅读，不由 token 结果推导。

## 新增注释的语义核对

| 主题与源码 | 实际核对结果 |
| --- | --- |
| [key_codec.cpp](../../src/core/key_codec.cpp) | 大端整数编码前翻转符号位，符合负数排在非负数前的说明。 |
| [message_source.cpp](../../src/ingest/message_source.cpp) | ACK 只推进已完成投递前缀；提交未确认时保留 inflight 与原 token。 |
| [computation.cpp](../../src/runtime/computation.cpp)、[catalog.cpp](../../src/sql/catalog.cpp) | 绑定在 Catalog/registry 副本上完成；隐藏身份列保留 SELECT 未显示的物理身份。 |
| [engine.cpp](../../src/runtime/engine.cpp) | UNKNOWN 保留原批及待发布结果；重试提交原批；Latest 先检验当前 epoch/成功状态，再按受理序号选择；Catalog 依赖顺序支撑本轮传播。 |
| [event_processor.cpp](../../src/runtime/event_processor.cpp) | `make_admission` 的匹配参数存入 pending intent；UNKNOWN 保存 operation 并以原 `target_us` 恢复；到期处理先于同刻数据。并对照 Trigger capture 和事件请求接入合同。 |
| [request_machine.cpp](../../src/runtime/request_machine.cpp)、[request_runtime.cpp](../../src/runtime/request_runtime.cpp) | 依赖完成后冻结参数，重试保留冻结值；`advance` 使用 `<=` 判断 attempt 到期；未确认任务暂存于 pending；capture/admission revision 分别为 R/R+1 且数据和请求状态同批提交。 |
| [coordinator_service.cpp](../../src/service/coordinator_service.cpp) | 转发前持久保存原投递和分配时间；源进度服从显式输入确认；checkpoint 校验源切面并核对 snapshot 中 worker/control revision。 |
| [exact_sum.cpp](../../src/runtime/exact_sum.cpp)、[expression.cpp](../../src/sql/expression.cpp) | 整数 limb 按最小 binary64 单位精确累积并在读取时舍入；AND/OR 遵循决定性短路和 NULL 三值规则。 |
| [cached_store.cpp](../../src/storage/cached_store.cpp)、[state_store.cpp](../../src/storage/state_store.cpp) | 清理缓存不清除 pending 身份；Rocks 数据和 receipt 同一 WriteBatch，确认写入保持 WAL 且 `sync=true`。 |

## 未验证边界

未重新构建、运行单元测试、sanitizer、真实进程或 Kafka；本轮是插入空行与注释的维护，以上结果不构成新的运行时正确性证明。空行可读性按覆盖所有修改文件的代表性位置抽查，未宣称人工逐一审美评判全部 2630 处。manifest 之外的公开头文件、示例、测试和第三方目录不在本 reviewer 的独立快照比较范围；范围完整性需结合另一位 reviewer 与实施者证据。没有修改生产源码，也没有新增产品完成声明。
