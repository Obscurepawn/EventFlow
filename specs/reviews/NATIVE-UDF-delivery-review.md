# 生产原生 UDF 交付独立审查

Date: 2026-09-16
Reviewer: native_udf_delivery_review（独立 fresh-context 全局 reviewer）
Status: reviewed; 一项文档发现待实施者处置，最终质量门禁待汇总

## 范围与结论

依据根目录及相关模块 AGENTS、[当前规格](../native-udf-delivery.md)，检查生产加载器、公开 SDK/提交定义、Coordinator 注册与持久化、Compute 安装、Trigger 接口增量、构建/安装、新旧 UDF 示例及对应测试。本轮要求是服务启动后上传 UDF、提交完整定义、首次 deploy 开始计算；运行中改图、隔离 Worker 和完整 UDTF/UDAF 家族不属于验收范围。

所审源码与已有端到端证据支持该有限切片，未发现需要阻止交付的生产正确性问题。此结论不替代正在运行的完整 Debug 回归、Release 构建与安装运行证据，也不代表任意第三方 C++ ABI 或不可信插件可安全运行。

## 发现

### D1 / 低：测试模块指南仍排除本轮已授权的动态 UDF 验收

`tests/AGENTS.md` 的“新的强类型静态 Function 已有实现与测试；动态 UDF/UDTF/UDAF、Process Function 不作为本轮测试完成门槛”仍是原静态阶段的表述。当前规格已经把有限生产原生 UDF 纳入验收，新加载器/提交定义/真实集群恢复测试也已加入；该表述会误导后续维护者跳过新接口回归。

建议区分“本轮受控 ABI 原生 UDF 切片”与“完整扩展函数族、Process Function 后置”，并链接当前规格。未修改该指南，交由实施者处置。

## 已核对的关键契约

- `src/runtime/udf.cpp` 从受限 ELF 字节建立封存 memfd，使用 RTLD_LOCAL/RTLD_NOW 加载；检查构建标识、入口和 Registry 签名。文件描述符与 dlopen handle 由 RAII 管理，回调捕获 shared_ptr 保持库存活。SHA-256 进入 implementation identity。
- 构建标识包含 SDK 相关头文件摘要、Clang/目标/构建选项和标准库 ABI；文档明确同一受控构建、可信进程内执行。检查发生在 dlopen 后，因此不是沙箱或任意二进制安全校验；示例文档已有明确说明。
- `register_udf` 只在显式启用的首次部署前接受上传；不可变 ID/version 的重试返回相同引用，换字节拒绝。元数据与原始库字节由 `save` 同批写入 control shard，`resolve` 确认原 pending batch 后发布状态。已有专门 UNKNOWN 注册测试。
- `submit_computation` 先对完整结构进行 codec/Schema/函数引用绑定，再保存定义与 Catalog 指纹；失败不发布部分 staging Catalog。Clock、条件 AND、固定当前字段 capture 和有限 session 复用已有生命周期与请求执行机制。
- Coordinator/Compute 恢复和安装均从 KV 读取注册制品并校验摘要，未按客户端文件路径定位。Compute 使用通用服务入口、既有 ComputationRuntime/线程池；生产 Server 没有静态链接新闻业务函数或提交定义。
- 新 Trigger 当前字段 capture 校验字段数量、可见性和类型；缺行或必需字段 NULL 返回 NotReady。函数回调持有 library，复用已有 Registry/执行器的输入输出校验与资源合同。
- 新测试目标在 BUILD_TESTING 分支使用现有 GoogleTest；Debug 配置三 sanitizer，Release 保持 O3、NDEBUG 和支持时 LTO。新增代码没有第三方实现下载或新构建系统。
- 新集群脚本支持源码与安装目录，等待真实进程 READY，保留退出状态与恢复前日志；恢复时从 snapshot 创建新 Rocks 活动目录，重新启动四个数据库进程，消息服务保留原日志。

## 实际证据

Reviewer 自行执行：

```sh
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
LSAN_OPTIONS=exitcode=23 ./build/bin/udf_test
bash -n examples/native_udf/run.sh examples/cpp_udf/run.sh
```

- `udf_test`：4/4 通过，退出 0；包括真实共享库、错误构建/缺失入口、摘要向量、Registry clone 后库生命周期与真实调用。
- 两份脚本语法检查退出 0。

Reviewer 阅读实施者已完成的 `/tmp/eventflow-native-e2e.log`，并核对 `examples/native_udf/verify.cpp` 与 `run.sh` 的断言和进程流程：

- 首轮五进程全部就绪后，SDK 上传两次、提交定义、首次部署；部署前两个 partition 均未 ACK。
- 两个 Compute shard 的阈值结果 51.5/128；第三条新闻后周期结果 53，固定捕获输入与阈值结果保持不变；无新消息仍产生成功 Clock 请求。
- checkpoint 完成，四个数据库进程退出 0 并从 Rocks snapshot 重启；恢复客户端使用不存在的本地库路径且不注册/提交，新增输入结果 54.75，原阈值 51.5 保持。
- 最终五个服务退出 0。该次运行是实施者执行的完整日志证据，不标为 reviewer 独立重跑。

## 未验证项与交接

- 审查时 `/tmp/eventflow-native-full-ctest.log` 与 `/tmp/eventflow-native-release-build.log` 仍在执行，尚无最终通过结论；实施者需汇总完成结果、Release/安装实跑及最终格式/静态约束检查。
- 未独立注入持久 KV 制品损坏、动态库运行中故障或任意依赖缺失；源码有摘要/加载失败返回路径，本报告不把源码阅读当作这些故障已实测。
- 没有新增性能、恶意插件安全、跨工具链 ABI、消息服务重启恢复或真实 Kafka 验证承诺。
- 只新增本报告，未编辑实现、测试或主规格状态。实施者可在本报告后追加 D1 处置与定向验证，无需自动发起第二轮 fresh-context 审查。

## 实施者处置

D1 已修正：tests/AGENTS.md 现链接生产 UDF 规格，并区分本轮原生切片与完整 UDTF/UDAF、Process Function、运行中图迁移。定向加载器/提交定义/本地示例 3/3 通过，注册 UNKNOWN 用例单独通过；完整回归、Release 和安装证据由 specs/native-udf-delivery.md 最终汇总。本轮不新增独立审查轮次。
