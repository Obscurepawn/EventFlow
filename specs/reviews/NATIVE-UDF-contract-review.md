# Native UDF 首次部署独立合同审查

Reviewer: native_udf_contract_review（独立 fresh-context agent）  
Date: 2026-09-16  
Round: 本次生产 Native UDF 交付的单轮并行全局审查  
Status: 审查完成；未发现额外阻断项，实施者自查修复及最终交付门禁仍需其定向证据

## 审查范围与结论

按 [本轮授权](../native-udf-delivery.md) 和 [控制面合同](../control-plane.md) 审查当前工作树：服务先启动，SDK 上传原生库、提交定义、开始首次部署；受控构建环境下直接传递 C++ 对象，使用已有 Compute 函数线程池。未把运行中追加计算图、独立 Worker、不可信代码隔离或完整 UDTF/UDAF 作为本轮要求。

已阅读根、公开头文件、源码、测试和 specs 约定，审查生产加载器、制品身份、结构化 codec/binder、SDK、Coordinator/Compute 接入、启动配置、CMake、测试、真实示例和恢复脚本。没有修改生产实现或测试，也没有运行并发项目构建。按 develop-with-specs 保存本报告，主规格的最终状态由实施者维护。

在实际检查范围内，**没有确认新的实质正确性缺陷**。当前代码具备首次部署所需链路；是否最终完成仍需实施者修复其已识别边界并绑定最终源码运行质量门禁，不能仅以本报告或历史绿灯替代。

## 合同追踪

| 合同 | 当前实现与证据 |
| --- | --- |
| 制品与不可变版本 | `UdfLibrary::open` 校验 ELF、大小、构建身份、入口与 FunctionRegistry 签名；SHA-256 进入 implementation identity。Coordinator 以 ID/version 检查同字节幂等与异字节冲突。`udf_test` 覆盖空串、abc、百万 a 的摘要向量及真实 `.so`。 |
| 库与调用生命周期 | 加载器使用封存 memfd 和 `RTLD_LOCAL`；注册及动态绑定回调捕获 `shared_ptr<UdfLibrary>`。克隆 Registry 后释放原引用仍可执行，最后销毁克隆与 Executor 后弱引用到期。该测试证明对象持有生命周期，不声称验证了所有动态链接器实际映射回收行为。 |
| 原子元数据与 UNKNOWN | `CoordinatorService::Impl::save` 将整体 Meta 记录与 artifact mutation 放进同一 control-shard CommitBatch；`resolve` 始终重试原 pending batch，确认 revision 后才发布 state，图指纹变化时重建 Catalog。所有 handle 先 resolve，未确认时不能越过该批提交新图。新增 UNKNOWN 测试检查重试只增加一次 revision，KV bytes 与原库一致，registry 只有一个引用。 |
| 首次部署与单图 | register/submit 只允许明确启用且 STAGING；submit 先完成 decode、加载和完整 bind，再保存新 staging 图。已部署后拒绝改图；已有静态模块和 SQL 路径保留。Compute 核对逻辑 shard_count、recipe、DDL 和最终 Catalog fingerprint，已有部署身份不允许替换。 |
| 函数与固定输入 | 动态 binder 仅接受一个 required、pure 输入，按 ID/version/digest 找到精确库；字段 projection 检查可见性和类型，必需字段为 NULL 时 NotReady。后续复用 `TriggerSet::bind`、生成请求关系和已有 RequestRuntime；capture/session 业务 key 形状由已有计算绑定器校验。 |
| Source、Clock 与 session | 新定义只开放输入表事件或 Clock、typed 列/常量 AND 条件、显式有限 candidate session。source 回调只在不存在/失活时开启 epoch，检查 deadline 加法溢出；Clock 复用已有 candidate/request 准入，不能自己开启 session。定义规范摘要进入 Trigger implementation identity，因此 session 参数等变化进入恢复图身份。 |
| 预算 | 库 1 MiB、登记 16 项、定义 256 KiB/64 DDL/16 Trigger、capture 64 字段、condition 16 项；签名另走 FunctionRegistry 数量/字段/字节约束，提交检查实际 KV batch/read 容量。Compute 保留已有每节点 cache 分配和线程执行器预算。受信任本地代码自身的任意内存分配并非沙箱预算。 |
| checkpoint 与恢复 | 元数据和库在共享 KV control shard，既有全局冻结/snapshot 协议保留。恢复按存储引用读取字节并重验摘要/ABI；Compute 无需本地库路径。示例从 Rocks snapshot 创建新目录，原活动库保留；四个数据库进程重启，HTTP 源进程保留其消息日志。 |

## 实际验证

独立执行：

```sh
ctest --test-dir build --output-on-failure -R '^(udf_test|submitted_computation_test)$'
```

结果：2/2 通过，7.37 秒。CTest 自带 ASan/UBSan/LSan 失败配置。此轮测试运行在实施者随后边界修复之前，不将它当作那些后续修复的验证。

另外直接阅读 `/tmp/eventflow-native-e2e.log` 与示例实现，核对真实五进程证据：两个 key 明确路由到不同 Compute/shard；首次结果 51.5、128；无消息仍有 Clock 新请求；第三条输入使周期结果为 53，阈值结果及固定输入仍保持 51.5；checkpoint 后从新 Rocks 目录重启，客户端传入不存在的 UDF 路径且走不注册分支，新增输入得到 54.75；全部子进程退出 0。该端到端是实施者运行、本 reviewer 阅读并对照代码的证据，不冒充独立重跑。

审查期间读取的源码 SHA-256：

```text
src/runtime/udf.cpp c49e4411963f61c3a738c75c1dee302568468467e8938378bc7742a5a0ea3644
src/runtime/submitted_computation.cpp 7c4366bde12d7deeb06840e45491b8d5e7a01921ce6dbbb7b513aee08e380175
src/service/coordinator_service.cpp 23f12a42e4d77716677be9f75ac9db93fe055af93948c7c30adc68ebd09842df
src/service/compute_service.cpp 11a41df6a9642871c0cb7f5046fc2ec0848d761322c239c5863081bfbd30d746
```

## 实施者自查处置与未验证项

以下三项由 root 主动告知，不是本 reviewer 独立发现：直接 C++ 提交在 JSON dump 前拒绝非法 UTF-8；FunctionRef 使用完整 uint64 version 解析；额外 artifact mutation 预检 KV max_mutations，避免容量拒绝使 Meta 进入 failed。报告写入时处于修复/定向验证阶段，实施者应在本轮交付证据中记录结果。

未独立执行完整 Debug/Release、安装目录运行或另一轮真实集群；这些由本轮实施者并行负责。没有新增对损坏 KV artifact、混合 ABI Compute、submit UNKNOWN、session 到期后新 epoch，以及多个不同库版本同时驻留的故障探针；源码有对应的拒绝/复用机制，但本报告不将其描述成已逐项运行通过。

现有恢复证据不包括 HTTP 消息服务自身宕机后日志恢复，也不包括 Kafka broker、跨构建 ABI 迁移、保留物理在途函数跨 checkpoint 或运行中图迁移。这些边界与当前明确授权一致，不新增为本轮阻断门槛。

## 实施者最终定向处置

三项自查均有失败复现与修复证据：非法 UTF-8 导致原 JSON dump 终止（`/tmp/eventflow-native-boundary-red.log`，exit=134）；UInt64 最大版本和低 mutation 预算用例失败（`/tmp/eventflow-native-boundary-other-red.log`）。修复前置 UTF-8 校验、非抛异常的 decoder 长度核算和字段验证、UInt64 精确解析、提交前 mutation 预算检查后，`/tmp/eventflow-native-codec-final.log` 的三个相关 CTest 目标全部通过（含扩展边界用例）。`/tmp/eventflow-native-unknown.log` 单独验证注册 UNKNOWN 重试不重复增加 control revision，制品与注册记录均保留。

上述处置不改变首次部署范围，不重新发起审查。最终全量、Release/安装及源码身份见主规格。
