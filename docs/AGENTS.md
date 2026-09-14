# 读者文档协作约定

本目录继承[根约定](../AGENTS.md)。正文使用中文，API 标识保留英文；示例代码、注释、日志和可执行命令使用英文及 ASCII。

- [README](README.md) 是文档导航，根 README 是使用入口，`core_delivery.md` 解释进程部署与运行，`feature_computation_architecture.md` 解释产品架构，`research.md` 保存调研。技术状态和验收进度放在 [specs](../specs/AGENTS.md)，不散落成多份互相冲突的当前状态。
- 解释实际 API 前读取当前 `include/eventflow/` 和相关实现；设计提案、历史调研与当前能力必须明确区分。新导航链接必须确认目标存在，不能因旧文档引用过就假设文件仍存在。
- 当前 Trigger/Pattern/静态函数范围以 [Trigger API 规格](../specs/trigger-api-ergonomics.md)的最新授权记录为准。旧核心验证覆盖原快照，不能证明本轮重构已通过；测试数量、性能和运行包能力只引用对应证据。
- 清楚区分嵌入式自定义计算定义与远程集群的已编译配置、KV snapshot 与一致 checkpoint、协议 mock 与真实 Kafka；保留动态 UDF/UDTF/UDAF、Process Function 等范围边界。
- 架构图应与单 Catalog、单 shard owner、真实 KV/Coordinator/Compute、固定输入与确认协议一致。改动图示时同步相邻解释，图片放在 `image/` 并使用本地相对链接。
- 调研引用需保留来源与适用范围；`.reference-repos/` 只能读取。禁止发布或上传私有题面 PDF、私有材料或包含其内容的附件。
- 修改标题时同步目录文字和 anchor；变更 API 示例时同步相应 examples README，且只展示实际存在的入口。不要把待实现 API 代码块写成可运行说明。

文档修改完成后核对受影响文件的相对链接、标题 anchor、代码围栏和命令路径。只有实际运行过的命令才登记为通过；纯叙述或链接编辑不需要编造构建/测试证据。
