# 未使用图片清理

Status: complete
Owner: root
Updated: 2026-09-16

用户要求删除 docs/image 未使用图片；若全部使用则不改。已扫描仓库（包括隐藏 skill 和 specs）的文件名引用，区分实际链接和历史记录中的纯文件名。

删除以下七张仅被历史审查文字提及、没有实际引用的图片：

- csp_event_execution_flow.png
- eventflow_architecture.png
- incremental_shared_computation_graph.png
- materialize_incremental_query_flow.png
- risingwave_query_dataflow.png
- risingwave_temporal_join.png
- timely_worker_communication_flow.png

其余 29 张保留；eventflow_architecture_overview.png 虽未被读者正文引用，但仍有审查文档的实际链接，因此保留。历史记录不改写。

已在仓库外备份删除图片；文件列表、原大小与 SHA-256 在 /tmp/eventflow-unused-images-manifest.json。删除后所有维护中的图片链接已核对；同轮两位独立引用审查完成。不改生产代码或其他既有工作树变更。


## 审查与证据

- image_cleanup_refs 与 image_cleanup_scope 两位 fresh-context reviewer 独立核对维护中的文档、代码、隐藏 skill 和 specs：均同意删除所列七张，没有遗漏的实际链接。
- image_cleanup_refs 进一步扫描被忽略文件，发现 specs/verification/reader-eventflow-image-prompts.json 与 reader-image-prompts.json 保存了其中三张旧图的生成参考路径。这些是历史生成记录，不是当前使用入口；原路径随图片删除失效，不据此声称全仓连历史提及都不存在。
- 删除后的维护中文档图片链接全部存在；未改写历史审查记录。未运行产品构建/测试，本次只有静态图片清理。

删除总计 1026517 字节，备份位于 `/tmp/eventflow-unused-images-jzskxq50`。清理完成，保留 29 张图片。
