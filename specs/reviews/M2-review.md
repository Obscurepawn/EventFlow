# M2 完成记录

日期：2026-09-14。范围：有限SQL parser/binder/Catalog、表达式、可撤回聚合、连续MV/lookup join、查询/初始化、同shard staging与UNKNOWN。窗口/FSM、请求计算和多进程部署属于后续阶段。

## 审查闭环

- 原runtime fresh审查的重入、同轮图反馈、raw状态旁路、计划来源和emit预算五项问题，均已修复并由独立reviewer复验关闭，见[M2整改复核](M2-runtime-remediation-review.md)。reviewer亲自运行54项review回归和48项执行合约。
- SQL/Binder审查指出的公开snapshot/plan合法性和来源问题，已在Catalog与Engine入口修复，19项Catalog测试中有明确回归。该SQL reviewer后续一次工具turn被平台中断，未将中断计为PASS；结论依据实际实现、回归和独立runtime/查询入口复核。
- 当时留下的逐查询Catalog深克隆成本偏差，已由可信Catalog复用和内部有界query cache关闭。[独立查询缓存审查](M3-query-cache-component-review.md)实际包装并统计`Catalog::from_snapshot`调用：稳定Catalog上的SQL miss/hit不重复clone，DDL后仅首次重建一次；公开BoundPlan仍强校验。该复核28/28通过。

## 验证证据

- Clang Debug ASan/UBSan/LSan主构建，M2原10个CTest/289项顶层GoogleTest通过；snake_case重命名后重跑通过，日志`/tmp/eventflow-snake-clang-test.log`。
- 查询缓存接入后的完整主构建17/17 CTest通过（44.12秒），包含所有M2回归与新M3组件，日志`/tmp/eventflow-m3-main-test.log`。后续M4元数据适配的matching build中全部M2相关测试继续通过；M3新增测试的一处非法limit用法单独修正，不冒充M2失败或生产缺陷。
- Bear全量捕获36个当前Clang TU，后续组件增量捕获扩展到39个；所有命令带`-fsanitize=address,undefined,leak`。`clangd --check=src/main.cpp`实际报告0 errors。数据库会随新增TU继续刷新，历史11条/GCC记录已过时。
- Clang Release重新构建成功，实际flags为`-O3 -DNDEBUG -flto=thin`，无sanitizer、无gtest链接。SQL demo实际输出插入后volume=120，撤回后阈值视图为空。日志`/tmp/eventflow-m3-release.log`与`/tmp/eventflow-m3-release-demo.log`。

M2范围已完成。此记录不表示完整项目或M3/M4/M5已完成；后续修改必须继续跑相应回归，最终仍需多位fresh-context全局reviewer/challenger。
