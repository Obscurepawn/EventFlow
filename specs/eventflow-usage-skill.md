# 面向使用者 Coding Agent 的 EventFlow skill

Status: done
Owner: root

## 目标与边界

用户要求在 .skills 中新增一个给其他 Coding Agent 使用 EventFlow 的 skill，说明现有 API、接口语义和常见 Feature Computation demo，并在 README 给 reviewer 明确使用入口。skill 面向应用接入，不要求使用者研发 EventFlow 内核，也不把后置产品能力写成可用 API。

## 交付结构与分工

- .skills/use-eventflow/SKILL.md：简短入口、接入选择、关键约束与按需参考导航；root。
- references/relational-api.md 与 trigger-api.md：Catalog/SQL、强类型 API、Trigger/Pattern/capture/session/请求与执行合同；api agent。
- references/recipes.md：贴出常用业务代码和执行路径，依托现有可运行 examples；recipes agent。
- references/cluster-and-operations.md：SDK 全方法语义、HTTP、输入确认、消息源、checkpoint 与构建运行；root。
- agents/openai.yaml：可选 UI 元数据；README：跨 Coding Agent 的显式读取提示词，不假定 .skills 会被所有工具自动发现。

## 验收

所有示例方法与限制对照当前公开头文件/实现。独立启动程序和依赖完整示例的节选须明确区别，不虚构 SDK 动态 Trigger 上传。校验 skill frontmatter/元数据、链接和代码围栏；运行采用的现有例子并让一位 fresh-context agent 从 skill 出发在临时目录完成真实接入练习，记录本轮发现和整改。仅文档/skill，无生产代码或构建系统修改，无重新安装全局 skill、镜像重构或全量系统验收。

## 进度

| 工作 | 状态 |
| --- | --- |
| skill 入口与 UI 元数据 | 已实现，quick_validate 与元数据检查通过 |
| API 语义参考 | 已实现并与当前声明/源码核对 |
| 常见计算示例 | 已实现，七个对应例子均退出 0 |
| README 导航 | 已完成，包含可复制的显式读取提示词 |
| 技能校验与独立使用验证 | 一轮独立使用审查、Debug 三 sanitizer 与 Release 均完成 |

## 实施记录

入口54行，详细材料按需读取：关系/执行 API、Trigger/typed/session/capture API、SDK/HTTP/消息源/存储/恢复/外部 CMake 接入、五类业务 recipes。示例节选明确列出依赖对象并链接完整可运行文件。既有 SQL 示例中的非 Committed 简化分类在 skill 中区分 Rejected 与 Unknown，不把真实拒绝标为不确定提交。

已运行 quick_validate.py、六个 skill 文件的 Markdown/ASCII/相对路径与页内锚点检查、openai.yaml 元数据检查。skill_usage_review 在 /tmp 独立工程按 skill 创建“成交量阈值＋五秒周期”的新消费程序，使用独立 CMake/Clang 三 sanitizer 构建；不修改仓库源码或共享构建树。其他当前任务状态保持原样，默认 Trigger 审计关系仍排队等待本 skill 完成。

Changed files: .skills/use-eventflow/SKILL.md、agents/openai.yaml、references/{relational-api,trigger-api,cluster-and-operations,recipes}.md，README.md，本规格及 INDEX.md。独立报告和实际结果将在完成后追加。

## 最终验证与交付

[独立使用报告](reviews/SKILL-usage-review.md)完成一轮 fresh-context 接入验证。唯一 skill 发现为外部 CMake 模板未在父工程启用编译数据库，已添加 CMAKE_EXPORT_COMPILE_COMMANDS ON 与 CMAKE_CXX_EXTENSIONS OFF；重新配置后消费者 main.cpp 包含在数据库中，使用严格 C++20，构建运行通过。消费者自身的一处 optional<Row> 初始化笔误由 reviewer 修复，不计为产品缺陷。

独立消费者位于 /tmp/eventflow-skill-usage-yfq8Fx，验证总量 100 不满足 >100 条件、110 满足一次条件、5/10/15 秒的三个周期结果为 100/110/135，同时核对固定输入、实际请求版本、Latest 身份、源 next_offset=4。Debug 由 reviewer 用三 sanitizer 运行；root 用同一冻结源码在独立 build-release 构建运行，O3/NDEBUG/ThinLTO、无 sanitizer，两种构建输出逐字相同且无编译警告。Release 日志为 /tmp/eventflow-skill-consumer-release-{configure,build,run}.log。

七个现有例子（SQL、news、typed Pattern、engine Clock、SDK Clock、condition、checkpoint）使用既有 Debug 二进制重新执行 run.sh，全部退出0；记录 /tmp/eventflow-skill-existing-demo-results.json。此处不宣称重新跑全量系统测试或新建 Kafka 集成。

最终 quick_validate、skill 引用路径/锚点/围栏/ASCII 与 UI 元数据均通过。README 已有 skill 链接、包含业务需求的可复制提示词，并明确 .skills 不是所有 Agent 通用的自动发现目录。没有全局安装、没有修改生产代码/依赖/构建系统，镜像与其他 active 任务未改动。当前 skill 工作已完成，接下来执行已授权的默认 Trigger 审计关系，并同步新增的使用语义。
