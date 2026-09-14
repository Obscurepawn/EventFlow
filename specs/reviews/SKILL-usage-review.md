# EventFlow 使用 skill：独立接入审查

Status: complete for the scoped independent usage review
Reviewer: skill_usage_review agent, fresh context
Date: 2026-09-14

## 范围与结论

从 `.skills/use-eventflow/SKILL.md` 开始，以其他使用者的 Coding Agent 身份，实现 README 中相同类型的真实接入任务：按 ticker 累计成交量，维护级联阈值 MV，在总量严格大于 100 时计算，并每五秒计算当前特征。检查原始要求、skill 的接入选择、当前公开 API、引用示例、外部 CMake 模板、README 可发现入口和运行证据。

**结论：独立消费者成功构建并运行；一个外部 CMake 编译数据库问题已修复并定向复验，无本范围内剩余阻塞发现。** 本结论不代表 EventFlow 全产品或后置扩展家族的新一轮验收。

本 reviewer 的唯一仓库写入为本报告。业务代码、构建树和日志全部位于新建的 `/tmp/eventflow-skill-usage-yfq8Fx/`；未写入或使用仓库共享构建树，未修改生产源码、依赖和 `.reference-repos/`。没有派生其他 reviewer，没有重复 fresh-context 审查轮次。

## 真实消费者与业务选择

临时目录内提供完整 [main.cpp](/tmp/eventflow-skill-usage-yfq8Fx/main.cpp)、[CMakeLists.txt](/tmp/eventflow-skill-usage-yfq8Fx/CMakeLists.txt) 和中文 [README.md](/tmp/eventflow-skill-usage-yfq8Fx/README.md)。CMake 文件直接提取自 `cluster-and-operations.md` 的完整模板，主程序按 `recipes.md` 的聚合例子与 `engine_clock_trigger_demo/main.cpp` 的完整 owner/executor 循环组合而成。

- 采用 embedded C++、`MemoryStateStore`、`SimpleFunctionExecutor`、一个 shard；时钟使用显式处理时间，验证有限的三个周期。
- `trades(ticker, trade_id, quantity)` 的主键为 `(ticker, trade_id)`，分区键为 `ticker`；输入的 schema 顺序、物理身份、scope 和 source offset 分别构造。
- `trade_totals` 对 ticker 执行 `SUM(quantity)`；后继 `volume_threshold` 计算 `total_volume > 100 AS ready`。
- 条件触发器读取稳定后的 `ready`，本例明确选择在同一 session 内只受理一次，即 `once_per_candidate=true`。这不是隐含的 false-to-true 边沿规则。
- Clock 使用五秒周期、`TryOnceAtFiring` 和 `once_per_candidate=false`；周期计算包括阈值不满足时的当前特征。
- Session 从首次输入激活，截止时间为显式的 60,000,000 微秒；测试只推进到 15,000,000 微秒。
- typed capture 通过 `std::optional` 匹配 `SUM` 及其比较结果的 nullable schema，再检查并转成非空函数输入。函数返回捕获的成交量和阈值状态。
- 对每个结果先读绑定返回的 generated relation 元数据，再查询结果和 capture root，调用 `read_request`，最后核对 Latest 的真实 request identity。没有构造内部请求 ID 或猜测 generated relation 名字。

| 时间与输入 | 当前级联 MV | 条件请求 | 周期请求 |
| --- | --- | --- | --- |
| 0.1 秒 +60，0.2 秒 +40 | `100,false` | 0，严格阈值未满足 | 0，首个周期尚未到 |
| 5 秒，只推进时间 | `100,false` | 0 | 新结果 100 |
| 6 秒 +10 | `110,true` | 新结果 110 | 原结果保持 100 |
| 10 秒，只推进时间 | `110,true` | 原结果保持 110 | 新结果 110 |
| 12 秒 +25，15 秒推进时间 | `135,true` | 因 once 策略仍为一个结果，原捕获保持 110 | 第三个结果 135 |

最终 source `next_offset=4`，Clock 的 `last_slot_us=15,000,000`、`next_slot_us=20,000,000`；计时器工作没有消耗 source offset。通过时不仅检查进程退出，还检查上述 MV、请求数量、成功状态、固定输入、candidate epoch、plan、受理时间/顺序、捕获与受理 revision、Latest 所选 ID。

## 构建与运行证据

实际工作目录为 `/tmp/eventflow-skill-usage-yfq8Fx`，下列配置、构建和执行在模板修正后均返回 0：

```sh
cmake -S . -B build -G 'Unix Makefiles' \
  -DEVENTFLOW_ROOT=/home/obscure/EventFlow \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=OFF \
  > configure-final.log 2>&1
cmake --build build --target my_features --parallel 4 > build-final.log 2>&1
env ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
  ./build/my_features > run-final.log 2>&1
```

检测到 Clang 18.1.3，编译器为 `/usr/bin/clang++`。最终消费者编译参数实际包含 `-std=c++20 -fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer`；链接包含 sanitizer flags、`--disable-new-dtags` 和指向 checkout `cpp3rdlib/lib` 的 RPATH。`ldd build/my_features` 没有 `not found`，包括 RocksDB、ANTLR 及其打包依赖。最终构建日志无 warning，运行日志无 sanitizer 诊断；这是该消费者路径的证据，不是第三方库全部内部路径的 sanitizer 覆盖承诺。

最终一次运行与修正模板前的成功运行输出逐字相同。末行如下：

```text
PASS condition_requests=1 periodic_requests=3 source_next_offset=4 next_clock_us=20000000
```

实际请求审计摘要如下。四个请求均为 `Success`、`candidate_epoch=1`；所有捕获都保持原值，`admission_revision=capture_revision+1`。

| 类型 | admitted_at_us | admission_seq | capture_revision | admission_revision | 结果 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 周期 1 | 5000000 | 1 | 6 | 7 | 100 |
| 条件 | 6000000 | 2 | 10 | 11 | 110 |
| 周期 2 | 10000000 | 3 | 14 | 15 | 110 |
| 周期 3 | 15000000 | 4 | 19 | 20 | 135 |

最终查询 revision 为 22，明显不同于各请求的 capture revision。下面两行直接来自执行结果，展示最终 Latest 选择的真实 ID，没有以推测的 ID 代替审计：

```text
LATEST kind=condition request_id=["condition_request",1,"[4294901760,\"034141504c0000\",\"volume_condition\",1,1]"] admission_seq=2 volume=110
LATEST kind=periodic request_id=["periodic_request",1,"[4294901760,\"034141504c0000\",\"periodic_volume\",1,3]"] admission_seq=4 volume=135
```

本地原始证据：`configure.log`、`build.log`、`build-consumer-fix.log`、`run-original-template.log`、`configure-final.log`、`build-final.log`、`run-final.log`、`evidence.txt`。原始模板保存在 `CMakeLists.original.txt`；最后的 compile database 位于 `build/compile_commands.json`。

最终文件 SHA-256：

| 文件 | SHA-256 |
| --- | --- |
| 临时 `main.cpp` | `c5e0c0f9042b27fc37695e2035638fc9d0ff9d165a7d143cc877c6f0bbca8fb9` |
| 临时 `CMakeLists.txt` | `6b01875a446674fbad5f0ae6f936cab3bbbcaa351f0399eecb33e23ad7336063` |
| Debug `build/my_features` | `557bf3f04c60208b0c32db8e76f748c282a9d0fc33c387025d0101c489da5d07` |
| `.skills/use-eventflow/SKILL.md` | `5684a5fdc385fae9f2b14b04d25586ef94ef0142f7b99e2e702096c763025000` |
| `references/cluster-and-operations.md` | `0a03b4ea587e4cfaae7be0310dedb29eb10ec576dd5104f8f5c1b484ff39f726` |

`evidence.txt` 同时保存了另三份 API/recipe 引用文档、所用完整 Clock 示例和关键公开头文件的 hash，以及实际链接命令和动态依赖解析。

## 原始发现、处置与定向复验

### SKILL-U1：外部模板的 compile database 未包含消费者自身源码

优先级：低。原始模板能成功构建运行，但 `cluster-and-operations.md` 建议让消费者 clangd 使用生成的编译数据库；实际 `build/compile_commands.json` 含 46 个 checkout 源文件条目，消费者 `main.cpp` 的条目数为 0。原因是 `add_subdirectory` 内设置的 `CMAKE_EXPORT_COMPILE_COMMANDS` 不回传给上层创建的 target。同样，子目录的 `CMAKE_CXX_EXTENSIONS OFF` 不会回传，消费者原先使用 `-std=gnu++20`。

建议和处置：主代理在模板的 `project()` 后增加 `set(CMAKE_EXPORT_COMPILE_COMMANDS ON)` 和 `set(CMAKE_CXX_EXTENSIONS OFF)`。本 reviewer 等首轮构建结束后重新提取最终模板、重新配置并增量构建，没有并行修改构建中的输入。

定向复验：编译数据库中消费者 `main.cpp` 恰好出现一次，参数含 `-std=c++20` 与全部 Debug sanitizer flags；构建和相同业务运行返回 0，输出与原来成功版本完全一致。**该发现已关闭。**

### 非 skill 缺陷：消费者自己的 optional 类型笔误

首轮全新构建在消费者 `main.cpp:211` 失败，返回 2：`QueryContext{.scope = {std::string{"AAPL"}}}` 无法构造 `optional<Row>`。技能已经给出正确的 `QueryContext{.scope = Row{...}}`；公开头文件也清楚区分 `QueryContext.scope` 的 `optional<Row>` 与 `RequestKey.scope` 的 `Row`。

本 reviewer 修正初始化，并在传递 request scope 时取出已经存在的 Row。随后同一命令成功，sanitizer 场景全部通过。该失败保留在 `build.log`，不得计为错误 API 指引或 host 编译能力不足。

另曾尝试对临时代码执行 `clang-format`，当前 shell PATH 没有该命令及 18/19/20 后缀版本，因此未执行格式化；没有把这个可选步骤报告为通过，也没有因此更换编译器、下载依赖或中断业务验证。主代理随后指出宿主的 pre-commit 缓存中存在 formatter；消费者源码已冻结，未为非必要排版重启构建。

## 元数据、参考合同与 SDK 边界

使用 Python YAML parser 实际解析 frontmatter 与 `agents/openai.yaml`：skill 名称与目录一致、描述在长度界限内、UI 短描述合规，default prompt 包含 `$use-eventflow`。检查全部 skill Markdown 的 64 个本地文件链接存在、代码围栏成对。README 提供显式加载入口和可直接交给 Coding Agent 的业务提示词，并正确说明不同工具可能不会自动发现 `.skills/`，没有误称全局安装成功。

本消费者实际使用的 `Catalog::define_sql`、Candidate policy、`Trigger::on_event`、`Trigger::clock`、typed capture/Function、`TriggerSet::bind`、`EventProcessor`、executor、请求审计和 Latest 指引与当前头文件及运行结果相符。额外抽查了 `StatusOr`、storage 接口、请求 manifest 以及 SDK 的 `read_request` 端点；未发现本任务相关的其他实质 API 错误。

同一任意定义**不能通过当前 SDK 上传**。`include/eventflow/cluster_client.hpp` 的 `deploy()` 没有计算模块或回调参数；`ddl(sql)` 只覆盖支持的 STAGING SQL 定义，不注册 C++ Trigger 或 Function。技能正确要求以 embedded C++ 构造新计算，并明确已部署的 cluster profile 可以通过 SDK 输入、查询和推进时间。不能把静态 typed Function 描述为动态 UDF/UDTF/UDAF，也不应把尚未实施的默认 Trigger 审计表当成本次 skill 的缺陷。

## 未验证项与交接

本 reviewer 未运行 Release、真实 Coordinator/KV/多 Compute 集群、Kafka、checkpoint 恢复、UNKNOWN 故障注入、多 ticker 隔离或无限持续墙钟循环。它们不是本次独立 embedded 接入练习的通过依据。主代理正在另行运行 Release 模板验证和现有示例；该证据由实际执行者记录，不冒充本 reviewer 的执行结果。

下一步：主代理可以引用本报告关闭 skill 独立接入验收项，并把其余本轮验证与规格状态同步。没有待该 reviewer 修改的生产代码或未关闭的 skill 使用阻塞。

## 实施者补充

Root 在同一冻结消费者源码与 CMake 文件上完成独立 build-release 构建运行，使用 O3/NDEBUG/ThinLTO、无 sanitizer，输出与最终 Debug 逐字相同，编译无 warning。日志见 /tmp/eventflow-skill-consumer-release-{configure,build,run}.log。最终 skill 校验仍通过；本补充不是第二轮独立审查，也不扩大原报告的产品覆盖范围。
