# M1：类型、共享 KV 与构建交付审查

Status: PASS
Milestone: M1
Updated: 2026-09-13

## 范围

类型/Schema、稳定键编码、Status/StatusOr/noexcept、IStateStore、Memory/RocksDB/Remote、KV服务生命周期、CMake/Make构建、cpp3rdlib、gtest、Bear/clangd、格式化。SQL、MV增量执行、FSM和完整Compute应用属于后续milestone，不以当前KV服务冒充完整产品。

## Fresh-context review

两个 reviewer 均以 fork_turns=none 启动，未参与本 milestone 实现：

- m1_review_api：类型/Status/键编码/构建隔离。
- m1_review_storage：原子提交、回执、远程协议、资源与生命周期。

两者初审均要求整改；合理问题全部采纳并加入回归，复核均为PASS。

| Finding | 复现 | 修复与验证 |
| --- | --- | --- |
| 原始JSON超范围整数回退double，绕过精度拒绝 | 18446744073709551617 被舍入，Float64/向量可错误接受 | parse_json_exact检查原始整数词法边界；decode_row/HTTP接入。新3测试先红后绿；原probe正确拒绝 |
| 非法endpoint触发第三方stoi并终止进程 | 超长port导致SIGABRT；空host和65536被接受 | 自有from_chars与authority/IPv6校验，调用host+int ctor；非法endpoint返回Status |
| HTTP正文限额可绕过 | chunked 17MiB和无CL 24MiB正文被累积/处理 | prerouting要求RPC恰好一个有效Content-Length且<=16MiB，拒绝TE/未知方法；headers-only立即拒绝并关闭，revision不变 |
| 不可能的COMMITTED revision仍被接受 | expected0收到revision0/2/UINT64_MAX | 校验expected+1与溢出，非法ACK为Unknown；新回归先红后绿 |

reviewer最终独立确认：非法/重复/负数CL、chunked立即400；超限CL立即413；拒绝后连接关闭，revision保持0，health正常。安装包RPATH、BUILD_TESTING=OFF、无gtest生产链接和cpp3rdlib校验也独立验证通过。

## 实现期间的其他正确性修复

- 请求的调用Status与CommitResult的Unknown分开，不将损坏ACK误报为确定拒绝。
- 读取预算同时限制数量和原始字节，包含重复键；远程保留相同ErrorCode。
- KV服务自己拥有监听线程；start就绪后返回，stop关闭并join，避免启动前stop失效。
- Status消息固定缓冲，截断保持UTF-8边界，无法编码的外部错误详情在RPC输出使用安全文本。
- RocksDB真实WAL写返回错误后实例fail-closed，重开后再确认；不以memtable读/FlushWAL推定未提交。
- 用户追加的无异常规范已替换早期异常接口；所有自有生产函数noexcept，正常错误显式返回，不使用throw/catch。

## Fresh 验证

最终 Debug：24 types + 61 store contracts + 14 remote/status/lifecycle = 99 个顶层GoogleTests，CTest 3/3通过。存储suite的故障测试另启子进程，不额外重复计入顶层计数。

- make compdb JOBS=4：真实Bear捕获9个TU，根compile_commands.json存在且覆盖所有当前源码/测试TU。
- ctest --test-dir build --output-on-failure：3/3，31.85秒。
- EVENTFLOW_SANITIZERS=ON构建及CTest：3/3，33.12秒，无Sanitizer诊断。
- BUILD_TESTING=OFF的Release构建成功，生产link无gtest；独立安装包--help可运行，RPATH为包内相对路径，ldd无missing。
- make format-check：全部自有C++源码和头文件通过。
- clangd --check src/main.cpp及state_store.hpp：报告0 errors；state_store.cpp的clangd20检查有ExtractFunction功能探测错误，已识别为工具refactoring probe，未声称该命令全绿。
- cpp3rdlib：仅发布头文件、预编译库及版本/许可证/校验文件；无第三方源码树、deb/jar或构建下载。

可复现命令和当前构建入口见README；原始临时执行日志在/tmp，不作为永久交付依赖。

## 保留边界

- 第三方二进制面向Ubuntu24.04/Linux x86_64，基础glibc/C++ ABI要求明确。
- MemOnly服务不跨进程重启持久化；真实持久性由RocksDB测试覆盖。
- 没有MVCC、自动HA或跨shard事务承诺。
- 本次milestone review不替代最终多个fresh-context全局review/challenge。

