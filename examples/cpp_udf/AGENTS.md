# C++ 动态函数示例

继承根目录与 [examples 约定](../AGENTS.md)。本目录是本地插件机制示例，完整多进程数据库使用仍见 streaming_database。

- 接口限定为同一受控构建的 C++ ABI，接口已迁移到生产 udf.hpp；真实集群使用见 ../native_udf。
- 新闻函数只处理输入并返回结果；宿主借助现有 Registry、Simple/ThreadPool 执行器展示调用。
- `.so` 生命周期必须覆盖回调和关联对象；不把直接加载描述为隔离执行。
- run.sh 同时支持源码构建和安装目录，具体范围与证据见 [规格](../../specs/cpp-udf-example.md)。
