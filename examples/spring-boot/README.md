# 可运行的 Spring Boot 示例

[starter 接入指南](../../spring-boot-starter/README.zh-CN.md)

这是独立 Maven 项目，只依赖 starter JAR。默认 Spring Boot 3.5.16，需要 Java 17+、Maven、Python 3.8+、Redis 6+（验证 ACL 用户）。

在仓库根目录运行：

```bash
python3 examples/spring-boot/run.py
```

脚本安装核心库和 Java 8 兼容的 starter，构建可执行 Boot JAR，启动临时 Redis 和三个 JVM，再验证：

- 无需手写 `@Configuration` 或手动创建 limiter，自动配置从 JAR 元数据发现。
- `application.yml` 配置每 2 秒最多 4 次、rate=2、burst=1。
- Redis 密码、ACL 用户和 DB 2 生效；默认 DB 0 不写入状态。
- 默认实例 ID 不同，三个节点初始 FIFO 轮次均有机会，每个完成 3 次 HTTP 提交。
- 公共获取时间边界内没有确定的窗口超发；不确定边界组数会单独报告。
- Spring 容器关闭后进程正常退出。

已有 Java 8 项目可运行：

```bash
python3 examples/spring-boot/run.py --boot-version 2.7.18
```

Java 17+ 可验证新版本：

```bash
python3 examples/spring-boot/run.py --boot-version 4.1.1
```

`JAVA_HOME` 决定 Java 版本；`--maven-repo /path/to/cache` 指定 Maven 缓存，`--skip-build` 仅适用于已经构建了对应 Boot 版本的 JAR。报告、JVM 日志与构建日志位于 `target/`。

[BatchWriter.java](src/main/java/example/BatchWriter.java) 就是 README 中的构造器注入示例。[BootDemo.java](src/main/java/example/BootDemo.java) 展示 WAIT 的完整处理；其中的等待循环只用于有限的命令行演示，服务应用应使用自己的任务调度机制保留和重试任务。

该验证连接脚本自己创建的临时 Redis/HTTP 服务。TLS 参数由 Jedis 处理，本示例不验证证书部署；不把令牌窗口检查等同于下游请求到达窗口保证。
