# Fair Grant Rate Limiter

[English](README.md) | 中文

[![CI](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml/badge.svg)](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**公平令牌桶分发，解决在时间窗口里给多机器公平分发固定令牌的问题。**

多个 Java 进程共用一个令牌桶，按等待顺序获取令牌；拿到令牌后执行自己的任务。适用于多节点写入、共享 API 调用限额等场景。

![10s 窗口里最多发放 6 个令牌，多台机器按顺序获取](docs/images/allocation.zh-CN.svg)

## 先跑起来

准备 Java 8+、Maven、Redis 5+ 和 Python 3.8+，然后在仓库根目录运行：

```bash
python3 examples/quickstart/run.py
```

脚本会构建库和独立示例项目，启动临时 Redis 和 **3 个 Java 进程**。三个进程各执行 4 次任务，共享 **10s／6 个令牌**的限制；你可以看到实际发放顺序和验证结果。结束后自动关闭测试服务，不使用已有 Redis。

[示例源码与更多场景](examples/quickstart/README.md) · [30 个进程、业务失败重试等验证](examples/quickstart/README.md#场景测试)

## Spring Boot 接入

添加 starter，在 YAML 中配置额度，再注入即可；连接池和关闭清理由 Spring 管理。

尚未发布到 Maven Central，先在仓库根目录安装：
```bash
mvn install -DskipTests
mvn -f spring-boot-starter/pom.xml install -DskipTests
```

```xml
<dependency>
  <groupId>io.github.longxiaoyun</groupId>
  <artifactId>fair-grant-spring-boot-starter</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```yaml
fair-grant:
  rate-per-sec: 0.5
  burst: 1
  window: 10s
  max-permits: 6
  redis:
    host: 127.0.0.1
    port: 6379
```

```java
import io.github.longxiaoyun.fairgrant.*;
import io.github.longxiaoyun.fairgrant.springboot.FairGrantOperations;
import org.springframework.stereotype.Service;

@Service
public class BatchWriter {
    private final FairGrantOperations grants;

    public BatchWriter(FairGrantOperations grants) { this.grants = grants; }

    public AcquireResult submit(String table, FairGrantExecutor.Action commitBatch) throws Exception {
        return grants.tryExecute(table, commitBatch);
    }
}
```

调用 `submit("table-a", () -> 提交已准备好的批次)`。返回 `WAIT` 时保留任务并按 `getRetryAfterMs()` 再试；`ERROR` 时保留任务并排查。starter 不自动重试业务。

共用额度的操作使用相同表名／`resourceKey`。每个应用实例默认生成独立的 `clientId`，无需手写节点名称或调用 `close()`。

[Spring Boot 完整示例与配置](spring-boot-starter/README.zh-CN.md) · [普通 Java 接入](docs/java-quickstart.zh-CN.md)

## 进一步阅读

- [可运行的独立项目](examples/quickstart/README.md)：完整处理等待、重试和多表任务。
- [Kafka → ODPS 实际接入案例](docs/kafka-odps.zh-CN.md)：30 个节点按表攒批与提交。
- [配置与行为边界](docs/reference.zh-CN.md)：公平队列、窗口、回执和 Redis 故障。
- [旧版升级](docs/reference.zh-CN.md#从旧-snapshot-升级)：v4 与旧协议不能混跑。

严格窗口限制的是令牌发放时间。实际接口调用应紧接获准执行，SDK 内部重试需由接入方控制。默认 Redis 故障时暂停发放。

[Apache License 2.0](LICENSE)
