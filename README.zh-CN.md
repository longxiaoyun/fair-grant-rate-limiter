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

## 接到自己的代码里

先执行 `mvn install -DskipTests`，再添加依赖（暂未发布到 Maven Central）：

```xml
<dependency>
  <groupId>io.github.longxiaoyun</groupId>
  <artifactId>fair-grant-rate-limiter</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```java
import io.github.longxiaoyun.fairgrant.*;

FairGrantConfig config = FairGrantConfig.builder()
    .ratePerSec(0.5).burst(1)
    .slidingWindow(10_000L, 6)
    .build();

// 每个进程创建一次，所有进程连接同一个 Redis。
RedisFairGrantLimiter limiter = FairGrantLimiters.redis("127.0.0.1", 6379, config);
FairGrantExecutor executor = new FairGrantExecutor(limiter);

AcquireResult result = executor.tryExecute("table-a", "node-a", () -> {
    // 在这里执行一次已准备好的操作，例如提交一个批次。
    System.out.println("执行任务");
});

if (!result.isGranted()) {
    // WAIT：保留当前任务，按 result.getRetryAfterMs() 安排再次尝试。
    // ERROR：保留任务并排查 result.getDetail()，不要执行任务。
}
// 应用退出时调用 limiter.close()。
```

只需先记住两点：**共用额度的操作使用相同的 `resourceKey`**（例中 `table-a`）；**不同进程使用不同且稳定的 `clientId`**（例中 `node-a`）。业务调用失败后重试，也要重新申请令牌。

## 进一步阅读

- [可运行的独立项目](examples/quickstart/README.md)：完整处理等待、重试和多表任务。
- [Kafka → ODPS 实际接入案例](docs/kafka-odps.zh-CN.md)：30 个节点按表攒批与提交。
- [配置与行为边界](docs/reference.zh-CN.md)：公平队列、窗口、回执和 Redis 故障。
- [旧版升级](docs/reference.zh-CN.md#从旧-snapshot-升级)：v4 与旧协议不能混跑。

严格窗口限制的是令牌发放时间。实际接口调用应紧接获准执行，SDK 内部重试需由接入方控制。默认 Redis 故障时暂停发放。

[Apache License 2.0](LICENSE)
