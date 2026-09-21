# 普通 Java 接入

[返回首页](../README.zh-CN.md)

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
