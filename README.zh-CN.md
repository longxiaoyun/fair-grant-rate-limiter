# Fair Grant Rate Limiter

[English](README.md) | 中文

[![CI](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml/badge.svg)](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**一个 Java 限流库，让多个程序共用一个调用速率，并让正在等待的程序按顺序获得调用机会。**

例如，3 个程序使用同一个账号调用第三方接口：你希望它们加起来平均每秒调用 5 次，而且每个有任务的程序都能轮到。Fair Grant 就放在每次接口调用之前，判断“这次可以调用”还是“需要稍后再试”。

你通过 Maven 把它引入各个 Java 程序，再让它们连接同一个 Redis 即可，无需部署额外的 Fair Grant 服务。

## 它具体解决什么问题？

假设你有 3 个 Java 程序，都在调用同一个**文档识别接口**：每次上传一个文件，得到识别结果。它们使用同一个第三方账号。

| 程序 | 工作内容 | 等待识别的文件 |
|---|---|---|
| A | 历史文件批量处理 | 1000 个 |
| B | 今天新增的文件 | 10 个 |
| C | 用户临时上传的文件 | 10 个 |

你给这个账号设定了两个要求：

1. **控制总速率**：A、B、C 加起来平均每秒发起 5 次调用。
2. **让各个程序都有机会**：A 的任务很多，也不能因为它反复申请额度，就一直让 B、C 等着。

仅在各程序内部限流，无法同时满足这两个要求：

| 做法 | 在这个例子里会怎样？ |
|---|---|
| 每个程序各限每秒 5 次 | 3 个程序加起来可能达到每秒 15 次，超过你设定的总速率。 |
| 每个程序固定分到总额度的三分之一 | B、C 做完任务后，A 仍只能使用自己那一份，剩下的额度空闲。 |
| 大家共用一个只负责扣额度的限流器 | 总速率可以控制，但没有规定下一次该轮到哪个程序；申请更频繁的程序可能拿走更多机会。 |

Fair Grant 在**共享总额度**的基础上，增加了**等待顺序**。

### 接入后会发生什么？

A、B、C 每次准备调用识别接口时，都先向 Fair Grant 申请一次调用机会。这里的一次机会叫作一个“许可”，它对应一次接口调用，与文件大小、页数无关。

假设三个程序已按 A、B、C 的顺序进入等待队列，并持续为后续任务排队：

| 下一次可用的调用机会 | 给谁 | 然后发生什么 |
|---|---|---|
| 第 1 次 | A | A 调用接口处理一个文件；还有文件要处理，就重新排到队尾。 |
| 第 2 次 | B | B 处理一个文件，再排到队尾。 |
| 第 3 次 | C | C 处理一个文件，再排到队尾。 |
| 第 4 次 | A | 轮到 A 的下一个文件。 |

这是排队顺序示意，实际发放还要等额度可用。B、C 没有待处理任务后，就不再占用等待位置；A 可以独自使用可用额度，不必继续保留三分之二给空闲程序。

库不会替你处理文件。它返回“允许”后，**由你的程序调用第三方接口**；返回“等待”时，文件留在程序自己的任务队列里，由程序稍后再次申请。库不会后台唤醒任务。

## 什么情况下适合用？

当你的场景同时符合下面两点时，它比较合适：

- 多个 Java 进程调用同一个受限对象，例如同账号的第三方 API，或同一张表的提交接口。
- 除了限制总调用速率，你还希望各个有待办任务的进程能按顺序获得机会。

它管理的是**调用次数**，不限制同时执行的任务数，也不限制数据量。任务队列、业务执行和失败重试仍由应用负责。只有一个进程需要限流时，通常用本地限流器即可。

速率通过令牌桶实现，可以允许短时间突发。若对方要求严格的“任意一秒最多 5 次”，需要先核对是否匹配它的计数规则；本库不提供严格滑动窗口计数。下例设 `burst=1`，以减少连续放行。

## 怎么接入？

需要 Java 8+ 和 Redis 5+。当前使用 Jedis 连接单节点 Redis，不提供 Redis Cluster 客户端适配。

### 1. 安装依赖

包尚未发布到 Maven Central。先在本仓库执行 `mvn clean install`，再在应用的 `pom.xml` 中加入：

```xml
<dependency>
  <groupId>io.github.longxiaoyun</groupId>
  <artifactId>fair-grant-rate-limiter</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

应用还需提供 SLF4J 日志实现。

### 2. 在程序启动时创建 limiter

三个程序使用相同的配置和 Redis 地址；每个程序创建一个 limiter，并在进程内共用。

```java
import io.github.longxiaoyun.fairgrant.*;
import java.util.UUID;

FairGrantConfig config = FairGrantConfig.builder()
    .ratePerSec(5.0) // 这个账号的总速率，不是每个程序各 5 次
    .burst(1.0)     // 最多积攒 1 次调用机会，避免集中发起一批调用
    .build();

// 本地演示地址；部署时让所有程序连接同一个 Redis 服务。
RedisFairGrantLimiter limiter = FairGrantLimiters.redis("127.0.0.1", 6379, config);

String clientId = UUID.randomUUID().toString(); // 每个进程启动时生成一次
String resourceKey = "document-api:account-001"; // 这份总额度对应的接口和账号
```

两个名字各有用途：

| 参数 | 在例子里的含义 | 各程序应该怎么填？ |
|---|---|---|
| `resourceKey` | “文档识别接口 + account-001 账号”的共同额度 | A、B、C 填相同值，才能共用一份额度。 |
| `clientId` | 正在申请调用机会的是哪个程序 | A、B、C 各不相同；每个进程运行期间保持不变。 |

资源名会转为小写。另一个独立账号可以使用另一个 `resourceKey`，获得独立额度。

### 3. 在每次业务调用之前申请许可

```java
AcquireResult result = limiter.tryAcquire(resourceKey, clientId);

if (result.isGranted()) {
    callRecognitionApi(document);              // 现在可以调用接口，处理这个文件
} else if (result.getStatus() == AcquireResult.Status.ERROR) {
    reportLimiterError(result.getDetail());     // 配置或 Redis 数据异常，需要排查
} else {
    retryLater(document, result.getRetryAfterMs()); // 保留文件，到建议时间再申请
}
```

`callRecognitionApi`、`reportLimiterError` 和 `retryLater` 代表你自己的业务代码。`tryAcquire` 返回结果，不会在方法里睡眠等待额度。

每次获准对应一次业务调用，无需释放许可。若程序取消了等待任务，且该资源已经没有其他等待工作，调用 `limiter.clearPending(resourceKey, clientId)` 退出排队。程序退出时调用 `limiter.close()` 关闭这里创建的 Redis 连接池。

默认情况下，Redis 不可用时返回 `WAIT`，暂停发起新的业务调用。程序若崩溃而没退出排队，其等待位置会在租约过期后由后续访问清理，默认租约为 5 秒。

## 进一步使用

上面的普通调用每次成功都会消耗一次额度。如果 Redis 的响应丢失，想重试**同一次许可申请**，可以使用带 `requestId` 的 `tryAcquireRequest`。它与业务接口失败后的重新调用是两件事，具体见调用参考。

- [配置、获取重试、故障策略与实现原理](docs/reference.zh-CN.md)
- [从旧 SNAPSHOT 升级](docs/reference.zh-CN.md#从旧-snapshot-升级)：新版采用不同的 Redis 数据格式，新旧客户端不能混跑。
- [英文说明](README.md)

## 开发与测试

```bash
mvn clean test                # 单元测试与模拟 Redis 测试
mvn -Preal-redis clean verify  # 再加真实 Redis 和多 JVM 端到端测试
```

第二条命令需要本机安装 `redis-server`。测试会启动临时 Redis 和 4 个独立 JVM，让它们实际执行 40 次测试 HTTP 调用，检查总额度和排队顺序。详见[验证说明](docs/reference.zh-CN.md#构建和端到端验证)。

欢迎提交 Issue 和 PR。[Apache License 2.0](LICENSE)。
