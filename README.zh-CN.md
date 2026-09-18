# Fair Grant Rate Limiter

[English](README.md) | 中文

[![CI](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml/badge.svg)](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-8%20%7C%2011%20%7C%2017-orange.svg)](#运行环境)

面向 Java 的**分布式公平令牌发放限流器**。

它在多个 JVM 之间维护**同一份**配额（Redis + Lua），并**公平地**发放许可，避免繁忙机器把安静的机器饿死。

典型场景：多台机器写同一个云资源，而云侧按 **key** 限制 QPS（例如 MaxCompute / ODPS Catalog 对每张表的 Commit 限制）。

---

## 要解决的问题

云上限制往往是「这张表每秒最多提交 5 次」，不是「每台机器 5 次」。下面四种做法，差别在三件事：

|  | 每台自己限速（总额 ÷ 机器数） | 大家抢同一个令牌桶 | 先抢一把分布式锁 | **本库** |
|--|--|--|--|--|
| 多台加起来，会不会超过云上的次数？ | 不会超，但经常用不满。没活的机器占着的那一份浪费了 | 不会超。所有机器共用这一桶 | 不一定。锁只避免两台同时改，不记账一共用了几次 | 不会超。所有机器共用这一桶 |
| 忙的机器会不会总抢在前面？ | 不会抢。每台只有自己那一小份 | 会。谁请求更勤，谁更容易拿到 | 会。谁先抢到锁，谁就先提交 | 不会。等最久的那台优先 |
| 会不会有机器一直提交不了？ | 一般不会，代价是每台都变慢 | 会。请求少的机器可能一直抢不到 | 会。锁不记谁等了多久 | 不会。这次没拿到的，下次排更前 |

---

## 运行环境

- **JDK** 8、11 或 17+（默认字节码目标是 Java 8）
- **Redis** 5+（需要 `EVAL` / `EVALSHA`）
- **Jedis** 4.x（已声明依赖）
- **SLF4J** API（日志门面）

测试使用 [jedis-mock](https://github.com/fppt/jedis-mock)，跑 `mvn test` 不需要外部 Redis。

---

## 安装

### Maven

```xml
<dependency>
  <groupId>io.github.longxiaoyun</groupId>
  <artifactId>fair-grant-rate-limiter</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

> 尚未发布到 Maven Central 时，请本地安装：`mvn clean install`。

### 构建

```bash
mvn clean test
mvn clean package

# 可选编译 profile
mvn -Pjdk8  clean test
mvn -Pjdk11 clean test
mvn -Pjdk17 clean test
```

---

## 快速开始

```java
import io.github.longxiaoyun.fairgrant.*;
import redis.clients.jedis.JedisPool;

FairGrantConfig config = FairGrantConfig.builder()
    .keyPrefix("odps:fair:")   // Redis key 前缀
    .ratePerSec(5.0)           // 这个 key 的共享许可速率（每秒）
    .burst(5.0)                // 桶容量
    .permitTtlMs(20_000L)      // 一次许可最长持有时间
    .writerNodes(35)           // 仅 LOCAL_SHARE 降级时使用
    .fallbackMode(FairGrantConfig.FallbackMode.LOCAL_SHARE)
    .build();

JedisPool jedisPool = /* 你的连接池 */;
RedisFairGrantLimiter limiter = FairGrantLimiters.redis(jedisPool, config);

String clientId = InetAddress.getLocalHost().getHostAddress();
String resourceKey = "my_project:my_table";

AcquireResult result = limiter.tryAcquire(resourceKey, clientId);

if (result.isGranted()) {
    try {
        // 执行被限流的动作（例如 Catalog commit）
    } finally {
        limiter.invalidatePermit(resourceKey, clientId);
        // 本机这个 key 已经没有待处理数据时：
        // limiter.clearPending(resourceKey, clientId);
    }
} else {
    // 非阻塞：把任务留在本地，过 result.getRetryAfterMs() 再试
}
```

便捷重载：

```java
limiter.tryAcquire("my_project", "my_table", clientId);
```

---

## API 一览

| 类型 | 作用 |
|------|------|
| `FairGrantLimiter` | 接口 |
| `RedisFairGrantLimiter` | Redis + Lua 实现 |
| `LocalShareFairGrantLimiter` | 进程内降级（`rate / writerNodes`） |
| `FairGrantConfig` | 不可变配置 |
| `AcquireResult` | `GRANTED` / `WAIT` / `DEGRADED_LOCAL` / `ERROR` |
| `FairGrantLimiters` | 工厂方法 |

### `AcquireResult` 状态

| 状态 | `isGranted()` | 含义 |
|------|---------------|------|
| `GRANTED` | true | 可以执行被保护的动作 |
| `WAIT` | false | 稍后重试（看 `getRetryAfterMs()`） |
| `DEGRADED_LOCAL` | true | Redis 不可用，降级策略允许这次调用 |
| `ERROR` | false | 意外失败，退避后重新排队 |

### 生命周期（重要）

1. 本机还有**待处理数据**时，持续调用 `tryAcquire`（或先调用 `registerPending`）。
2. 被保护的动作结束后，调用 **`invalidatePermit`**。
3. 这个 key 的**本地队列已空**时，调用 **`clearPending`**。  
   如果不调用，该客户端会一直留在公平队列里，空闲时也可能挡住别人。
4. 遇到 `WAIT` **不要**在热工作线程上 sleep，把任务重新入队并延迟重试。

---

## 工作原理

### Redis key（每个资源一份）

默认前缀 `fair:grant:`。资源名通常是 `project:table`（会转成小写）。

| Key | 类型 | 含义 |
|-----|------|------|
| `{prefix}{resource}:bucket` | HASH | `tokens`、`ts`（共享令牌桶） |
| `{prefix}{resource}:wait` | ZSET | score = 上次发放时间（公平顺序） |
| `{prefix}{resource}:pending` | SET | 仍有本地待处理数据的客户端 |
| `{prefix}{resource}:permit:{clientId}` | STRING | 动作执行期间持有的许可（`PX` TTL） |

### 发放算法（Lua，原子执行）

1. 确保调用方在 `pending` / `wait` 中。
2. 补充令牌：`tokens = min(burst, tokens + rate * elapsedSeconds)`。
3. 若 `tokens < 1` → 返回 `WAIT` 和 `retryAfterMs`。
4. 否则选出 wait score **最小**的 pending 客户端（等最久 / 最久没拿到许可）。
5. 只有这个客户端得到 `GRANTED`（扣 1 个令牌，写入 permit，更新 wait score）。
6. 其他人得到 `WAIT`，detail 为 `not_selected:<winner>`。
7. 已持有有效 permit 时是**幂等**的（`existing_permit`），重试不会重复扣令牌。

Lua 脚本位于 `src/main/resources/lua/`。

```
┌────────────┐   tryAcquire    ┌─────────────────────┐
│  JVM A/B/… │ ──────────────► │ Redis Lua fair_grant │
└────────────┘                 │  bucket + wait ZSET  │
                               └─────────────────────┘
                                         │
                    GRANTED（仅公平赢家） / WAIT
```

---

## Redis 不可用时的降级

| 模式 | 行为 |
|------|------|
| `LOCAL_SHARE`（默认） | 本地速率约等于 `ratePerSec / writerNodes` |
| `DENY` | 一律 `WAIT` |
| `ALLOW` | 一律放行（硬配额场景下**不安全**） |

---

## 配置项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `keyPrefix` | `fair:grant:` | Redis key 命名空间 |
| `ratePerSec` | `5` | 共享补充速率 |
| `burst` | 与 rate 相同 | 桶容量 |
| `permitTtlMs` | `20000` | 许可 TTL 兜底 |
| `writerNodes` | `10` | 仅降级时做除数 |
| `fallbackMode` | `LOCAL_SHARE` | Redis 失败策略 |
| `redisTimeoutMs` | `200` | 工厂超时 / DENY 的重试提示 |

---

## 包结构

```
io.github.longxiaoyun.fairgrant
├── FairGrantLimiter
├── RedisFairGrantLimiter
├── LocalShareFairGrantLimiter
├── FairGrantConfig
├── AcquireResult
└── FairGrantLimiters

src/main/resources/lua
├── fair_grant.lua
├── register_pending.lua
└── clear_pending.lua
```

---

## 测试

```bash
mvn test
```

覆盖范围：

- 配置、key、结果对象、本地降级的单元测试
- 内嵌 Redis 上的发放、公平轮转、防饥饿
- 并发突发与吞吐上限
- Redis 宕机降级（`LOCAL_SHARE` / `DENY` / `ALLOW`）

---

## 贡献

欢迎提 Issue 和 PR。请：

1. 除非另行讨论，保持 Java 8 源码兼容。
2. 行为变化时补上或更新测试。
3. 开 PR 前跑一遍 `mvn clean test`。

---

## 许可证

基于 [Apache License 2.0](LICENSE)。

---

## 声明

这是 `io.github.longxiaoyun` 下的个人开源库。  
不是阿里巴巴官方产品。
