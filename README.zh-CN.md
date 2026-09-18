# Fair Grant Rate Limiter

[English](README.md) | 中文

[![CI](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml/badge.svg)](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-8%20%7C%2011%20%7C%2017-orange.svg)](#运行环境)

多台机器共用一份次数，而且不能让跑得勤的那台一直占着。

这是一个很小的 Java 库。它用 Redis 和一段 Lua，给「这张表每秒最多提交 5 次」这种共享上限发许可。所有 JVM 用同一个桶。有好几台机器在等的时候，等最久的那台拿到下一次。

它不搬你的数据。它只回答两件事：这次还有没有额度，以及轮到谁。

## 目录

- [要解决什么](#要解决什么)
- [它管什么，不管什么](#它管什么不管什么)
- [和常见做法比](#和常见做法比)
- [什么时候用](#什么时候用)
- [运行环境](#运行环境)
- [安装](#安装)
- [怎么调用](#怎么调用)
- [许可是怎么发出去的](#许可是怎么发出去的)
- [配置](#配置)
- [Redis 挂了](#redis-挂了)
- [目录结构](#目录结构)
- [构建和测试](#构建和测试)
- [使用时要注意](#使用时要注意)
- [参与修改](#参与修改)
- [许可证](#许可证)

## 要解决什么

云上的限制通常是按资源算的，不是按机器算的。一张表可能每秒只能提交 5 次。机器有 35 台时，常见的三种省事做法都缺一块：

- 每台自己限速，把 5 次除以 35。总数不会超，但没活的机器把那一份空占着，每台也都比实际需要的更慢。
- 在 Redis 里放一个令牌桶，大家一起抢。总数能守住，可是谁调用得勤，谁就一直抢到。
- 先拿一把分布式锁，再改本地计数。锁只能避免两台同时改，不记谁已经等了多久。

缺的是：一个大家共用的计数，再加一条「谁还在等」的队列。

|  | 每台自己限速（总额 ÷ 机器数） | 大家抢同一个桶 | 先抢一把锁 | **本库** |
|--|--|--|--|--|
| 多台加起来会不会超过云上的次数？ | 不会，但空闲机器的份额浪费了 | 不会，共用一个桶 | 不一定。锁不记账一共用了几次 | 不会，共用一个桶 |
| 忙的那台会不会总排在前面？ | 没有互抢。每台只有自己那一小份 | 会。调用越多越容易拿到 | 会。谁先拿到锁谁就先走 | 不会。等最久的先走 |
| 会不会有机器很久都提交不了？ | 不太会，但每台都被拖慢 | 会。不怎么来抢的机器可能一直输 | 会。锁不记等待时间 | 不会。这次没拿到，下次排更前 |

## 它管什么，不管什么

它管这些：

- 每个资源一个令牌桶，能连上同一个 Redis 的 JVM 都用这一份。
- 在还在等待的客户端里，选上次拿到许可最早的那个。
- 马上返回。`WAIT` 的意思是「过一会儿再试」，不是「在这里睡」。
- 许可会短时间留着，同一次调用重试不会再扣一个令牌。
- Redis 连不上时，退回到本机限速。

它不管这些：

- 不把数据、行、提交内容放进 Redis。里面只有计数、等待顺序和一个许可标记。
- 不替代 Sentinel 这类通用流控。那些产品负责卡住 QPS，不负责决定哪台机器轮到了。
- 不改 Kafka 分区，也不把一张表绑死在某个消费者上。这里的公平，只发生在「本地还有活」的客户端之间。
- 不调用 MaxCompute、ODPS 或别的云 API。拿到许可之后，那些调用还是你自己发。

## 什么时候用

几台进程共用一份硬配额，而且不能让最吵的那台把别人挤掉，就用它。一开始要解决的就是：很多写入进程往同一张表提交，云上按表限制提交次数。

配额只属于一个进程，或者本机一个 `RateLimiter` 就够了，就不必用。那种情况多一次 Redis 来回没有意义。

## 运行环境

- JDK 8、11 或 17。默认编出来的是 Java 8 字节码，所以 Java 11、17 也能跑这个 jar。
- Redis 5 或更新。脚本用 `EVAL` / `EVALSHA`。
- Jedis 4.4.6，Maven 会带上。
- 你的应用里要有一个 SLF4J 实现。这个库只依赖 `slf4j-api`。

`mvn test` 用的是 [jedis-mock](https://github.com/fppt/jedis-mock)，跑测试不用另起 Redis。

## 安装

包还没发到 Maven Central。先在本仓库装到本地：

```bash
mvn clean install
```

再依赖它：

```xml
<dependency>
  <groupId>io.github.longxiaoyun</groupId>
  <artifactId>fair-grant-rate-limiter</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## 怎么调用

创建一个 limiter，整个进程共用。多线程调用是安全的。`clientId` 在进程活着的时候不要变，一般用机器地址。`resourceKey` 是云上配额对应的那个东西，通常写成 `项目:表`。存进 Redis 之前会转成小写，所以 `MyTable` 和 `mytable` 是同一个资源。

```java
FairGrantConfig config = FairGrantConfig.builder()
    .keyPrefix("odps:fair:")
    .ratePerSec(5.0)          // 这个 key 在云上的上限
    .burst(5.0)               // 空闲时最多攒多少次
    .permitTtlMs(20_000L)     // 调用方拿到许可后中途挂了，多久后把许可丢掉
    .writerNodes(35)          // 只在 Redis 挂了的时候用
    .fallbackMode(FairGrantConfig.FallbackMode.LOCAL_SHARE)
    .build();

RedisFairGrantLimiter limiter = FairGrantLimiters.redis(jedisPool, config);
String clientId = InetAddress.getLocalHost().getHostAddress();
String resourceKey = "my_project:my_table";

AcquireResult result = limiter.tryAcquire(resourceKey, clientId);
if (result.getStatus() == AcquireResult.Status.GRANTED
        || result.getStatus() == AcquireResult.Status.DEGRADED_LOCAL) {
    try {
        commitTable();
    } finally {
        limiter.invalidatePermit(resourceKey, clientId);
    }
} else {
    // WAIT 或 ERROR。批次留在本地，过 result.getRetryAfterMs() 再试。
}
```

项目名和表名本来就是分开的，可以用这个重载：

```java
limiter.tryAcquire("my_project", "my_table", clientId);
```

不想自己传连接池的话：

```java
RedisFairGrantLimiter limiter = FairGrantLimiters.redis("127.0.0.1", 6379, config);
// limiter.close() 会把这里创建的连接池一起关掉
```

### 调用顺序

1. 这个进程对这个 key 还有活，就调用 `tryAcquire`。这一步也会把本机登记成正在等。想更早占位，可以先调 `registerPending`。
2. 拿到 `GRANTED` 之后做那次受限调用，然后 `invalidatePermit`。失败也要调，不然许可会一直占到 `permitTtlMs`。
3. 这个 key 的本地队列空了，调用 `clearPending`。忘了的话，这台机器还留在队伍里，可能挡住真正有活的机器。
4. 拿到 `WAIT` 时，不要在工作线程上睡。把批次放回去，过 `getRetryAfterMs()` 再试。

许可在你作废它、或者它过期之前是幂等的。进程因为网络抖了一下又调了一次 `tryAcquire`，许可还在的话，Redis 返回 `existing_permit`，不会再扣一个令牌。

### 返回值

| 状态 | 能往下做吗 | 接下来 |
|------|------------|--------|
| `GRANTED` | 能 | 做调用，然后 `invalidatePermit` |
| `WAIT` | 不能 | 过 `getRetryAfterMs()` 再试。`getDetail()` 会写原因：`no_token`，或者 `not_selected:<clientId>` |
| `DEGRADED_LOCAL` | 按降级规则可以 | Redis 失败了，而且当前降级策略允许这次。同时看 `getRetryAfterMs()`：大于 0 说明这一拍的本地份额已经用过 |
| `ERROR` | 不能 | 结果不符合预期。退避后再试 |

`isGranted()` 对 `GRANTED` 和 `DEGRADED_LOCAL` 都是 true。要区分 Redis 是否挂了，读 `getStatus()`。

## 许可是怎么发出去的

每个资源四个 Redis key。默认前缀是 `fair:grant:`。

| Key | 类型 | 里面是什么 |
|-----|------|------------|
| `{prefix}{resource}:bucket` | hash | `tokens` 和 `ts`，大家共用的桶 |
| `{prefix}{resource}:wait` | 有序集合 | 每个客户端一条，分数是它上次拿到许可的时间 |
| `{prefix}{resource}:pending` | 集合 | 本地还有活的客户端 |
| `{prefix}{resource}:permit:{clientId}` | string | 这个客户端被允许执行期间的标记，带毫秒过期 |

`tryAcquire` 在一次 Redis 调用里跑 `src/main/resources/lua/fair_grant.lua`：

1. 确认这个客户端在 `pending` 里。它如果从没拿到过许可，等待分数就是「现在」，所以更早在等的人仍然在前面。
2. 补令牌：`min(burst, tokens + rate * 距上次补充的秒数)`。
3. 不够 1 个令牌，就返回 `WAIT`，并告诉你还要等多久才有下一个。
4. 否则从等待集合里按分数从旧到新找，选第一个仍在 `pending` 里的客户端。已经不在 pending 里的会被删掉。
5. 如果选中的就是这次调用方，扣 1 个令牌，写上许可，把它的等待分数改成现在。
6. 如果选中的是别人，返回 `WAIT`，detail 是 `not_selected:<那个客户端>`。这次不扣令牌。

刚拿到的机器排到后面。还没拿到的留在前面。公平就这一条。

## 配置

| 选项 | 默认 | 含义 |
|------|------|------|
| `keyPrefix` | `fair:grant:` | 前缀。几个应用共用一个 Redis 时用来隔开 |
| `ratePerSec` | `5` | 每秒补充多少令牌，这个 key 的所有客户端共用 |
| `burst` | 和 `ratePerSec` 一样 | 桶里最多存多少。空闲很久之后，不会一下子放出去一大批 |
| `permitTtlMs` | `20000` | 保险时间。进程拿到 `GRANTED` 之后挂了、又没作废许可，到期后别人还能继续 |
| `writerNodes` | `10` | 只给 `LOCAL_SHARE` 用。这是你估计的写入机数量，Redis 不会自己去数 |
| `fallbackMode` | `LOCAL_SHARE` | Redis 抛错时怎么办 |
| `redisTimeoutMs` | `200` | `FairGrantLimiters.redis(host, port, config)` 的套接字超时，也是 `DENY` 时建议的重试间隔 |

`ratePerSec` 填这个 key 在云上的上限，不要填「上限 ÷ 机器数」。除法只在 Redis 挂了、走降级的时候用。

## Redis 挂了

| 模式 | 调用方看到什么 |
|------|----------------|
| `LOCAL_SHARE` | 这个进程大约按 `ratePerSec / writerNodes` 放行。状态是 `DEGRADED_LOCAL`。如果每台机器同时降级，总数仍可能超过云上上限，因为每台只知道自己那一份 |
| `DENY` | 一律 `WAIT`。Redis 恢复前不提交。硬配额下比较安全，写入会停住 |
| `ALLOW` | 一律放行。超限代价高的时候不要用 |

就算 Redis 那次 `clearPending` 失败了，本机降级用的那个时间槽仍会被清掉。

## 目录结构

```
src/main/java/io/github/longxiaoyun/fairgrant
├── FairGrantLimiter.java            接口
├── RedisFairGrantLimiter.java       Redis + Lua
├── LocalShareFairGrantLimiter.java  进程内降级
├── FairGrantConfig.java
├── AcquireResult.java
└── FairGrantLimiters.java           创建方法

src/main/resources/lua
├── fair_grant.lua
├── register_pending.lua
└── clear_pending.lua
```

## 构建和测试

```bash
mvn clean test
mvn -Pjdk8  clean test
mvn -Pjdk11 clean test
mvn -Pjdk17 clean test
```

测试覆盖了配置和 key 格式、本机降级、内嵌 Redis 上的发放和轮转、一台机器不能每轮都赢、并发时不会超过桶容量，以及 Redis 挂掉时的三种模式。

GitHub Actions 用 Temurin 8、11、17 跑同一套测试。

## 使用时要注意

- 公平只在还登记着 pending 的客户端之间。从不调用、或者已经 `clearPending` 的，不在队伍里。
- `clientId` 要稳定。每次调用都变，这个进程就会被当成新来的，可能插队。
- 所有写入机必须看到同一个 Redis。两套 Redis 就是两个桶。
- Lua 脚本本身就是临界区。不要在 `tryAcquire` 外面再套一把分布式锁，那又回到「谁先拿到锁谁先走」。
- 机器之间的时钟差不决定顺序。等待分数用的是调用方传入的时间，脚本里只有没传入时间才用 Redis `TIME`。某一台机器时钟跳得很大，仍可能把它自己的分数带偏。
- 这是 `io.github.longxiaoyun` 下的个人项目，不是阿里巴巴的产品。

## 参与修改

欢迎提 Issue 和 Pull Request。

- 除非另外说好，源码保持能在 Java 8 上编译。
- 改了发放行为，就补上或改对应测试。
- 开 PR 之前跑一遍 `mvn clean test`。

## 许可证

[Apache License 2.0](LICENSE)。
