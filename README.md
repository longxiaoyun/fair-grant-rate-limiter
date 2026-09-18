# Fair Grant Rate Limiter

English | [中文](README.zh-CN.md)

[![CI](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml/badge.svg)](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-8%20%7C%2011%20%7C%2017-orange.svg)](#requirements)

A **distributed fair token-grant rate limiter** for Java.

It keeps a **shared** rate limit across many JVM processes (via Redis + Lua), and grants permits **fairly** so that busy writers cannot starve quieter ones.

Typical use case: many machines writing to the same cloud resource that enforces a **per-key** QPS quota (for example MaxCompute / ODPS Catalog commit limits per table).

---

## Problem

A cloud limit is usually “this table may commit 5 times per second”, not “each machine may commit 5 times”. The four approaches differ on three questions:

|  | Each machine limits itself (total ÷ machine count) | Everyone races for one token bucket | Grab a distributed lock first | **This library** |
|--|--|--|--|--|
| Can many machines together exceed the cloud limit? | No, but the quota is often wasted. Idle machines sit on their share | No. All machines share one bucket | Not guaranteed. A lock only stops two writers at once; it does not count total uses | No. All machines share one bucket |
| Does the busiest machine keep winning? | No race. Each machine only has its own small share | Yes. Whoever calls more often gets the token | Yes. Whoever takes the lock submits first | No. The machine that has waited longest goes first |
| Can some machine never get to submit? | Usually no, but every machine is slowed down | Yes. A quiet machine can lose forever | Yes. The lock does not remember who has been waiting | No. A miss moves that machine to the front next time |

---

## Requirements

- **JDK** 8, 11, or 17+ (bytecode targets Java 8 by default)
- **Redis** 5+ (needs `EVAL` / `EVALSHA`)
- **Jedis** 4.x (declared dependency)
- **SLF4J** API (logging facade)

Tests use [jedis-mock](https://github.com/fppt/jedis-mock); no external Redis is required to run `mvn test`.

---

## Install

### Maven

```xml
<dependency>
  <groupId>io.github.longxiaoyun</groupId>
  <artifactId>fair-grant-rate-limiter</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

> Until published to Maven Central, install locally: `mvn clean install`.

### Build

```bash
mvn clean test
mvn clean package

# Optional compiler profiles
mvn -Pjdk8  clean test
mvn -Pjdk11 clean test
mvn -Pjdk17 clean test
```

---

## Quick start

```java
import io.github.longxiaoyun.fairgrant.*;
import redis.clients.jedis.JedisPool;

FairGrantConfig config = FairGrantConfig.builder()
    .keyPrefix("odps:fair:")   // Redis key namespace
    .ratePerSec(5.0)           // shared permits per second for this key
    .burst(5.0)                // bucket capacity
    .permitTtlMs(20_000L)      // max hold time for a granted permit
    .writerNodes(35)           // only used by LOCAL_SHARE fallback
    .fallbackMode(FairGrantConfig.FallbackMode.LOCAL_SHARE)
    .build();

JedisPool jedisPool = /* your pool */;
RedisFairGrantLimiter limiter = FairGrantLimiters.redis(jedisPool, config);

String clientId = InetAddress.getLocalHost().getHostAddress();
String resourceKey = "my_project:my_table";

AcquireResult result = limiter.tryAcquire(resourceKey, clientId);

if (result.isGranted()) {
    try {
        // perform the rate-limited action (e.g. Catalog commit)
    } finally {
        limiter.invalidatePermit(resourceKey, clientId);
        // when this JVM has no more local work for the key:
        // limiter.clearPending(resourceKey, clientId);
    }
} else {
    // Non-blocking: keep work local and retry after result.getRetryAfterMs()
}
```

Convenience overload:

```java
limiter.tryAcquire("my_project", "my_table", clientId);
```

---

## API overview

| Type | Role |
|------|------|
| `FairGrantLimiter` | Interface |
| `RedisFairGrantLimiter` | Redis + Lua implementation |
| `LocalShareFairGrantLimiter` | Process-local fallback (`rate / writerNodes`) |
| `FairGrantConfig` | Immutable settings |
| `AcquireResult` | `GRANTED` / `WAIT` / `DEGRADED_LOCAL` / `ERROR` |
| `FairGrantLimiters` | Factory helpers |

### `AcquireResult` statuses

| Status | `isGranted()` | Meaning |
|--------|---------------|---------|
| `GRANTED` | true | Proceed with the protected action |
| `WAIT` | false | Retry later (`getRetryAfterMs()`) |
| `DEGRADED_LOCAL` | true | Redis unavailable; fallback allowed the call |
| `ERROR` | false | Unexpected failure; back off / requeue |

### Lifecycle rules (important)

1. Machines with **local pending work** should keep calling `tryAcquire` (or call `registerPending` first).
2. After the protected action finishes, call **`invalidatePermit`**.
3. When the **local queue for that key is empty**, call **`clearPending`**.  
   If you skip this, the client stays in the fair queue and can block others even while idle.
4. Do **not** sleep on a hot worker thread for `WAIT` — requeue with delay instead.

---

## How it works

### Redis keys (per resource)

Prefix defaults to `fair:grant:`. Resource is usually `project:table` (lower-cased).

| Key | Type | Meaning |
|-----|------|---------|
| `{prefix}{resource}:bucket` | HASH | `tokens`, `ts` (shared token bucket) |
| `{prefix}{resource}:wait` | ZSET | score = last grant time (fairness order) |
| `{prefix}{resource}:pending` | SET | clients that still have local work |
| `{prefix}{resource}:permit:{clientId}` | STRING | held while the action runs (`PX` TTL) |

### Grant algorithm (Lua, atomic)

1. Ensure the caller is in `pending` / `wait`.
2. Refill: `tokens = min(burst, tokens + rate * elapsedSeconds)`.
3. If `tokens < 1` → `WAIT` + `retryAfterMs`.
4. Else select the pending client with the **lowest** wait score (longest waiting / least recently granted).
5. Only that client receives `GRANTED` (consumes 1 token, sets permit, updates wait score).
6. Others receive `WAIT` with detail `not_selected:<winner>`.
7. Holding a valid permit is **idempotent** (`existing_permit`) so retries do not double-consume.

Lua scripts live under `src/main/resources/lua/`.

```
┌────────────┐   tryAcquire    ┌─────────────────────┐
│  JVM A/B/… │ ──────────────► │ Redis Lua fair_grant │
└────────────┘                 │  bucket + wait ZSET  │
                               └─────────────────────┘
                                         │
                    GRANTED (only fair winner) / WAIT
```

---

## Fallback when Redis is down

| Mode | Behavior |
|------|----------|
| `LOCAL_SHARE` (default) | Local rate ≈ `ratePerSec / writerNodes` |
| `DENY` | Always `WAIT` |
| `ALLOW` | Always grant (**unsafe** for hard cloud quotas) |

---

## Configuration reference

| Option | Default | Description |
|--------|---------|-------------|
| `keyPrefix` | `fair:grant:` | Redis key namespace |
| `ratePerSec` | `5` | Shared refill rate |
| `burst` | same as rate | Bucket capacity |
| `permitTtlMs` | `20000` | Permit TTL safety net |
| `writerNodes` | `10` | Fallback divisor only |
| `fallbackMode` | `LOCAL_SHARE` | Redis failure policy |
| `redisTimeoutMs` | `200` | Used by factory / DENY retry hint |

---

## Package layout

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

## Testing

```bash
mvn test
```

Coverage includes:

- unit tests for config / keys / results / local fallback
- embedded Redis tests for grant, fairness rotation, anti-starvation
- concurrent burst / throughput bounds
- Redis-down fallback modes (`LOCAL_SHARE` / `DENY` / `ALLOW`)

---

## Contributing

Bug reports and PRs are welcome. Please:

1. Keep Java 8 source compatibility unless discussed otherwise.
2. Add or update tests with behavior changes.
3. Run `mvn clean test` before opening a PR.

---

## License

Licensed under the [Apache License 2.0](LICENSE).

---

## Disclaimer

This is a personal open-source library under `io.github.longxiaoyun`.  
It is not an official Alibaba product.
