# Fair Grant Rate Limiter

English | [中文](README.zh-CN.md)

[![CI](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml/badge.svg)](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-8%20%7C%2011%20%7C%2017-orange.svg)](#requirements)

Many machines, one shared quota, and nobody gets stuck at the back of the line.

This is a small Java library. It uses Redis and a Lua script to hand out permits for a shared limit, such as “this table may be committed 5 times per second”. All JVMs draw from the same bucket. When more than one machine is waiting, the one that has waited the longest gets the next permit.

It does not move your data. It only answers two questions: is there a permit left, and whose turn is it.

## Contents

- [The problem](#the-problem)
- [What it does and what it does not](#what-it-does-and-what-it-does-not)
- [Compared with the usual options](#compared-with-the-usual-options)
- [When to use it](#when-to-use-it)
- [Requirements](#requirements)
- [Install](#install)
- [Usage](#usage)
- [How a permit is chosen](#how-a-permit-is-chosen)
- [Configuration](#configuration)
- [If Redis is down](#if-redis-is-down)
- [Layout](#layout)
- [Build and test](#build-and-test)
- [Limitations](#limitations)
- [Contributing](#contributing)
- [License](#license)

## The problem

Cloud quotas are often per resource, not per machine. A table might allow 5 commits per second. If you have 35 writers, three common shortcuts all miss something:

- Split the limit locally (`5 / 35` on each machine). You will not exceed the cloud limit, but idle machines waste their share, and every machine is slower than it needs to be.
- Put one token bucket in Redis and let everyone race. The total stays under the limit, but the machine that calls most often keeps winning.
- Take a distributed lock, then update a local counter. The lock stops two writers from updating at the same instant. It does not remember who has been waiting.

The missing piece is a shared counter plus a queue of who is still waiting.

|  | Each machine limits itself (total ÷ machine count) | Everyone races for one bucket | Grab a lock first | **This library** |
|--|--|--|--|--|
| Can the machines together go over the cloud limit? | No, but spare quota on idle machines is wasted | No. One shared bucket | Not really guaranteed. A lock does not count uses | No. One shared bucket |
| Does the busiest machine keep going first? | There is no race. Each machine only has its own slice | Yes. More calls, more wins | Yes. Whoever takes the lock goes first | No. Longest wait goes first |
| Can a machine fail to submit for a long time? | Unlikely, but every machine is slowed down | Yes. A quiet caller can lose forever | Yes. The lock does not track wait time | No. A miss moves that machine forward |

## What it does and what it does not

It does:

- Keep one token bucket per resource key, shared by every JVM that can reach the same Redis.
- Pick the waiting client with the oldest “last granted” time.
- Return immediately. `WAIT` means “try again later”, not “sleep here”.
- Keep a short-lived permit so a retry of the same call does not spend a second token.
- Fall back to a local limit if Redis cannot be reached.

It does not:

- Store payloads, rows, or commit bodies in Redis. Only counters, wait order, and a permit flag.
- Replace a general traffic product such as Sentinel. Those products cap QPS. They do not decide which machine’s turn it is.
- Partition Kafka or pin a table to one consumer. Fairness here is across clients that still have local work, not across message keys.
- Talk to MaxCompute, ODPS, or any other cloud API. You call those yourself after a permit is granted.

## When to use it

Use it when several processes share one hard quota, and a chatty process must not crowd out the others. The original case is many writers committing to the same table under a per-table commit limit.

Skip it when one process owns the quota, or when you only need a local `RateLimiter`. A Redis round trip is wasted in that case.

## Requirements

- JDK 8, 11, or 17. The default build target is Java 8, so a Java 11 or 17 runtime can still run the jar.
- Redis 5 or newer. The scripts use `EVAL` / `EVALSHA`.
- Jedis 4.4.6, pulled in by Maven.
- An SLF4J binding in your application. The library only depends on `slf4j-api`.

`mvn test` uses [jedis-mock](https://github.com/fppt/jedis-mock). You do not need a Redis server to run the tests.

## Install

The artifact is not on Maven Central yet. Build it from this repo:

```bash
mvn clean install
```

Then depend on it:

```xml
<dependency>
  <groupId>io.github.longxiaoyun</groupId>
  <artifactId>fair-grant-rate-limiter</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Usage

Create one limiter and share it. It is safe to call from many threads. `clientId` should stay the same for the life of the process, usually the host address. `resourceKey` is the thing the cloud quota applies to, usually `project:table`. Keys are stored in lower case, so `MyTable` and `mytable` are the same resource.

```java
FairGrantConfig config = FairGrantConfig.builder()
    .keyPrefix("odps:fair:")
    .ratePerSec(5.0)          // the cloud limit for this key
    .burst(5.0)               // how many permits may pile up while idle
    .permitTtlMs(20_000L)     // drop a permit if the caller dies mid-call
    .writerNodes(35)          // only used when Redis is down
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
    // WAIT or ERROR. Keep the batch local and retry after result.getRetryAfterMs().
}
```

There is a shortcut if you already split project and table:

```java
limiter.tryAcquire("my_project", "my_table", clientId);
```

If you would rather not pass in a pool:

```java
RedisFairGrantLimiter limiter = FairGrantLimiters.redis("127.0.0.1", 6379, config);
// limiter.close() also closes the pool it created
```

### Call it in this order

1. While this process still has work for the key, call `tryAcquire`. That also registers the client as waiting. You can call `registerPending` earlier if you want a place in line before the first try.
2. On `GRANTED`, do the limited call, then `invalidatePermit`. Do this on failure too, or the permit sits until `permitTtlMs`.
3. When the local queue for that key is empty, call `clearPending`. If you forget, this client stays in the line and can block machines that actually have work.
4. On `WAIT`, do not sleep on the worker thread. Put the batch back and retry after `getRetryAfterMs()`.

A granted permit is idempotent until you invalidate it or it expires. If the process retries `tryAcquire` after a network blip and the permit is still there, Redis returns `existing_permit` and does not spend another token.

### What the result means

| Status | Go ahead? | What to do |
|--------|-----------|------------|
| `GRANTED` | Yes | Run the call, then `invalidatePermit` |
| `WAIT` | No | Retry after `getRetryAfterMs()`. `getDetail()` says why: `no_token`, or `not_selected:<clientId>` |
| `DEGRADED_LOCAL` | Yes, under the fallback rules | Redis failed and the configured fallback allowed this call. Check `getRetryAfterMs()` as well: if it is greater than 0, the local slice for this interval is already used |
| `ERROR` | No | Unexpected result. Back off and retry |

`isGranted()` is true for both `GRANTED` and `DEGRADED_LOCAL`. If you care about the Redis-down case, read `getStatus()` too.

## How a permit is chosen

Each resource has four Redis keys. The default prefix is `fair:grant:`.

| Key | Type | What it holds |
|-----|------|----------------|
| `{prefix}{resource}:bucket` | hash | `tokens` and `ts`, the shared bucket |
| `{prefix}{resource}:wait` | sorted set | one entry per client, score is the last time that client was granted |
| `{prefix}{resource}:pending` | set | clients that still have local work |
| `{prefix}{resource}:permit:{clientId}` | string | set while that client is allowed to run, with a millisecond TTL |

`tryAcquire` runs `src/main/resources/lua/fair_grant.lua` in one Redis call:

1. Make sure this client is in `pending`. If it has never been granted, its wait score is “now”, so older waiters stay ahead.
2. Refill the bucket: `min(burst, tokens + rate * seconds since last refill)`.
3. If there is less than 1 token, return `WAIT` and how long to wait for the next token.
4. Otherwise walk the wait set from the oldest score and pick the first client that is still pending. Clients that left the pending set are removed.
5. If this caller is that client, subtract one token, set the permit, and move its wait score to now.
6. If someone else was picked, return `WAIT` with `not_selected:<that client>`. No token is spent.

So a machine that just won goes to the back. A machine that has not won stays near the front. That is the whole fairness rule.

## Configuration

| Option | Default | Meaning |
|--------|---------|---------|
| `keyPrefix` | `fair:grant:` | Namespace, so several apps can share one Redis |
| `ratePerSec` | `5` | Tokens added per second, shared by all clients of this key |
| `burst` | same as `ratePerSec` | Cap on stored tokens. A long idle period cannot dump a huge burst later |
| `permitTtlMs` | `20000` | Safety timer. If a process dies after `GRANTED` and never invalidates, the permit disappears and others can proceed |
| `writerNodes` | `10` | Used only by `LOCAL_SHARE`. It is your estimate of how many writers exist, not something Redis discovers |
| `fallbackMode` | `LOCAL_SHARE` | What to do when Redis throws |
| `redisTimeoutMs` | `200` | Socket timeout for `FairGrantLimiters.redis(host, port, config)`, and the retry hint used by `DENY` |

Set `ratePerSec` to the cloud limit for that key, not to `cloud limit / machine count`. The division is only for the Redis-down fallback.

## If Redis is down

| Mode | What the caller sees |
|------|----------------------|
| `LOCAL_SHARE` | This process allows about `ratePerSec / writerNodes`. The result status is `DEGRADED_LOCAL`. This can still exceed the cloud limit if every machine falls back at once, because each one only knows its own slice |
| `DENY` | Always `WAIT`. Nothing is submitted until Redis is back. Safe for a hard quota, and it can stall writers |
| `ALLOW` | Always grants. Do not use this when going over the cloud limit is expensive |

`clearPending` still clears the local fallback slot for that key, even if the Redis call fails.

## Layout

```
src/main/java/io/github/longxiaoyun/fairgrant
├── FairGrantLimiter.java            API
├── RedisFairGrantLimiter.java       Redis + Lua
├── LocalShareFairGrantLimiter.java  in-process fallback
├── FairGrantConfig.java
├── AcquireResult.java
└── FairGrantLimiters.java           factories

src/main/resources/lua
├── fair_grant.lua
├── register_pending.lua
└── clear_pending.lua
```

## Build and test

```bash
mvn clean test
mvn -Pjdk8  clean test
mvn -Pjdk11 clean test
mvn -Pjdk17 clean test
```

The suite covers config and key formatting, the local fallback, grant and rotation against embedded Redis, a check that one client cannot win every round, a concurrent burst cap, and the three Redis-down modes.

GitHub Actions runs the same tests on Temurin 8, 11, and 17.

## Limitations

- Fairness is only among clients that are still pending. A client that never calls, or that called `clearPending`, is not in line.
- `clientId` must be stable. If it changes every call, that process looks like a new waiter and can cut in.
- All writers must see the same Redis. Two Redis instances means two buckets.
- The Lua script is the critical section. Do not wrap `tryAcquire` in your own distributed lock. That brings back the “whoever grabs the lock goes first” problem.
- Clock skew between machines does not decide the order. Wait scores are written from the time each caller passes in, and Redis `TIME` is only a fallback inside the script. Large clock jumps on one machine can still skew its own score.
- This is a personal project under `io.github.longxiaoyun`. It is not an Alibaba product.

## Contributing

Issues and pull requests are welcome.

- Keep the source compatible with Java 8 unless we agree otherwise.
- If you change grant behavior, add or adjust a test.
- Run `mvn clean test` before opening a PR.

## License

[Apache License 2.0](LICENSE).
