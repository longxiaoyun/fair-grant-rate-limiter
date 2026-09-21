# Fair Grant Rate Limiter

English | [中文](README.zh-CN.md)

[![CI](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml/badge.svg)](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**A Java rate limiter that lets multiple programs share one request rate and gives waiting programs their turns in order.**

For example, three programs call a third-party API using the same account. You want them to make five calls per second on average **in total**, while giving every program with pending work a turn. Fair Grant runs before each API call and answers: “you may call now” or “try again later.”

Add it as a Maven dependency in each Java program and connect them to the same Redis. There is no separate Fair Grant service to deploy.

## What problem does it solve?

Suppose three Java programs call the same **document recognition API**. Each call uploads one file and receives the recognition result. All three use the same third-party account.

| Program | Work | Files waiting for recognition |
|---|---|---|
| A | Process a historical archive | 1,000 |
| B | Process today's new files | 10 |
| C | Process files just uploaded by users | 10 |

You set two requirements for this account:

1. **Control the combined rate:** A, B and C together should start five calls per second on average.
2. **Give each program a turn:** A has much more work, but repeatedly asking for quota should not leave B and C waiting indefinitely.

Limiting calls inside each program alone does not meet both requirements:

| Approach | What happens in this example? |
|---|---|
| Allow each program five calls per second | Together they could make fifteen calls per second, exceeding your combined target. |
| Reserve one third of the quota for each program | After B and C finish, A still gets only its own share and the rest sits unused. |
| Share a limiter that only deducts available quota | It can control the combined rate, but does not decide whose turn comes next; more frequent applicants may obtain more opportunities. |

Fair Grant adds **a waiting order** to **the shared quota**.

### What changes after integration?

Before each recognition call, A, B and C ask Fair Grant for permission to make one call. This permission is called a **permit**. One permit covers one API call, regardless of the file's size or page count.

Suppose all three have joined the queue in A, B, C order and keep queueing for their remaining work:

| Next available opportunity | Goes to | What happens next |
|---|---|---|
| 1 | A | A calls the API for one file. It has more work, so it rejoins the tail. |
| 2 | B | B processes one file, then rejoins the tail. |
| 3 | C | C processes one file, then rejoins the tail. |
| 4 | A | A gets a turn for its next file. |

This illustrates queue order; each grant must also wait for quota to become available. Once B and C have no work left, they stop queueing. A can use the available quota without reserving two thirds for idle programs.

The library does not process the files. On “granted,” **your program calls the third-party API**. On “wait,” the file stays in your program's own task queue and your program asks again later. The library does not wake tasks in the background.

## When should I use it?

It fits when both of these apply:

- Multiple Java processes call the same limited target, such as an API account or a table's submission endpoint.
- You want to control their combined request rate and let processes with pending work take turns.

It counts **calls**, not concurrent tasks or data volume. Your application still owns its task queue, business execution and retries. If only one process needs rate limiting, a local limiter is usually sufficient.

The rate is implemented with a token bucket, which can allow bursts. If the provider requires a strict “at most five calls in any one-second window,” check its counting rules first: this library does not implement a strict sliding-window counter. The example below uses `burst=1` to reduce consecutive grants.

## How do I integrate it?

You need Java 8+ and Redis 5+. The current client uses Jedis for a single Redis node; it does not provide a Redis Cluster adapter.

### 1. Install the dependency

The package is not on Maven Central yet. Run `mvn clean install` in this repository first, then add this to your application's `pom.xml`:

```xml
<dependency>
  <groupId>io.github.longxiaoyun</groupId>
  <artifactId>fair-grant-rate-limiter</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Your application also needs an SLF4J implementation for logging.

### 2. Create the limiter at startup

Use the same configuration and Redis address in all three programs. Create one limiter per process and share it within that process.

```java
import io.github.longxiaoyun.fairgrant.*;
import java.util.UUID;

FairGrantConfig config = FairGrantConfig.builder()
    .ratePerSec(5.0) // Combined rate for the account, NOT five calls per program
    .burst(1.0)     // Save at most one opportunity, avoiding a batch of immediate calls
    .build();

// Local demo address; in deployment, all programs must use the same Redis service.
RedisFairGrantLimiter limiter = FairGrantLimiters.redis("127.0.0.1", 6379, config);

String clientId = UUID.randomUUID().toString(); // Once at process startup
String resourceKey = "document-api:account-001"; // API and account sharing this quota
```

The two identifiers serve different purposes:

| Parameter | Meaning in the example | What should each program use? |
|---|---|---|
| `resourceKey` | The shared quota for the document API and account-001 | The same value in A, B and C. |
| `clientId` | Which program is asking for a turn | A different value in each process, stable throughout its lifetime. |

Resource names are lowercased. A different account with an independent quota can use a different resourceKey.

### 3. Ask for a permit before each business call

```java
AcquireResult result = limiter.tryAcquire(resourceKey, clientId);

if (result.isGranted()) {
    callRecognitionApi(document);              // Call the API for this file now
} else if (result.getStatus() == AcquireResult.Status.ERROR) {
    reportLimiterError(result.getDetail());     // Investigate a config or Redis data error
} else {
    retryLater(document, result.getRetryAfterMs()); // Keep the file and ask again later
}
```

`callRecognitionApi`, `reportLimiterError` and `retryLater` stand for your application's code. `tryAcquire` returns a result instead of sleeping until quota becomes available.

Each successful acquisition covers one business call; no release is required. If the program cancels waiting work and has no other waiting work for this resource, call `limiter.clearPending(resourceKey, clientId)` to leave the queue. Call `limiter.close()` at shutdown to close the Redis pool created in this example.

By default, an unavailable Redis produces `WAIT`, pausing new business calls. If a program crashes without leaving the queue, a subsequent access removes its position after the waiting lease expires. The default lease is five seconds.

## Further usage

Every successful ordinary acquisition consumes new quota. If a Redis response is lost and you want to retry **the same permit acquisition**, use `tryAcquireRequest` with a requestId. That is separate from making another business API call after a failure; see the reference for details.

- [Configuration, acquisition retries, failure modes and internals](docs/reference.md)
- [Migrating from an older SNAPSHOT](docs/reference.md#migrating-from-the-old-snapshot): the new Redis data format must not be mixed with old clients.
- [中文说明](README.zh-CN.md)

## Development and tests

```bash
mvn clean test                # Unit tests and mock Redis tests
mvn -Preal-redis clean verify  # Also run real Redis and multi-JVM end-to-end tests
```

The second command requires a local `redis-server`. Tests start a temporary Redis and four independent JVMs that perform 40 actual test HTTP calls, checking the combined quota and waiting order. See the [verification reference](docs/reference.md#build-and-end-to-end-validation).

Issues and pull requests are welcome. [Apache License 2.0](LICENSE).
