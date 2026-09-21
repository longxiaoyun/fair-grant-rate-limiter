# Fair Grant Rate Limiter

English | [中文](README.zh-CN.md)

[![CI](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml/badge.svg)](https://github.com/longxiaoyun/fair-grant-rate-limiter/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**Fair token-bucket allocation across machines within a time window and a fixed token budget.**

Java processes share a token bucket, acquire tokens in waiting order, and execute their own tasks. Use it for distributed writes or shared API quotas.

![At most 6 tokens in a 10s window, allocated fairly across machines](docs/images/allocation.en.svg)

## Try it locally

With Java 8+, Maven, Redis 5+ and Python 3.8+ installed, run from the repository root:

```bash
python3 examples/quickstart/run.py
```

The script builds the library and a standalone consumer, starts temporary Redis and **three Java processes**, and runs four operations per process under a shared **6-token / 10s** limit. It prints the observed grant order and validation results, then stops its services. It does not use an existing Redis instance.

[Standalone example and scenarios](examples/quickstart/README.md)

## Use it in your application

Run `mvn install -DskipTests` first, then add the dependency (not yet on Maven Central):

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

// Create once per process. All processes connect to the same Redis.
RedisFairGrantLimiter limiter = FairGrantLimiters.redis("127.0.0.1", 6379, config);
FairGrantExecutor executor = new FairGrantExecutor(limiter);

AcquireResult result = executor.tryExecute("table-a", "node-a", () -> {
    // Execute one prepared operation, such as committing a batch.
    System.out.println("Execute task");
});

if (!result.isGranted()) {
    // WAIT: retain the task and reschedule using result.getRetryAfterMs().
    // ERROR: retain the task and investigate result.getDetail(); do not execute it.
}
// Call limiter.close() when the application shuts down.
```

Use the same **resourceKey** for operations sharing quota (`table-a` above), and a distinct, stable **clientId** for each process (`node-a`). Each business retry must acquire a new token.

## Further reading

- [Runnable standalone project](examples/quickstart/README.md): waiting, retries and multiple resources.
- [Kafka → ODPS integration](docs/kafka-odps.en.md): batching and commits across thirty nodes.
- [Configuration and guarantees](docs/reference.md): queues, windows, receipts and Redis failures.
- [Migration](docs/reference.md#migrating-from-the-old-snapshot): v4 must not run alongside older protocols.

The strict window governs grant timestamps. Execute immediately after acquisition and account for SDK-internal retries. Redis failure pauses grants by default.

[Apache License 2.0](LICENSE)
