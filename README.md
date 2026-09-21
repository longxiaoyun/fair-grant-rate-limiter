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

## Spring Boot integration

Add the starter, configure the quota in YAML, and inject it. Spring manages the pool and shutdown.

Not yet on Maven Central; install from the repository root first:
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

Call `submit("table-a", () -> commitPreparedBatch())`. On `WAIT`, retain the task and retry after `getRetryAfterMs()`; investigate `ERROR` without executing it. The starter does not retry business operations automatically.

Operations sharing quota use the same `resourceKey`. Each application instance gets its own default `clientId`; no manual node name or `close()` call is needed.

[Spring Boot example and configuration](spring-boot-starter/README.md) · [Plain Java integration](docs/java-quickstart.en.md)

## Further reading

- [Runnable standalone project](examples/quickstart/README.md): waiting, retries and multiple resources.
- [Kafka → ODPS integration](docs/kafka-odps.en.md): batching and commits across thirty nodes.
- [Configuration and guarantees](docs/reference.md): queues, windows, receipts and Redis failures.
- [Migration](docs/reference.md#migrating-from-the-old-snapshot): v4 must not run alongside older protocols.

The strict window governs grant timestamps. Execute immediately after acquisition and account for SDK-internal retries. Redis failure pauses grants by default.

[Apache License 2.0](LICENSE)
