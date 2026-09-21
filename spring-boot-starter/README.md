# Spring Boot integration

[中文](README.zh-CN.md) · [Project README](../README.md)

Configure `fair-grant` in `application.yml` and constructor-inject `FairGrantOperations`. Spring manages the limiter and pool lifecycle. Each application instance gets a unique, lifetime-stable default client ID.

The default consumer example uses Boot 3.5.16 / Java 17+. Compatibility targets also include Boot 2.7.18 / Java 8 for existing projects and Boot 4.1.1 / Java 17+. This does not extend Spring's own support policy or claim every minor version/native image is compatible.

Install locally (not yet published to Maven Central):

```bash
mvn install -DskipTests
mvn -f spring-boot-starter/pom.xml install -DskipTests
```

Add `io.github.longxiaoyun:fair-grant-spring-boot-starter:1.0.0-SNAPSHOT` to your Boot application, then configure:

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

Inject the facade as shown in [BatchWriter](../examples/spring-boot/src/main/java/example/BatchWriter.java), then call `grants.tryExecute(resourceKey, callback)`. GRANTED means the callback ran once; WAIT/ERROR never execute it. Retain work on WAIT and schedule a retry after `getRetryAfterMs()`; investigate ERROR. Business exceptions propagate without refunding quota. The synchronous API does not queue/retry work in the background; use appropriate threads in reactive applications.

Configuration keys are prefixed with `fair-grant.`. Duration values accept whole milliseconds (`200ms`, `5s`):

| Key | Default |
|---|---|
| `enabled` | `true` |
| `client-id` | Generated once per application context |
| `key-prefix` | `fair:grant:` |
| `rate-per-sec` / `burst` | `5` / `max(1, rate-per-sec)` |
| `window` / `max-permits` | `0s` / `0`; set both to enable |
| `permit-ttl` / `pending-ttl` / `state-idle-ttl` | `20s` / `5s` / `60s` |
| `fallback-mode` / `writer-nodes` | `DENY` / `10` |
| `redis.host` / `redis.port` | `127.0.0.1` / `6379` |
| `redis.username` / `redis.password` | unset |
| `redis.database` / `redis.ssl` | `0` / `false` |
| `redis.timeout` | `200ms`, per stage rather than a total deadline |
| `redis.max-total` / `redis.max-idle` / `redis.min-idle` | `32` / `8` / `1` |
| `redis.test-on-borrow` | `false`; idle validation is enabled |

Invalid/unknown properties fail startup. A strict window requires DENY fallback. Explicit client IDs must differ across concurrent application instances. Share the Redis endpoint, prefix, resource and quota settings to share a budget. Passwords can reference environment variables; TLS uses JVM trust configuration. Sentinel, Cluster and native-image support are outside this integration.

No Spring Data Redis dependency is required. Settings use the independent `fair-grant.redis.*` namespace; existing Boot Redis properties can be referenced through YAML placeholders.

Custom beans take precedence: a `FairGrantLimiter` prevents creation of default Redis infrastructure; a `FairGrantConfig` replaces the default policy; a `JedisPool` is reused without transferring ownership to the limiter. Spring applies the pool bean's own destroy policy. Mark one pool `@Primary` if multiple exist, otherwise startup fails. Core limiter/executor beans remain injectable for advanced APIs. The plain Java core keeps no Spring dependency and retains its v4 protocol.

[Runnable packaged-JAR verification](../examples/spring-boot/README.md) covers three JVMs, authenticated Redis DB 2, distinct identities, FIFO, rolling-window checks, nine HTTP operations and shutdown. Auto-configuration tests cover activation, validation, overrides and lifecycle. Tests use each consumer Boot BOM's actual Jedis version.

[Core semantics and migration](../docs/reference.md) · [Official auto-configuration mechanism](https://docs.spring.io/spring-boot/reference/features/developing-auto-configuration.html)
