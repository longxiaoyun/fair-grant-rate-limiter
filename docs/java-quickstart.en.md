# Plain Java integration

[Back to README](../README.md)

Plain Java and the Spring Boot starter share the same core and release version on `main`. Depend only on the core artifact below; Spring and the starter are not required.

[Run the plain Java multi-process example](../examples/quickstart/README.md).

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
