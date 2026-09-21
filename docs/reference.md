# Usage reference and implementation

[Back to the project introduction](../README.md) · [中文](reference.zh-CN.md)

Start with the README for the business example and integration steps. Use this page to look up configuration, handle retries and failures, or migrate an older version. The README explains mixed-table Kafka consumption and local batching; the [scenario review (Chinese)](scenario-review.zh-CN.md) records the 75-per-15-second validation gap.

## Configuration

| Option | Default | Meaning |
|---|---|---|
| `keyPrefix` | `fair:grant:` | Application namespace |
| `ratePerSec` | 5 | Finite positive shared average rate |
| `burst` | `max(1, ratePerSec)` | Finite capacity of at least one token |
| `pendingTtlMs` | 5000 | Waiting lease; allow for normal scheduling, Redis latency and GC |
| `permitTtlMs` | 20000 | Request receipt retention, not a business lock timeout |
| `fallbackMode` | `DENY` | Redis failure behavior |
| `writerNodes` | 10 | Estimated process count for LOCAL_SHARE only |
| `redisTimeoutMs` | 200 | Factory connection/pool timeout and DENY retry hint |

All JVMs for one resource must agree on rate and burst, otherwise `ERROR / config_mismatch` is returned. To change either value, first stop and drain the resource's callers and coordinate a bucket reset. Mixed configurations are not a dynamic configuration protocol.

### Redis failure

| Mode | Behavior |
|---|---|
| `DENY` | Default; return WAIT |
| `LOCAL_SHARE` | Per-limiter rate of `ratePerSec / writerNodes`; local rejection remains WAIT |
| `ALLOW` | Allow everything; only for callers explicitly accepting overrun |

LOCAL_SHARE uses `System.nanoTime()` and atomic per-resource state; clearing pending does not reset cooldown. Multiple limiter instances in one process each receive their own local share. Partial Redis outages, transitions, underestimated process counts and simultaneous first fallback grants can violate the shared rate. Redis and local idempotency records are separate; cross-mode retries have no unified deduplication guarantee.

Redis data/script errors such as WRONGTYPE or ACL rejection return ERROR instead of being hidden by ALLOW. Scripts use EVALSHA with EVAL recovery after cache loss. A successful grant does not depend on a subsequent SCRIPT LOAD operation.

## Calls and retries

### New attempts and acquisition retries

Each `tryAcquire` invocation requests **new quota**. Two concurrent threads using one client identity consume two tokens if both succeed.

For retries after an ambiguous/lost Redis response, persist one request ID on the call-attempt object:

```java
String requestId = UUID.randomUUID().toString(); // do not regenerate on Redis retry
AcquireResult result = limiter.tryAcquireRequest(resourceKey, clientId, requestId);
```

- Distinct quota-counted business calls must use different request IDs.
- Retry the same acquisition with the same ID. A retained receipt returns `GRANTED / existing_permit` without another debit.
- Receipts expire `permitTtlMs` after the first grant; retries do not extend them. After expiration, the same ID can be charged again.
- Set an acquisition retry deadline shorter than receipt retention. IDs are not permanent deduplication keys.
- Do not independently execute one request ID from multiple threads. A replay means acquisition was granted, not that the business action remains unexecuted. The application owns execution deduplication.
- No release is required. The deprecated `invalidatePermit` is a no-op. Receipts survive completion until TTL, so delayed cleanup cannot erase newer receipts.

### Waiting and cancellation

1. Acquire automatically joins or renews a waiting lease. Use `registerPending` only for commit-ready batches, never merely because an unfinished local buffer contains some rows.
2. `WAIT` is non-blocking. Suggested retry intervals are at most half the waiting lease; low-rate clients must still renew regularly.
3. A grant finishes the client's queue turn. In-flight business work does not retain the head position. Queue promptly for the next turn if more work remains.
4. A client that misses its lease rejoins at the tail. Long GC pauses, scheduling delays and network stalls can lose its position.
5. `clearPending` cancels waiting, without refunding tokens, deleting receipts or resetting local cooldown.
6. With threads sharing an identity, a central queue owner must decide when the client is idle. One thread must not clear another thread's pending work.

Redis failures in `registerPending` / `clearPending` are logged; leases eventually remove abandoned entries. These methods are not business transactions.

| Status | `isGranted()` | Meaning |
|---|---|---|
| `GRANTED` | true | New Redis grant or same-request receipt |
| `WAIT` | false | No token, another waiter, deny fallback or exhausted local share |
| `DEGRADED_LOCAL` | true | The selected fallback allows execution; retry delay is zero |
| `ERROR` | false | Conflicting configuration, Redis data/script error or unexpected result |

## Guarantees and boundaries

- **Rate:** on the normal Redis path, an interval of `t` seconds admits approximately at most `burst + ratePerSec × t` new grants. This permits bursts; it is **not a strict N-per-sliding-second limiter**.
- **Fairness:** waiting clients with live leases queue FIFO. A grant removes the client from the queue; its next request joins the tail. Fairness is per `clientId`, not per thread or request.
- **Liveness:** retries or `registerPending` renew a waiting lease. The next acquire/register removes expired waiters, including crashed processes.
- **Idempotency:** explicit request IDs prevent duplicate debits only within the `permitTtlMs` receipt lifetime. This does not provide exactly-once business execution.
- **Failure:** the default is `DENY`. `LOCAL_SHARE` and `ALLOW` explicitly give up the strict shared quota guarantee.

Execute the protected action promptly after obtaining a grant. Accumulating grants and then issuing a batch of delayed actions can exceed the downstream action rate. If a cloud API call times out and you issue another call, acquire a new grant with a **new request ID**; the business idempotency key may remain unchanged.

Redis state loss, asynchronous replication failover, independent Redis instances or mixed protocol versions can invalidate the shared quota. This is not a strongly consistent quota system under Redis failure.

## Design

One Lua invocation atomically:

1. Checks the existing bucket's rate and burst against the caller's configuration.
2. Returns an existing request receipt if present.
3. Removes expired leases using Redis time, assigns a new waiter an increasing FIFO sequence and renews its lease.
4. Refills using Redis time. Bucket timestamps never decrease, preventing repeated refill during a server clock rollback.
5. If a token is available and this client is first, debits one, writes a request receipt and removes the client's waiting turn.

Sequence scores avoid client-ID tie breaking when registrations/grants share a millisecond. Head selection uses `ZRANGE 0 0`; stale leases are cleaned lazily on acquire/register.

The versioned key base is `{prefix}v2:{<base64url(resource)>}:`, where angle brackets are placeholders and the resource hash-tag braces are literal:

| Suffix | Type | Contents |
|---|---|---|
| `bucket` | hash | `tokens`, `ts`, `seq`, `rate`, `burst` |
| `wait` | zset | clientId → FIFO sequence |
| `pending` | zset | clientId → lease expiry time |
| `permit:<base64url(client)>:<base64url(request)>` | string + TTL | Redis grant time in milliseconds |

Encoding is UTF-8, URL-safe Base64 without padding, preventing separator collisions between identities. With the default prefix, a resource's keys share a hash tag; this does not add Redis Cluster support to the Java client.

Buckets retain rate history and do not expire automatically. Local fallback retains per-resource state too. Plan lifecycle cleanup for high-cardinality, one-off resources. Do not delete live buckets or cooldown state: deletion restores the initial burst. Stale waiters on idle resources are removed on the next access.

## Migrating from the old SNAPSHOT

This is a behavioral correction with a new Redis protocol. **Do not mix old and new clients**: old keys and `v2` keys are independent buckets and can both issue quota.

1. Stop old acquisitions and drain already-granted business actions.
2. Allow the old bucket to refill (conservatively `burst / ratePerSec` seconds); ensure old actions/retries cannot resume later.
3. Switch all clients together. Ordinary tryAcquire now debits each successful call; migrate acquisition retries to tryAcquireRequest.
4. Remove invalidatePermit calls. Use clearPending only to cancel remaining waiting work.
5. Choose fallback explicitly; the default changed from LOCAL_SHARE to DENY.
6. Remove old Redis keys only after confirming no old clients remain. The library does not delete old data.

## Build and end-to-end validation

```bash
mvn clean test                      # unit tests + jedis-mock contract regressions
mvn -Preal-redis clean verify        # plus isolated Redis and multi-JVM HTTP E2E
mvn -Preal-redis -Dredis.server=/absolute/path/redis-server clean verify
```

Real tests require a local `redis-server` executable and loopback-port permissions. They allocate ports, start isolated instances with persistence disabled, and stop them on teardown; they do not use an existing Redis. Missing prerequisites fail tests rather than silently skipping them. Logs are in `target/redis-it-*` and `target/worker-*`.

The E2E case starts four JVMs sharing rate=20/burst=2, performs 40 distinct HTTP commits and 40 acquisition receipt replays, verifies ten completions per client and initial FIFO fairness, and checks the token envelope for every interval between grant timestamps.

The additional `MaxComputeScenarioIT` uses real Redis with 30 logical consumers and synthetic mixed-table messages. It checks independent local batches, FIFO tableA grants, independent tableB quota and a frequent applicant that cannot leapfrog waiting nodes. It does not start Kafka or call an ODPS SDK; the actual commit window and SDK retries still require pipeline validation.

Real Redis coverage includes shutdown/recovery, all fallback modes, expired waiters, same-client concurrency, lost-response retry, SCRIPT FLUSH, timestamp rollback protection, fractional rates, mismatched configuration and script errors. CI runs Java 8/11/17, with separate Redis 5 and 7 coverage.

The `jdk8` / `jdk11` / `jdk17` compatibility profiles remain available. Higher-version profiles produce their respective bytecode versions; use the default build or jdk8 for a Java 8-compatible release artifact.

## Contributing and license

Keep source compatible with Java 8. Add regression tests for grant behavior and run `mvn -Preal-redis clean verify` before proposing changes.

[Apache License 2.0](../LICENSE). A personal project, not an Alibaba product.
