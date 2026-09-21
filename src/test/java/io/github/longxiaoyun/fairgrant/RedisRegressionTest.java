package io.github.longxiaoyun.fairgrant;

import com.github.fppt.jedismock.RedisServer;
import org.junit.*;
import redis.clients.jedis.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.Assert.*;

/** Contract regressions, also inherited by the real-Redis integration suite. */
public class RedisRegressionTest {
    protected JedisPool pool;
    private RedisServer mock;
    protected final String prefix = "regression:";
    protected final FairGrantKeys keys = new FairGrantKeys(prefix);
    @Before public void start() throws Exception { startBackend(); }
    protected void startBackend() throws Exception {
        mock = RedisServer.newRedisServer().start();
        pool = new JedisPool("127.0.0.1", mock.getBindPort());
    }
    @After public void stop() throws Exception {
        if (pool != null) pool.close();
        stopBackend();
    }
    protected void stopBackend() throws Exception { if (mock != null) mock.stop(); }
    protected FairGrantConfig.Builder config() {
        return FairGrantConfig.builder().keyPrefix(prefix).fallbackMode(FairGrantConfig.FallbackMode.DENY);
    }
    protected RedisFairGrantLimiter limiter(FairGrantConfig.Builder builder) {
        return new RedisFairGrantLimiter(pool, builder.build());
    }
    @Test public void grantedClientDoesNotBlockOtherClients() {
        RedisFairGrantLimiter lim = limiter(config().burst(2));
        assertTrue(lim.tryAcquire("k", "a").isGranted());
        assertTrue(lim.tryAcquire("k", "b").isGranted());
    }
    @Test public void deadWaiterIsRemovedAfterLease() throws Exception {
        RedisFairGrantLimiter lim = limiter(config().pendingTtlMs(150));
        lim.registerPending("k", "dead");
        assertEquals("not_selected:dead", lim.tryAcquire("k", "alive").getDetail());
        Thread.sleep(250);
        assertTrue(lim.tryAcquire("k", "alive").isGranted());
        try (Jedis j = pool.getResource()) {
            assertNull(j.zscore(keys.wait("k"), "dead"));
            assertNull(j.zscore(keys.pending("k"), "dead"));
        }
    }
    @Test public void renewalKeepsPositionAndExpiredClientRejoinsAtTail() throws Exception {
        RedisFairGrantLimiter lim = limiter(config().pendingTtlMs(500));
        lim.registerPending("k", "z");
        lim.registerPending("k", "a");
        lim.registerPending("k", "z");
        try (Jedis j = pool.getResource()) {
            assertEquals(Arrays.asList("z", "a"), new ArrayList<String>(j.zrange(keys.wait("k"), 0, -1)));
            // Simulate only z's lease expiring, without relying on sleep precision.
            j.zadd(keys.pending("k"), 0, "z");
        }
        lim.registerPending("k", "z");
        assertEquals("not_selected:a", lim.tryAcquire("k", "z").getDetail());
        assertTrue(lim.tryAcquire("k", "a").isGranted());
        assertTrue(lim.tryAcquire("k", "z").isGranted());
    }
    @Test public void fifoRotationDoesNotDependOnMillisecondTies() {
        RedisFairGrantLimiter lim = limiter(config().ratePerSec(1000).burst(30));
        List<String> clients = Arrays.asList("z", "b", "a");
        for (String c : clients) lim.registerPending("k", c);
        for (int round = 0; round < 5; round++) {
            for (String c : clients) {
                assertTrue(c, lim.tryAcquire("k", c).isGranted());
                lim.registerPending("k", c);
            }
        }
    }
    @Test public void concurrentDistinctRequestsWithSameClientConsumeSeparateTokens() throws Exception {
        final RedisFairGrantLimiter lim = limiter(config().ratePerSec(.001).burst(1));
        ExecutorService executor = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
        try {
            for (int i = 0; i < 32; i++) results.add(executor.submit(() -> {
                start.await();
                return lim.tryAcquire("k", "one-jvm").isGranted();
            }));
            start.countDown();
            int granted = 0;
            for (Future<Boolean> f : results) if (f.get(10, TimeUnit.SECONDS)) granted++;
            assertEquals(1, granted);
        } finally { executor.shutdownNow(); }
    }
    @Test public void receiptsSurviveCompletionAndNeverApplyToOtherRequests() {
        RedisFairGrantLimiter lim = limiter(config().ratePerSec(.001).burst(1));
        assertTrue(lim.tryAcquireRequest("k", "a", "request-1").isGranted());
        lim.invalidatePermit("k", "a");
        lim.clearPending("k", "a");
        assertEquals("existing_permit", lim.tryAcquireRequest("k", "a", "request-1").getDetail());
        assertFalse(lim.tryAcquireRequest("k", "a", "request-2").isGranted());
        assertFalse(lim.tryAcquireRequest("k", "b", "request-1").isGranted());
    }
    @Test public void lostResponseCanBeRetriedWithoutDoubleDebit() {
        RedisFairGrantLimiter lim = limiter(config().ratePerSec(.001).burst(2));
        // Execute Lua directly, discard its response, then retry through the public API.
        try (Jedis j = pool.getResource()) {
            j.eval(LuaScriptLoader.load("lua/fair_grant.lua"), 4,
                    keys.bucket("k"), keys.wait("k"), keys.pending("k"), keys.permit("k", "a", "r"),
                    "a", ".001", "2", "20000", "5000");
        }
        assertEquals("existing_permit", lim.tryAcquireRequest("k", "a", "r").getDetail());
        assertTrue(lim.tryAcquireRequest("k", "a", "new").isGranted());
        assertFalse(lim.tryAcquire("k", "a").isGranted());
    }
    @Test public void receiptExpiresAtDocumentedRetryHorizon() throws Exception {
        RedisFairGrantLimiter lim = limiter(config().ratePerSec(100).permitTtlMs(100));
        assertTrue(lim.tryAcquireRequest("k", "a", "r").isGranted());
        assertEquals("existing_permit", lim.tryAcquireRequest("k", "a", "r").getDetail());
        Thread.sleep(200);
        assertEquals("ok", lim.tryAcquireRequest("k", "a", "r").getDetail());
    }
    @Test public void fractionalRateEventuallyRefills() throws Exception {
        RedisFairGrantLimiter lim = limiter(config().ratePerSec(.5));
        assertTrue(lim.tryAcquire("k", "a").isGranted());
        assertFalse(lim.tryAcquire("k", "a").isGranted());
        Thread.sleep(2100);
        assertTrue(lim.tryAcquire("k", "a").isGranted());
    }
    @Test public void suggestedWaitRenewsLeaseBeforeItExpires() {
        RedisFairGrantLimiter lim = limiter(config().ratePerSec(.001).pendingTtlMs(1000));
        assertTrue(lim.tryAcquire("k", "a").isGranted());
        AcquireResult wait = lim.tryAcquire("k", "a");
        assertFalse(wait.isGranted());
        assertTrue(wait.getRetryAfterMs() > 0);
        assertTrue(wait.getRetryAfterMs() <= 500);
    }
    @Test public void bucketTimestampNeverRegressesAndCallerClockIsIgnored() {
        RedisFairGrantLimiter lim = limiter(config().ratePerSec(1).burst(1));
        assertTrue(lim.tryAcquire("k", "a").isGranted());
        try (Jedis j = pool.getResource()) {
            long future = Long.parseLong(j.hget(keys.bucket("k"), "ts")) + 60000;
            j.hset(keys.bucket("k"), "ts", Long.toString(future));
            for (int i = 0; i < 5; i++) {
                assertFalse(lim.tryAcquire("k", "a").isGranted());
                assertEquals(Long.toString(future), j.hget(keys.bucket("k"), "ts"));
            }
        }
    }
    @Test public void mismatchedConfigFailsClosedEvenInAllowMode() {
        RedisFairGrantLimiter first = limiter(config().ratePerSec(1));
        assertTrue(first.tryAcquire("k", "a").isGranted());
        RedisFairGrantLimiter other = limiter(config().ratePerSec(2).fallbackMode(FairGrantConfig.FallbackMode.ALLOW));
        AcquireResult result = other.tryAcquire("k", "b");
        assertEquals(AcquireResult.Status.ERROR, result.getStatus());
        assertEquals("config_mismatch", result.getDetail());
        assertFalse(result.isGranted());
    }
    @Test public void scriptErrorsMustNotBecomeFallbackGrants() {
        try (Jedis j = pool.getResource()) { j.set(keys.bucket("k"), "wrong-type"); }
        RedisFairGrantLimiter lim = limiter(config().fallbackMode(FairGrantConfig.FallbackMode.ALLOW));
        assertEquals(AcquireResult.Status.ERROR, lim.tryAcquire("k", "a").getStatus());
    }
    @Test public void scriptCacheFlushRecoversWithoutLosingGrant() {
        RedisFairGrantLimiter lim = limiter(config());
        assertTrue(lim.tryAcquire("k", "a").isGranted());
        try (Jedis j = pool.getResource()) { j.scriptFlush(); }
        assertTrue(lim.tryAcquireRequest("k", "a", "r").isGranted());
        assertEquals("existing_permit", lim.tryAcquireRequest("k", "a", "r").getDetail());
    }
}
