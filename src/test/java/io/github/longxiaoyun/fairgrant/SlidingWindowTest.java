package io.github.longxiaoyun.fairgrant;

import com.github.fppt.jedismock.RedisServer;
import org.junit.*;
import redis.clients.jedis.*;
import java.util.*;
import static org.junit.Assert.*;

/** Same production Lua, with only TIME replaced for exact boundary assertions. */
public class SlidingWindowTest {
    protected JedisPool pool;
    private RedisServer mock;
    protected final FairGrantKeys keys = new FairGrantKeys("window:");
    private final long epoch = 1_800_000_000_000_000L;
    @Before public void start() throws Exception { startBackend(); }
    protected void startBackend() throws Exception {
        mock = RedisServer.newRedisServer().start();
        pool = new JedisPool("127.0.0.1", mock.getBindPort());
    }
    @After public void stop() throws Exception { if (pool != null) pool.close(); stopBackend(); }
    protected void stopBackend() throws Exception { if (mock != null) mock.stop(); }
    protected FairGrantConfig.Builder config() {
        return FairGrantConfig.builder().keyPrefix("window:").ratePerSec(1000).burst(100).slidingWindow(1000, 2);
    }
    @SuppressWarnings("unchecked")
    private List<Object> grant(long offsetUs, String client, String request, double rate, double burst,
                               long windowMs, int count, long leaseMs) {
        long now = epoch + offsetUs;
        String script = LuaScriptLoader.load("lua/fair_grant.lua")
                .replace("local t = redis.call('TIME')", "local t = {ARGV[8], ARGV[9]}");
        try (Jedis j = pool.getResource()) {
            return (List<Object>) j.eval(script, 5, keys.bucket("k"), keys.wait("k"), keys.pending("k"),
                    keys.permit("k", client, request), keys.window("k"), client,
                    Double.toString(rate), Double.toString(burst), "20000", Long.toString(leaseMs),
                    Long.toString(windowMs), Integer.toString(count),
                    Long.toString(now / 1000000), Long.toString(now % 1000000));
        }
    }
    private List<Object> grant(long us, String c, String r) { return grant(us, c, r, 1000, 100, 1000, 2, 5000); }
    private void status(String expected, List<Object> result) { assertEquals(result.toString(), expected, result.get(0)); }
    private long size() { try (Jedis j = pool.getResource()) { return j.zcard(keys.window("k")); } }

    @Test public void exactMicrosecondBoundaryAndSameTimestampGrants() {
        status("GRANTED", grant(123, "a", "1"));
        status("GRANTED", grant(123, "b", "2"));
        assertEquals(2, size());
        List<Object> blocked = grant(1000122, "c", "3");
        status("WAIT", blocked); assertEquals("1", blocked.get(1));
        status("GRANTED", grant(1000123, "c", "3"));
        assertEquals(1, size());
        try (Jedis j = pool.getResource()) {
            assertEquals(epoch + 1000123, new java.math.BigDecimal(j.get(keys.permit("k", "c", "3"))).longValueExact());
        }
    }
    @Test public void rollingWindowExpiresOnlyOldestAndDoesNotResetAtCalendarBoundary() {
        status("GRANTED", grant(900000, "a", "1"));
        status("GRANTED", grant(999999, "b", "2"));
        status("WAIT", grant(1000000, "c", "3"));
        status("GRANTED", grant(1900000, "c", "3"));
        assertEquals(2, size());
        status("WAIT", grant(1900001, "d", "4"));
        status("GRANTED", grant(1999999, "d", "4"));
    }
    @Test public void replayAndClearNeverRefundOrExtendWindow() {
        status("GRANTED", grant(0, "a", "1"));
        status("GRANTED", grant(0, "b", "2"));
        RedisFairGrantLimiter lim = new RedisFairGrantLimiter(pool, config().build());
        lim.clearPending("k", "a"); lim.invalidatePermit("k", "a");
        assertEquals("existing_permit", grant(999999, "a", "1").get(3));
        assertEquals(2, size());
        status("WAIT", grant(999999, "c", "3"));
        status("GRANTED", grant(1000000, "c", "3"));
    }
    @Test public void reusedRequestAfterReceiptRemovalCannotOverwriteWindowHistory() {
        status("GRANTED", grant(0, "a", "same"));
        try (Jedis j = pool.getResource()) { j.del(keys.permit("k", "a", "same")); }
        status("GRANTED", grant(100, "a", "same"));
        assertEquals(2, size());
        status("WAIT", grant(200, "a", "other"));
    }
    @Test public void windowDenialDoesNotDebitTokens() {
        status("GRANTED", grant(0, "a", "1"));
        status("GRANTED", grant(0, "b", "2"));
        for (int i = 0; i < 20; i++) {
            List<Object> result = grant(0, "c", "3");
            status("WAIT", result); assertEquals(98D, Double.parseDouble(result.get(2).toString()), 0D);
        }
    }
    @Test public void tokenDenialDoesNotConsumeWindowAndRetryUsesBothGates() {
        status("GRANTED", grant(0, "a", "1", .5, 1, 1000, 1, 10000));
        List<Object> result = grant(0, "b", "2", .5, 1, 1000, 1, 10000);
        status("WAIT", result); assertEquals("2000", result.get(1)); assertEquals(1, size());
        status("WAIT", grant(1000000, "b", "2", .5, 1, 1000, 1, 10000));
        assertEquals(0, size());
        status("GRANTED", grant(2000000, "b", "2", .5, 1, 1000, 1, 10000));
    }
    @Test public void clockRollbackCannotReleaseWindowOrRefillTokens() {
        status("GRANTED", grant(1000, "a", "1"));
        status("GRANTED", grant(1000, "b", "2"));
        status("WAIT", grant(-1000000, "c", "3"));
        status("WAIT", grant(1000999, "c", "3"));
        status("GRANTED", grant(1001000, "c", "3"));
    }
    @Test public void hotPollerCannotOvertakeWaiterWhenWindowOpens() {
        status("GRANTED", grant(0, "a", "1")); status("GRANTED", grant(0, "b", "2"));
        status("WAIT", grant(10, "older", "3"));
        for (int i = 0; i < 20; i++) status("WAIT", grant(20+i, "hot", "4"));
        assertEquals("not_selected:older", grant(1000000, "hot", "4").get(3));
        status("GRANTED", grant(1000000, "older", "3"));
        status("GRANTED", grant(1000000, "hot", "4"));
    }
    @Test public void heartbeatWaitIsCappedBelowLeaseAndRenewalPreservesPosition() {
        status("GRANTED", grant(0, "a", "1", 100, 10, 1000, 1, 100));
        for (int i = 1; i < 20; i++) {
            List<Object> r = grant(i*50000, "b", "2", 100, 10, 1000, 1, 100);
            status("WAIT", r); assertTrue(Long.parseLong(r.get(1).toString()) <= 50);
        }
        status("GRANTED", grant(1000000, "b", "2", 100, 10, 1000, 1, 100));
    }
    @Test public void windowSettingsMustMatchIncludingReplayAndDisabledClients() {
        RedisFairGrantLimiter first = new RedisFairGrantLimiter(pool, config().build());
        assertTrue(first.tryAcquireRequest("k", "a", "r").isGranted());
        FairGrantConfig[] conflicts = {config().slidingWindow(2000,2).build(),
            config().slidingWindow(1000,3).build(),
            FairGrantConfig.builder().keyPrefix("window:").ratePerSec(1000).burst(100).build()};
        for (FairGrantConfig c : conflicts) {
            AcquireResult r = new RedisFairGrantLimiter(pool,c).tryAcquireRequest("k","a","r");
            assertEquals(AcquireResult.Status.ERROR, r.getStatus()); assertEquals("config_mismatch",r.getDetail());
        }
    }
    @Test public void independentResourcesAndReceiptExpiryUsePublicApi() throws Exception {
        RedisFairGrantLimiter lim = new RedisFairGrantLimiter(pool, config().permitTtlMs(20).build());
        assertTrue(lim.tryAcquireRequest("a","node","r").isGranted());
        Thread.sleep(40);
        assertEquals("ok",lim.tryAcquireRequest("a","node","r").getDetail());
        assertFalse(lim.tryAcquire("a","node").isGranted());
        assertTrue(lim.tryAcquire("b","node").isGranted());
        try (Jedis j = pool.getResource()) { assertEquals(2,j.zcard(keys.window("a"))); }
    }

    @Test public void executorBusinessFailureStillConsumesStrictQuota() throws Exception {
        RedisFairGrantLimiter lim=new RedisFairGrantLimiter(pool,config().slidingWindow(10000,1).build());
        FairGrantExecutor executor=new FairGrantExecutor(lim);
        Exception failure=new Exception("commit failed");
        try { executor.tryExecute("k","a",()->{throw failure;}); fail(); }
        catch(Exception e) { assertSame(failure,e); }
        assertFalse(executor.tryExecute("k","a",()->fail("must not execute without fresh quota")).isGranted());
        assertEquals(1,size());
    }
}
