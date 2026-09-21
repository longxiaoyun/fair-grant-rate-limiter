package io.github.longxiaoyun.fairgrant;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FairGrantConfigTest {

    @Test public void fractionalRateGetsUsableBurst() {
        assertEquals(1D, FairGrantConfig.builder().ratePerSec(.5).build().getBurst(), 0D);
    }
    @Test(expected = IllegalArgumentException.class) public void rejectsNaNRate() {
        FairGrantConfig.builder().ratePerSec(Double.NaN);
    }
    @Test(expected = IllegalArgumentException.class) public void rejectsInfiniteRate() {
        FairGrantConfig.builder().ratePerSec(Double.POSITIVE_INFINITY);
    }
    @Test(expected = IllegalArgumentException.class) public void rejectsTinyBurst() {
        FairGrantConfig.builder().burst(.5);
    }
    @Test(expected = IllegalArgumentException.class) public void rejectsNaNBurst() {
        FairGrantConfig.builder().burst(Double.NaN);
    }
    @Test(expected = IllegalArgumentException.class) public void rejectsBadLease() {
        FairGrantConfig.builder().pendingTtlMs(0);
    }
    @Test
    public void defaults() {
        FairGrantConfig c = FairGrantConfig.builder().build();
        assertEquals("fair:grant:", c.getKeyPrefix());
        assertEquals(5D, c.getRatePerSec(), 0.0001);
        assertEquals(5D, c.getBurst(), 0.0001);
        assertEquals(20_000L, c.getPermitTtlMs());
        assertEquals(10, c.getWriterNodes());
        assertEquals(FairGrantConfig.FallbackMode.DENY, c.getFallbackMode());
        assertEquals(200, c.getRedisTimeoutMs());
    }

    @Test
    public void burstDefaultsToRateWhenUnset() {
        FairGrantConfig c = FairGrantConfig.builder().ratePerSec(3D).build();
        assertEquals(3D, c.getBurst(), 0.0001);
    }

    @Test
    public void customBurstPreserved() {
        FairGrantConfig c = FairGrantConfig.builder().ratePerSec(5D).burst(10D).build();
        assertEquals(10D, c.getBurst(), 0.0001);
    }

    @Test
    public void localShareIntervalMsForManyWriters() {
        FairGrantConfig config = FairGrantConfig.builder()
                .ratePerSec(5D)
                .writerNodes(35)
                .build();
        long interval = config.localShareIntervalMs();
        assertTrue("interval=" + interval, interval >= 6900 && interval <= 7100);
    }

    @Test
    public void localShareIntervalMsSingleWriter() {
        FairGrantConfig config = FairGrantConfig.builder()
                .ratePerSec(5D)
                .writerNodes(1)
                .build();
        assertEquals(200L, config.localShareIntervalMs());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonPositiveRate() {
        FairGrantConfig.builder().ratePerSec(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonPositivePermitTtl() {
        FairGrantConfig.builder().permitTtlMs(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonPositiveWriterNodes() {
        FairGrantConfig.builder().writerNodes(0).build();
    }

    @Test(expected = NullPointerException.class)
    public void rejectsNullFallback() {
        FairGrantConfig.builder().fallbackMode(null).build();
    }

    @Test(expected = NullPointerException.class)
    public void rejectsNullKeyPrefix() {
        FairGrantConfig.builder().keyPrefix(null).build();
    }

    @Test public void windowDefaultsAndExplicitConfiguration() {
        assertFalse(FairGrantConfig.builder().build().hasSlidingWindow());
        FairGrantConfig c=FairGrantConfig.builder().slidingWindow(15000,75).build();
        assertTrue(c.hasSlidingWindow()); assertEquals(15000,c.getWindowMs()); assertEquals(75,c.getWindowMaxPermits());
    }
    @Test public void invalidWindowsAndUnsafeFallbacksAreRejected() {
        Runnable[] invalid={
            () -> FairGrantConfig.builder().slidingWindow(0,1),
            () -> FairGrantConfig.builder().slidingWindow(-1,1),
            () -> FairGrantConfig.builder().slidingWindow(1,0),
            () -> FairGrantConfig.builder().slidingWindow(1,-1),
            () -> FairGrantConfig.builder().slidingWindow(Long.MAX_VALUE,1).build(),
            () -> FairGrantConfig.builder().slidingWindow(1,1).fallbackMode(FairGrantConfig.FallbackMode.ALLOW).build(),
            () -> FairGrantConfig.builder().fallbackMode(FairGrantConfig.FallbackMode.LOCAL_SHARE).slidingWindow(1,1).build(),
            () -> new LocalShareFairGrantLimiter(FairGrantConfig.builder().slidingWindow(1,1).build()),
            () -> FairGrantConfig.builder().ratePerSec(Double.MIN_VALUE).build(),
            () -> FairGrantConfig.builder().burst(1e16).build()
        };
        for(Runnable r:invalid) { try { r.run(); org.junit.Assert.fail("accepted unsafe config"); }
            catch(IllegalArgumentException expected) { } }
    }

    @Test public void stateRetentionIncludesRefillWindowAndReceiptHorizons() {
        FairGrantConfig c = FairGrantConfig.builder().ratePerSec(.5).burst(2)
                .stateIdleTtlMs(10).pendingTtlMs(20).permitTtlMs(30).slidingWindow(1000, 2).build();
        assertEquals(4001, c.getEffectiveStateTtlMs());
        assertFalse(c.isRedisTestOnBorrow());
        assertTrue(FairGrantConfig.builder().redisTestOnBorrow(true).build().isRedisTestOnBorrow());
    }
    @Test(expected = IllegalArgumentException.class) public void invalidIdleRetentionRejected() {
        FairGrantConfig.builder().stateIdleTtlMs(0);
    }
    @Test(expected = IllegalArgumentException.class) public void unrepresentableRefillHorizonRejected() {
        FairGrantConfig.builder().ratePerSec(.001).burst(9007199254740991D).build();
    }
}
