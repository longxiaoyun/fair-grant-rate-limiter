package io.github.longxiaoyun.fairgrant;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FairGrantConfigTest {

    @Test
    public void defaults() {
        FairGrantConfig c = FairGrantConfig.builder().build();
        assertEquals("fair:grant:", c.getKeyPrefix());
        assertEquals(5D, c.getRatePerSec(), 0.0001);
        assertEquals(5D, c.getBurst(), 0.0001);
        assertEquals(20_000L, c.getPermitTtlMs());
        assertEquals(10, c.getWriterNodes());
        assertEquals(FairGrantConfig.FallbackMode.LOCAL_SHARE, c.getFallbackMode());
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
}
