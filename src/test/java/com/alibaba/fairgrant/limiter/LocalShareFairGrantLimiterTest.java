package com.alibaba.fairgrant.limiter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalShareFairGrantLimiterTest {

    @Test
    public void grantsThenWaitsWithinInterval() {
        FairGrantConfig config = FairGrantConfig.builder()
                .ratePerSec(5D)
                .writerNodes(5)
                .build();
        LocalShareFairGrantLimiter limiter = new LocalShareFairGrantLimiter(config);

        AcquireResult first = limiter.tryAcquire("p:t", "m1");
        assertTrue(first.isGranted());
        assertEquals(AcquireResult.Status.GRANTED, first.getStatus());
        assertEquals("local_share", first.getDetail());

        AcquireResult second = limiter.tryAcquire("p:t", "m1");
        assertFalse(second.isGranted());
        assertEquals(AcquireResult.Status.WAIT, second.getStatus());
        assertTrue(second.getRetryAfterMs() > 0);
        assertTrue(second.getRetryAfterMs() <= 1000L);
    }

    @Test
    public void differentKeysIndependent() {
        FairGrantConfig config = FairGrantConfig.builder()
                .ratePerSec(1D)
                .writerNodes(1)
                .build();
        LocalShareFairGrantLimiter limiter = new LocalShareFairGrantLimiter(config);
        assertTrue(limiter.tryAcquire("a", "m1").isGranted());
        assertTrue(limiter.tryAcquire("b", "m1").isGranted());
        assertFalse(limiter.tryAcquire("a", "m1").isGranted());
    }

    @Test
    public void projectTableOverload() {
        FairGrantConfig config = FairGrantConfig.builder()
                .ratePerSec(10D)
                .writerNodes(1)
                .build();
        LocalShareFairGrantLimiter limiter = new LocalShareFairGrantLimiter(config);
        assertTrue(limiter.tryAcquire("proj", "tbl", "m1").isGranted());
    }

    @Test
    public void clearPendingResetsSlot() {
        FairGrantConfig config = FairGrantConfig.builder()
                .ratePerSec(1D)
                .writerNodes(1)
                .build();
        LocalShareFairGrantLimiter limiter = new LocalShareFairGrantLimiter(config);
        assertTrue(limiter.tryAcquire("k", "m1").isGranted());
        assertFalse(limiter.tryAcquire("k", "m1").isGranted());
        limiter.clearPending("k", "m1");
        assertTrue(limiter.tryAcquire("k", "m1").isGranted());
    }

    @Test
    public void registerAndInvalidateAreNoOps() {
        FairGrantConfig config = FairGrantConfig.builder().ratePerSec(10D).writerNodes(1).build();
        LocalShareFairGrantLimiter limiter = new LocalShareFairGrantLimiter(config);
        limiter.registerPending("k", "m1");
        limiter.invalidatePermit("k", "m1");
        assertTrue(limiter.tryAcquire("k", "m1").isGranted());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsBlankClient() {
        FairGrantConfig config = FairGrantConfig.builder().build();
        new LocalShareFairGrantLimiter(config).tryAcquire("k", "  ");
    }
}
