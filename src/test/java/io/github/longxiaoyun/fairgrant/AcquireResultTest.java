package io.github.longxiaoyun.fairgrant;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AcquireResultTest {

    @Test
    public void grantedHelpers() {
        AcquireResult r = AcquireResult.granted(3.5D, "ok");
        assertEquals(AcquireResult.Status.GRANTED, r.getStatus());
        assertTrue(r.isGranted());
        assertEquals(0L, r.getRetryAfterMs());
        assertEquals(3.5D, r.getTokens(), 0.0001);
        assertEquals("ok", r.getDetail());
    }

    @Test
    public void waitHelpersClampNegativeRetry() {
        AcquireResult r = AcquireResult.waitFor(-10L, 0.2D, "no_token");
        assertEquals(AcquireResult.Status.WAIT, r.getStatus());
        assertFalse(r.isGranted());
        assertEquals(0L, r.getRetryAfterMs());
        assertEquals(0.2D, r.getTokens(), 0.0001);
    }

    @Test
    public void degradedLocalIsGranted() {
        AcquireResult r = AcquireResult.degradedLocal(0L, "redis_down");
        assertEquals(AcquireResult.Status.DEGRADED_LOCAL, r.getStatus());
        assertTrue(r.isGranted());
        assertEquals(0L, r.getRetryAfterMs());
    }

    @Test public void degradedWaitIsNeverGranted() {
        assertFalse(AcquireResult.degradedLocal(100L, "waiting").isGranted());
        assertFalse(new AcquireResult(AcquireResult.Status.DEGRADED_LOCAL, 100, 0, "waiting").isGranted());
    }
    @Test
    public void errorDefaultsRetry() {
        AcquireResult r = AcquireResult.error("boom");
        assertEquals(AcquireResult.Status.ERROR, r.getStatus());
        assertFalse(r.isGranted());
        assertEquals(200L, r.getRetryAfterMs());
        assertEquals("boom", r.getDetail());
    }

    @Test
    public void nullDetailBecomesEmpty() {
        AcquireResult r = new AcquireResult(AcquireResult.Status.WAIT, 1L, 0D, null);
        assertEquals("", r.getDetail());
        assertTrue(r.toString().contains("WAIT"));
    }
}
