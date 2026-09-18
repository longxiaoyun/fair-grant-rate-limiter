package io.github.longxiaoyun.fairgrant;

import com.github.fppt.jedismock.RedisServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies Redis-down fallback modes without requiring a live Redis after pool close.
 */
public class RedisFairGrantFallbackTest {

    private RedisServer server;
    private JedisPool pool;

    @Before
    public void setUp() throws IOException {
        server = RedisServer.newRedisServer().start();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(8);
        pool = new JedisPool(poolConfig, "127.0.0.1", server.getBindPort(), 500);
    }

    @After
    public void tearDown() throws IOException {
        if (pool != null) {
            pool.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void localShareFallbackWhenPoolClosed() {
        FairGrantConfig config = FairGrantConfig.builder()
                .keyPrefix("ut:fb:local:")
                .ratePerSec(5D)
                .writerNodes(5)
                .fallbackMode(FairGrantConfig.FallbackMode.LOCAL_SHARE)
                .build();
        RedisFairGrantLimiter limiter = new RedisFairGrantLimiter(pool, config);
        pool.close();

        AcquireResult r = limiter.tryAcquire("k", "m1");
        assertEquals(AcquireResult.Status.DEGRADED_LOCAL, r.getStatus());
        assertTrue(r.isGranted());
        assertTrue(r.getDetail(), r.getDetail().startsWith("redis_down"));

        AcquireResult wait = limiter.tryAcquire("k", "m1");
        // second call within local interval should wait (still degraded path)
        assertTrue(wait.getStatus() == AcquireResult.Status.WAIT
                || wait.getStatus() == AcquireResult.Status.DEGRADED_LOCAL);
        limiter.close();
    }

    @Test
    public void denyFallbackWhenPoolClosed() {
        FairGrantConfig config = FairGrantConfig.builder()
                .keyPrefix("ut:fb:deny:")
                .fallbackMode(FairGrantConfig.FallbackMode.DENY)
                .redisTimeoutMs(150)
                .build();
        RedisFairGrantLimiter limiter = new RedisFairGrantLimiter(pool, config);
        pool.close();

        AcquireResult r = limiter.tryAcquire("k", "m1");
        assertEquals(AcquireResult.Status.WAIT, r.getStatus());
        assertFalse(r.isGranted());
        assertTrue(r.getDetail(), r.getDetail().startsWith("redis_down_deny"));
        assertEquals(150L, r.getRetryAfterMs());
        limiter.close();
    }

    @Test
    public void allowFallbackWhenPoolClosed() {
        FairGrantConfig config = FairGrantConfig.builder()
                .keyPrefix("ut:fb:allow:")
                .fallbackMode(FairGrantConfig.FallbackMode.ALLOW)
                .build();
        RedisFairGrantLimiter limiter = new RedisFairGrantLimiter(pool, config);
        pool.close();

        AcquireResult r = limiter.tryAcquire("k", "m1");
        assertEquals(AcquireResult.Status.DEGRADED_LOCAL, r.getStatus());
        assertTrue(r.isGranted());
        assertTrue(r.getDetail(), r.getDetail().startsWith("redis_down_allow"));
        limiter.close();
    }
}
