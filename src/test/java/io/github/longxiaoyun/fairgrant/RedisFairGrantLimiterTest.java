package io.github.longxiaoyun.fairgrant;

import com.github.fppt.jedismock.RedisServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration-style tests against an embedded Redis (jedis-mock).
 */
public class RedisFairGrantLimiterTest {

    private RedisServer server;
    private JedisPool pool;
    private RedisFairGrantLimiter limiter;
    private FairGrantConfig config;

    @Before
    public void setUp() throws IOException {
        server = RedisServer.newRedisServer().start();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(32);
        pool = new JedisPool(poolConfig, "127.0.0.1", server.getBindPort(), 2000);
        config = FairGrantConfig.builder()
                .keyPrefix("ut:fair:")
                .ratePerSec(5D)
                .burst(5D)
                .permitTtlMs(5_000L)
                .writerNodes(10)
                .fallbackMode(FairGrantConfig.FallbackMode.LOCAL_SHARE)
                .build();
        limiter = new RedisFairGrantLimiter(pool, config);
    }

    @After
    public void tearDown() throws IOException {
        if (limiter != null) {
            limiter.close();
        }
        if (pool != null) {
            pool.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void singleClientGetsGrant() {
        AcquireResult r = limiter.tryAcquire("proj", "tbl", "m1");
        assertEquals(AcquireResult.Status.GRANTED, r.getStatus());
        assertTrue(r.isGranted());
        assertEquals("ok", r.getDetail());
    }

    @Test
    public void existingPermitIsIdempotentAndDoesNotDoubleConsume() {
        assertEquals(AcquireResult.Status.GRANTED, limiter.tryAcquireRequest("k1", "m1", "request-1").getStatus());
        AcquireResult again = limiter.tryAcquireRequest("k1", "m1", "request-1");
        assertEquals(AcquireResult.Status.GRANTED, again.getStatus());
        assertEquals("existing_permit", again.getDetail());

        // burn remaining burst with other clients after m1 clears
        limiter.clearPending("k1", "m1");
        int grants = 0;
        for (int i = 0; i < 10; i++) {
            AcquireResult r = limiter.tryAcquire("k1", "other-" + i);
            if (r.getStatus() == AcquireResult.Status.GRANTED) {
                grants++;
                limiter.clearPending("k1", "other-" + i);
            }
        }
        // first grant consumed 1 of burst=5; idempotent retries must not consume more
        assertEquals(4, grants);
    }

    @Test
    public void invalidateThenReacquireConsumesAnotherToken() {
        assertTrue(limiter.tryAcquire("k2", "m1").isGranted());
        limiter.invalidatePermit("k2", "m1");
        assertTrue(limiter.tryAcquire("k2", "m1").isGranted());
    }

    @Test
    public void clearPendingAllowsOtherClientToWinImmediately() {
        limiter.registerPending("k3", "m1");
        AcquireResult blocked = limiter.tryAcquire("k3", "m2");
        assertEquals(AcquireResult.Status.WAIT, blocked.getStatus());
        assertTrue(blocked.getDetail(), blocked.getDetail().startsWith("not_selected"));

        limiter.clearPending("k3", "m1");
        assertTrue(limiter.tryAcquire("k3", "m2").isGranted());
    }

    @Test
    public void waitWhenOtherSelected() {
        limiter.registerPending("sel", "mA");
        AcquireResult r = limiter.tryAcquire("sel", "mB");
        assertFalse(r.isGranted());
        assertEquals(AcquireResult.Status.WAIT, r.getStatus());
        assertTrue(r.getDetail(), r.getDetail().startsWith("not_selected:mA"));
    }

    @Test
    public void projectTableConvenienceNormalizesCase() {
        assertTrue(limiter.tryAcquireRequest("MyProject:MyTable", "host-1", "req").isGranted());
        // same logical key
        AcquireResult again = limiter.tryAcquireRequest("myproject:mytable", "host-1", "req");
        assertEquals("existing_permit", again.getDetail());
    }

    @Test
    public void independentResourceKeys() {
        assertTrue(limiter.tryAcquire("a", "m1").isGranted());
        assertTrue(limiter.tryAcquire("b", "m1").isGranted());
    }

    @Test
    public void noTokenWhenBurstExhausted() {
        FairGrantConfig tight = FairGrantConfig.builder()
                .keyPrefix("ut:tight:")
                .ratePerSec(1D)
                .burst(1D)
                .permitTtlMs(5_000L)
                .writerNodes(2)
                .build();
        RedisFairGrantLimiter tightLimiter = new RedisFairGrantLimiter(pool, tight);
        try {
            assertTrue(tightLimiter.tryAcquire("only", "m1").isGranted());
            tightLimiter.invalidatePermit("only", "m1");
            tightLimiter.clearPending("only", "m1");

            AcquireResult wait = tightLimiter.tryAcquire("only", "m2");
            assertEquals(AcquireResult.Status.WAIT, wait.getStatus());
            assertTrue(wait.getDetail(), wait.getDetail().startsWith("no_token"));
            assertTrue(wait.getRetryAfterMs() >= 1L);
        } finally {
            tightLimiter.close();
        }
    }

    @Test
    public void tokenRefillsAfterWait() throws Exception {
        FairGrantConfig slow = FairGrantConfig.builder()
                .keyPrefix("ut:refill:")
                .ratePerSec(2D)
                .burst(1D)
                .permitTtlMs(5_000L)
                .writerNodes(2)
                .build();
        RedisFairGrantLimiter refillLimiter = new RedisFairGrantLimiter(pool, slow);
        try {
            assertTrue(refillLimiter.tryAcquire("r", "m1").isGranted());
            refillLimiter.clearPending("r", "m1");

            assertEquals(AcquireResult.Status.WAIT, refillLimiter.tryAcquire("r", "m2").getStatus());
            Thread.sleep(600L);
            assertTrue(refillLimiter.tryAcquire("r", "m2").isGranted());
        } finally {
            refillLimiter.close();
        }
    }

    @Test
    public void registerPendingGivesPriorityToEarlierWaiter() throws Exception {
        limiter.registerPending("reg", "latecomer");
        Thread.sleep(20L);

        // early joins later; fair pick must prefer latecomer
        AcquireResult early = limiter.tryAcquire("reg", "early");
        assertEquals(AcquireResult.Status.WAIT, early.getStatus());
        assertTrue(early.getDetail(), early.getDetail().startsWith("not_selected:latecomer"));

        AcquireResult winner = limiter.tryAcquire("reg", "latecomer");
        assertEquals(AcquireResult.Status.GRANTED, winner.getStatus());
    }

    @Test
    public void fairRotationAcrossClients() throws Exception {
        String key = "fair-rot";
        List<String> machines = new ArrayList<String>();
        for (int i = 0; i < 5; i++) {
            machines.add("m" + i);
            limiter.registerPending(key, machines.get(i));
            Thread.sleep(5L);
        }

        Set<String> firstWave = new HashSet<String>();
        for (int round = 0; round < 5; round++) {
            String winner = pollGrant(key, machines);
            assertNotNull("round " + round + " winner", winner);
            firstWave.add(winner);
            limiter.invalidatePermit(key, winner);
            limiter.registerPending(key, winner);
        }
        assertEquals(5, firstWave.size());

        AcquireResult wait = limiter.tryAcquire(key, "m0");
        assertEquals(AcquireResult.Status.WAIT, wait.getStatus());
        assertTrue(wait.getDetail(), wait.getDetail().startsWith("no_token"));

        Thread.sleep(1100L);

        Set<String> secondWave = new HashSet<String>();
        for (int round = 0; round < 5; round++) {
            String winner = pollGrant(key, machines);
            assertNotNull("refill round " + round, winner);
            secondWave.add(winner);
            limiter.invalidatePermit(key, winner);
            limiter.registerPending(key, winner);
        }
        assertEquals(5, secondWave.size());
    }

    @Test
    public void noStarvationOverManyRounds() throws Exception {
        FairGrantConfig cfg = FairGrantConfig.builder()
                .keyPrefix("ut:starve:")
                .ratePerSec(50D)
                .burst(1D)
                .permitTtlMs(5_000L)
                .writerNodes(10)
                .build();
        RedisFairGrantLimiter lim = new RedisFairGrantLimiter(pool, cfg);
        try {
            List<String> machines = new ArrayList<String>();
            Map<String, Integer> counts = new HashMap<String, Integer>();
            for (int i = 0; i < 5; i++) {
                String m = "s" + i;
                machines.add(m);
                counts.put(m, 0);
                lim.registerPending("sk", m);
                Thread.sleep(2L);
            }

            int rounds = 25;
            for (int round = 0; round < rounds; round++) {
                String winner = null;
                for (int attempt = 0; attempt < 50 && winner == null; attempt++) {
                    winner = pollGrantWith(lim, "sk", machines);
                    if (winner == null) {
                        Thread.sleep(25L);
                    }
                }
                assertNotNull("no winner at round " + round, winner);
                counts.put(winner, counts.get(winner) + 1);
                lim.invalidatePermit("sk", winner);
            }

            for (String m : machines) {
                int c = counts.get(m);
                assertTrue(m + " starved, counts=" + counts, c >= 3);
                assertTrue(m + " hogged, counts=" + counts, c <= 8);
            }
        } finally {
            lim.close();
        }
    }

    @Test
    public void concurrentClientsDoNotExceedBurst() throws Exception {
        final String key = "conc";
        final int clients = 20;
        final AtomicInteger grants = new AtomicInteger();
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(clients);
        ExecutorService es = Executors.newFixedThreadPool(clients);
        for (int i = 0; i < clients; i++) {
            final String mid = "c" + i;
            es.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();
                        AcquireResult r = limiter.tryAcquire(key, mid);
                        if (r.getStatus() == AcquireResult.Status.GRANTED) {
                            grants.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                }
            });
        }
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        es.shutdownNow();
        assertTrue("grants=" + grants.get(), grants.get() <= 5);
        assertTrue("grants=" + grants.get(), grants.get() >= 1);
    }

    @Test
    public void concurrentThroughputRespectsRateOverWindow() throws Exception {
        FairGrantConfig cfg = FairGrantConfig.builder()
                .keyPrefix("ut:tp:")
                .ratePerSec(10D)
                .burst(2D)
                .permitTtlMs(2_000L)
                .writerNodes(8)
                .build();
        final RedisFairGrantLimiter lim = new RedisFairGrantLimiter(pool, cfg);
        try {
            final AtomicInteger grants = new AtomicInteger();
            final int workers = 8;
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(workers);
            ExecutorService es = Executors.newFixedThreadPool(workers);
            final long deadline = System.currentTimeMillis() + 1000L;
            for (int i = 0; i < workers; i++) {
                final String mid = "w" + i;
                es.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            start.await();
                            while (System.currentTimeMillis() < deadline) {
                                AcquireResult r = lim.tryAcquire("tp", mid);
                                if (r.getStatus() == AcquireResult.Status.GRANTED) {
                                    grants.incrementAndGet();
                                    lim.invalidatePermit("tp", mid);
                                } else {
                                    Thread.sleep(Math.min(20L, Math.max(1L, r.getRetryAfterMs())));
                                }
                            }
                            lim.clearPending("tp", mid);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS));
            es.shutdownNow();
            // ~10/s + burst 2 over ~1s → soft upper bound with mock timing slack
            int g = grants.get();
            assertTrue("too few grants=" + g, g >= 5);
            assertTrue("too many grants=" + g, g <= 20);
        } finally {
            lim.close();
        }
    }

    @Test
    public void redisKeysAreWrittenAsDocumented() {
        assertTrue(limiter.tryAcquireRequest("proj:tbl", "host-a", "req").isGranted());
        FairGrantKeys keys = new FairGrantKeys(config.getKeyPrefix());
        try (Jedis jedis = pool.getResource()) {
            assertTrue(jedis.exists(keys.bucket("proj:tbl")));
            assertEquals(0L, jedis.zcard(keys.wait("proj:tbl")));
            assertEquals(0L, jedis.zcard(keys.pending("proj:tbl")));
            assertTrue(Long.parseLong(jedis.get(keys.permit("proj:tbl", "host-a", "req"))) > 0);
        }
    }

    @Test
    public void closedLimiterRejectsAcquire() {
        limiter.close();
        try {
            limiter.tryAcquire("x", "m1");
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // ok
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsBlankClientId() {
        limiter.tryAcquire("k", " ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsBlankResource() {
        limiter.tryAcquire("  ", "m1");
    }

    @Test
    public void factoryCreatesWorkingLimiter() {
        FairGrantConfig cfg = FairGrantConfig.builder()
                .keyPrefix("ut:factory:")
                .ratePerSec(5D)
                .burst(5D)
                .build();
        RedisFairGrantLimiter created = FairGrantLimiters.redis(pool, cfg);
        try {
            assertTrue(created.tryAcquire("f", "m1").isGranted());
        } finally {
            created.close();
        }
    }

    private String pollGrant(String key, List<String> machines) {
        return pollGrantWith(limiter, key, machines);
    }

    private static String pollGrantWith(RedisFairGrantLimiter lim, String key, List<String> machines) {
        for (String m : machines) {
            AcquireResult r = lim.tryAcquire(key, m);
            if (r.getStatus() == AcquireResult.Status.GRANTED) {
                return m;
            }
        }
        return null;
    }
}
