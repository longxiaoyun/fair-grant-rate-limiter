package io.github.longxiaoyun.fairgrant;

import org.junit.Test;
import redis.clients.jedis.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.Assert.*;

/** Repeats exact Lua contract tests on real Redis, plus production-clock scenarios. */
public class SlidingWindowIT extends SlidingWindowTest {
    private RealRedisProcess redis;
    @Override protected void startBackend() throws Exception {
        redis = new RealRedisProcess(); redis.start(); pool = new JedisPool("127.0.0.1",redis.port);
    }
    @Override protected void stopBackend() throws Exception { if(redis != null) redis.close(); }
    @Test public void thirtyConcurrentNodesShareExactlySeventyFivePermits() throws Exception {
        RedisFairGrantLimiter lim = new RedisFairGrantLimiter(pool,
                config().burst(1000).slidingWindow(15000,75).build());
        ExecutorService workers = Executors.newFixedThreadPool(30);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Integer>> results = new ArrayList<Future<Integer>>();
        try {
            for(int n=0;n<30;n++) {
                final String node="node-"+n;
                lim.registerPending("tableA",node);
                results.add(workers.submit(() -> {
                    start.await(); int granted=0;
                    // Each node stays ready; retries continuously for 2 seconds, within one 15s window.
                    long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);
                    while(System.nanoTime()<end) {
                        AcquireResult r=lim.tryAcquire("tableA",node);
                        assertNotEquals(AcquireResult.Status.ERROR,r.getStatus());
                        if(r.isGranted()) granted++; else Thread.sleep(1);
                    }
                    return granted;
                }));
            }
            start.countDown(); int total=0;
            for(Future<Integer> f:results) { int n=f.get(10,TimeUnit.SECONDS); assertTrue("node progress",n>0); total+=n; }
            assertEquals(75,total);
            assertTrue(lim.tryAcquire("tableB","independent").isGranted());
            try(Jedis j=pool.getResource()) { assertEquals(75,j.zcard(keys.window("tablea"))); }
            System.out.println("WINDOW: 30 concurrent nodes, 75 total grants/15s, all nodes progressed, tableB independent");
        } finally { workers.shutdownNow(); workers.awaitTermination(5,TimeUnit.SECONDS); }
    }
    @Test public void fivePerSecondBurstCannotProduceSeventySixGrantsBeforeFifteenSeconds() throws Exception {
        RedisFairGrantLimiter lim = new RedisFairGrantLimiter(pool,
                config().ratePerSec(5).burst(5).slidingWindow(15000,75).build());
        List<Long> times=new ArrayList<Long>(); long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(22);
        for(int n=0;n<76;n++) {
            AcquireResult r;
            do {
                r=lim.tryAcquireRequest("table","node","batch-"+n);
                assertNotEquals(AcquireResult.Status.ERROR,r.getStatus());
                assertTrue("scenario deadline",System.nanoTime()<deadline);
                if(!r.isGranted()) Thread.sleep(r.getRetryAfterMs());
            } while(!r.isGranted());
            try(Jedis j=pool.getResource()) { times.add(Long.parseLong(j.get(keys.permit("table","node","batch-"+n)))); }
        }
        assertTrue(times.get(75)-times.get(0)>=15_000_000L);
        System.out.println("WINDOW: rate=5 burst=5, grant76 after "+(times.get(75)-times.get(0))/1000D+"ms");
    }
    @Test public void strictWindowDeniesDuringRedisOutage() throws Exception {
        RedisFairGrantLimiter lim=new RedisFairGrantLimiter(pool,config().build());
        assertTrue(lim.tryAcquire("k","a").isGranted()); redis.close();
        assertFalse(lim.tryAcquire("k","a").isGranted()); lim.clearPending("k","a");
    }
}
