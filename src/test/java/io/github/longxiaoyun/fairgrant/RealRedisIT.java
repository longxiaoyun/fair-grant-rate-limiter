package io.github.longxiaoyun.fairgrant;

import org.junit.Test;
import redis.clients.jedis.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.Assert.*;

/** Runs the contract regressions plus Redis lifecycle and multi-JVM HTTP scenarios. */
public class RealRedisIT extends RedisRegressionTest {
    private RealRedisProcess redis;
    @Override protected void startBackend() throws Exception {
        redis = new RealRedisProcess();
        redis.start();
        pool = new JedisPool("127.0.0.1", redis.port);
    }
    @Override protected void stopBackend() throws Exception { if (redis != null) redis.close(); }

    @Test public void redisOutageDenyAndRecovery() throws Exception {
        RedisFairGrantLimiter lim = limiter(config());
        assertTrue(lim.tryAcquire("k", "a").isGranted());
        redis.close();
        assertFalse(lim.tryAcquire("k", "a").isGranted());
        redis.start();
        // Old pooled connections can fail once after restart; retry through the public API.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        AcquireResult result;
        do {
            result = lim.tryAcquire("k", "a");
            if (result.isGranted()) break;
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        assertEquals(AcquireResult.Status.GRANTED, result.getStatus());
    }
    @Test public void redisOutageLocalShareStillDeniesRepeatedActions() throws Exception {
        RedisFairGrantLimiter lim = limiter(config().ratePerSec(2).writerNodes(1)
                .fallbackMode(FairGrantConfig.FallbackMode.LOCAL_SHARE));
        redis.close();
        assertEquals(AcquireResult.Status.DEGRADED_LOCAL, lim.tryAcquire("k", "a").getStatus());
        lim.clearPending("k", "a");
        assertFalse(lim.tryAcquire("k", "a").isGranted());
        Thread.sleep(550);
        assertTrue(lim.tryAcquire("k", "a").isGranted());
    }
    @Test public void redisOutageAllowIsExplicit() throws Exception {
        RedisFairGrantLimiter lim = limiter(config().fallbackMode(FairGrantConfig.FallbackMode.ALLOW));
        redis.close();
        assertTrue(lim.tryAcquire("k", "a").isGranted());
        assertTrue(lim.tryAcquire("k", "a").isGranted());
    }
    @Test public void fourJvmsExecuteFortyDistinctHttpActionsWithinSharedBudget() throws Exception {
        final List<String> actions = Collections.synchronizedList(new ArrayList<String>());
        HttpServer http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/commit", exchange -> {
            actions.add(exchange.getRequestURI().getQuery());
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        http.start();
        List<Process> workers = new ArrayList<Process>();
        List<Path> logs = new ArrayList<Path>();
        try {
            String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            for (int i = 0; i < 4; i++) {
                Path log = Files.createTempFile(Paths.get("target"), "worker-" + i, ".log");
                logs.add(log);
                workers.add(new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp", cp,
                        EndToEndWorker.class.getName(), Integer.toString(redis.port), "worker-" + i,
                        "http://127.0.0.1:" + http.getAddress().getPort() + "/commit")
                        .redirectErrorStream(true).redirectOutput(log.toFile()).start());
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            try (Jedis j = pool.getResource()) {
                while (j.scard("e2e:ready") < 4 && System.nanoTime() < deadline) Thread.sleep(20);
                assertEquals("all JVMs ready", 4, j.scard("e2e:ready"));
                long started = System.nanoTime();
                j.set("e2e:go", "1");
                for (int i = 0; i < workers.size(); i++) {
                    assertTrue("worker deadline", workers.get(i).waitFor(25, TimeUnit.SECONDS));
                    assertEquals(new String(Files.readAllBytes(logs.get(i)), java.nio.charset.StandardCharsets.UTF_8),
                            0, workers.get(i).exitValue());
                }
                double elapsed = (System.nanoTime() - started) / 1e9;
                assertEquals(40, actions.size());
                assertEquals("no duplicated business actions", 40, new HashSet<String>(actions).size());
                assertTrue("token envelope: actions=" + actions.size() + " seconds=" + elapsed,
                        actions.size() <= 2 + 20 * elapsed + .05);
                for (int i = 0; i < 4; i++) {
                    final String client = "worker-" + i;
                    assertEquals(10, actions.stream().filter(a -> a.startsWith(client + "-")).count());
                }
                FairGrantKeys k = new FairGrantKeys("e2e:");
                List<Long> grantTimes = new ArrayList<Long>();
                Map<String, Long> firstGrants = new HashMap<String, Long>();
                for (int worker = 0; worker < 4; worker++) {
                    String client = "worker-" + worker;
                    for (int batch = 0; batch < 10; batch++) {
                        long at = Long.parseLong(j.get(k.permit("table", client, client + "-batch-" + batch)));
                        grantTimes.add(at);
                        if (batch == 0) firstGrants.put(client, at);
                    }
                }
                Collections.sort(grantTimes);
                // Check EVERY interval between grant timestamps, not just total runtime.
                for (int begin = 0; begin < grantTimes.size(); begin++) {
                    for (int end = begin; end < grantTimes.size(); end++) {
                        double allowed = 2 + 20 * (grantTimes.get(end) - grantTimes.get(begin)) / 1000D;
                        assertTrue("window " + begin + ".." + end + " budget=" + allowed,
                                end - begin + 1 <= allowed + .001);
                    }
                }
                for (long first : firstGrants.values()) assertTrue(first <= grantTimes.get(3));
                assertEquals(0, j.zcard(k.wait("table")));
                assertEquals(0, j.zcard(k.pending("table")));
                System.out.println("E2E: 4 JVMs, 40 unique HTTP commits, 40 receipt replays, elapsed=" + elapsed + "s");
            }
        } finally {
            for (Process p : workers) if (p.isAlive()) { p.destroyForcibly(); p.waitFor(5, TimeUnit.SECONDS); }
            http.stop(0);
        }
    }
}
