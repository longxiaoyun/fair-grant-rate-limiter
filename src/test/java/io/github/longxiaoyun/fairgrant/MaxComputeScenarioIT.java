package io.github.longxiaoyun.fairgrant;

import org.junit.Test;
import redis.clients.jedis.JedisPool;
import java.util.*;
import static org.junit.Assert.*;

/** Real Redis, 30 logical consumers, synthetic mixed-table message delivery.
 * Does not start Kafka or call an ODPS SDK. Each token represents one Commit attempt.
 */
public class MaxComputeScenarioIT {
    @Test public void readyBatchesShareTableTokensWithoutSharingLocalData() throws Exception {
        try (RealRedisProcess redis = new RealRedisProcess()) {
            redis.start();
            try (JedisPool pool = new JedisPool("127.0.0.1", redis.port)) {
                FairGrantConfig config = FairGrantConfig.builder().keyPrefix("odps-scenario:")
                        .ratePerSec(5).burst(1)
                        // This deterministic simulation visits clients sequentially, without
                        // their production schedulers/heartbeats. Keep their leases alive.
                        .pendingTtlMs(30000).build();
                RedisFairGrantLimiter limiter = new RedisFairGrantLimiter(pool, config);
                List<Map<String, List<Integer>>> localBuffers = new ArrayList<Map<String, List<Integer>>>();
                for (int node = 0; node < 30; node++) {
                    Map<String, List<Integer>> buffers = new HashMap<String, List<Integer>>();
                    buffers.put("demo:tableA", new ArrayList<Integer>());
                    buffers.put("demo:tableB", new ArrayList<Integer>());
                    localBuffers.add(buffers);
                }
                // One mixed stream, two tables, each consumer receives two rows per table.
                for (int offset = 0; offset < 120; offset++) {
                    int node = offset / 2 % 30;
                    String table = offset % 2 == 0 ? "demo:tableA" : "demo:tableB";
                    localBuffers.get(node).get(table).add(offset);
                }
                for (int node = 0; node < 30; node++) limiter.registerPending("demo:tableA", "node-" + node);
                List<String> commits = new ArrayList<String>();
                for (int node = 0; node < 30; node++) {
                    String client = "node-" + node;
                    long deadline = System.nanoTime() + 3_000_000_000L;
                    AcquireResult result;
                    do {
                        result = limiter.tryAcquire("demo:tableA", client);
                        assertNotEquals(AcquireResult.Status.ERROR, result.getStatus());
                        if (result.isGranted()) break;
                        Thread.sleep(Math.max(1, result.getRetryAfterMs()));
                    } while (System.nanoTime() < deadline);
                    assertTrue(client + " failed to get its turn: " + result, result.isGranted());
                    assertEquals(2, localBuffers.get(node).get("demo:tableA").size());
                    commits.add(client); // Simulate one successful Commit for this local batch.
                    localBuffers.get(node).get("demo:tableA").clear();
                    if (node == 0) {
                        limiter.registerPending("demo:tableA", client);
                        Thread.sleep(230); // Ensure a token exists: test fairness, not token exhaustion.
                        for (int attempt = 0; attempt < 20; attempt++) {
                            AcquireResult hot = limiter.tryAcquire("demo:tableA", client);
                            assertFalse(hot.isGranted());
                            assertEquals("not_selected:node-1", hot.getDetail());
                        }
                        assertTrue("tableB quota is independent", limiter.tryAcquire("demo:tableB", client).isGranted());
                        localBuffers.get(node).get("demo:tableB").clear();
                        limiter.clearPending("demo:tableA", client); // Cancel the extra queued attempt.
                    }
                }
                assertEquals(30, new HashSet<String>(commits).size());
                for (int node = 0; node < 30; node++) {
                    assertEquals("node-" + node, commits.get(node));
                    assertTrue(localBuffers.get(node).get("demo:tableA").isEmpty());
                    assertEquals(node == 0 ? 0 : 2, localBuffers.get(node).get("demo:tableB").size());
                }
                System.out.println("ODPS_SCENARIO: 30 logical consumers; 60 tableA rows in 30 local batches; "
                        + "FIFO passed; 20 hot-node retries did not leapfrog; tableB independent. "
                        + "Synthetic messages/commits, no Kafka broker or ODPS SDK.");
            }
        }
    }
}
