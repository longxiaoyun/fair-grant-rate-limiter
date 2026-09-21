package io.github.longxiaoyun.fairgrant;

import redis.clients.jedis.*;
import java.net.*;
import java.io.*;
import java.util.concurrent.TimeUnit;

/** Separate JVM: queues work, acquires/retries, then calls a protected HTTP endpoint. */
public final class EndToEndWorker {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        String client = args[1];
        String endpoint = args[2];
        FairGrantConfig config = FairGrantConfig.builder().keyPrefix("e2e:")
                .ratePerSec(20).burst(2).slidingWindow(500, 6).pendingTtlMs(10000).build();
        try (JedisPool pool = new JedisPool("127.0.0.1", port);
             RedisFairGrantLimiter limiter = new RedisFairGrantLimiter(pool, config);
             Jedis coordination = pool.getResource()) {
            limiter.registerPending("table", client);
            coordination.sadd("e2e:ready", client);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
            while (!coordination.exists("e2e:go")) {
                if (System.nanoTime() > deadline) throw new AssertionError("start barrier timed out");
                Thread.sleep(10);
            }
            for (int i = 0; i < 10; i++) {
                String request = client + "-batch-" + i;
                AcquireResult result;
                do {
                    result = limiter.tryAcquireRequest("table", client, request);
                    if (result.getStatus() == AcquireResult.Status.ERROR) throw new AssertionError(result);
                    if (!result.isGranted()) Thread.sleep(Math.max(1, result.getRetryAfterMs()));
                    if (System.nanoTime() > deadline) throw new AssertionError("acquire timed out: " + result);
                } while (!result.isGranted());
                // Simulate retry of an ambiguous Redis response before executing the batch.
                AcquireResult replay = limiter.tryAcquireRequest("table", client, request);
                if (!replay.isGranted() || !"existing_permit".equals(replay.getDetail())) throw new AssertionError(replay);
                if (i < 9) limiter.registerPending("table", client);
                HttpURLConnection connection = (HttpURLConnection) new URL(endpoint + "?" + request).openConnection();
                connection.setRequestMethod("POST");
                connection.setConnectTimeout(3000);
                connection.setReadTimeout(3000);
                try {
                    if (connection.getResponseCode() != 204) throw new AssertionError("protected action failed");
                } finally { connection.disconnect(); }
            }
            limiter.clearPending("table", client);
        }
    }
}
