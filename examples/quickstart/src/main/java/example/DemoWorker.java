package example;

import io.github.longxiaoyun.fairgrant.*;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** One JVM represents one machine, with its own ready batches for each resource. */
public final class DemoWorker {
    private static final class Batch {
        int completed;
        int attempt;
        long nextTryNanos;
    }

    private static long micros() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000L + now.getNano() / 1000;
    }

    private static void request(String url, String method) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setConnectTimeout(2000);
        connection.setReadTimeout(2000);
        connection.setRequestMethod(method);
        try {
            int status = connection.getResponseCode();
            if (status != 204) throw new IOException("HTTP " + status);
        } finally {
            connection.disconnect();
        }
    }

    public static void main(String[] args) throws Exception {
        int redisPort = Integer.parseInt(args[0]);
        String endpoint = args[1];
        String node = args[2];
        int batches = Integer.parseInt(args[3]);
        FairGrantConfig config = FairGrantConfig.builder()
                .keyPrefix("quickstart:")
                .ratePerSec(Double.parseDouble(args[4]))
                .burst(Double.parseDouble(args[5]))
                .slidingWindow(Long.parseLong(args[6]), Integer.parseInt(args[7]))
                .pendingTtlMs(30_000)
                .build();
        Map<String, Batch> pending = new LinkedHashMap<String, Batch>();
        for (String resource : args[8].split(",")) pending.put(resource, new Batch());
        long deadline = System.nanoTime() + 90_000_000_000L;
        try (RedisFairGrantLimiter limiter = FairGrantLimiters.redis("127.0.0.1", redisPort, config)) {
            FairGrantExecutor executor = new FairGrantExecutor(limiter);
            // All batches in this demo are ready. Never register an unfinished batch.
            if (batches > 0) {
                for (String resource : pending.keySet()) limiter.registerPending(resource, node);
            }
            request(endpoint + "/ready?node=" + node, "POST");
            while (true) {
                try { request(endpoint + "/start", "GET"); break; }
                catch (IOException waiting) {
                    if (System.nanoTime() > deadline) throw waiting;
                    if (batches > 0) {
                        for (String resource : pending.keySet()) limiter.registerPending(resource, node);
                    }
                    Thread.sleep(50);
                }
            }
            while (true) {
                boolean finished = true;
                for (Map.Entry<String, Batch> entry : pending.entrySet()) {
                    String resource = entry.getKey();
                    Batch batch = entry.getValue();
                    if (batch.completed == batches) continue;
                    finished = false;
                    if (System.nanoTime() < batch.nextTryNanos) continue;
                    if (System.nanoTime() > deadline) throw new AssertionError("worker deadline: " + node);
                    String operation = node + "-" + resource + "-" + batch.completed;
                    long acquisitionStart = micros();
                    try {
                        AcquireResult result = executor.tryExecute(resource, node, () -> {
                            // Grant occurred between acquisitionStart and this callback timestamp.
                            // Only public API is used; no Redis keys or Lua are inspected.
                            System.out.println("GRANT," + resource + "," + operation + "," + batch.attempt
                                    + "," + acquisitionStart + "," + micros());
                            System.out.flush();
                            request(endpoint + "/commit?node=" + node + "&resource=" + resource
                                    + "&operation=" + operation + "&attempt=" + batch.attempt, "POST");
                        });
                        if (result.isGranted()) {
                            batch.completed++;
                            batch.attempt = 0;
                        } else if (result.getStatus() == AcquireResult.Status.ERROR) {
                            throw new IllegalStateException(result.toString());
                        } else {
                            // Keep this exact batch; other resources can still proceed.
                            batch.nextTryNanos = System.nanoTime() + result.getRetryAfterMs() * 1_000_000L;
                        }
                    } catch (IOException businessFailure) {
                        if (++batch.attempt > 3) throw businessFailure;
                        // Retry the same business batch, but acquire NEW quota for each HTTP attempt.
                        System.out.println("RETRY," + operation + "," + businessFailure.getMessage());
                        batch.nextTryNanos = System.nanoTime() + 50_000_000L;
                    }
                }
                if (finished) break;
                Thread.sleep(1); // Dedicated demo scheduler, never a Kafka poll thread.
            }
        }
    }
}
