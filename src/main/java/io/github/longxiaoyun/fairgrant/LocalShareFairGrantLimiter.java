package io.github.longxiaoyun.fairgrant;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Best-effort per-instance fallback. Cannot enforce a shared quota during partitions. */
public final class LocalShareFairGrantLimiter implements FairGrantLimiter {
    private final FairGrantConfig config;
    private final FairGrantKeys keys;
    private final LongSupplier clock;
    private final ConcurrentHashMap<String, State> states = new ConcurrentHashMap<String, State>();
    private static final class State {
        boolean granted;
        long last;
        final Map<String, Long> receipts = new HashMap<String, Long>();
    }
    public LocalShareFairGrantLimiter(FairGrantConfig config) { this(config, System::nanoTime); }
    LocalShareFairGrantLimiter(FairGrantConfig config, LongSupplier clock) {
        this.config = Objects.requireNonNull(config, "config");
        if (config.hasSlidingWindow()) {
            throw new IllegalArgumentException("LocalShare cannot enforce a distributed sliding window");
        }
        this.keys = new FairGrantKeys(config.getKeyPrefix());
        this.clock = clock;
    }
    @Override public AcquireResult tryAcquire(String project, String table, String client) {
        return tryAcquire(keys.resourceKey(project, table), client);
    }
    @Override public AcquireResult tryAcquire(String resource, String client) {
        return tryAcquireRequest(resource, client, UUID.randomUUID().toString());
    }
    @Override public AcquireResult tryAcquireRequest(String resource, String client, String request) {
        String key = keys.normalizeResource(resource);
        String receipt = keys.permit(key, FairGrantKeys.requireId(client, "clientId"),
                FairGrantKeys.requireId(request, "requestId"));
        State state = states.computeIfAbsent(key, ignored -> new State());
        synchronized (state) {
            long now = clock.getAsLong();
            long ttl = TimeUnit.MILLISECONDS.toNanos(config.getPermitTtlMs());
            Iterator<Long> it = state.receipts.values().iterator();
            while (it.hasNext()) if (now - it.next() >= ttl) it.remove();
            if (state.receipts.containsKey(receipt)) return AcquireResult.granted(0, "existing_permit");
            long interval = TimeUnit.MILLISECONDS.toNanos(config.localShareIntervalMs());
            long elapsed = now - state.last;
            if (state.granted && elapsed < interval) {
                long left = interval - elapsed;
                return AcquireResult.waitFor(1 + (left - 1) / 1_000_000, 0, "local_share_wait");
            }
            state.granted = true;
            state.last = now;
            state.receipts.put(receipt, now);
            return AcquireResult.granted(0, "local_share");
        }
    }
    @Override public void registerPending(String resource, String client) { validate(resource, client); }
    @Override public void clearPending(String resource, String client) { validate(resource, client); }
    @Deprecated
    @Override public void invalidatePermit(String resource, String client) { validate(resource, client); }
    private void validate(String resource, String client) {
        keys.normalizeResource(resource);
        FairGrantKeys.requireId(client, "clientId");
    }
}
