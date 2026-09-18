package io.github.longxiaoyun.fairgrant;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local fallback: each JVM uses ratePerSec / writerNodes.
 * Fairness across machines is not guaranteed; used only when Redis is unavailable.
 */
public final class LocalShareFairGrantLimiter implements FairGrantLimiter {

    private final FairGrantConfig config;
    private final ConcurrentHashMap<String, AtomicLong> nextAllowed = new ConcurrentHashMap<String, AtomicLong>();

    public LocalShareFairGrantLimiter(FairGrantConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public AcquireResult tryAcquire(String project, String table, String clientId) {
        return tryAcquire(project + ":" + table, clientId);
    }

    @Override
    public AcquireResult tryAcquire(String resourceKey, String clientId) {
        requireClient(clientId);
        String key = resourceKey == null ? "" : resourceKey.trim();
        long interval = config.localShareIntervalMs();
        AtomicLong slot = nextAllowed.get(key);
        if (slot == null) {
            AtomicLong created = new AtomicLong(0L);
            AtomicLong existing = nextAllowed.putIfAbsent(key, created);
            slot = existing == null ? created : existing;
        }
        long now = System.currentTimeMillis();
        for (;;) {
            long next = slot.get();
            if (now < next) {
                return AcquireResult.waitFor(next - now, 0D, "local_share_wait");
            }
            long newNext = now + interval;
            if (slot.compareAndSet(next, newNext)) {
                return AcquireResult.granted(0D, "local_share");
            }
        }
    }

    @Override
    public void registerPending(String resourceKey, String clientId) {
        // no-op locally
    }

    @Override
    public void clearPending(String resourceKey, String clientId) {
        if (resourceKey != null) {
            nextAllowed.remove(resourceKey.trim());
        }
    }

    @Override
    public void invalidatePermit(String resourceKey, String clientId) {
        // local share has no persistent permit
    }

    private static void requireClient(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException("clientId is blank");
        }
    }
}
