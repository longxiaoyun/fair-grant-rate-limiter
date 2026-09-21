package io.github.longxiaoyun.fairgrant;

import java.util.Objects;

/**
 * Immutable configuration for {@link FairGrantLimiter}.
 */
public final class FairGrantConfig {

    public enum FallbackMode {
        /** On Redis failure, use local rate = ratePerSec / writerNodes. */
        LOCAL_SHARE,
        /** On Redis failure, deny with WAIT (safe but may stall writers). */
        DENY,
        /** On Redis failure, allow (not recommended in production). */
        ALLOW
    }

    private final String keyPrefix;
    private final double ratePerSec;
    private final double burst;
    private final long permitTtlMs;
    private final int writerNodes;
    private final long pendingTtlMs;
    private final FallbackMode fallbackMode;
    private final int redisTimeoutMs;
    private final long windowMs;
    private final int windowMaxPermits;

    private FairGrantConfig(Builder b) {
        this.keyPrefix = b.keyPrefix;
        this.ratePerSec = b.ratePerSec;
        this.burst = b.burst > 0D ? b.burst : Math.max(1D, b.ratePerSec);
        this.permitTtlMs = b.permitTtlMs;
        this.writerNodes = b.writerNodes;
        this.pendingTtlMs = b.pendingTtlMs;
        this.fallbackMode = b.fallbackMode;
        this.redisTimeoutMs = b.redisTimeoutMs;
        this.windowMs = b.windowMs;
        this.windowMaxPermits = b.windowMaxPermits;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public double getRatePerSec() {
        return ratePerSec;
    }

    public double getBurst() {
        return burst;
    }

    public long getPermitTtlMs() {
        return permitTtlMs;
    }

    /** Waiting clients must retry or register before this lease expires. */
    public long getPendingTtlMs() { return pendingTtlMs; }

    public int getWriterNodes() {
        return writerNodes;
    }

    public FallbackMode getFallbackMode() {
        return fallbackMode;
    }

    public int getRedisTimeoutMs() {
        return redisTimeoutMs;
    }

    /** Optional rolling window duration; zero means disabled. */
    public long getWindowMs() { return windowMs; }

    /** Maximum NEW grants in (now - windowMs, now]. */
    public int getWindowMaxPermits() { return windowMaxPermits; }

    public boolean hasSlidingWindow() { return windowMaxPermits > 0; }

    /** Local-share interval when Redis is down: rate/N. */
    public long localShareIntervalMs() {
        double perNode = ratePerSec / Math.max(1, writerNodes);
        if (perNode <= 0D) {
            return Long.MAX_VALUE;
        }
        return Math.max(1L, (long) Math.ceil(1000D / perNode));
    }

    public static final class Builder {
        private String keyPrefix = "fair:grant:";
        private double ratePerSec = 5D;
        private double burst = -1D;
        private long permitTtlMs = 20_000L;
        private int writerNodes = 10;
        private long pendingTtlMs = 5_000L;
        private FallbackMode fallbackMode = FallbackMode.DENY;
        private int redisTimeoutMs = 200;
        private long windowMs;
        private int windowMaxPermits;

        /**
         * Add a strict rolling-window gate alongside the token bucket. Requires DENY
         * fallback: a local instance cannot enforce a distributed window during outages.
         */
        public Builder slidingWindow(long windowMs, int maxPermits) {
            if (windowMs <= 0 || maxPermits <= 0) {
                throw new IllegalArgumentException("windowMs and maxPermits must be > 0");
            }
            this.windowMs = windowMs;
            this.windowMaxPermits = maxPermits;
            return this;
        }

        public Builder keyPrefix(String keyPrefix) {
            this.keyPrefix = Objects.requireNonNull(keyPrefix, "keyPrefix");
            return this;
        }

        public Builder ratePerSec(double ratePerSec) {
            if (!Double.isFinite(ratePerSec) || ratePerSec <= 0D) {
                throw new IllegalArgumentException("ratePerSec must be > 0");
            }
            this.ratePerSec = ratePerSec;
            return this;
        }

        public Builder burst(double burst) {
            if (!Double.isFinite(burst) || burst < 1D) {
                throw new IllegalArgumentException("burst must be finite and >= 1");
            }
            this.burst = burst;
            return this;
        }

        public Builder permitTtlMs(long permitTtlMs) {
            if (permitTtlMs <= 0L) {
                throw new IllegalArgumentException("permitTtlMs must be > 0");
            }
            this.permitTtlMs = permitTtlMs;
            return this;
        }

        public Builder pendingTtlMs(long pendingTtlMs) {
            if (pendingTtlMs <= 0L) throw new IllegalArgumentException("pendingTtlMs must be > 0");
            this.pendingTtlMs = pendingTtlMs;
            return this;
        }

        public Builder writerNodes(int writerNodes) {
            if (writerNodes <= 0) {
                throw new IllegalArgumentException("writerNodes must be > 0");
            }
            this.writerNodes = writerNodes;
            return this;
        }

        public Builder fallbackMode(FallbackMode fallbackMode) {
            this.fallbackMode = Objects.requireNonNull(fallbackMode, "fallbackMode");
            return this;
        }

        public Builder redisTimeoutMs(int redisTimeoutMs) {
            if (redisTimeoutMs <= 0) {
                throw new IllegalArgumentException("redisTimeoutMs must be > 0");
            }
            this.redisTimeoutMs = redisTimeoutMs;
            return this;
        }

        public FairGrantConfig build() {
            FairGrantConfig config = new FairGrantConfig(this);
            if (config.getBurst() > 9_007_199_254_740_991D) {
                throw new IllegalArgumentException("burst exceeds exact token-count precision");
            }
            if (config.hasSlidingWindow() && fallbackMode != FallbackMode.DENY) {
                throw new IllegalArgumentException("slidingWindow requires DENY fallback");
            }
            if (config.localShareIntervalMs() > Long.MAX_VALUE / 1_000_000L
                    || permitTtlMs > Long.MAX_VALUE / 1_000_000L
                    || pendingTtlMs > Long.MAX_VALUE / 1_000_000L
                    || windowMs > Long.MAX_VALUE / 1_000_000L) {
                throw new IllegalArgumentException("interval/TTL is too large");
            }
            return config;
        }
    }
}
