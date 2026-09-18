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
    private final FallbackMode fallbackMode;
    private final int redisTimeoutMs;

    private FairGrantConfig(Builder b) {
        this.keyPrefix = b.keyPrefix;
        this.ratePerSec = b.ratePerSec;
        this.burst = b.burst > 0D ? b.burst : b.ratePerSec;
        this.permitTtlMs = b.permitTtlMs;
        this.writerNodes = b.writerNodes;
        this.fallbackMode = b.fallbackMode;
        this.redisTimeoutMs = b.redisTimeoutMs;
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

    public int getWriterNodes() {
        return writerNodes;
    }

    public FallbackMode getFallbackMode() {
        return fallbackMode;
    }

    public int getRedisTimeoutMs() {
        return redisTimeoutMs;
    }

    /** Local-share interval when Redis is down: rate/N. */
    public long localShareIntervalMs() {
        double perNode = ratePerSec / Math.max(1, writerNodes);
        if (perNode <= 0D) {
            return 1000L;
        }
        return Math.max(1L, (long) Math.ceil(1000D / perNode));
    }

    public static final class Builder {
        private String keyPrefix = "fair:grant:";
        private double ratePerSec = 5D;
        private double burst = -1D;
        private long permitTtlMs = 20_000L;
        private int writerNodes = 10;
        private FallbackMode fallbackMode = FallbackMode.LOCAL_SHARE;
        private int redisTimeoutMs = 200;

        public Builder keyPrefix(String keyPrefix) {
            this.keyPrefix = Objects.requireNonNull(keyPrefix, "keyPrefix");
            return this;
        }

        public Builder ratePerSec(double ratePerSec) {
            if (ratePerSec <= 0D) {
                throw new IllegalArgumentException("ratePerSec must be > 0");
            }
            this.ratePerSec = ratePerSec;
            return this;
        }

        public Builder burst(double burst) {
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
            return new FairGrantConfig(this);
        }
    }
}
