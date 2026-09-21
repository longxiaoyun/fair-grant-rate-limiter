package io.github.longxiaoyun.fairgrant.springboot;

import io.github.longxiaoyun.fairgrant.FairGrantConfig;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Properties for one shared quota policy; each resource has an independent bucket. */
@ConfigurationProperties(prefix = "fair-grant", ignoreUnknownFields = false)
public class FairGrantProperties {
    private boolean enabled = true;
    private String clientId = null;
    private String keyPrefix = "fair:grant:";
    private double ratePerSec = 5D;
    private Double burst = null;
    private Duration window = Duration.ZERO;
    private int maxPermits = 0;
    private Duration permitTtl = Duration.ofSeconds(20);
    private Duration pendingTtl = Duration.ofSeconds(5);
    private Duration stateIdleTtl = Duration.ofSeconds(60);
    private int writerNodes = 10;
    private FairGrantConfig.FallbackMode fallbackMode = FairGrantConfig.FallbackMode.DENY;
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean value) { this.enabled = value; }
    public String getClientId() { return clientId; }
    public void setClientId(String value) { this.clientId = value; }
    public String getKeyPrefix() { return keyPrefix; }
    public void setKeyPrefix(String value) { this.keyPrefix = value; }
    public double getRatePerSec() { return ratePerSec; }
    public void setRatePerSec(double value) { this.ratePerSec = value; }
    public Double getBurst() { return burst; }
    public void setBurst(Double value) { this.burst = value; }
    public Duration getWindow() { return window; }
    public void setWindow(Duration value) { this.window = value; }
    public int getMaxPermits() { return maxPermits; }
    public void setMaxPermits(int value) { this.maxPermits = value; }
    public Duration getPermitTtl() { return permitTtl; }
    public void setPermitTtl(Duration value) { this.permitTtl = value; }
    public Duration getPendingTtl() { return pendingTtl; }
    public void setPendingTtl(Duration value) { this.pendingTtl = value; }
    public Duration getStateIdleTtl() { return stateIdleTtl; }
    public void setStateIdleTtl(Duration value) { this.stateIdleTtl = value; }
    public int getWriterNodes() { return writerNodes; }
    public void setWriterNodes(int value) { this.writerNodes = value; }
    public FairGrantConfig.FallbackMode getFallbackMode() { return fallbackMode; }
    public void setFallbackMode(FairGrantConfig.FallbackMode value) { this.fallbackMode = value; }
    private final Redis redis = new Redis();
    public Redis getRedis() { return redis; }

    public FairGrantConfig toConfig() {
        FairGrantConfig.Builder b = FairGrantConfig.builder().keyPrefix(keyPrefix).ratePerSec(ratePerSec)
                .permitTtlMs(millis(permitTtl, "permit-ttl"))
                .pendingTtlMs(millis(pendingTtl, "pending-ttl"))
                .stateIdleTtlMs(millis(stateIdleTtl, "state-idle-ttl"))
                .writerNodes(writerNodes).fallbackMode(fallbackMode)
                .redisTimeoutMs(timeoutMillis()).redisTestOnBorrow(redis.testOnBorrow);
        if (keyPrefix == null || keyPrefix.trim().isEmpty()) throw new IllegalArgumentException("fair-grant.key-prefix must not be blank");
        if (burst != null) b.burst(burst);
        if (window == null || window.isNegative() || maxPermits < 0)
            throw new IllegalArgumentException("fair-grant.window and max-permits must be non-negative");
        if (!window.isZero() || maxPermits != 0) b.slidingWindow(millis(window, "window"), maxPermits);
        return b.build();
    }
    int timeoutMillis() {
        long ms = millis(redis.timeout, "redis.timeout");
        if (ms > Integer.MAX_VALUE) throw new IllegalArgumentException("fair-grant.redis.timeout is too large");
        return (int) ms;
    }
    private static long millis(Duration value, String name) {
        if (value == null) throw new IllegalArgumentException("fair-grant." + name + " is required");
        long ms = value.toMillis();
        if (ms <= 0 || !value.equals(Duration.ofMillis(ms)))
            throw new IllegalArgumentException("fair-grant." + name + " must be positive whole milliseconds");
        return ms;
    }
    public static class Redis {
        private String host = "127.0.0.1";
        private int port = 6379;
        private String username = null;
        private String password = null;
        private int database = 0;
        private boolean ssl = false;
        private Duration timeout = Duration.ofMillis(200);
        private boolean testOnBorrow = false;
        private int maxTotal = 32;
        private int maxIdle = 8;
        private int minIdle = 1;
        public String getHost() { return host; }
        public void setHost(String value) { this.host = value; }
        public int getPort() { return port; }
        public void setPort(int value) { this.port = value; }
        public String getUsername() { return username; }
        public void setUsername(String value) { this.username = value; }
        public String getPassword() { return password; }
        public void setPassword(String value) { this.password = value; }
        public int getDatabase() { return database; }
        public void setDatabase(int value) { this.database = value; }
        public boolean isSsl() { return ssl; }
        public void setSsl(boolean value) { this.ssl = value; }
        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration value) { this.timeout = value; }
        public boolean isTestOnBorrow() { return testOnBorrow; }
        public void setTestOnBorrow(boolean value) { this.testOnBorrow = value; }
        public int getMaxTotal() { return maxTotal; }
        public void setMaxTotal(int value) { this.maxTotal = value; }
        public int getMaxIdle() { return maxIdle; }
        public void setMaxIdle(int value) { this.maxIdle = value; }
        public int getMinIdle() { return minIdle; }
        public void setMinIdle(int value) { this.minIdle = value; }
        void validate() {
            if (host == null || host.trim().isEmpty() || port < 1 || port > 65535 || database < 0)
                throw new IllegalArgumentException("Invalid fair-grant.redis host, port or database");
            if (maxTotal < 1 || maxIdle < 0 || maxIdle > maxTotal || minIdle < 0 || minIdle > maxIdle)
                throw new IllegalArgumentException("fair-grant.redis requires 0 <= min-idle <= max-idle <= max-total and max-total > 0");
        }
    }
}
