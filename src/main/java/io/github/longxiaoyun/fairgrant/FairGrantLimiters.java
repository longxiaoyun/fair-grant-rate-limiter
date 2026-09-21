package io.github.longxiaoyun.fairgrant;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Factory helpers for {@link RedisFairGrantLimiter}.
 */
public final class FairGrantLimiters {

    private FairGrantLimiters() {
    }

    public static RedisFairGrantLimiter redis(JedisPool pool, FairGrantConfig config) {
        return new RedisFairGrantLimiter(pool, config, false);
    }

    /**
     * Create limiter with an owned pool (closed when limiter.close() is called).
     */
    public static RedisFairGrantLimiter redis(String host, int port, FairGrantConfig config) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(32);
        poolConfig.setMaxIdle(8);
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(config.isRedisTestOnBorrow());
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(30_000L);
        poolConfig.setMaxWaitMillis(config.getRedisTimeoutMs());
        JedisPool pool = new JedisPool(poolConfig, host, port, config.getRedisTimeoutMs());
        return new RedisFairGrantLimiter(pool, config, true);
    }

    public static LocalShareFairGrantLimiter localShare(FairGrantConfig config) {
        return new LocalShareFairGrantLimiter(config);
    }
}
