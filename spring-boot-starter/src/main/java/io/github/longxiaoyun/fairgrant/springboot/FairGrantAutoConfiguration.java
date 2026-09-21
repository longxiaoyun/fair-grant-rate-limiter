package io.github.longxiaoyun.fairgrant.springboot;

import io.github.longxiaoyun.fairgrant.*;
import java.util.UUID;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.*;

@AutoConfiguration
@ConditionalOnClass({FairGrantLimiter.class, JedisPool.class})
@ConditionalOnProperty(prefix = "fair-grant", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(FairGrantProperties.class)
public class FairGrantAutoConfiguration {
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnMissingBean(FairGrantLimiter.class)
    static class RedisConfiguration {
        @Bean
        @ConditionalOnMissingBean
        FairGrantConfig fairGrantConfig(FairGrantProperties properties) { return properties.toConfig(); }

        @Bean(destroyMethod = "close")
        @ConditionalOnMissingBean(JedisPool.class)
        JedisPool fairGrantJedisPool(FairGrantProperties properties, FairGrantConfig config) {
            FairGrantProperties.Redis r = properties.getRedis();
            r.validate();
            JedisPoolConfig pool = new JedisPoolConfig();
            pool.setMaxTotal(r.getMaxTotal()); pool.setMaxIdle(r.getMaxIdle()); pool.setMinIdle(r.getMinIdle());
            pool.setTestOnBorrow(config.isRedisTestOnBorrow()); pool.setTestWhileIdle(true);
            pool.setTimeBetweenEvictionRunsMillis(30000L); pool.setMaxWaitMillis(config.getRedisTimeoutMs());
            DefaultJedisClientConfig client = DefaultJedisClientConfig.builder()
                    .connectionTimeoutMillis(config.getRedisTimeoutMs()).socketTimeoutMillis(config.getRedisTimeoutMs())
                    .user(r.getUsername()).password(r.getPassword()).database(r.getDatabase()).ssl(r.isSsl()).build();
            return new JedisPool(pool, new HostAndPort(r.getHost(), r.getPort()), client);
        }
        @Bean(destroyMethod = "close")
        RedisFairGrantLimiter fairGrantLimiter(JedisPool pool, FairGrantConfig config) {
            // Spring owns the pool lifecycle; closing the limiter must not close an external pool.
            return FairGrantLimiters.redis(pool, config);
        }
    }
    @Bean
    @ConditionalOnMissingBean
    FairGrantExecutor fairGrantExecutor(FairGrantLimiter limiter) { return new FairGrantExecutor(limiter); }

    @Bean
    @ConditionalOnMissingBean
    FairGrantOperations fairGrantOperations(FairGrantLimiter limiter, FairGrantExecutor executor, FairGrantProperties properties) {
        String client = properties.getClientId();
        if (client == null || client.trim().isEmpty()) client = "instance-" + UUID.randomUUID();
        return new FairGrantOperations(limiter, executor, client);
    }
}
