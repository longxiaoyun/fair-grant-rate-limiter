package io.github.longxiaoyun.fairgrant.springboot;

import io.github.longxiaoyun.fairgrant.*;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.*;
import redis.clients.jedis.JedisPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

public class FairGrantAutoConfigurationTest {
    // Discover the auto-configuration through packaged metadata, not a direct @Import.
    private final ApplicationContextRunner runner = new ApplicationContextRunner().withUserConfiguration(App.class);
    @Configuration(proxyBeanMethods = false) @EnableAutoConfiguration static class App {}

    @Test public void discoversBeansAndClosesOwnedPoolAndLimiter() {
        AtomicReference<JedisPool> pool = new AtomicReference<>();
        AtomicReference<RedisFairGrantLimiter> limiter = new AtomicReference<>();
        runner.run(c -> {
            assertThat(c).hasNotFailed().hasSingleBean(FairGrantConfig.class).hasSingleBean(FairGrantLimiter.class)
                    .hasSingleBean(FairGrantExecutor.class).hasSingleBean(FairGrantOperations.class);
            pool.set(c.getBean(JedisPool.class)); limiter.set(c.getBean(RedisFairGrantLimiter.class));
            assertThat(c.getBean(FairGrantConfig.class).isRedisTestOnBorrow()).isFalse();
            assertThat(c.getBean(FairGrantConfig.class).getRatePerSec()).isEqualTo(5);
            assertThat(c.getBean(FairGrantOperations.class).getClientId()).startsWith("instance-");
        });
        assertThat(pool.get().isClosed()).isTrue();
        assertThatThrownBy(() -> limiter.get().tryAcquire("k", "a")).isInstanceOf(IllegalStateException.class);
    }
    @Test public void bindsDurationQuotaAndIdentity() {
        runner.withPropertyValues("fair-grant.rate-per-sec=0.5", "fair-grant.burst=1",
                "fair-grant.window=10s", "fair-grant.max-permits=6", "fair-grant.client-id=worker-a",
                "fair-grant.redis.timeout=350ms", "fair-grant.pending-ttl=3s", "fair-grant.redis.test-on-borrow=true")
                .run(c -> {
                    assertThat(c).hasNotFailed();
                    FairGrantConfig cfg = c.getBean(FairGrantConfig.class);
                    assertThat(cfg.getWindowMs()).isEqualTo(10000); assertThat(cfg.getWindowMaxPermits()).isEqualTo(6);
                    assertThat(cfg.getPendingTtlMs()).isEqualTo(3000); assertThat(cfg.getRedisTimeoutMs()).isEqualTo(350);
                    assertThat(cfg.isRedisTestOnBorrow()).isTrue();
                    assertThat(c.getBean(FairGrantOperations.class).getClientId()).isEqualTo("worker-a");
                });
    }
    @Test public void independentContextsHaveDifferentStableDefaultIdentities() {
        AtomicReference<String> first = new AtomicReference<>();
        runner.run(c -> {
            FairGrantOperations ops = c.getBean(FairGrantOperations.class);
            first.set(ops.getClientId()); assertThat(ops.getClientId()).isEqualTo(first.get());
        });
        runner.run(c -> assertThat(c.getBean(FairGrantOperations.class).getClientId()).isNotEqualTo(first.get()));
    }
    @Test public void disablingStarterCreatesNoInfrastructure() {
        runner.withPropertyValues("fair-grant.enabled=false").run(c -> assertThat(c).hasNotFailed()
                .doesNotHaveBean(FairGrantLimiter.class).doesNotHaveBean(JedisPool.class).doesNotHaveBean(FairGrantOperations.class));
    }
    @Test public void partialWindowAndUnsafeFallbackFailStartup() {
        for (String[] properties : new String[][]{{"fair-grant.window=1s"}, {"fair-grant.max-permits=1"},
                {"fair-grant.window=1s", "fair-grant.max-permits=1", "fair-grant.fallback-mode=ALLOW"}})
            runner.withPropertyValues(properties).run(c -> assertThat(c).hasFailed());
    }
    @Test public void malformedValuesFailStartup() {
        for (String property : new String[]{"fair-grant.redis.port=0", "fair-grant.redis.min-idle=99",
                "fair-grant.rate-per-sec=0", "fair-grant.pending-ttl=0.1ms", "fair-grant.redis.timeout=0ms",
                "fair-grant.redis.database=-1", "fair-grant.burst=0"})
            runner.withPropertyValues(property).run(c -> assertThat(c).hasFailed());
    }
    @Test public void typosDoNotSilentlyDisableQuota() {
        runner.withPropertyValues("fair-grant.max-permit=6").run(c -> assertThat(c).hasFailed());
    }
    @Configuration(proxyBeanMethods = false) static class CustomLimiter {
        @Bean FairGrantLimiter customLimiter() { return FairGrantLimiters.localShare(FairGrantConfig.builder().ratePerSec(.001).build()); }
    }
    @Test public void customLimiterBacksOffAndFacadeDoesNotRetryOrExecuteOnWait() {
        runner.withUserConfiguration(CustomLimiter.class).withPropertyValues("fair-grant.redis.port=-1").run(c -> {
            assertThat(c).hasNotFailed().hasSingleBean(FairGrantLimiter.class).doesNotHaveBean(JedisPool.class);
            FairGrantOperations ops = c.getBean(FairGrantOperations.class);
            AtomicInteger calls = new AtomicInteger();
            try {
                assertThat(ops.tryExecute("k", () -> calls.incrementAndGet()).isGranted()).isTrue();
                assertThat(ops.tryExecute("k", () -> calls.incrementAndGet()).isGranted()).isFalse();
                assertThat(calls.get()).isEqualTo(1);
                Exception expected = new Exception("business failure");
                assertThatThrownBy(() -> ops.tryExecute("other", () -> { throw expected; })).isSameAs(expected);
                assertThat(ops.tryAcquire("other").isGranted()).isFalse();
            } catch (Exception e) { throw new AssertionError(e); }
        });
    }
    @Configuration(proxyBeanMethods = false) static class CustomConfig {
        @Bean FairGrantConfig customConfig() { return FairGrantConfig.builder().ratePerSec(17).build(); }
    }
    @Test public void customConfigIsUsed() {
        runner.withUserConfiguration(CustomConfig.class).run(c -> {
            assertThat(c).hasNotFailed().hasSingleBean(FairGrantConfig.class);
            assertThat(c.getBean(FairGrantConfig.class).getRatePerSec()).isEqualTo(17);
        });
    }
    @Configuration(proxyBeanMethods = false) static class ExternalPool {
        @Bean(destroyMethod = "") JedisPool externalPool() { return new JedisPool(); }
    }
    @Test public void limiterDoesNotTakeOwnershipOfExternalPool() {
        AtomicReference<JedisPool> pool = new AtomicReference<>();
        runner.withUserConfiguration(ExternalPool.class).run(c -> {
            assertThat(c).hasNotFailed().hasSingleBean(JedisPool.class); pool.set(c.getBean(JedisPool.class));
            c.getBean(RedisFairGrantLimiter.class).close(); assertThat(pool.get().isClosed()).isFalse();
        });
        assertThat(pool.get().isClosed()).isFalse(); pool.get().close();
    }
    @Configuration(proxyBeanMethods = false) static class TwoPools {
        @Bean JedisPool firstPool() { return new JedisPool(); }
        @Bean JedisPool secondPool() { return new JedisPool(); }
    }
    @Test public void ambiguousPoolsFailRatherThanChooseTheWrongRedis() {
        runner.withUserConfiguration(TwoPools.class).run(c -> assertThat(c).hasFailed());
    }
}
