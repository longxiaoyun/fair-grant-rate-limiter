# Spring Boot 接入

[English](README.md) · [返回首页](../README.zh-CN.md)

通过 `application.yml` 配置，直接注入 `FairGrantOperations`。Spring 创建并关闭 limiter 和 Redis 连接池；每个应用实例默认获得独立、实例生命周期内不变的客户端 ID。

## 版本选择

| 用途 | Spring Boot | Java |
|---|---|---|
| 本项目默认示例、现代项目接入基准 | 3.5.16 | 17+ |
| Java 8 存量项目兼容 | 2.7.18 | 8+ |
| 新版本兼容验证 | 4.1.1 | 17+ |

使用同一个 starter。2.7 用于兼容存量项目；上表表示本库的兼容性，不改变 Spring 各版本自身的支持周期。不宣称覆盖所有 2.x/3.x/4.x 小版本或原生镜像。

## 安装与使用

尚未发布到 Maven Central，在仓库根目录安装核心库和 starter：

```bash
mvn install -DskipTests
mvn -f spring-boot-starter/pom.xml install -DskipTests
```

在已有 Spring Boot 项目中添加：

```xml
<dependency>
  <groupId>io.github.longxiaoyun</groupId>
  <artifactId>fair-grant-spring-boot-starter</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

在 `application.yml` 中设置 **10s 窗口里最多发放 6 个令牌**：

```yaml
fair-grant:
  rate-per-sec: 0.5
  burst: 1
  window: 10s
  max-permits: 6
  redis:
    host: 127.0.0.1
    port: 6379
```

完整的构造器注入示例见 [BatchWriter.java](../examples/spring-boot/src/main/java/example/BatchWriter.java)。业务方法调用：

```java
AcquireResult result = grants.tryExecute("table-a", () -> commitPreparedBatch());
```

- `GRANTED`：回调已执行一次。
- `WAIT`：回调没有执行，保留批次，按 `result.getRetryAfterMs()` 安排重试。
- `ERROR`：回调没有执行，保留批次并排查 `result.getDetail()`。
- 回调抛异常时原样向外抛出，令牌不退还；业务重试需要重新获取令牌。

`FairGrantOperations` 是阻塞的同步接口。它不负责后台排队或业务重试；在 WebFlux 中使用时应安排到适合阻塞操作的线程。窗口约束的是令牌发放时间。

## 配置与覆盖

| 配置 | 默认值 | 含义 |
|---|---|---|
| `enabled` | `true` | `false` 时关闭本 starter 的自动配置 |
| `client-id` | 实例启动时生成 UUID | 通常不需要设置；手动设置时每个并发实例必须不同 |
| `rate-per-sec` | `5` | 每秒补充令牌数 |
| `burst` | `max(1, rate-per-sec)` | 桶容量 |
| `window` / `max-permits` | `0s` / `0` | 同时设置才启用严格滑动窗口 |
| `key-prefix` | `fair:grant:` | 共用额度的应用使用相同前缀和资源名 |
| `permit-ttl` / `pending-ttl` | `20s` / `5s` | 获取回执保留期／等待租约 |
| `state-idle-ttl` | `60s` | 最短闲置保留期，安全期限可延长 |
| `fallback-mode` | `DENY` | 严格窗口只允许 DENY |
| `writer-nodes` | `10` | 仅本地份额降级使用 |
| `redis.host` / `redis.port` | `127.0.0.1` / `6379` | 单节点 Redis 地址 |
| `redis.username` / `redis.password` | 未设置 | Redis ACL 用户／密码 |
| `redis.database` / `redis.ssl` | `0` / `false` | 数据库／TLS |
| `redis.timeout` | `200ms` | 各阶段超时，不是整个获取调用的 deadline |
| `redis.max-total` / `redis.max-idle` / `redis.min-idle` | `32` / `8` / `1` | 连接池大小 |
| `redis.test-on-borrow` | `false` | 可选借出 PING；默认启用空闲校验 |

所有配置以 `fair-grant.` 开头。时长支持 `200ms`、`5s` 等，要求正整数毫秒（禁用窗口时为 0）。错误参数、单独设置窗口的一半以及未知配置项会导致启动失败，避免拼写错误悄悄改变额度。

Redis 密码可使用环境变量：`password: ${REDIS_PASSWORD}`。TLS 使用 JVM 信任配置；自定义证书或特殊连接配置可提供自己的 `JedisPool` Bean。当前支持单节点连接；未增加 Sentinel、Cluster 或原生镜像适配。

不需要添加 Spring Data Redis。这里使用独立的 `fair-grant.redis.*` 配置；如已有 `spring.data.redis.*`，可在 YAML 中通过 `${spring.data.redis.host}` 等引用，避免重复填写。

可直接注入 `FairGrantLimiter`、`RedisFairGrantLimiter` 或 `FairGrantExecutor` 使用核心 API。已有同类型 Bean 时自动配置会让位：

- 自定义 `FairGrantLimiter`：不再创建默认 limiter、配置或 Redis 池。
- 自定义 `FairGrantConfig`：使用该对象配置 limiter。
- 自定义 `JedisPool`：复用这个池，limiter 不接管它的关闭；Bean 自身的关闭规则由应用决定。
- 多个 `JedisPool` 时用 `@Primary` 指定；不明确时启动失败，不任意选择。

本库的纯 Java 核心没有新增 Spring 依赖，也不改变 v4 配额协议。[核心语义与迁移](../docs/reference.zh-CN.md)。

## 真实运行验证

[Spring Boot 独立示例](../examples/spring-boot/README.md)会启动带密码的临时 Redis 和三个可执行 Boot JAR，验证自动配置发现、YAML、身份隔离、DB 2、FIFO、窗口、9 次 HTTP 提交与正常退出。只连接脚本启动的临时服务。

自动配置测试覆盖启停、配置校验、Bean 覆盖和关闭行为。CI 分别运行上述三个 Boot 版本；示例使用 Boot 实际管理的 Jedis 依赖，不强制改成核心库原来的 Jedis 版本。
