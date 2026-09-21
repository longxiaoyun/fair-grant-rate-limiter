# 使用参考与实现说明

[返回项目介绍](../README.zh-CN.md) · [English](reference.md)

首次了解项目，请先读 README 的业务例子和接入步骤。本页供接入时查配置、处理重试与故障，以及升级旧版本时使用。Kafka 多表消息、本地攒批和同表提交的业务背景见 README；[本轮场景评审](scenario-review.zh-CN.md)记录了窗口反例及其修复验证。

## 配置

| 选项 | 默认 | 说明 |
|---|---|---|
| `keyPrefix` | `fair:grant:` | 应用隔离前缀 |
| `ratePerSec` | 5 | 有限正数；资源所有 JVM 共用的平均速率 |
| `slidingWindow(windowMs, maxPermits)` | 关闭 | 任意窗口内的新令牌上限；正整数参数；要求 DENY |
| `burst` | `max(1, ratePerSec)` | 有限且至少为 1；最大突发容量 |
| `pendingTtlMs` | 5000 | 等待租约；应覆盖正常调度、Redis 请求延迟与 GC 抖动 |
| `permitTtlMs` | 20000 | 请求回执保留期；不是业务锁超时 |
| `fallbackMode` | `DENY` | Redis 不可用时的策略 |
| `writerNodes` | 10 | 仅用于 LOCAL_SHARE 的估计进程数 |
| `redisTimeoutMs` | 200 | 工厂连接池超时及 DENY 建议重试间隔 |

同一资源的所有 JVM 必须使用相同 rate、burst、窗口时长和次数，否则返回 `ERROR / config_mismatch`。修改这些参数需先停止并排空该资源的调用，再协调重建桶；不能靠混用不同配置动态修改。

### 严格滑动窗口

`.slidingWindow(15_000L, 75)` 表示 Redis 逻辑时间的任意 `(t−15000ms, t]` 内最多 75 个新令牌。窗口与令牌桶同时生效；`window_full` 表示窗口尚未腾出名额。到达窗口左边界的记录移出，其他记录继续有效，不做整窗重置。Redis TIME 使用微秒精度，时钟回拨时逻辑时间不倒退；此时可能额外等待。

回执重放不新增窗口记录，也不延长记录有效期。窗口成员使用独立的递增序号，因此请求回执提前到期后重用 ID，也不能覆盖之前的窗口记录。`clearPending` 与 `invalidatePermit` 均不返还窗口额度。启用窗口后仅允许 DENY；本地 limiter 会拒绝该配置。

### Redis 不可用

| 策略 | 行为 |
|---|---|
| `DENY` | 默认，返回 WAIT |
| `LOCAL_SHARE` | 单 limiter 实例按 `ratePerSec / writerNodes` 发放，本地拒绝仍返回 WAIT |
| `ALLOW` | 全部放行，仅适用于明确接受超限的业务 |

LOCAL_SHARE 使用 `System.nanoTime()` 和原子化的资源状态，`clearPending` 不重置冷却。单进程使用多个 limiter 实例会得到多份本地额度。Redis 部分故障、恢复切换、实际进程数超过估计或各实例同时首次降级时，均不能保证全局速率。Redis 与本地的幂等记录不共享，跨降级切换也没有统一幂等保证。

Redis WRONGTYPE、ACL 拒绝等数据/脚本错误返回 ERROR，不通过 ALLOW 掩盖。Lua 使用 EVALSHA，缓存丢失后退回 EVAL；不会在成功发放后再依赖一次 SCRIPT LOAD 成功。

## 调用与重试

### 就绪操作执行入口

`new FairGrantExecutor(limiter).tryExecute(resourceKey, clientId, action)` 在调用线程申请新额度，获准后立即执行一次回调，WAIT/ERROR 时不执行。业务异常原样抛出，已消耗额度不退还；业务重试再次调用此方法申请新额度。它不使用回执重放、不自动重试、不调度任务，也不接管 SDK 内部重试。

### 普通调用与请求重试

`tryAcquire` **每次调用都申请新额度**。同一个 `clientId` 的两个并发线程如果都成功，会扣除两个令牌，不共享一个进程级许可。

如果需要在 Redis 响应丢失后重试同一次获取，使用新方法：

```java
// 在业务批次/调用尝试对象上保存，不能在每次 Redis 重试时重新生成。
String requestId = UUID.randomUUID().toString();
AcquireResult result = limiter.tryAcquireRequest(resourceKey, clientId, requestId);
```

- 不同的受限业务调用必须使用不同 `requestId`。
- 同一次获取重试使用原 ID；已有回执会返回 `GRANTED / existing_permit`，不重复扣费。
- 回执从首次获准时开始计时，重试不会延长保留期。超过 `permitTtlMs` 后，库不再记得该 ID，可能再次扣费。
- 在应用层设置比回执保留期更短的获取重试期限。不要把旧 ID 当成永久幂等键。
- 同一 ID 不能由多个线程独立执行业务。`existing_permit` 表示获取回执重放，不表示业务尚未执行；业务执行去重由调用方负责。
- 不需要释放许可。旧的 `invalidatePermit` 已废弃并变为无操作；回执保留到 TTL 到期，避免迟到的清理删除新回执。

### 等待与退出

1. `tryAcquire` / `tryAcquireRequest` 自动加入或续租等待队列；`registerPending` 只能用于已经具备提交条件的批次；不能因为内存里已有少量数据，就让尚未攒好的批次占位。
2. `WAIT` 是非阻塞结果。建议重试时间不会超过本次等待租约的一半，低速率下也需要定期续租。
3. 获得许可即完成这一轮排队，正在执行业务的客户端不会占住队首。持续有工作时，及时为下一次获取排队。
4. 租约过期后重新加入会排到队尾；长时间 GC、暂停、网络延迟超过租约，可能失去排队位置。
5. 取消等待时调用 `clearPending`。它只退出队列，不退还令牌、不删除回执、不重置本地冷却时间。
6. 多线程共享 `clientId` 时，由统一的队列管理者判断是否空闲；单个线程不能在其他线程仍等待时清理整个客户端。

`registerPending` / `clearPending` 的 Redis 失败会记录日志；遗留等待成员最终靠租约清理。它们不是业务事务。

### 返回值

| 状态 | `isGranted()` | 含义 |
|---|---|---|
| `GRANTED` | true | Redis 发放的新许可或同请求回执 |
| `WAIT` | false | 缺令牌、窗口已满、尚未轮到、Redis 拒绝降级或本地份额已用完 |
| `DEGRADED_LOCAL` | true | Redis 失败后，所选降级策略允许执行；重试间隔为 0 |
| `ERROR` | false | 配置冲突、Redis 数据/脚本错误或异常结果，需要排查 |

## 保证与边界

- **速率**：正常 Redis 路径上，长度为 `t` 秒的区间最多发放约 `burst + ratePerSec × t` 个新许可。令牌桶允许突发。启用 `slidingWindow` 后，还必须满足配置窗口内的新令牌上限。
- **公平**：仍在租约期内的等待客户端按 FIFO 排队。获得一次许可后退出队列，有新请求再排到队尾。公平按 `clientId`，不是按线程或业务请求。
- **存活**：等待客户端通过重试或 `registerPending` 续租。崩溃后，下一次获取/登记会清除过期的等待成员，不依赖进程主动退出。
- **幂等**：显式 `requestId` 只在 `permitTtlMs` 回执保留期内避免重复扣费；它不能保证业务调用只执行一次。
- **失败**：默认 `DENY`，Redis 不可用时返回 `WAIT`。选择 `LOCAL_SHARE` 或 `ALLOW` 后，就接受失去严格共享配额保证。

发放许可与真正发出业务请求是两件事：拿到许可后应及时执行，不能先囤许可再集中提交。业务超时后如果要再发一次云 API 请求，应申请新许可、使用新 `requestId`；业务幂等键可以保持不变。

Redis 时钟大幅向前跳变会提前结束真实时间的等待窗口；Redis 状态丢失、异步复制故障切换、两个独立 Redis、客户端混合使用不同版本，均可能使共享配额失真。该库不是 Redis 故障下的强一致配额系统。

## 设计

一次 Lua 调用原子完成：

1. 校验资源已有的 `rate` / `burst` / 窗口配置与调用方相同。
2. 如该请求有回执，返回原获准状态。
3. 按 Redis 时间删除过期租约，把新等待客户端分配到递增序号的队尾，续租。
4. 按 Redis 时间补令牌；桶时间戳只前进，服务器时钟回拨期间不重复计算时间。
5. 清除已离开滑动窗口的发放记录，检查窗口名额。
6. 令牌足够、窗口未满且轮到调用方时扣 1 个，追加唯一窗口记录，写请求回执，并退出本轮等待。拒绝时两个预算均不扣除。

队列使用序号而非毫秒时间作分数，同一毫秒内不会按 clientId 字典序反复选同一个客户端。取队首使用 `ZRANGE 0 0`；过期成员在后续获取/登记时惰性清理。

版本化 Redis key 的基础部分为 `{prefix}v3:{<base64url(resource)>}:`（尖括号是占位符，花括号是 key 的实际字符）：

| 后缀 | 类型 | 内容 |
|---|---|---|
| `bucket` | hash | `tokens`、`tsUs`、`clockUs`、`seq`、`rate`、`burst`、`windowMs`、`windowMaxPermits`、`grantSeq` |
| `window` | zset | 发放序号 → Redis 微秒时间；只在启用窗口时创建，记录数最多为窗口次数上限 |
| `wait` | zset | clientId → FIFO 序号 |
| `pending` | zset | clientId → 等待租约过期微秒时间 |
| `permit:<base64url(client)>:<base64url(request)>` | string + TTL | 发放许可的 Redis 微秒时间 |

编码使用 UTF-8、URL 安全 Base64、不带 padding，避免 client/request 中的分隔符造成 key 碰撞。默认前缀下，同一资源 key 具有相同 hash tag；这不意味着当前 Java 客户端已支持 Redis Cluster。

桶保留速率历史，不自动过期；本地 fallback 也按资源保留状态。高基数、一次性资源需要应用规划清理生命周期。不能在客户端仍会重试或冷却尚未结束时随意删桶；删除会重置初始突发额度。闲置资源的过期等待成员会在下一次访问时清理。

## 从旧 SNAPSHOT 升级

这是一次有行为变化的修复，**不支持新旧客户端混跑**：旧版（含 v2）key 与 `v3` key 是两个独立桶，混跑会双重放行。

1. 停止旧客户端获取许可，排空已获准的业务请求。
2. 等待旧桶恢复初始突发额度所需的时间（保守取 `max(burst / ratePerSec, 新旧窗口时长)` 秒），并确认旧请求不会继续执行或重试。
3. 一次性切换所有客户端。继续使用普通 tryAcquire 的业务每次成功都会扣一个令牌；需要获取幂等时迁移到 tryAcquireRequest。
4. 删除旧的 invalidatePermit 调用；clearPending 仅用于取消剩余等待。
5. 明确选择降级策略，默认已由 LOCAL_SHARE 改为 DENY。
6. 旧 key 确认无客户端使用后再由运维清理，库不会自动删旧数据。

## 构建和端到端验证

```bash
mvn clean test                     # 单元测试 + jedis-mock 合约回归
mvn -Preal-redis clean verify       # 上述测试 + 独立真实 Redis + 多 JVM HTTP 端到端
mvn -Preal-redis -Dredis.server=/absolute/path/redis-server clean verify
```

真实测试需要本机可执行的 `redis-server` 和回环端口权限；测试自行分配端口、启动临时实例、关闭持久化，并在结束时停止进程，不接入现有 Redis。不具备环境时测试会失败，不静默跳过。日志在 `target/redis-it-*` 与 `target/worker-*`。

端到端场景启动 4 个独立 JVM，共享一份 rate=20、burst=2、窗口 500ms／6 次的额度，执行 40 次不同的 HTTP 提交，重放 40 次获取回执，验证每个客户端完成 10 次、初始等待队列公平、每个发放时间区间均不超过令牌桶预算，且跨多个窗口均不超过 6 次／500ms。

新增的 `MaxComputeScenarioIT` 用真实 Redis 模拟 30 个逻辑消费节点：混合表消息分入独立本地批次，tableA 轮流获得令牌，tableB 额度独立，并验证高频节点不能越过等待者。该模拟没有 Kafka broker，也没有 ODPS SDK；实际 Commit 到达时序和 SDK 重试仍需在真实链路验证。

`SlidingWindowTest` 的测试时钟仅替换生产 Lua 的 TIME 读取，精确检查微秒边界、滚动淘汰、回执、双预算、FIFO 和时钟回拨；`SlidingWindowIT` 在真实 Redis 重跑同一合约，并用真实时间验证 30 节点共同获得 75 个名额、每个节点都有进展、75／15 秒阻止过早发出第 76 个令牌，以及严格模式故障拒绝。

真实 Redis 测试还覆盖停机/恢复、三种故障策略、租约过期、同身份并发、响应丢失重试、SCRIPT FLUSH、时钟回拨保护、小数速率、错误配置及脚本错误。CI 在 Java 8/11/17 上运行；独立任务覆盖 Redis 5 与 7。

Java 版本 profile `jdk8` / `jdk11` / `jdk17` 保留用于兼容性验证；高版本 profile 生成对应版本字节码，发布 Java 8 兼容 jar 应使用默认构建或 jdk8。

## 贡献与许可证

修改发放逻辑请补回归测试，并运行 `mvn -Preal-redis clean verify`。源码保持 Java 8 兼容。

[Apache License 2.0](../LICENSE)。
