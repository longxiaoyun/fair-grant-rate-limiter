# 公平令牌桶分发库：按 Kafka → ODPS 场景重新评审

[项目介绍](../README.zh-CN.md) · [使用参考](reference.zh-CN.md)

评审日期：2026-09-21。最初评审基线为 PR #1 的 `587dcce`；后续已实现下述 v3 修复。历史反例保留用于解释变更原因。

## 结论

**按资源共享令牌桶，再按存活等待节点分发令牌，这个通用架构适合保留。Kafka → ODPS 是解释它、验证它的真实案例，不应成为库的硬编码业务。**

已增加通用严格滑动窗口，与令牌桶和 FIFO 在同一 Lua 中原子校验；公共 API 为 `slidingWindow(windowMs, maxPermits)`。默认仍保留普通令牌桶，ODPS 示例显式启用 75／15 秒窗口。真实 SDK 调用边界和消费侧的数据可靠性，也不在已有令牌测试的证明范围内。

## 一、需求和通用模型是否对应

| 实际业务 | 通用抽象 | 评审判断 |
|---|---|---|
| 一 Topic 混合多张表，消息携带表名、表结构、行数据 | 调用方的数据组织方式 | 库无需依赖消息格式、Kafka 或 ODPS SDK。 |
| 30 个节点各自按表缓存、攒批 | 节点持有就绪任务 | 批次保持本地；Redis 不存批次数据。 |
| 同一张表跨节点共享提交额度 | resourceKey 对应一份共享令牌桶 | 按完整物理表身份分桶；其他业务可改用 API 账号等额度身份。 |
| 有可提交批次的节点应公平获得机会 | 同一 resourceKey 下，clientId 进入 FIFO 等待队列 | 按节点分配操作机会，不按消息或批次大小分配权重。 |
| 一次 Tunnel UploadSession.commit | 一次受控操作，对应一个令牌 | 抽象可用于其他 API 调用，无需 ODPS 专属方法。 |

不需要把同表消息强行绑定到同一 Kafka 分区或同一消费节点。每个节点仍持有自己的同表批次，只把“下一次提交机会给谁”交给库协调。

## 二、阻塞生产验收的问题

### P1：默认令牌桶不满足严格的 75／15 秒窗口声明

[FairGrantConfig.java](https://github.com/longxiaoyun/fair-grant-rate-limiter/blob/587dcce/src/main/java/io/github/longxiaoyun/fairgrant/FairGrantConfig.java#L28-L31) 默认令牌补充速率为 5，桶容量也为 5。[fair_grant.lua](https://github.com/longxiaoyun/fair-grant-rate-limiter/blob/587dcce/src/main/resources/lua/fair_grant.lua#L29-L46) 初始装满桶，随后持续补充，没有另一个 15 秒计数状态。

实测：真实 Redis 8.0.2，调用公共 `tryAcquireRequest`，每次使用不同的请求 ID，记录 Redis 回执中的发放时间。前 76 次获准的时间跨度为 **14203ms**。

```text
DEFAULT_WINDOW_COUNTEREXAMPLE grants=76 spanMs=14203 rate=5 burst=5 limit=75/15000ms
```

这是正常令牌桶的突发行为，不是并发原子性失效；但它与该业务希望满足的窗口约束不同。阿里云官方给出的对应指标是“单表写入 Commit 每 15 秒 75 次”，未在所引用页面说明可据以实现完全相同判定的窗口算法。[官方限制](https://help.aliyun.com/zh/maxcompute/overview-of-dts)

已落地：

- 新增严格滑动窗口；满窗时不扣令牌，缺令牌时不占窗口，只有队首可以拿到新额度。
- 窗口历史独立于回执 TTL；重放不重复计数、重用过期 ID 不覆盖历史、清理等待不返还额度。
- 微秒时间与单调逻辑时钟处理窗口边界、时钟回拨；窗口配置不一致返回 ERROR。
- 启用窗口强制 DENY；通用执行入口 FairGrantExecutor 限制每次获准只调用一次回调。
- v3 隔离旧协议，迁移需要停机排空并等待完整窗口；不支持混跑。
- 真实 Redis 测试确认 rate=5/burst=5 的第 76 次新发放至少等待 15 秒。

### P1：令牌获准与实际 Commit 的边界需要验证

用户已确认使用 Tunnel UploadSession。官方说明的调用顺序包含创建 Session、写 Block、关闭 Writer，最后 `commit(blocks)`；Commit 失败可以重试。[UploadSession 接口说明](https://help.aliyun.com/zh/maxcompute/user-guide/uploadsession)

应在上传准备完成之后、实际 Commit 之前获取令牌。若先取令牌再进行耗时上传，不同节点可能在稍后集中 Commit；若一次获准后由应用或 SDK 重试多次实际 Commit，则计费次数和请求次数不一致。

[RedisFairGrantLimiter.tryAcquireRequest](https://github.com/longxiaoyun/fair-grant-rate-limiter/blob/587dcce/src/main/java/io/github/longxiaoyun/fairgrant/RedisFairGrantLimiter.java#L64-L85) 只返回获准结果，不执行业务调用；回执也不能证明某个批次是否已成功落表。

用户尚未提供 SDK 版本、重试配置或实际调用包装，因此本轮不能证明一次 Java 方法调用对应几次 Commit RPC。官方关于 BufferedWriter 自动重试的描述不能直接用于断言每个版本的 commit 是否自动重试。

通用集成要求：每次重新发起受控操作都应重新取令牌；同 requestId 只重试同一次“获取许可”，不应作为反复执行业务的通行证。应用负责区分响应不明、操作成功和需要重发的状态。

## 三、必须写清的接入契约

### 只有就绪任务参与公平分发

`registerPending` 只认识“这个节点登记了”，不知道内存批次是否已攒好。[选人逻辑](https://github.com/longxiaoyun/fair-grant-rate-limiter/blob/587dcce/src/main/resources/lua/fair_grant.lua#L40-L42) 会等待队首节点发起获取。

一个活着、持续续租但尚未准备好提交的节点，可以一直挡住其他就绪节点。因此“本地缓存非空”不等于“应该排队”；只有获得令牌后即可执行的任务才参加分发。以前“可以提前占位”的表述过宽，本轮已修正文档。

FIFO 只在存活的等待者之间成立；排队节点必须按 retryAfter 重试/续租。30 个节点在 5 个令牌／秒下，一轮理想耗时约 6 秒，而默认等待租约为 5 秒，不能只登记一次后睡满一轮。现有重试提示不超过本次租约的一半，可供调度器续租使用。

### 一个物理资源只能对应一份额度身份

ODPS 案例的 key 必须在所有提交节点间一致，并包含需要的项目和 Schema 命名空间。不能根据 Kafka 分区、消息中的列结构哈希、本地批次或 ODPS 分区值为同一受限表另开桶。

表结构是消息解释和批次兼容性问题；若应用需要按结构版本拆本地批次，这些批次仍共享同一物理表的额度。对其他业务，同样必须按服务端真正合并计数的资源范围选择 key。

### 公平分发不能替代消费侧数据可靠性

库中没有 Kafka 消费和 offset 管理代码，不应因此扩成一个强绑定 Kafka/ODPS 的写入框架。但接入应用必须验证：

- WAIT 时保留当前批次，并限制内存积压、适时暂停消费。
- 同节点、同表的批次调度有统一所有者，避免重复提交、过早清理另一个线程的等待资格。
- 数据仅进入内存时，不能当作已经持久化。
- 同一 Kafka 分区混有多张表时，后面表的批次先提交，不代表可以跨过前面尚未持久化的消息推进 offset。具体策略由消费侧实现。

### 故障与规模边界

- 严格共享配额场景应使用 DENY。LOCAL_SHARE/ALLOW 明确牺牲共享额度保证，增加消费节点时也不能靠固定 writerNodes 获得自动一致性。
- 本实现按客户端主动重试驱动，队首迟迟不重试会影响吞吐；它不是后台主动调度服务。
- 多表、高并发下应测量 Redis 请求量和调度延迟，不把 30 节点模拟当作容量压测。
- 桶和本地 fallback 按资源保留状态，高基数历史资源需要生命周期管理；不能删除活跃桶来“清理内存”，因为这会恢复初始突发额度。

## 四、本轮验证

### 实现和仓库检查范围

重新检查了公共 API、配置与 key 规则、三段 Lua、Redis 异常/回执路径、本地降级、现有测试、构建和 CI。令牌计费与等待队列仍是通用组件；本轮修改通用发放协议、配置、执行入口和测试，没有引入 Kafka 或 ODPS 依赖。

### 新增场景回归

[MaxComputeScenarioIT.java](../src/test/java/io/github/longxiaoyun/fairgrant/MaxComputeScenarioIT.java) 用真实 Redis 和 30 个逻辑消费节点验证：

1. 一条合成消息流交错包含 tableA、tableB，分别进入各节点独立的本地缓存。
2. 30 个节点各有一个 tableA 就绪批次，总计 60 行；完成 30 次模拟 Commit，而不是为每行申请令牌。
3. 在令牌已可用的情况下，高频节点连续重试 20 次仍不能越过队首等待者。
4. tableA 的队列不阻塞 tableB 的令牌；提交 tableA 不会清空其他节点或其他表的本地数据。
5. 就绪节点按登记次序依次获准，无重复占用同一轮。

该测试在一个 JVM 中模拟节点，不包含 Kafka broker，不调用真实 UploadSession。为顺序驱动模拟保留等待资格，测试租约设为 30 秒；不是对生产默认租约及心跳调度的证明。

运行方式：

```bash
mvn -Preal-redis -Dtest=MaxComputeScenarioIT test
mvn -Preal-redis test
```

新增 SlidingWindowTest / SlidingWindowIT 验证精确窗口边界、30 个并发节点共享 75 个令牌及真实 15 秒窗口；4 JVM／40 次 HTTP 场景同时检查两个预算。验证命令执行全部测试；它不是 Kafka → ODPS 生产端到端测试。

### 尚未验证

真实 Kafka 分配及 rebalance、内存上限、批次与 offset 一致性、实际 SDK 自动重试、服务端 Commit 接收窗口、多写入方竞争和生产容量。这些需要接入应用与真实环境，不能通过修改 README 声称已完成。
