# 独立接入示例

[项目首页](../../README.zh-CN.md) · [独立接入评审](../../docs/consumer-review.zh-CN.md)

这是一个独立 Maven 项目，通过 Maven 依赖使用本库的 JAR，不引用库源码、测试类或 Redis 内部 key。

准备 Java 8+、Maven、Redis 5+、Python 3.8+，在仓库根目录执行：

```bash
python3 examples/quickstart/run.py
```

脚本自动构建、启动独立 Redis 和 HTTP 测试服务，再启动 3 个 JVM。每个 JVM 有 4 个待执行任务；拿到令牌才调用 HTTP 接口，等待时保留当前任务。默认使用每秒 0.5 个令牌、burst=1 平滑发放，并叠加 6／10s 窗口，给实际请求到达留出余量。12 次操作约 22 秒完成（不含首次构建）。

- [DemoWorker.java](src/main/java/example/DemoWorker.java)：真实消费者代码，只使用库的公共 API。
- [pom.xml](pom.xml)：独立项目的依赖。
- [run.py](run.py)：构建、启动多进程、记录 HTTP 调用、校验结果及关闭服务。

示例在专用调度线程中等待；生产环境请接入自己的调度器，不要阻塞 Kafka poll 线程。业务异常不会退还令牌；示例重新申请新额度后才重试 HTTP 调用。这里主动注入的 503 表示服务拒绝执行，不涉及网络超时后执行结果不明的业务去重问题。

## 场景测试

```bash
python3 examples/quickstart/run.py --scenario all
```

| 场景 | 进程与任务 | 检查内容 |
|---|---|---|
| `quick` | 3 JVM，各 4 次操作 | 共用 6／10s；12 次全部完成 |
| `idle` | 3 JVM，C 无任务，A、B 各 3 次 | C 不登记等待；A、B 用完 6 个初始令牌，无需等 C 的租约到期 |
| `fleet` | 30 JVM，各向两张表提交 3 次 | 两表独立；180 次完成，每个节点都有进展 |
| `window` | 3 JVM，共 78 次 | rate=5、burst=5，加 75／15s 窗口，复查早发第 76 个令牌的问题 |
| `retry-paced` | 3 JVM，两张表，12 个任务，主动拒绝 1 次 | rate=2.5、burst=1，留余量；验证下游 3／1s、13 次请求、12 次成功 |
| `retry` | 3 JVM，两张表，共 12 个任务 | 服务主动拒绝 1 次，重新取令牌；13 次请求，12 次成功，无重复成功 |

`retry` 故意使用 rate=4、burst=2 对窗口施压。本地 Java 8 曾观测到 HTTP 到达窗口 4／1s，而 Redis 配置为 3／1s：发放与请求到达之间的时序差会让下游集中收到请求。Java 21 的 `window` 场景也曾观测到 HTTP 峰值 76／15s。出现这一现象时，场景输出 WARN 并保留证据，不把 HTTP 超限掩盖为成功。`retry-paced` 是留余量的对照。

`fleet` 使用 rate=4.8、burst=1，为实际 HTTP 调用留一点速率余量；`window` 单独使用 rate=5、burst=5 检查窗口约束。两者的用途和配置不同。

只跑一个场景：`--scenario window`。构建完成后重复运行可加 `--skip-build`；源码改变后应重新构建。可使用 `--maven-repo /path/to/cache` 指定隔离的 Maven 缓存。

结果保存在 `target/run-<场景>-<时间>/`，包括逐 JVM 日志、HTTP 事件、获取记录和报告；最新汇总为 `target/latest-report.json`。进程和临时服务会在结束时关闭。`target` 中的日志保留供排查。

## 验证范围

- 检查每个 HTTP 请求都有一次对应的额度获取。
- 检查所有预期任务成功、无重复成功、每个有任务的节点完成预置任务；无任务的节点不占队列。
- 公共 API 不返回 Redis 发放时间，因此用“获取调用开始到回调开始”界定实际发放时间。同时报告 HTTP 服务实际观测到的窗口峰值；它与 Redis 发放窗口可能因网络时序而不同。若发放边界无法确定，报告 `uncertain_boundary_groups`，不把它算作精确通过。
- 库自身的 `SlidingWindowTest` / `SlidingWindowIT` 检查 Redis 微秒边界；本项目检查外部接入，不读取内部实现来替代公共接口测试。
- 这里没有生产 Kafka 或 ODPS，也不是容量压测；有限任务全部完成不能证明任意调度条件下的长期公平性。

即使平滑限速留余量在这些场景中通过，也不构成任意网络延迟下的服务端窗口保证；需要按真实环境选择余量。

## 性能与稳定性

[负载与故障测试](LOAD_TESTS.md)：获取延迟、有效吞吐、连接池开销、资源占用，以及 30 JVM 持续运行和 Redis 故障恢复。
