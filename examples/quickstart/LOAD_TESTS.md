# 性能与稳定性验证

[返回独立示例](README.md) · [本机实测结果与瓶颈](../../docs/performance-review.zh-CN.md)

测试只使用脚本启动的隔离 Redis，避免访问已有 Redis。先构建示例（例如先运行一次 `run.py`），再在仓库根目录执行：

```bash
python3 examples/quickstart/load_test.py
```

需要 Java 8+、Python 3.8+、Redis。故障注入使用 POSIX 进程信号，适用于 Linux/macOS；Windows 请在 WSL 中运行。

## 性能对比

```bash
python3 examples/quickstart/load_test.py --mode perf --seconds 10 --repeats 2
python3 examples/quickstart/load_test.py --mode perf --case hot-30-cooperative
```

每轮先预热 3 秒，再测量指定时长。第二轮按相反顺序运行，降低固定顺序带来的偏差。每个案例重启一个独立 Redis，关闭持久化，使用本机 TCP 连接。

| 案例 | 负载 |
|---|---|
| `single` | 1 JVM、1 线程、1 桶，只开令牌桶 |
| `single-window` | 同上，加滑动窗口 |
| `hot-30` | 1 JVM、30 客户端线程竞争 1 桶；忽略 WAIT 的重试提示，测试高频申请 |
| `hot-30-cooperative` | 同上，按 retryAfter 重试，代表推荐的调用方式 |
| `resources-100` | 1 JVM、30 线程、100 个资源 |
| `resources-100-no-ping` | 同上，工厂连接池关闭 testOnBorrow，对比每次借连接时 PING 的开销 |
| `hot-30-default` | 自适应重试，关闭借出校验，使用当前默认连接策略 |
| `resources-5000-retention` | 5,000 个资源，设置 1 秒最短闲置期；安全租约期限仍为 5 秒，停止 7 秒后验证 key 全部回收 |
| `resources-5000` | 1 JVM、1 线程轮询 5000 个资源，观察高基数资源的状态保留 |

性能场景的 rate/burst 和窗口上限故意设高，测量实现开销；不是下游真实配额配置。`hot-30` 的大量 WAIT 不应算作有效吞吐，也不是推荐的生产重试方式。

输出区分 calls/s（所有获取）、grants/s（获准）、WAIT、ERROR 和 Redis 不可用时的拒绝。所有案例通过公共工厂创建连接池；连接池比较只调整借出校验选项；当前库默认关闭借出校验并启用空闲校验；脚本保留开启借出校验的对照案例。

## 持续运行与故障

```bash
python3 examples/quickstart/load_test.py --mode soak --soak-seconds 180
# 可选：再验证关闭借出 PING 后的故障恢复
python3 examples/quickstart/load_test.py --mode soak --soak-seconds 90 --no-ping
```

`--no-ping` 使用当前默认连接策略，不加则保留旧借出校验策略作对照。30 个独立 JVM 持续共享一个 rate=5、burst=1、75/15s 的桶，按重试提示等待。中途按持续时间比例依次：

1. 终止一个 JVM，不主动清理它的等待资格。
2. 断开 Redis 的客户端连接。
3. 清空 Redis 脚本缓存。
4. 暂停 Redis 进程 3 秒，触发 socket 超时。
5. 正常停止 Redis，停机 2 秒，再从同一 AOF 目录启动。

该场景使用 AOF everysec；正常停止并重启保留状态，**不是断电、主从切换或数据丢失测试**。短时间采样允许上报观测误差，不能替代长时间稳定性验证。

检查 Redis 故障时 DENY、恢复后继续发放、死亡客户端不会永久阻塞、所有幸存客户端在最后一次恢复后仍能获准、没有确定的滑动窗口超发。获取回执的精确时间不可从公共 API 读取，因此边界不确定的组数会单独报告。

## 指标与限制

- 获取耗时包含连接池借出、可选的 PING、TCP、Lua 与响应解析。闭环负载不等于固定到达率，不能拿它的 P99 直接当生产 SLA。
- 每线程最多保留 20,000 个均匀抽样的延迟。P50/P95/P99 根据调用数加权计算，属于样本估计；平均值和最大值来自完整计数。
- Redis INFO 提供内存、RSS、CPU、连接和命令计数。总命令数包含 Lua 内部命令；EVAL/EVALSHA 和 PING 单独计数，更接近客户端协议请求量。
- JVM 指标是堆用量、进程 CPU 和 GC，不是完整 RSS；包含压测程序自身开销。累计 CPU 和 GC 包含预热、测量与清理；Redis 采样速率只用测量时段内的样本计算。
- 结束后等待 5 秒，观察 2 秒 TTL 回执过期后的 Redis 内存和 key 数。普通案例的闲置保留期为 60 秒，因此此时仍保留桶和窗口历史；专门的 retention 案例等待 7 秒并断言所有状态已回收。有效保留期不会短于令牌补满、窗口、回执和租约的安全期限。
- 被终止进程的聚合计数来自最后一次快照，最多缺少约 1 秒数据；该进程的延迟样本不计入故障场景的分位数。发放日志单独刷新保留。
- 本机回环测试会受共享机器负载影响；短时测试和数分钟持续运行不能证明数天稳定、集群故障转移安全或生产容量。

结果位于 `examples/quickstart/target/load-*/`：每轮报告、每秒采样、JVM 日志、故障时间及 Redis 日志。默认运行约数分钟；可调整时长，不自动安排后台定时任务。
