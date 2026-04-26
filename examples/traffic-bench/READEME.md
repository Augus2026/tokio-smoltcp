# traffic-bench

`traffic-bench` 是当前项目里的流量压测工具，用于对 `transparent-proxy` 做 TCP、UDP、ICMP 单项压测，以及三类流量混合压测。

当前实现目标是两件事：

- 稳定地产生可控流量
- 持续输出统一统计，便于观察代理在压力下是否异常

## 1. 使用前提

压测前先确认：

- `transparent-proxy` 已启动
- 目标地址命中 `transparent-proxy` 的 `--tun-route`
- TUN/Wintun 路由已经生效
- 目标服务可达

如果是 ICMP 压测，还需要：

- 当前进程具备 raw socket 权限

如果路由没有经过 TUN，压测结果没有意义，因为流量可能直接走宿主机网络栈，没有经过透明代理。

## 2. 编译检查

先确认 example 可以正常编译：

```powershell
cargo check --example traffic-bench
```

## 3. 命令结构

运行形式：

```powershell
cargo run --example traffic-bench -- --target <IPv4> <mode> [options]
```

当前支持的模式：

- `tcp`
- `udp`
- `icmp`
- `mixed`

公共参数：

- `--target`：目标 IPv4 地址，必填
- `--duration`：压测时长，单位秒，默认 `60`
- `--concurrency`：并发数，默认 `100`
- `--payload-size`：负载大小，默认 `64`
- `--rate`：发送速率，默认 `100`
- `--tcp-target-port`：TCP 目标端口，默认 `80`
- `--udp-target-port`：UDP 目标端口，默认 `9000`
- `--log-level`：日志级别，默认 `info`

## 4. TCP 压测

TCP 模式会创建并发短连接。每个连接会：

- 建立 TCP 连接
- 写入一段 payload
- 尝试读取一次响应

示例：

```powershell
cargo run --example traffic-bench -- --target 1.1.1.1 tcp
```

指定端口、并发和时长：

```powershell
cargo run --example traffic-bench -- --target 1.1.1.1 --tcp-target-port 8080 --concurrency 500 --duration 300 tcp
```

适用场景：

- 测试大量短连接是否能稳定通过透明代理
- 观察高并发 TCP 下的失败率、吞吐和资源占用

注意：

- 目标端最好是一个会响应的 TCP 服务
- 如果目标服务建立连接后不返回数据，`tcp` 模式里的读操作可能超时并记为失败

## 5. UDP 压测

UDP 模式会按设定速率持续发包，适合测试高 PPS 和高吞吐下的代理行为。

示例：

```powershell
cargo run --example traffic-bench -- --target 1.1.1.1 udp
```

指定端口、速率和并发：

```powershell
cargo run --example traffic-bench -- --target 1.1.1.1 --udp-target-port 9000 --rate 1000 --concurrency 50 udp
```

适用场景：

- 测试 UDP 单向发包能力
- 观察代理在高频小包或持续大流量下是否丢包、卡顿或报错

注意：

- 当前实现默认只统计发送，不统计 UDP reply
- 如果要验证双向 UDP，目标端需要是一个 echo server 或明确会回包的服务

## 6. ICMP 压测

ICMP 模式会通过 raw socket 持续发送 Echo Request，并校验收到的 Echo Reply 是否匹配当前 `id`。

示例：

```powershell
cargo run --example traffic-bench -- --target 114.114.114.114 icmp
```

指定并发时间和 payload：

```powershell
cargo run --example traffic-bench -- --target 114.114.114.114 --duration 300 --payload-size 128 icmp
```

适用场景：

- 验证 ICMP 透明代理是否稳定
- 观察高频 ping 下的成功率和异常日志

注意：

- 当前 ICMP 压测使用 raw socket，权限不足时会失败
- 当前实现校验的是 Echo Reply 的 `id`，这是和当前透明代理的 `id` 重写逻辑对应的

## 7. 混合压测

`mixed` 模式会同时启动 TCP、UDP、ICMP 三类压测任务。

示例：

```powershell
cargo run --example traffic-bench -- --target 1.1.1.1 --duration 600 --concurrency 200 mixed
```

适用场景：

- 模拟真实网络环境下的多协议混合流量
- 观察代理在协议竞争、任务并发和资源压力下是否稳定

建议先单项压测，再做混合压测。否则一旦失败，很难快速定位是 TCP、UDP 还是 ICMP 路径出了问题。

## 8. 日志与统计

`traffic-bench` 每 5 秒会输出一次统计：

```text
started=... ok=... fail=... tx_bytes=... rx_bytes=...
```

字段含义：

- `started`：已发起请求或发送动作总数
- `ok`：成功次数
- `fail`：失败次数
- `tx_bytes`：已发送字节数
- `rx_bytes`：已接收字节数

压测结束时会再输出一次汇总。

## 9. 推荐压测步骤

建议按下面顺序执行：

1. 启动 `transparent-proxy`
2. 确认 `--tun-route` 已命中目标地址
3. 单独测试 `icmp`
4. 单独测试 `tcp`
5. 单独测试 `udp`
6. 最后执行 `mixed`

建议在压测同时做抓包：

- 抓 TUN/Wintun 适配器
- 抓物理网卡

需要同时确认：

- 请求确实进入 TUN
- 代理确实把流量转发到物理网卡
- 返回流量确实回到了代理

## 10. 如何判断结果

至少关注下面几项：

- 进程是否崩溃
- 日志是否持续报错
- 成功率是否明显下降
- 吞吐是否异常抖动
- 长时间运行后内存是否持续增长
- 高并发下线程数、句柄数是否只增不减

不要只看程序“没退出”。更可靠的结论应该是：

- 在某个时长
- 某个并发
- 某组协议组合
- 某个目标服务条件下

代理没有观察到明显错误、资源泄漏或吞吐异常。

## 11. 当前限制

当前 `traffic-bench` 有几个明确限制：

- `tcp` 模式依赖目标端有可读响应，否则可能被统计为失败
- `udp` 模式默认偏向单向发包，不是完整双向验证
- `icmp` 模式依赖 raw socket 权限
- 当前只输出日志统计，不导出 CSV 或延迟分布

如果后续要做更严格的稳定性测试，建议继续补：

- 每秒统计导出
- p50 / p95 / p99 延迟
- 失败类型分类
- UDP reply 统计
- 专用 echo server 示例
