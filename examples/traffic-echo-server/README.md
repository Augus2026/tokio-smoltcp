# traffic-echo-server

`traffic-echo-server` is a TCP/UDP echo target for `traffic-bench`.

## Build

```powershell
cargo check --example traffic-echo-server
```

## Run

Run both TCP and UDP echo services:

```powershell
cargo run --example traffic-echo-server -- mixed
```

Run TCP only:

```powershell
cargo run --example traffic-echo-server -- --tcp-port 80 tcp
```

Run UDP only:

```powershell
cargo run --example traffic-echo-server -- --udp-port 9000 udp
```

By default the server listens on `0.0.0.0`, TCP port `80`, and UDP port `9000`,
matching `traffic-bench` defaults.

On systems that require elevated privileges for low ports, use a higher TCP
port and pass the same port to `traffic-bench`:

```powershell
cargo run --example traffic-echo-server -- --tcp-port 8080 mixed
cargo run --example traffic-bench -- --target 10.0.0.2 --tcp-target-port 8080 tcp
```

## Test with transparent-proxy upstream

Start a local test server for TCP and UDP:

```powershell
cargo run --example traffic-echo-server -- --bind 127.0.0.1 mixed
```

Start `transparent-proxy` and send every TCP, UDP, and ICMP destination to the
same test server. TCP and UDP keep the original destination port.

```powershell
cargo run --example transparent-proxy -- --tun-route 100.100.100.100/32 --upstream-server 127.0.0.1
```

Run the benchmark against the virtual target:

```powershell
cargo run --example traffic-bench -- --target 100.100.100.100 --tcp-target-port 80 tcp
```

```powershell
cargo run --example traffic-bench -- --target 100.100.100.100 --tcp-target-port 80 --udp-target-port 9000 mixed
```
