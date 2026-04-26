use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::{BenchStats, Cli};

const UDP_REPLY_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) async fn run(cli: Arc<Cli>, stats: Arc<BenchStats>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(cli.duration);
    let interval = if cli.rate == 0 {
        Duration::from_secs(1)
    } else {
        Duration::from_secs_f64(1.0 / cli.rate as f64)
    };
    let mut tasks = Vec::with_capacity(cli.concurrency);

    for _ in 0..cli.concurrency {
        let cli = cli.clone();
        let stats = stats.clone();
        tasks.push(tokio::spawn(async move {
            let socket = match tokio::net::UdpSocket::bind(("0.0.0.0", 0)).await {
                Ok(socket) => socket,
                Err(_) => {
                    stats.record_failure();
                    return;
                }
            };
            let payload = vec![0u8; cli.payload_size];
            let mut buf = vec![0u8; payload.len().max(1)];
            let mut ticker = tokio::time::interval(interval);

            while Instant::now() < deadline {
                ticker.tick().await;
                stats.record_start();
                match socket
                    .send_to(&payload, (cli.target, cli.udp_target_port))
                    .await
                {
                    Ok(n) => {
                        stats.add_bytes_sent(n);
                        match tokio::time::timeout(UDP_REPLY_TIMEOUT, socket.recv_from(&mut buf))
                            .await
                        {
                            Ok(Ok((reply_len, _))) => {
                                stats.add_bytes_recv(reply_len);
                                if reply_len == payload.len() {
                                    stats.record_success();
                                } else {
                                    stats.record_failure();
                                }
                            }
                            Ok(Err(_)) | Err(_) => stats.record_failure(),
                        }
                    }
                    Err(_) => stats.record_failure(),
                }
            }
        }));
    }

    for task in tasks {
        task.await?;
    }
    Ok(())
}
