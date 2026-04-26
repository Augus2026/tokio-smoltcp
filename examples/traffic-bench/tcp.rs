use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{BenchStats, Cli};

pub(crate) async fn run(cli: Arc<Cli>, stats: Arc<BenchStats>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(cli.duration);
    let mut tasks = Vec::with_capacity(cli.concurrency);

    for _ in 0..cli.concurrency {
        let cli = cli.clone();
        let stats = stats.clone();
        tasks.push(tokio::spawn(async move {
            let payload = vec![0u8; cli.payload_size];
            while Instant::now() < deadline {
                stats.record_start();
                match tokio::net::TcpStream::connect((cli.target, cli.tcp_target_port)).await {
                    Ok(mut stream) => {
                        if stream.write_all(&payload).await.is_err() {
                            stats.record_failure();
                            continue;
                        }
                        stats.add_bytes_sent(payload.len());

                        let mut buf = vec![0u8; payload.len().max(1)];
                        match tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf)).await {
                            Ok(Ok(n)) => {
                                stats.add_bytes_recv(n);
                                stats.record_success();
                            }
                            Ok(Err(_)) | Err(_) => {
                                stats.record_failure();
                            }
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

