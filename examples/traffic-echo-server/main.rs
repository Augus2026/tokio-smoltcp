use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use env_logger::Env;
use structopt::StructOpt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};

#[derive(Debug, Clone, Copy, StructOpt)]
enum Mode {
    Tcp,
    Udp,
    Mixed,
}

#[derive(Debug, StructOpt, Clone)]
#[structopt(
    name = "traffic-echo-server",
    about = "TCP/UDP echo server for traffic-bench targets."
)]
struct Cli {
    #[structopt(long, default_value = "0.0.0.0")]
    bind: Ipv4Addr,

    #[structopt(long, default_value = "80")]
    tcp_port: u16,

    #[structopt(long, default_value = "9000")]
    udp_port: u16,

    #[structopt(long, default_value = "65536")]
    buffer_size: usize,

    #[structopt(long, default_value = "info")]
    log_level: String,

    #[structopt(subcommand)]
    mode: Mode,
}

#[derive(Default)]
struct EchoStats {
    tcp_connections: AtomicU64,
    tcp_errors: AtomicU64,
    udp_datagrams: AtomicU64,
    udp_errors: AtomicU64,
    bytes_recv: AtomicU64,
    bytes_sent: AtomicU64,
}

impl EchoStats {
    fn add_recv(&self, n: usize) {
        self.bytes_recv.fetch_add(n as u64, Ordering::Relaxed);
    }

    fn add_sent(&self, n: usize) {
        self.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Arc::new(Cli::from_args());
    env_logger::Builder::from_env(Env::default().default_filter_or(&cli.log_level))
        .format_timestamp_millis()
        .try_init()?;

    let stats = Arc::new(EchoStats::default());
    let reporter = tokio::spawn(report(stats.clone()));

    match cli.mode {
        Mode::Tcp => run_tcp(cli.clone(), stats.clone()).await?,
        Mode::Udp => run_udp(cli.clone(), stats.clone()).await?,
        Mode::Mixed => {
            let tcp = tokio::spawn(run_tcp(cli.clone(), stats.clone()));
            let udp = tokio::spawn(run_udp(cli.clone(), stats.clone()));

            tcp.await??;
            udp.await??;
        }
    }

    reporter.abort();
    Ok(())
}

async fn run_tcp(cli: Arc<Cli>, stats: Arc<EchoStats>) -> Result<()> {
    let addr = SocketAddrV4::new(cli.bind, cli.tcp_port);
    let listener = TcpListener::bind(addr).await?;
    log::info!("tcp echo listening on {}", listener.local_addr()?);

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                stats.tcp_connections.fetch_add(1, Ordering::Relaxed);
                log::debug!("accepted tcp connection from {peer}");

                let stats = stats.clone();
                let buffer_size = cli.buffer_size;
                tokio::spawn(async move {
                    if let Err(err) = echo_tcp(stream, buffer_size, stats.clone()).await {
                        stats.tcp_errors.fetch_add(1, Ordering::Relaxed);
                        log::debug!("tcp echo failed for {peer}: {err}");
                    }
                });
            }
            Err(err) => {
                stats.tcp_errors.fetch_add(1, Ordering::Relaxed);
                log::warn!("tcp accept failed: {err}");
            }
        }
    }
}

async fn echo_tcp(mut stream: TcpStream, buffer_size: usize, stats: Arc<EchoStats>) -> Result<()> {
    let mut buf = vec![0u8; buffer_size.max(1)];

    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        stats.add_recv(n);
        stream.write_all(&buf[..n]).await?;
        stats.add_sent(n);
    }
}

async fn run_udp(cli: Arc<Cli>, stats: Arc<EchoStats>) -> Result<()> {
    let addr = SocketAddrV4::new(cli.bind, cli.udp_port);
    let socket = UdpSocket::bind(addr).await?;
    log::info!("udp echo listening on {}", socket.local_addr()?);

    let mut buf = vec![0u8; cli.buffer_size.max(1)];
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((n, peer)) => {
                stats.udp_datagrams.fetch_add(1, Ordering::Relaxed);
                stats.add_recv(n);

                match socket.send_to(&buf[..n], peer).await {
                    Ok(sent) => stats.add_sent(sent),
                    Err(err) => {
                        stats.udp_errors.fetch_add(1, Ordering::Relaxed);
                        log::debug!("udp send to {peer} failed: {err}");
                    }
                }
            }
            Err(err) => {
                stats.udp_errors.fetch_add(1, Ordering::Relaxed);
                log::warn!("udp recv failed: {err}");
            }
        }
    }
}

async fn report(stats: Arc<EchoStats>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    loop {
        ticker.tick().await;
        log_summary(&stats);
    }
}

fn log_summary(stats: &EchoStats) {
    log::info!(
        "tcp_conn={} tcp_err={} udp_pkt={} udp_err={} rx_bytes={} tx_bytes={}",
        stats.tcp_connections.load(Ordering::Relaxed),
        stats.tcp_errors.load(Ordering::Relaxed),
        stats.udp_datagrams.load(Ordering::Relaxed),
        stats.udp_errors.load(Ordering::Relaxed),
        stats.bytes_recv.load(Ordering::Relaxed),
        stats.bytes_sent.load(Ordering::Relaxed),
    );
}
