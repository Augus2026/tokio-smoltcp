use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use env_logger::Env;
use structopt::StructOpt;

mod icmp;
mod tcp;
mod udp;

#[derive(Debug, Clone, Copy, StructOpt)]
enum Mode {
    Tcp,
    Udp,
    Icmp,
    Mixed,
}

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "traffic-bench", about = "Generate TCP/UDP/ICMP traffic for transparent proxy testing.")]
struct Cli {
    #[structopt(long)]
    target: Ipv4Addr,

    #[structopt(long, default_value = "60")]
    duration: u64,

    #[structopt(long, default_value = "100")]
    concurrency: usize,

    #[structopt(long, default_value = "64")]
    payload_size: usize,

    #[structopt(long, default_value = "100")]
    rate: u64,

    #[structopt(long, default_value = "80")]
    tcp_target_port: u16,

    #[structopt(long, default_value = "9000")]
    udp_target_port: u16,

    #[structopt(long, default_value = "info")]
    log_level: String,

    #[structopt(subcommand)]
    mode: Mode,
}

#[derive(Default)]
pub(crate) struct BenchStats {
    pub(crate) started: AtomicU64,
    pub(crate) succeeded: AtomicU64,
    pub(crate) failed: AtomicU64,
    pub(crate) bytes_sent: AtomicU64,
    pub(crate) bytes_recv: AtomicU64,
}

impl BenchStats {
    pub(crate) fn record_start(&self) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_success(&self) {
        self.succeeded.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_bytes_sent(&self, n: usize) {
        self.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
    }

    pub(crate) fn add_bytes_recv(&self, n: usize) {
        self.bytes_recv.fetch_add(n as u64, Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Arc::new(Cli::from_args());
    env_logger::Builder::from_env(Env::default().default_filter_or(&cli.log_level))
        .format_timestamp_millis()
        .try_init()?;

    let stats = Arc::new(BenchStats::default());
    let reporter = tokio::spawn(report(stats.clone()));

    match cli.mode {
        Mode::Tcp => tcp::run(cli.clone(), stats.clone()).await?,
        Mode::Udp => udp::run(cli.clone(), stats.clone()).await?,
        Mode::Icmp => icmp::run(cli.clone(), stats.clone()).await?,
        Mode::Mixed => {
            let tcp = tokio::spawn(tcp::run(cli.clone(), stats.clone()));
            let udp = tokio::spawn(udp::run(cli.clone(), stats.clone()));
            let icmp = tokio::spawn(icmp::run(cli.clone(), stats.clone()));

            tcp.await??;
            udp.await??;
            icmp.await??;
        }
    }

    reporter.abort();
    log_summary(&stats);
    Ok(())
}

async fn report(stats: Arc<BenchStats>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    loop {
        ticker.tick().await;
        log_summary(&stats);
    }
}

fn log_summary(stats: &BenchStats) {
    log::info!(
        "started={} ok={} fail={} tx_bytes={} rx_bytes={}",
        stats.started.load(Ordering::Relaxed),
        stats.succeeded.load(Ordering::Relaxed),
        stats.failed.load(Ordering::Relaxed),
        stats.bytes_sent.load(Ordering::Relaxed),
        stats.bytes_recv.load(Ordering::Relaxed),
    );
}

