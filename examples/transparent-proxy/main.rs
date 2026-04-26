use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use env_logger::Env;
use smoltcp::{
    phy::{DeviceCapabilities, Medium},
    wire::{HardwareAddress, IpAddress, IpCidr, IpProtocol, IpVersion, Ipv4Cidr},
};
use structopt::StructOpt;
use tokio_smoltcp::{BufferSize, Net, NetConfig, device::ChannelCapture, smoltcp::iface};
#[cfg(windows)]
use tun2::AbstractDevice;

mod filter;
mod icmp_proxy;
mod tcp_proxy;
mod udp_proxy;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "transparent-proxy",
    about = "Accept TCP/UDP/ICMP traffic from a TUN device and transparently forward it."
)]
pub(crate) struct Cli {
    /// Physical egress interface used for outbound connections. Leave empty to use system routing.
    #[structopt(short = "i", long = "interface", default_value = "以太网")]
    interface: String,

    /// Name of the TUN device to create/open.
    #[structopt(short = "n", long = "name", default_value = "utun8")]
    name: String,

    /// TUN local IPv4 address.
    #[structopt(long = "tun-addr", default_value = "10.10.10.2")]
    tun_addr: Ipv4Addr,

    /// TUN peer or destination IPv4 address.
    #[structopt(long = "tun-gateway", default_value = "10.10.10.1")]
    tun_gateway: Ipv4Addr,

    /// TUN IPv4 netmask.
    #[structopt(long = "tun-netmask", default_value = "255.255.255.0")]
    tun_netmask: Ipv4Addr,

    /// TUN route CIDR.
    #[structopt(long = "tun-route", default_value = "1.1.1.1/32", parse(try_from_str = parse_ipv4_cidr))]
    tun_route: Ipv4Cidr,

    /// TUN MTU.
    #[structopt(long = "mtu", default_value = "1500")]
    mtu: u16,

    /// Env logger log level.
    #[structopt(long = "log-level", default_value = "info")]
    log_level: String,

    /// Rewrite every TCP/UDP/ICMP destination IP to this upstream server.
    /// TCP and UDP keep the original destination port.
    #[structopt(long = "upstream-server")]
    upstream_server: Option<IpAddr>,
}

#[derive(Debug, Default)]
pub(crate) struct UpstreamServer {
    ip: Option<IpAddr>,
}

#[derive(Debug, Default)]
pub(crate) struct ProxyStats {
    pub(crate) active_tcp: AtomicUsize,
    pub(crate) active_udp: AtomicUsize,
    pub(crate) active_icmp: AtomicUsize,
}

impl UpstreamServer {
    fn new(ip: Option<IpAddr>) -> Self {
        Self { ip }
    }

    pub(crate) fn translate_socket(&self, addr: SocketAddr) -> SocketAddr {
        match self.ip {
            Some(ip) => SocketAddr::new(ip, addr.port()),
            None => addr,
        }
    }

    pub(crate) fn translate_ipv4(&self, addr: Ipv4Addr) -> Result<Ipv4Addr, IpAddr> {
        match self.ip {
            Some(IpAddr::V4(ip)) => Ok(ip),
            Some(IpAddr::V6(ip)) => Err(IpAddr::V6(ip)),
            None => Ok(addr),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::from_args();

    env_logger::Builder::from_env(Env::default().default_filter_or(&cli.log_level))
        .format_timestamp_millis()
        .try_init()
        .context("failed to install env logger")?;
    let net = Arc::new(create_tun_device(&cli).context("failed to create proxy net")?);
    net.set_any_ip(true);

    let mut tcp_listener = net
        .tcp_bind_all()
        .await
        .context("failed to create wildcard tcp listener")?;
    let udp_socket = Arc::new(
        net.udp_bind_all()
            .await
            .context("failed to create wildcard udp socket")?,
    );
    let icmp_socket = Arc::new(
        net.raw_socket(IpVersion::Ipv4, IpProtocol::Icmp)
            .await
            .context("failed to create wildcard icmp socket")?,
    );
    let filters = Arc::new(filter::IpFilters::with_non_broadcast());
    let upstream = Arc::new(UpstreamServer::new(cli.upstream_server));
    let stats = Arc::new(ProxyStats::default());
    let stats_reporter = tokio::spawn(report_proxy_stats(stats.clone()));
    if let Some(ip) = cli.upstream_server {
        log::info!("configured upstream server: all destination ips -> {}", ip);
    }

    tokio::select! {
        result = tcp_proxy::handle_inbound_stream(&mut tcp_listener, cli.interface.clone(), filters.clone(), upstream.clone(), stats.clone()) => {
            if let Err(err) = result {
                log::error!("tcp proxy failed: err={:?}", err);
            }
        }
        result = udp_proxy::handle_inbound_datagram(udp_socket.clone(), cli.interface.clone(), filters.clone(), upstream.clone(), stats.clone()) => {
            if let Err(err) = result {
                log::error!("udp proxy failed: err={:?}", err);
            }
        }
        result = icmp_proxy::handle_inbound_icmp(icmp_socket.clone(), cli.interface.clone(), filters.clone(), upstream.clone(), stats.clone()) => {
            if let Err(err) = result {
                log::error!("icmp proxy failed: err={:?}", err);
            }
        }
    }

    stats_reporter.abort();
    log::info!("shutting down...");
    Ok(())
}

async fn report_proxy_stats(stats: Arc<ProxyStats>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    loop {
        ticker.tick().await;
        log::info!(
            "active_sessions tcp={} udp={} icmp={}",
            stats.active_tcp.load(Ordering::Relaxed),
            stats.active_udp.load(Ordering::Relaxed),
            stats.active_icmp.load(Ordering::Relaxed),
        );
    }
}

fn create_tun_device(cli: &Cli) -> Result<Net> {
    let mut cfg = tun2::Configuration::default();
    cfg.layer(tun2::Layer::L3);
    cfg.tun_name(&cli.name)
        .address(cli.tun_addr)
        .destination(cli.tun_gateway)
        .mtu(cli.mtu);
    #[cfg(not(any(target_arch = "mips", target_arch = "mips64")))]
    {
        cfg.netmask(cli.tun_netmask);
    }
    cfg.up();

    let device = tun2::create(&cfg)?;
    #[cfg(windows)]
    configure_wintun_route(&device, cli)?;
    let (mut reader, mut writer) = device.split();

    let mut caps = DeviceCapabilities::default();
    caps.max_transmission_unit = cli.mtu as usize;
    caps.medium = Medium::Ip;
    caps.max_burst_size = Some(64);

    let mtu = caps.max_transmission_unit;
    let capture = ChannelCapture::new(
        move |tx| {
            let mut buf = vec![0u8; mtu];
            loop {
                match reader.read(&mut buf) {
                    Ok(size) => {
                        if tx.blocking_send(Ok(buf[..size].to_vec())).is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = tx.blocking_send(Err(err));
                        break;
                    }
                }
            }
        },
        move |mut rx| {
            while let Some(pkt) = rx.blocking_recv() {
                if let Err(err) = writer.write_all(&pkt) {
                    log::error!("[tun] write packet failed: err={:?}", err);
                    break;
                }
            }
        },
        caps,
    );

    let mut interface_config = iface::Config::new(HardwareAddress::Ip);
    interface_config.random_seed = rand::random();
    let tun_prefix_len = netmask_to_prefix_len(cli.tun_netmask)?;

    let mut net_config = NetConfig::new(
        interface_config,
        IpCidr::new(IpAddress::Ipv4(cli.tun_gateway.into()), tun_prefix_len),
        vec![IpAddress::Ipv4(cli.tun_addr.into())],
    );
    net_config.buffer_size = BufferSize {
        tcp_rx_size: 4 * 1024 * 1024,
        tcp_tx_size: 4 * 1024 * 1024,
        udp_rx_size: 128 * 1024,
        udp_tx_size: 128 * 1024,
        udp_rx_meta_size: 128,
        udp_tx_meta_size: 128,
        raw_rx_size: 128 * 1024,
        raw_tx_size: 128 * 1024,
        raw_rx_meta_size: 128,
        raw_tx_meta_size: 128,
    };

    Ok(Net::new(capture, net_config))
}

/// Runs a command and returns an error if the command fails, just convenience for users.
#[doc(hidden)]
#[allow(dead_code)]
pub fn run_command(command: &str, args: &[&str]) -> std::io::Result<Vec<u8>> {
    let full_cmd = format!("{} {}", command, args.join(" "));
    log::debug!("Running command: \"{full_cmd}\"...");
    let out = match std::process::Command::new(command).args(args).output() {
        Ok(out) => out,
        Err(e) => {
            log::error!("Run command: \"{full_cmd}\" failed with: {e}");
            return Err(e);
        }
    };
    if !out.status.success() {
        let err = String::from_utf8_lossy(if out.stderr.is_empty() {
            &out.stdout
        } else {
            &out.stderr
        });
        let info = format!("Run command: \"{full_cmd}\" failed with {err}");
        log::error!("{}", info);
        return Err(std::io::Error::new(std::io::ErrorKind::Other, info));
    }
    Ok(out.stdout)
}

fn parse_ipv4_cidr(src: &str) -> Result<Ipv4Cidr, String> {
    src.parse::<Ipv4Cidr>()
        .map_err(|_| format!("failed to parse IPv4 CIDR: {src}"))
}

fn netmask_to_prefix_len(netmask: Ipv4Addr) -> Result<u8> {
    let bits = u32::from(netmask);
    let prefix_len = bits.leading_ones() as u8;
    let expected = if prefix_len == 0 {
        0
    } else {
        u32::MAX << (32 - prefix_len)
    };

    if bits != expected {
        bail!("invalid ipv4 netmask: {}", netmask);
    }

    Ok(prefix_len)
}

#[cfg(windows)]
fn configure_wintun_route(device: &tun2::Device, cli: &Cli) -> Result<()> {
    let network = cli.tun_route.network();
    let netmask = prefix_to_netmask(cli.tun_route.prefix_len())?;
    let adapter_index = device
        .tun_index()
        .context("failed to query wintun adapter index")?;

    let destination = network.address().to_string();
    let mask = netmask.to_string();
    let gateway = cli.tun_gateway.to_string();
    let index = adapter_index.to_string();

    let add_args = ["ADD", &destination, "MASK", &mask, &gateway, "IF", &index];
    if run_command("route", &add_args).is_err() {
        let change_args = [
            "CHANGE",
            &destination,
            "MASK",
            &mask,
            &gateway,
            "IF",
            &index,
        ];
        if run_command("route", &change_args).is_err() {
            bail!(
                "failed to configure wintun route {} via {} on ifindex {}",
                network,
                cli.tun_gateway,
                adapter_index
            );
        }
    }

    log::info!(
        "configured wintun route: {} via {} ifindex={}",
        network,
        cli.tun_gateway,
        adapter_index
    );

    Ok(())
}

#[cfg(windows)]
fn prefix_to_netmask(prefix_len: u8) -> Result<Ipv4Addr> {
    if prefix_len > 32 {
        bail!("invalid ipv4 prefix length: {}", prefix_len);
    }

    let mask = if prefix_len == 0 {
        0
    } else {
        u32::MAX << (32 - prefix_len)
    };
    Ok(Ipv4Addr::from(mask))
}
