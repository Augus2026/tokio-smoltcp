use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::Result;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpSocket, TcpStream};
use tokio_smoltcp::TcpListener;

use crate::{ProxyStats, UpstreamServer, filter::IpFilters};

pub(crate) async fn handle_inbound_stream(
    listener: &mut TcpListener,
    interface: String,
    filters: Arc<IpFilters<'static>>,
    upstream: Arc<UpstreamServer>,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    loop {
        let (mut inbound, peer_addr) = listener.accept().await?;
        let local_addr = inbound.local_addr()?;
        if !filters.is_allowed(&peer_addr.ip(), &local_addr.ip()) {
            log::debug!(
                "tcp filtered connection: client={} target={}",
                peer_addr,
                local_addr
            );
            continue;
        }
        let interface = interface.clone();
        let upstream = upstream.clone();
        let stats = stats.clone();

        tokio::spawn(async move {
            let outbound_addr = upstream.translate_socket(local_addr);
            let active = stats.active_tcp.fetch_add(1, Ordering::Relaxed) + 1;
            log::info!(
                "new tcp connection: client={} target={} outbound={} active={}",
                peer_addr,
                local_addr,
                outbound_addr,
                active
            );

            match new_tcp_stream(outbound_addr, &interface).await {
                Ok(mut outbound) => match copy_bidirectional(&mut inbound, &mut outbound).await {
                    Ok(_) => {
                        let active = stats.active_tcp.fetch_sub(1, Ordering::Relaxed) - 1;
                        log::info!(
                            "tcp relay finished: client={} target={} outbound={} active={}",
                            peer_addr,
                            local_addr,
                            outbound_addr,
                            active
                        );
                    }
                    Err(err) => {
                        let active = stats.active_tcp.fetch_sub(1, Ordering::Relaxed) - 1;
                        log::warn!(
                            "tcp relay failed: client={} target={} outbound={} active={} err={:?}",
                            peer_addr,
                            local_addr,
                            outbound_addr,
                            active,
                            err
                        );
                    }
                },
                Err(err) => {
                    let active = stats.active_tcp.fetch_sub(1, Ordering::Relaxed) - 1;
                    log::warn!(
                        "tcp outbound connect failed: client={} target={} outbound={} active={} err={:?}",
                        peer_addr,
                        local_addr,
                        outbound_addr,
                        active,
                        err
                    );
                }
            }
        });
    }
}

async fn new_tcp_stream(addr: SocketAddr, interface: &str) -> io::Result<TcpStream> {
    use socket2_ext::{AddressBinding, BindDeviceOption};

    let domain = match addr.ip() {
        IpAddr::V4(_) => socket2::Domain::IPV4,
        IpAddr::V6(_) => socket2::Domain::IPV6,
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, None)?;

    if !interface.is_empty() && !addr.ip().is_loopback() {
        match addr.ip() {
            IpAddr::V4(_) => socket.bind_to_device(BindDeviceOption::v4(interface))?,
            IpAddr::V6(_) => socket.bind_to_device(BindDeviceOption::v6(interface))?,
        }
    }

    socket.set_keepalive(true)?;
    socket.set_nodelay(true)?;
    socket.set_nonblocking(true)?;

    let stream = TcpSocket::from_std_stream(socket.into())
        .connect(addr)
        .await?;
    Ok(stream)
}
