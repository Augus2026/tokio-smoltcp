use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use tokio::net::UdpSocket as TokioUdpSocket;
use tokio::sync::{Mutex, mpsc};
use tokio_smoltcp::UdpSocket;

use crate::{ProxyStats, UpstreamServer, filter::IpFilters};

const MAX_UDP_DATAGRAM_SIZE: usize = 9000;
const UDP_SESSION_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) async fn handle_inbound_datagram(
    socket: Arc<UdpSocket>,
    interface: String,
    filters: Arc<IpFilters<'static>>,
    upstream: Arc<UpstreamServer>,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    let sessions = Arc::new(Mutex::new(std::collections::HashMap::<
        (SocketAddr, SocketAddr),
        mpsc::UnboundedSender<Vec<u8>>,
    >::new()));

    loop {
        let mut buf = vec![0u8; MAX_UDP_DATAGRAM_SIZE];
        let (size, local, remote) = match socket.recv_from_full(&mut buf).await {
            Ok(parts) => parts,
            Err(err) => {
                log::warn!("udp recv from tun stack failed: err={:?}", err);
                continue;
            }
        };
        if !filters.is_allowed(&remote.ip(), &local.ip()) {
            log::debug!("udp filtered datagram: client={} target={}", remote, local);
            continue;
        }
        let payload = buf[..size].to_vec();
        let key = (remote, local);

        let tx = {
            let mut guard = sessions.lock().await;
            if let Some(tx) = guard.get(&key) {
                tx.clone()
            } else {
                let (tx, rx) = mpsc::unbounded_channel();
                guard.insert(key, tx.clone());
                tokio::spawn(handle_udp_session(
                    socket.clone(),
                    sessions.clone(),
                    remote,
                    local,
                    interface.clone(),
                    upstream.clone(),
                    stats.clone(),
                    rx,
                ));
                tx
            }
        };

        if tx.send(payload).is_err() {
            sessions.lock().await.remove(&key);
        }
    }
}

async fn handle_udp_session(
    socket: Arc<UdpSocket>,
    sessions: Arc<
        Mutex<std::collections::HashMap<(SocketAddr, SocketAddr), mpsc::UnboundedSender<Vec<u8>>>>,
    >,
    client: SocketAddr,
    target: SocketAddr,
    interface: String,
    upstream: Arc<UpstreamServer>,
    stats: Arc<ProxyStats>,
    mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let outbound_target = upstream.translate_socket(target);
    let active = stats.active_udp.fetch_add(1, Ordering::Relaxed) + 1;
    log::info!(
        "new udp session: client={} target={} outbound={} active={}",
        client,
        target,
        outbound_target,
        active
    );
    let key = (client, target);

    let result = async {
        let outbound = new_udp_packet(outbound_target, &interface).await?;
        let mut buf = vec![0u8; MAX_UDP_DATAGRAM_SIZE];

        loop {
            tokio::select! {
                maybe_data = rx.recv() => {
                    match maybe_data {
                        Some(data) => {
                            outbound.send(&data).await?;
                        }
                        None => break,
                    }
                }
                result = outbound.recv(&mut buf) => {
                    let size = result?;
                    let send_result = tokio::time::timeout(
                        Duration::from_millis(100),
                        socket.send_from(&buf[..size], target, client),
                    )
                    .await;

                    match send_result {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => return Err(err),
                        Err(_) => {
                            log::debug!(
                                "udp reply dropped because tun send timed out: client={} target={}",
                                client,
                                target
                            );
                        }
                    }
                }
                _ = tokio::time::sleep(UDP_SESSION_TIMEOUT) => {
                    break;
                }
            }
        }

        Ok::<(), io::Error>(())
    }
    .await;

    sessions.lock().await.remove(&key);

    match result {
        Ok(()) => {
            let active = stats.active_udp.fetch_sub(1, Ordering::Relaxed) - 1;
            log::info!(
                "udp session finished: client={} target={} outbound={} active={}",
                client,
                target,
                outbound_target,
                active
            );
        }
        Err(err) => {
            let active = stats.active_udp.fetch_sub(1, Ordering::Relaxed) - 1;
            log::warn!(
                "udp session failed: client={} target={} outbound={} active={} err={:?}",
                client,
                target,
                outbound_target,
                active,
                err
            );
        }
    }
}

async fn new_udp_packet(addr: SocketAddr, interface: &str) -> io::Result<TokioUdpSocket> {
    use socket2_ext::{AddressBinding, BindDeviceOption};

    let domain = match addr.ip() {
        std::net::IpAddr::V4(_) => socket2::Domain::IPV4,
        std::net::IpAddr::V6(_) => socket2::Domain::IPV6,
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, None)?;

    if !interface.is_empty() && !addr.ip().is_loopback() {
        match addr.ip() {
            std::net::IpAddr::V4(_) => socket.bind_to_device(BindDeviceOption::v4(interface))?,
            std::net::IpAddr::V6(_) => socket.bind_to_device(BindDeviceOption::v6(interface))?,
        }
    }

    socket.set_nonblocking(true)?;
    let socket = TokioUdpSocket::from_std(socket.into())?;
    socket.connect(addr).await?;
    Ok(socket)
}
