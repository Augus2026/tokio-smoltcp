use std::io;
use std::mem::MaybeUninit;
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use smoltcp::wire::{Icmpv4Message, Icmpv4Packet, IpProtocol, Ipv4Packet, Ipv4Repr};
use tokio::sync::{Mutex, mpsc};
use tokio_smoltcp::RawSocket;

use crate::{ProxyStats, UpstreamServer, filter::IpFilters};

const DEFAULT_IP_TTL: u8 = 64;
const MAX_ICMP_PACKET_SIZE: usize = 9000;
static NEXT_ICMP_PROXY_IDENT: AtomicU16 = AtomicU16::new(0x4000);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct IcmpSessionKey {
    client: Ipv4Addr,
    target: Ipv4Addr,
    kind: u8,
    code: u8,
    identifier: Option<u16>,
}

pub(crate) async fn handle_inbound_icmp(
    socket: Arc<RawSocket>,
    interface: String,
    filters: Arc<IpFilters<'static>>,
    upstream: Arc<UpstreamServer>,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    let sessions = Arc::new(Mutex::new(std::collections::HashMap::<
        IcmpSessionKey,
        mpsc::UnboundedSender<Vec<u8>>,
    >::new()));

    loop {
        let mut buf = vec![0u8; MAX_ICMP_PACKET_SIZE];
        let size = match socket.recv(&mut buf).await {
            Ok(size) => size,
            Err(err) => {
                log::warn!("icmp recv from tun stack failed: err={:?}", err);
                continue;
            }
        };
        let packet = &buf[..size];
        let inbound = match parse_inbound_icmp_packet(packet) {
            Ok(packet) => packet,
            Err(err) => {
                log::warn!("icmp parse failed: err={:?}", err);
                continue;
            }
        };
        if !filters.is_allowed(&IpAddr::V4(inbound.client), &IpAddr::V4(inbound.target)) {
            log::debug!(
                "icmp filtered packet: client={} target={} kind={} code={}",
                inbound.client,
                inbound.target,
                inbound.kind,
                inbound.code
            );
            continue;
        }

        let tx = {
            let mut guard = sessions.lock().await;
            if let Some(tx) = guard.get(&inbound.key) {
                tx.clone()
            } else {
                let (tx, rx) = mpsc::unbounded_channel();
                guard.insert(inbound.key, tx.clone());
                tokio::spawn(handle_icmp_session(
                    socket.clone(),
                    sessions.clone(),
                    inbound.key,
                    interface.clone(),
                    upstream.clone(),
                    stats.clone(),
                    rx,
                ));
                tx
            }
        };

        if tx.send(packet.to_vec()).is_err() {
            sessions.lock().await.remove(&inbound.key);
        }
    }
}

struct InboundIcmpPacket<'a> {
    key: IcmpSessionKey,
    client: Ipv4Addr,
    target: Ipv4Addr,
    kind: u8,
    code: u8,
    #[allow(dead_code)]
    icmp_payload: &'a [u8],
}

fn parse_inbound_icmp_packet(packet: &[u8]) -> io::Result<InboundIcmpPacket<'_>> {
    let ipv4 = Ipv4Packet::new_checked(packet)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    if ipv4.next_header() != IpProtocol::Icmp {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raw packet is not icmp",
        ));
    }

    let payload = ipv4.payload();
    if payload.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "icmp payload too short",
        ));
    }

    let client: Ipv4Addr = ipv4.src_addr().into();
    let target: Ipv4Addr = ipv4.dst_addr().into();
    let kind = payload[0];
    let code = payload[1];
    let identifier = extract_icmp_identifier(payload);

    Ok(InboundIcmpPacket {
        key: IcmpSessionKey {
            client,
            target,
            kind,
            code,
            identifier,
        },
        client,
        target,
        kind,
        code,
        icmp_payload: payload,
    })
}

async fn handle_icmp_session(
    tun_socket: Arc<RawSocket>,
    sessions: Arc<Mutex<std::collections::HashMap<IcmpSessionKey, mpsc::UnboundedSender<Vec<u8>>>>>,
    key: IcmpSessionKey,
    interface: String,
    upstream: Arc<UpstreamServer>,
    stats: Arc<ProxyStats>,
    mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let outbound_target = match upstream.translate_ipv4(key.target) {
        Ok(addr) => addr,
        Err(addr) => {
            log::warn!(
                "icmp upstream server must be ipv4: client={} target={} upstream={}",
                key.client,
                key.target,
                addr
            );
            return;
        }
    };
    let active = stats.active_icmp.fetch_add(1, Ordering::Relaxed) + 1;
    log::info!(
        "new icmp session: client={} target={} outbound={} kind={} code={} id={:?} active={}",
        key.client,
        key.target,
        outbound_target,
        key.kind,
        key.code,
        key.identifier,
        active
    );

    let result = async {
        let outbound = new_icmp_socket(outbound_target, &interface)?;
        let recv_socket = outbound.try_clone()?;
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
        let proxy_ident = next_icmp_proxy_ident();

        std::thread::spawn(move || {
            let mut buf = vec![MaybeUninit::<u8>::uninit(); MAX_ICMP_PACKET_SIZE];
            loop {
                match recv_socket.recv(&mut buf) {
                    Ok(size) => {
                        let packet = unsafe {
                            std::slice::from_raw_parts(buf.as_ptr() as *const u8, size).to_vec()
                        };
                        if reply_tx.send(packet).is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        log::warn!("icmp host recv failed: err={:?}", err);
                        break;
                    }
                }
            }
        });

        loop {
            tokio::select! {
                maybe_packet = rx.recv() => {
                    match maybe_packet {
                        Some(packet) => {
                            forward_icmp_request(
                                &outbound,
                                &packet,
                                key.identifier,
                                proxy_ident,
                            )?;
                        }
                        None => break,
                    }
                }
                maybe_reply = reply_rx.recv() => {
                    match maybe_reply {
                        Some(reply) => {
                            let packet = build_icmp_reply_packet(
                                key.client,
                                key.target,
                                &reply,
                                key.identifier,
                                proxy_ident,
                            )?;
                            tun_socket.send(&packet).await?;
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
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
            let active = stats.active_icmp.fetch_sub(1, Ordering::Relaxed) - 1;
            log::info!(
                "icmp session finished: client={} target={} outbound={} kind={} code={} id={:?} active={}",
                key.client,
                key.target,
                outbound_target,
                key.kind,
                key.code,
                key.identifier,
                active
            );
        }
        Err(err) => {
            let active = stats.active_icmp.fetch_sub(1, Ordering::Relaxed) - 1;
            log::warn!(
                "icmp session failed: client={} target={} outbound={} kind={} code={} id={:?} active={} err={:?}",
                key.client,
                key.target,
                outbound_target,
                key.kind,
                key.code,
                key.identifier,
                active,
                err
            );
        }
    }
}

fn extract_icmp_identifier(payload: &[u8]) -> Option<u16> {
    if payload.len() < 8 {
        return None;
    }

    match payload[0] {
        0 | 8 => Some(u16::from_be_bytes([payload[4], payload[5]])),
        _ => None,
    }
}

fn next_icmp_proxy_ident() -> u16 {
    NEXT_ICMP_PROXY_IDENT.fetch_add(1, Ordering::Relaxed)
}

fn forward_icmp_request(
    outbound: &socket2::Socket,
    packet: &[u8],
    original_ident: Option<u16>,
    proxy_ident: u16,
) -> io::Result<()> {
    let ipv4 = Ipv4Packet::new_checked(packet)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    if ipv4.next_header() != IpProtocol::Icmp {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "outbound packet is not icmp",
        ));
    }

    let mut payload = ipv4.payload().to_vec();
    rewrite_icmp_echo_request(&mut payload, original_ident, proxy_ident)?;

    let sent = outbound.send(&payload)?;
    if sent != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "failed to send full icmp payload",
        ));
    }

    Ok(())
}

fn build_icmp_reply_packet(
    client: Ipv4Addr,
    target: Ipv4Addr,
    reply: &[u8],
    original_ident: Option<u16>,
    proxy_ident: u16,
) -> io::Result<Vec<u8>> {
    let (src_addr, hop_limit, payload) = match Ipv4Packet::new_checked(reply) {
        Ok(ipv4) if ipv4.next_header() == IpProtocol::Icmp => {
            let mut payload = ipv4.payload().to_vec();
            rewrite_icmp_echo_reply(&mut payload, original_ident, proxy_ident)?;
            (target, ipv4.hop_limit(), payload)
        }
        _ => {
            let mut payload = reply.to_vec();
            rewrite_icmp_echo_reply(&mut payload, original_ident, proxy_ident)?;
            (target, DEFAULT_IP_TTL, payload)
        }
    };

    let repr = Ipv4Repr {
        src_addr: src_addr.into(),
        dst_addr: client.into(),
        next_header: IpProtocol::Icmp,
        payload_len: payload.len(),
        hop_limit,
    };
    let mut bytes = vec![0u8; repr.buffer_len() + payload.len()];
    let mut packet = Ipv4Packet::new_unchecked(&mut bytes);
    repr.emit(&mut packet, &smoltcp::phy::ChecksumCapabilities::default());
    packet.payload_mut().copy_from_slice(&payload);
    packet.fill_checksum();
    Ok(bytes)
}

fn rewrite_icmp_echo_request(
    payload: &mut [u8],
    original_ident: Option<u16>,
    proxy_ident: u16,
) -> io::Result<()> {
    let mut icmp = Icmpv4Packet::new_checked(payload)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    if icmp.msg_type() != Icmpv4Message::EchoRequest {
        return Ok(());
    }

    let original_ident = original_ident.unwrap_or_else(|| icmp.echo_ident());

    icmp.set_echo_ident(proxy_ident);
    icmp.fill_checksum();

    log::debug!(
        "icmp echo request rewritten: target_ident={} proxy_ident={} seq={}",
        original_ident,
        proxy_ident,
        icmp.echo_seq_no()
    );

    Ok(())
}

fn rewrite_icmp_echo_reply(
    payload: &mut [u8],
    original_ident: Option<u16>,
    proxy_ident: u16,
) -> io::Result<()> {
    let mut icmp = Icmpv4Packet::new_checked(payload)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    if icmp.msg_type() != Icmpv4Message::EchoReply {
        return Ok(());
    }

    if icmp.echo_ident() != proxy_ident {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "icmp echo reply proxy ident mismatch",
        ));
    }

    let original_ident = original_ident.unwrap_or(proxy_ident);

    icmp.set_echo_ident(original_ident);
    icmp.fill_checksum();

    log::debug!(
        "icmp echo reply restored: proxy_ident={} target_ident={} seq={}",
        proxy_ident,
        original_ident,
        icmp.echo_seq_no()
    );

    Ok(())
}

fn new_icmp_socket(addr: Ipv4Addr, interface: &str) -> io::Result<socket2::Socket> {
    use socket2_ext::{AddressBinding, BindDeviceOption};

    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::RAW,
        Some(socket2::Protocol::ICMPV4),
    )?;

    if !interface.is_empty() && !addr.is_loopback() {
        socket.bind_to_device(BindDeviceOption::v4(interface))?;
    }

    socket.connect(&socket2::SockAddr::from(SocketAddrV4::new(addr, 0)))?;
    Ok(socket)
}
