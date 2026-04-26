use std::io;
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use smoltcp::wire::{Icmpv4Message, Icmpv4Packet, IpProtocol, Ipv4Packet};

use crate::{BenchStats, Cli};

static NEXT_ICMP_IDENT: AtomicU16 = AtomicU16::new(0x2000);
const MAX_ICMP_PACKET_SIZE: usize = 65_535;

pub(crate) async fn run(cli: Arc<Cli>, stats: Arc<BenchStats>) -> Result<()> {
    let cli = cli.clone();
    let stats = stats.clone();
    tokio::task::spawn_blocking(move || run_blocking(cli, stats)).await?
}

fn run_blocking(cli: Arc<Cli>, stats: Arc<BenchStats>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(cli.duration);
    let socket = new_icmp_socket(cli.target)?;
    let id = NEXT_ICMP_IDENT.fetch_add(1, Ordering::Relaxed);
    let mut seq = 0u16;
    let payload_size = cli.payload_size.max(8).saturating_sub(8);
    let interval = if cli.rate == 0 {
        Duration::from_secs(1)
    } else {
        Duration::from_secs_f64(1.0 / cli.rate as f64)
    };

    socket.set_read_timeout(Some(Duration::from_secs(1)))?;

    while Instant::now() < deadline {
        stats.record_start();
        let request = build_echo_request(id, seq, payload_size);
        seq = seq.wrapping_add(1);

        match socket.send(&request) {
            Ok(n) => stats.add_bytes_sent(n),
            Err(_) => {
                stats.record_failure();
                std::thread::sleep(interval);
                continue;
            }
        }

        let mut buf = vec![MaybeUninit::<u8>::uninit(); MAX_ICMP_PACKET_SIZE];
        match socket.recv(&mut buf) {
            Ok(size) => {
                let packet = unsafe {
                    std::slice::from_raw_parts(buf.as_ptr() as *const u8, size)
                };
                if is_expected_echo_reply(packet, cli.target, id)? {
                    stats.add_bytes_recv(size);
                    stats.record_success();
                } else {
                    stats.record_failure();
                }
            }
            Err(_) => stats.record_failure(),
        }

        std::thread::sleep(interval);
    }

    Ok(())
}

fn build_echo_request(id: u16, seq: u16, payload_size: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; 8 + payload_size];
    let mut icmp = Icmpv4Packet::new_unchecked(&mut bytes);
    icmp.set_msg_type(Icmpv4Message::EchoRequest);
    icmp.set_msg_code(0);
    icmp.set_echo_ident(id);
    icmp.set_echo_seq_no(seq);
    icmp.fill_checksum();
    bytes[8..].fill(0x5a);
    let mut icmp = Icmpv4Packet::new_unchecked(&mut bytes);
    icmp.fill_checksum();
    bytes
}

fn is_expected_echo_reply(packet: &[u8], target: Ipv4Addr, id: u16) -> io::Result<bool> {
    match Ipv4Packet::new_checked(packet) {
        Ok(ipv4) if ipv4.next_header() == IpProtocol::Icmp => {
            if Ipv4Addr::from(ipv4.src_addr()) != target {
                return Ok(false);
            }
            let icmp = Icmpv4Packet::new_checked(ipv4.payload())
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
            Ok(icmp.msg_type() == Icmpv4Message::EchoReply && icmp.echo_ident() == id)
        }
        _ => {
            let icmp = Icmpv4Packet::new_checked(packet)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
            Ok(icmp.msg_type() == Icmpv4Message::EchoReply && icmp.echo_ident() == id)
        }
    }
}

fn new_icmp_socket(addr: Ipv4Addr) -> io::Result<socket2::Socket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::RAW,
        Some(socket2::Protocol::ICMPV4),
    )?;
    socket.connect(&socket2::SockAddr::from(SocketAddrV4::new(addr, 0)))?;
    Ok(socket)
}
