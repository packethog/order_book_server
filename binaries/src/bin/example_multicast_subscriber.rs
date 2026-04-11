#![allow(unused_crate_dependencies)]
use std::net::{Ipv4Addr, SocketAddrV4};

use clap::Parser;
use server::protocol::constants::*;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

#[derive(Debug, Parser)]
#[command(author, version, about = "Join a multicast group and decode binary DZ frames")]
struct Args {
    /// Multicast group IP to join (e.g., 239.0.0.1)
    #[arg(long)]
    group: Ipv4Addr,

    /// UDP port to listen on
    #[arg(long, default_value_t = 5000)]
    port: u16,

    /// Local interface address to bind for multicast membership
    #[arg(long, default_value_t = Ipv4Addr::UNSPECIFIED)]
    interface: Ipv4Addr,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, args.port).into())?;
    socket.join_multicast_v4(&args.group, &args.interface)?;
    socket.set_nonblocking(true)?;

    let std_socket: std::net::UdpSocket = socket.into();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(recv_loop(std_socket))?;

    Ok(())
}

async fn recv_loop(std_socket: std::net::UdpSocket) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::from_std(std_socket)?;

    println!("listening for DZ binary frames on port {} ...", socket.local_addr()?.port());

    let mut buf = [0u8; 2048];

    loop {
        let (len, addr) = socket.recv_from(&mut buf).await?;
        let frame = &buf[..len];

        if len < FRAME_HEADER_SIZE {
            println!("[from {addr}] short frame ({len} bytes), skipping");
            continue;
        }

        let magic = u16::from_le_bytes([frame[0], frame[1]]);
        if magic != MAGIC {
            println!("[from {addr}] bad magic: 0x{magic:04X}, skipping");
            continue;
        }

        let schema_ver = frame[2];
        let channel_id = frame[3];
        let seq = u64::from_le_bytes(frame[4..12].try_into().unwrap());
        let send_ts = u64::from_le_bytes(frame[12..20].try_into().unwrap());
        let msg_count = frame[20];
        let frame_len = u16::from_le_bytes(frame[22..24].try_into().unwrap());

        println!(
            "[from {addr}] frame: ver={schema_ver} ch={channel_id} seq={seq} ts={send_ts} msgs={msg_count} len={frame_len}"
        );

        let mut offset = FRAME_HEADER_SIZE;
        for i in 0..msg_count {
            if offset + APP_MSG_HEADER_SIZE > len {
                println!("  msg[{i}]: truncated header");
                break;
            }
            let msg_type = frame[offset];
            let msg_len = frame[offset + 1] as usize;
            let flags = u16::from_le_bytes(frame[offset + 2..offset + 4].try_into().unwrap());

            if offset + msg_len > len {
                println!("  msg[{i}]: truncated body (need {msg_len}, have {})", len - offset);
                break;
            }

            let body = &frame[offset..offset + msg_len];
            match msg_type {
                MSG_TYPE_QUOTE => {
                    let inst_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
                    let src_id = u16::from_le_bytes(body[8..10].try_into().unwrap());
                    let upd_flags = body[10];
                    let src_ts = u64::from_le_bytes(body[12..20].try_into().unwrap());
                    let bid_px = i64::from_le_bytes(body[20..28].try_into().unwrap());
                    let bid_qty = u64::from_le_bytes(body[28..36].try_into().unwrap());
                    let ask_px = i64::from_le_bytes(body[36..44].try_into().unwrap());
                    let ask_qty = u64::from_le_bytes(body[44..52].try_into().unwrap());
                    let bid_n = u16::from_le_bytes(body[52..54].try_into().unwrap());
                    let ask_n = u16::from_le_bytes(body[54..56].try_into().unwrap());
                    println!(
                        "  Quote: inst={inst_id} src={src_id} flags=0x{upd_flags:02X} snap={} \
                         bid={bid_px}({bid_n}) ask={ask_px}({ask_n}) bid_qty={bid_qty} ask_qty={ask_qty} ts={src_ts}",
                        flags & FLAG_SNAPSHOT != 0
                    );
                }
                MSG_TYPE_TRADE => {
                    let inst_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
                    let src_id = u16::from_le_bytes(body[8..10].try_into().unwrap());
                    let side = body[10];
                    let src_ts = u64::from_le_bytes(body[12..20].try_into().unwrap());
                    let px = i64::from_le_bytes(body[20..28].try_into().unwrap());
                    let qty = u64::from_le_bytes(body[28..36].try_into().unwrap());
                    let tid = u64::from_le_bytes(body[36..44].try_into().unwrap());
                    let side_str = match side {
                        AGGRESSOR_BUY => "BUY",
                        AGGRESSOR_SELL => "SELL",
                        _ => "UNK",
                    };
                    println!(
                        "  Trade: inst={inst_id} src={src_id} side={side_str} px={px} qty={qty} tid={tid} ts={src_ts}"
                    );
                }
                MSG_TYPE_HEARTBEAT => {
                    let ts = u64::from_le_bytes(body[8..16].try_into().unwrap());
                    println!("  Heartbeat: ts={ts}");
                }
                MSG_TYPE_CHANNEL_RESET => {
                    let ts = u64::from_le_bytes(body[4..12].try_into().unwrap());
                    println!("  ChannelReset: ts={ts}");
                }
                MSG_TYPE_END_OF_SESSION => {
                    let ts = u64::from_le_bytes(body[4..12].try_into().unwrap());
                    println!("  EndOfSession: ts={ts}");
                }
                MSG_TYPE_INSTRUMENT_DEF => {
                    let inst_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
                    let symbol = std::str::from_utf8(&body[8..24]).unwrap_or("<bad-utf8>").trim_end_matches('\0');
                    let asset_class = body[40];
                    let px_exp = body[41] as i8;
                    let qty_exp = body[42] as i8;
                    let market_model = body[43];
                    let manifest_seq = u16::from_le_bytes(body[78..80].try_into().unwrap());
                    println!(
                        "  InstrumentDefinition: inst={inst_id} sym={symbol} asset_class={asset_class} market={market_model} px_exp={px_exp} qty_exp={qty_exp} manifest_seq={manifest_seq}"
                    );
                }
                MSG_TYPE_MANIFEST_SUMMARY => {
                    let channel = body[4];
                    let manifest_seq = u16::from_le_bytes(body[8..10].try_into().unwrap());
                    let inst_count = u32::from_le_bytes(body[12..16].try_into().unwrap());
                    let ts = u64::from_le_bytes(body[16..24].try_into().unwrap());
                    println!(
                        "  ManifestSummary: channel={channel} manifest_seq={manifest_seq} instrument_count={inst_count} ts={ts}"
                    );
                }
                _ => {
                    println!("  msg[{i}]: unknown type=0x{msg_type:02X} len={msg_len}");
                }
            }

            offset += msg_len;
        }
    }
}
