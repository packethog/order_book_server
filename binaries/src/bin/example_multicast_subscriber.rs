#![allow(unused_crate_dependencies)]
use std::net::{Ipv4Addr, SocketAddrV4};

use clap::Parser;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

#[derive(Debug, Parser)]
#[command(author, version, about = "Join a multicast group and print received UDP datagrams")]
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

    // Build the socket with socket2 for multicast options.
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, args.port).into())?;
    socket.join_multicast_v4(&args.group, &args.interface)?;
    socket.set_nonblocking(true)?;

    let std_socket: std::net::UdpSocket = socket.into();

    // Build the tokio runtime manually so we can keep `main` synchronous and
    // avoid the `#[tokio::main]` attribute (which is fine too, but this keeps
    // the socket setup clearly outside the async context).
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(recv_loop(std_socket))?;

    Ok(())
}

async fn recv_loop(std_socket: std::net::UdpSocket) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::from_std(std_socket)?;

    println!("listening for multicast datagrams on port {} ...", socket.local_addr()?.port(),);

    let mut buf = [0u8; 2048];
    let mut count: u64 = 0;

    loop {
        let (len, addr) = socket.recv_from(&mut buf).await?;
        let text = std::str::from_utf8(&buf[..len]).unwrap_or("<non-utf8>");
        println!("[{count}] from {addr} ({len} bytes): {text}");
        count += 1;
    }
}
