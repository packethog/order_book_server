#![allow(unused_crate_dependencies)]
use std::net::Ipv4Addr;
use std::time::Duration;

use clap::Parser;
use server::{MulticastConfig, Result, parse_channels, run_websocket_server};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long)]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long)]
    port: u16,

    /// Compression level for WebSocket connections.
    /// Accepts values in the range `0..=9`.
    /// * `0` – compression disabled.
    /// * `1` – fastest compression, low compression ratio (default).
    /// * `9` – slowest compression, highest compression ratio.
    ///
    /// The level is passed to `flate2::Compression::new(level)`; see the
    /// documentation for <https://docs.rs/flate2/1.1.2/flate2/struct.Compression.html#method.new> for more info.
    #[arg(long)]
    websocket_compression_level: Option<u32>,

    /// Multicast group address (e.g., 239.0.0.1). Enables multicast when set.
    #[arg(long)]
    multicast_group: Option<Ipv4Addr>,

    /// UDP port for multicast traffic.
    #[arg(long, default_value_t = 5000)]
    multicast_port: u16,

    /// Local address to bind the multicast UDP socket. Required when
    /// `--multicast-group` is set.
    #[arg(long)]
    multicast_bind_addr: Option<Ipv4Addr>,

    /// Comma-separated list of channels to publish via multicast (e.g., "l2,trades").
    #[arg(long, default_value = "l2,trades")]
    multicast_channels: String,

    /// Number of price levels to include in multicast L2 snapshots.
    #[arg(long, default_value_t = 5)]
    multicast_l2_levels: usize,

    /// Interval in seconds between full multicast book snapshots.
    #[arg(long, default_value_t = 5)]
    multicast_snapshot_interval: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let compression_level = args.websocket_compression_level.unwrap_or(/* Some compression */ 1);

    let multicast_config = if let Some(group_addr) = args.multicast_group {
        let Some(bind_addr) = args.multicast_bind_addr else {
            eprintln!("error: --multicast-bind-addr is required when --multicast-group is set");
            std::process::exit(1);
        };
        let Ok(channels) = parse_channels(&args.multicast_channels) else {
            eprintln!("error: invalid --multicast-channels value: {}", args.multicast_channels);
            std::process::exit(1);
        };
        Some(MulticastConfig {
            group_addr,
            port: args.multicast_port,
            bind_addr,
            channels,
            l2_levels: args.multicast_l2_levels,
            snapshot_interval: Duration::from_secs(args.multicast_snapshot_interval),
        })
    } else {
        None
    };

    run_websocket_server(&full_address, true, compression_level, multicast_config).await?;

    Ok(())
}
