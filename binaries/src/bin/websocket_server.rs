#![allow(unused_crate_dependencies)]
use std::net::Ipv4Addr;
use std::time::Duration;

use clap::Parser;
use server::{MulticastConfig, Result, run_websocket_server};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long)]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long)]
    port: u16,

    /// Compression level for WebSocket connections (0-9, default 1).
    #[arg(long)]
    websocket_compression_level: Option<u32>,

    /// Multicast group address (e.g., 239.0.0.1). Enables multicast when set.
    #[arg(long)]
    multicast_group: Option<Ipv4Addr>,

    /// UDP port for hot-path multicast traffic (Quote/Trade/Heartbeat/EndOfSession).
    #[arg(long, default_value_t = 5000)]
    multicast_port: u16,

    /// UDP port for reference-data multicast traffic (InstrumentDefinition/ManifestSummary/ChannelReset).
    #[arg(long, default_value_t = 5001)]
    multicast_ref_data_port: u16,

    /// Local address to bind the multicast UDP socket.
    #[arg(long)]
    multicast_bind_addr: Option<Ipv4Addr>,

    /// Interval in seconds between full BBO snapshot resends.
    #[arg(long, default_value_t = 5)]
    multicast_snapshot_interval: u64,

    /// Max frame size in bytes (default 1448 for GRE tunnels).
    #[arg(long, default_value_t = 1448)]
    multicast_mtu: u16,

    /// Hyperliquid REST API URL for instrument metadata.
    #[arg(long, default_value = "https://api.hyperliquid.xyz")]
    hl_api_url: String,

    /// Source ID for Quote/Trade messages.
    #[arg(long, default_value_t = 1)]
    source_id: u16,

    /// Seconds of silence before sending a Heartbeat.
    #[arg(long, default_value_t = 5)]
    heartbeat_interval: u64,

    /// How often (seconds) to re-poll the HL API to detect listings/delistings.
    #[arg(long, default_value_t = 60)]
    instruments_refresh_interval: u64,

    /// Full InstrumentDefinition retransmission cycle (seconds).
    #[arg(long, default_value_t = 30)]
    definition_cycle: u64,

    /// ManifestSummary cadence (seconds).
    #[arg(long, default_value_t = 1)]
    manifest_cadence: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let compression_level = args.websocket_compression_level.unwrap_or(1);

    let multicast_config = if let Some(group_addr) = args.multicast_group {
        let Some(bind_addr) = args.multicast_bind_addr else {
            eprintln!("error: --multicast-bind-addr is required when --multicast-group is set");
            std::process::exit(1);
        };
        Some(MulticastConfig {
            group_addr,
            port: args.multicast_port,
            ref_data_port: args.multicast_ref_data_port,
            bind_addr,
            snapshot_interval: Duration::from_secs(args.multicast_snapshot_interval),
            mtu: args.multicast_mtu,
            source_id: args.source_id,
            heartbeat_interval: Duration::from_secs(args.heartbeat_interval),
            hl_api_url: args.hl_api_url,
            instruments_refresh_interval: Duration::from_secs(args.instruments_refresh_interval),
            definition_cycle: Duration::from_secs(args.definition_cycle),
            manifest_cadence: Duration::from_secs(args.manifest_cadence),
        })
    } else {
        None
    };

    run_websocket_server(&full_address, true, compression_level, multicast_config).await?;

    Ok(())
}
