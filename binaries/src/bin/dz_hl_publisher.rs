#![allow(unused_crate_dependencies)]
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use clap::{Parser, ValueEnum};
use server::{DobConfig, IngestMode, MulticastConfig, Result, run_websocket_server};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliIngestMode {
    Block,
    Stream,
}

impl From<CliIngestMode> for IngestMode {
    fn from(value: CliIngestMode) -> Self {
        match value {
            CliIngestMode::Block => Self::Block,
            CliIngestMode::Stream => Self::Stream,
        }
    }
}

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

    /// Ingest mode for hl-node disk output.
    #[arg(long, value_enum, default_value_t = CliIngestMode::Block)]
    ingest_mode: CliIngestMode,

    /// Root containing hl-node node_* output directories. Defaults to $HOME/hl/data.
    #[arg(long)]
    hl_data_root: Option<PathBuf>,

    /// Multicast group address (e.g., 239.0.0.1). Enables multicast when set.
    #[arg(long)]
    multicast_group: Option<Ipv4Addr>,

    /// UDP port for marketdata multicast traffic (Quote/Trade/Heartbeat/EndOfSession).
    #[arg(long, default_value_t = 5000)]
    multicast_port: u16,

    /// UDP port for refdata multicast traffic (InstrumentDefinition/ManifestSummary/ChannelReset).
    #[arg(long, default_value_t = 5001)]
    multicast_refdata_port: u16,

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

    /// DoB multicast group address (e.g., 239.0.0.2). Enables DoB emission when set.
    #[arg(long)]
    dob_group: Option<Ipv4Addr>,

    /// UDP port for DoB mktdata traffic.
    #[arg(long, default_value_t = 6000)]
    dob_mktdata_port: u16,

    /// UDP port for DoB refdata traffic.
    #[arg(long, default_value_t = 6001)]
    dob_refdata_port: u16,

    /// UDP port for DoB snapshot traffic (phase 2).
    #[arg(long, default_value_t = 6002)]
    dob_snapshot_port: u16,

    /// Local interface for DoB multicast socket.
    #[arg(long)]
    dob_bind_addr: Option<Ipv4Addr>,

    /// DoB channel ID.
    #[arg(long, default_value_t = 0)]
    dob_channel_id: u8,

    /// DoB source ID (must match the Source ID Registry entry).
    #[arg(long, default_value_t = 1)]
    dob_source_id: u16,

    /// DoB max frame size (default 1232 per DoB spec).
    #[arg(long, default_value_t = 1232)]
    dob_mtu: u16,

    /// Bound on the MPSC channel between L4 apply and the DoB emitter.
    #[arg(long, default_value_t = 4096)]
    dob_channel_bound: usize,

    /// Target round-robin duration for the DoB snapshot stream (seconds).
    #[arg(long, default_value_t = 30)]
    dob_snapshot_round_duration: u64,

    /// Max frame size for DoB snapshot frames. Defaults to the same MTU as
    /// the mktdata stream.
    #[arg(long, default_value_t = 1232)]
    dob_snapshot_mtu: u16,

    /// Address for the Prometheus metrics HTTP listener.
    #[arg(long, default_value_t = Ipv4Addr::LOCALHOST)]
    metrics_address: Ipv4Addr,

    /// Port for the Prometheus metrics HTTP listener.
    #[arg(long, default_value_t = 9090)]
    metrics_port: u16,

    /// Disable the Prometheus metrics HTTP listener.
    #[arg(long, default_value_t = false)]
    disable_metrics: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let compression_level = args.websocket_compression_level.unwrap_or(1);
    if !args.disable_metrics {
        let metrics_address = SocketAddr::new(IpAddr::V4(args.metrics_address), args.metrics_port);
        tokio::spawn(async move {
            if let Err(err) = server::metrics::run_metrics_server(metrics_address).await {
                eprintln!("metrics server stopped: {err}");
            }
        });
    }

    let multicast_config = if let Some(group_addr) = args.multicast_group {
        let Some(bind_addr) = args.multicast_bind_addr else {
            eprintln!("error: --multicast-bind-addr is required when --multicast-group is set");
            std::process::exit(1);
        };
        Some(MulticastConfig {
            group_addr,
            port: args.multicast_port,
            refdata_port: args.multicast_refdata_port,
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

    let dob_config = args.dob_group.map(|group_addr| {
        let bind_addr = args.dob_bind_addr.or(args.multicast_bind_addr).unwrap_or(Ipv4Addr::UNSPECIFIED);
        DobConfig {
            group_addr,
            mktdata_port: args.dob_mktdata_port,
            refdata_port: args.dob_refdata_port,
            snapshot_port: args.dob_snapshot_port,
            bind_addr,
            channel_id: args.dob_channel_id,
            source_id: args.dob_source_id,
            mtu: args.dob_mtu,
            heartbeat_interval: Duration::from_secs(args.heartbeat_interval),
            definition_cycle: Duration::from_secs(args.definition_cycle),
            manifest_cadence: Duration::from_secs(args.manifest_cadence),
            channel_bound: args.dob_channel_bound,
            snapshot_round_duration: Duration::from_secs(args.dob_snapshot_round_duration),
            snapshot_mtu: args.dob_snapshot_mtu,
        }
    });

    run_websocket_server(
        &full_address,
        true,
        compression_level,
        multicast_config,
        dob_config,
        args.ingest_mode.into(),
        args.hl_data_root,
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_cli_defaults_to_localhost_9090_enabled() {
        let args = Args::parse_from(["dz_hl_publisher", "--address", "127.0.0.1", "--port", "8000"]);
        assert_eq!(args.metrics_address, Ipv4Addr::LOCALHOST);
        assert_eq!(args.metrics_port, 9090);
        assert!(!args.disable_metrics);
    }

    #[test]
    fn metrics_cli_accepts_disable_and_custom_address() {
        let args = Args::parse_from([
            "dz_hl_publisher",
            "--address",
            "127.0.0.1",
            "--port",
            "8000",
            "--metrics-address",
            "0.0.0.0",
            "--metrics-port",
            "19090",
            "--disable-metrics",
        ]);
        assert_eq!(args.metrics_address, Ipv4Addr::UNSPECIFIED);
        assert_eq!(args.metrics_port, 19090);
        assert!(args.disable_metrics);
    }
}
