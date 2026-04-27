use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

/// Configuration for UDP multicast market data distribution.
#[derive(Debug, Clone)]
pub struct MulticastConfig {
    /// Multicast group address to join.
    pub group_addr: Ipv4Addr,
    /// UDP port for marketdata multicast traffic (Quote/Trade/Heartbeat/EndOfSession).
    pub port: u16,
    /// UDP port for refdata traffic (InstrumentDefinition/ManifestSummary/ChannelReset).
    pub refdata_port: u16,
    /// Local address to bind the socket to.
    pub bind_addr: Ipv4Addr,
    /// How often to send full BBO snapshots.
    pub snapshot_interval: Duration,
    /// Max frame size in bytes (default 1448 for GRE tunnels).
    pub mtu: u16,
    /// Source ID for Quote/Trade messages.
    pub source_id: u16,
    /// How long to wait with no data before sending a Heartbeat on the marketdata port.
    pub heartbeat_interval: Duration,
    /// Hyperliquid REST API URL for instrument metadata.
    pub hl_api_url: String,
    /// How often to re-poll the HL API to detect listings/delistings.
    pub instruments_refresh_interval: Duration,
    /// How long a full cycle of InstrumentDefinition retransmissions takes.
    /// Definitions are spaced evenly across this period.
    pub definition_cycle: Duration,
    /// How often to send a ManifestSummary on the refdata port.
    pub manifest_cadence: Duration,
}

impl MulticastConfig {
    /// Returns the destination socket address for marketdata traffic.
    #[must_use]
    pub fn dest(&self) -> SocketAddr {
        SocketAddr::from((self.group_addr, self.port))
    }

    /// Returns the destination socket address for refdata traffic.
    #[must_use]
    pub fn refdata_dest(&self) -> SocketAddr {
        SocketAddr::from((self.group_addr, self.refdata_port))
    }
}

/// Configuration for the DoB (depth-of-book) UDP multicast channel.
#[derive(Debug, Clone)]
pub struct DobConfig {
    pub group_addr: Ipv4Addr,
    pub mktdata_port: u16,
    pub refdata_port: u16,
    pub snapshot_port: u16, // bound in phase 2; still parsed in phase 1 for stability
    pub bind_addr: Ipv4Addr,
    pub channel_id: u8,
    pub source_id: u16,
    pub mtu: u16,
    pub heartbeat_interval: Duration,
    pub definition_cycle: Duration,
    pub manifest_cadence: Duration,
    pub channel_bound: usize,
    /// Target round-robin cycle duration for the DoB snapshot stream.
    pub snapshot_round_duration: Duration,
    /// Max frame size for DoB snapshot frames (typically the same MTU as
    /// the DoB mktdata stream).
    pub snapshot_mtu: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MulticastConfig {
        MulticastConfig {
            group_addr: Ipv4Addr::new(239, 0, 0, 1),
            port: 5000,
            refdata_port: 5001,
            bind_addr: Ipv4Addr::UNSPECIFIED,
            snapshot_interval: Duration::from_secs(5),
            mtu: 1448,
            source_id: 1,
            heartbeat_interval: Duration::from_secs(5),
            hl_api_url: "https://api.hyperliquid.xyz".to_string(),
            instruments_refresh_interval: Duration::from_secs(60),
            definition_cycle: Duration::from_secs(30),
            manifest_cadence: Duration::from_secs(1),
        }
    }

    #[test]
    fn dest_returns_correct_socket_addr() {
        let config = test_config();
        assert_eq!(config.dest(), SocketAddr::from((Ipv4Addr::new(239, 0, 0, 1), 5000)));
    }

    #[test]
    fn refdata_dest_returns_correct_socket_addr() {
        let config = test_config();
        assert_eq!(config.refdata_dest(), SocketAddr::from((Ipv4Addr::new(239, 0, 0, 1), 5001)));
    }
}
