use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

/// Configuration for UDP multicast market data distribution.
#[derive(Debug, Clone)]
pub struct MulticastConfig {
    /// Multicast group address to join.
    pub group_addr: Ipv4Addr,
    /// UDP port for hot-path multicast traffic.
    pub port: u16,
    /// Local address to bind the socket to.
    pub bind_addr: Ipv4Addr,
    /// How often to send full BBO snapshots.
    pub snapshot_interval: Duration,
    /// Max frame size in bytes (default 1448 for GRE tunnels).
    pub mtu: u16,
    /// Source ID for Quote/Trade messages.
    pub source_id: u16,
    /// How long to wait with no data before sending a Heartbeat.
    pub heartbeat_interval: Duration,
    /// Hyperliquid REST API URL for instrument metadata.
    pub hl_api_url: String,
}

impl MulticastConfig {
    /// Returns the destination socket address (group address + port).
    #[must_use]
    pub fn dest(&self) -> SocketAddr {
        SocketAddr::from((self.group_addr, self.port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dest_returns_correct_socket_addr() {
        let config = MulticastConfig {
            group_addr: Ipv4Addr::new(239, 0, 0, 1),
            port: 5000,
            bind_addr: Ipv4Addr::UNSPECIFIED,
            snapshot_interval: Duration::from_secs(5),
            mtu: 1448,
            source_id: 1,
            heartbeat_interval: Duration::from_secs(5),
            hl_api_url: "https://api.hyperliquid.xyz".to_string(),
        };
        let addr = config.dest();
        assert_eq!(addr, SocketAddr::from((Ipv4Addr::new(239, 0, 0, 1), 5000)));
    }
}
