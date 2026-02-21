use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

/// Market data channel types available for multicast distribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    /// Level 2 order book data.
    L2,
    /// Trade execution data.
    Trades,
}

/// Configuration for UDP multicast market data distribution.
#[derive(Debug, Clone)]
pub struct MulticastConfig {
    /// Multicast group address to join.
    pub group_addr: Ipv4Addr,
    /// UDP port for multicast traffic.
    pub port: u16,
    /// Local address to bind the socket to.
    pub bind_addr: Ipv4Addr,
    /// Set of channels to publish on this multicast group.
    pub channels: HashSet<Channel>,
    /// Number of price levels to include in L2 snapshots.
    pub l2_levels: usize,
    /// How often to send full book snapshots.
    pub snapshot_interval: Duration,
}

impl MulticastConfig {
    /// Returns the destination socket address (group address + port).
    #[must_use]
    pub fn dest(&self) -> SocketAddr {
        SocketAddr::from((self.group_addr, self.port))
    }
}

/// Parses a comma-separated string of channel names into a `HashSet<Channel>`.
///
/// Valid channel names are `l2` and `trades` (case-insensitive).
///
/// # Errors
///
/// Returns `Err` if the input is empty or contains an unknown channel name.
pub fn parse_channels(input: &str) -> Result<HashSet<Channel>, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("channel list must not be empty".to_owned());
    }

    let mut channels = HashSet::new();
    for token in trimmed.split(',') {
        let name = token.trim().to_lowercase();
        match name.as_str() {
            "l2" => {
                channels.insert(Channel::L2);
            }
            "trades" => {
                channels.insert(Channel::Trades);
            }
            other => {
                return Err(format!("unknown channel: {other}"));
            }
        }
    }
    Ok(channels)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_channels_l2_only() {
        let result = parse_channels("l2").unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&Channel::L2));
    }

    #[test]
    fn parse_channels_both() {
        let result = parse_channels("l2,trades").unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&Channel::L2));
        assert!(result.contains(&Channel::Trades));
    }

    #[test]
    fn parse_channels_trades_only() {
        let result = parse_channels("trades").unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&Channel::Trades));
    }

    #[test]
    fn parse_channels_unknown_error() {
        let result = parse_channels("l2,unknown");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "unknown channel: unknown");
    }

    #[test]
    fn parse_channels_empty_error() {
        let result = parse_channels("");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "channel list must not be empty");
    }

    #[test]
    fn dest_returns_correct_socket_addr() {
        let config = MulticastConfig {
            group_addr: Ipv4Addr::new(239, 0, 0, 1),
            port: 5000,
            bind_addr: Ipv4Addr::UNSPECIFIED,
            channels: HashSet::new(),
            l2_levels: 10,
            snapshot_interval: Duration::from_secs(1),
        };
        let addr = config.dest();
        assert_eq!(addr, SocketAddr::from((Ipv4Addr::new(239, 0, 0, 1), 5000)));
    }
}
