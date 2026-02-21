use std::sync::atomic::{AtomicU64, Ordering};

use log::warn;
use serde::Serialize;
use tokio::net::UdpSocket;
use uuid::Uuid;

use crate::multicast::config::MulticastConfig;

/// Maximum UDP datagram payload size. Messages exceeding this are dropped.
const MAX_DATAGRAM_SIZE: usize = 1400;

/// JSON envelope wrapping all multicast messages.
#[derive(Serialize)]
struct MulticastEnvelope<T> {
    session: String,
    seq: u64,
    channel: String,
    data: T,
}

/// A trade without the `users` field, suitable for public multicast distribution.
#[derive(Serialize)]
struct MulticastTrade {
    coin: String,
    side: crate::order_book::types::Side,
    px: String,
    sz: String,
    time: u64,
    hash: String,
    tid: u64,
}

impl MulticastTrade {
    fn from_trade(trade: &crate::types::Trade) -> Self {
        Self {
            coin: trade.coin.clone(),
            side: trade.side,
            px: trade.px.clone(),
            sz: trade.sz.clone(),
            time: trade.time,
            hash: trade.hash.clone(),
            tid: trade.tid,
        }
    }
}

pub(crate) struct MulticastPublisher {
    socket: UdpSocket,
    config: MulticastConfig,
    session_id: String,
    seq: AtomicU64,
}

impl MulticastPublisher {
    /// Creates a new publisher with a random UUID v4 session ID.
    pub(crate) fn new(socket: UdpSocket, config: MulticastConfig) -> Self {
        Self { socket, config, session_id: Uuid::new_v4().to_string(), seq: AtomicU64::new(0) }
    }

    /// Test helper that accepts a predetermined session ID.
    #[cfg(test)]
    const fn with_session_id(socket: UdpSocket, config: MulticastConfig, session_id: String) -> Self {
        Self { socket, config, session_id, seq: AtomicU64::new(0) }
    }

    /// Returns the next sequence number, atomically incrementing the counter.
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Serializes the envelope to JSON, checks the size, and sends it as a UDP datagram.
    /// Logs a warning and returns without panicking on any error.
    async fn send_envelope<T: Serialize>(&self, channel: &str, data: T) {
        let envelope =
            MulticastEnvelope { session: self.session_id.clone(), seq: self.next_seq(), channel: channel.to_owned(), data };
        let json = match serde_json::to_vec(&envelope) {
            Ok(json) => json,
            Err(err) => {
                warn!("multicast: failed to serialize envelope: {err}");
                return;
            }
        };
        if json.len() > MAX_DATAGRAM_SIZE {
            warn!("multicast: dropping datagram of {} bytes (max {MAX_DATAGRAM_SIZE})", json.len());
            return;
        }
        if let Err(err) = self.socket.send_to(&json, self.config.dest()).await {
            warn!("multicast: failed to send datagram: {err}");
        }
    }

    /// Main loop: subscribes to the broadcast channel and publishes multicast datagrams.
    pub(crate) async fn run(
        &self,
        mut rx: tokio::sync::broadcast::Receiver<std::sync::Arc<crate::listeners::order_book::InternalMessage>>,
    ) {
        use crate::listeners::order_book::{InternalMessage, L2SnapshotParams};
        use crate::multicast::config::Channel;
        use crate::types::L2Book;

        let mut snapshot_interval = tokio::time::interval(self.config.snapshot_interval);
        // The first tick completes immediately; consume it so we don't send a spurious snapshot.
        snapshot_interval.tick().await;

        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(msg) => match msg.as_ref() {
                            InternalMessage::Snapshot { l2_snapshots, time } => {
                                if !self.config.channels.contains(&Channel::L2) {
                                    continue;
                                }
                                let snapshot_map = l2_snapshots.as_ref();
                                for (coin, params_map) in snapshot_map {
                                    let default_params = L2SnapshotParams::new(None, None);
                                    if let Some(snapshot) = params_map.get(&default_params) {
                                        let truncated = snapshot.truncate(self.config.l2_levels);
                                        let levels = truncated.export_inner_snapshot();
                                        let book = L2Book::from_l2_snapshot(coin.value(), levels, *time);
                                        self.send_envelope("l2Book", book).await;
                                    }
                                }
                            }
                            InternalMessage::Fills { batch } => {
                                if !self.config.channels.contains(&Channel::Trades) {
                                    continue;
                                }
                                let trades_by_coin = crate::servers::websocket_server::coin_to_trades(batch);
                                for trades in trades_by_coin.values() {
                                    for trade in trades {
                                        let mc_trade = MulticastTrade::from_trade(trade);
                                        self.send_envelope("trades", mc_trade).await;
                                    }
                                }
                            }
                            InternalMessage::L4BookUpdates { .. } => {
                                // Ignored for multicast.
                            }
                        },
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("multicast: broadcast receiver lagged by {n} messages");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            warn!("multicast: broadcast channel closed, exiting");
                            return;
                        }
                    }
                }
                _ = snapshot_interval.tick() => {
                    // Periodic snapshot timer tick.
                    // L2Snapshots is not Clone, so we cannot cache them.
                    // Real snapshots arrive via the broadcast channel; this is a no-op.
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::collections::HashSet;
    use std::net::Ipv4Addr;
    use std::time::Duration;

    use crate::multicast::config::{Channel, MulticastConfig};
    use crate::order_book::types::Side;

    fn test_config() -> MulticastConfig {
        MulticastConfig {
            group_addr: Ipv4Addr::LOCALHOST,
            port: 0, // will be overridden by actual bound port
            bind_addr: Ipv4Addr::LOCALHOST,
            channels: HashSet::from([Channel::L2, Channel::Trades]),
            l2_levels: 5,
            snapshot_interval: Duration::from_secs(60),
        }
    }

    #[test]
    fn test_multicast_trade_omits_users() {
        let trade = MulticastTrade {
            coin: "BTC".to_string(),
            side: Side::Ask,
            px: "50000.0".to_string(),
            sz: "1.5".to_string(),
            time: 1_700_000_000,
            hash: "0xabc".to_string(),
            tid: 42,
        };
        let json = serde_json::to_string(&trade).unwrap();
        assert!(!json.contains("users"), "MulticastTrade JSON should not contain 'users' field");
        // Verify expected fields are present.
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["coin"], "BTC");
        assert_eq!(v["side"], "A");
        assert_eq!(v["px"], "50000.0");
        assert_eq!(v["sz"], "1.5");
        assert_eq!(v["time"], 1_700_000_000_u64);
        assert_eq!(v["hash"], "0xabc");
        assert_eq!(v["tid"], 42);
    }

    #[test]
    fn test_envelope_serialization() {
        let envelope = MulticastEnvelope {
            session: "test-session-123".to_string(),
            seq: 7,
            channel: "l2Book".to_string(),
            data: serde_json::json!({"coin": "ETH"}),
        };
        let json = serde_json::to_string(&envelope).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["session"], "test-session-123");
        assert_eq!(v["seq"], 7);
        assert_eq!(v["channel"], "l2Book");
        assert_eq!(v["data"]["coin"], "ETH");
    }

    #[test]
    fn test_envelope_l2_snapshot_channel() {
        let envelope = MulticastEnvelope {
            session: "snap-session".to_string(),
            seq: 0,
            channel: "l2Snapshot".to_string(),
            data: vec!["level1", "level2"],
        };
        let json = serde_json::to_string(&envelope).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["channel"], "l2Snapshot");
        assert_eq!(v["session"], "snap-session");
        assert_eq!(v["seq"], 0);
        assert!(v["data"].is_array());
    }

    #[tokio::test]
    async fn test_publisher_sends_l2_envelope() {
        // Bind two loopback UDP sockets: one for sending, one for receiving.
        let send_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();

        let mut config = test_config();
        config.group_addr = Ipv4Addr::LOCALHOST;
        config.port = recv_addr.port();

        let publisher = MulticastPublisher::with_session_id(send_socket, config, "test-session".to_string());

        publisher.send_envelope("l2Book", serde_json::json!({"coin": "BTC", "levels": []})).await;

        let mut buf = [0u8; 2048];
        let n = tokio::time::timeout(Duration::from_secs(2), recv_socket.recv(&mut buf))
            .await
            .expect("timed out waiting for UDP datagram")
            .expect("recv failed");

        let v: serde_json::Value = serde_json::from_slice(&buf[..n]).unwrap();
        assert_eq!(v["session"], "test-session");
        assert_eq!(v["seq"], 0);
        assert_eq!(v["channel"], "l2Book");
        assert_eq!(v["data"]["coin"], "BTC");
    }

    #[tokio::test]
    async fn test_publisher_sequence_increments() {
        let send_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();

        let mut config = test_config();
        config.port = recv_addr.port();

        let publisher = MulticastPublisher::with_session_id(send_socket, config, "seq-test".to_string());

        // Send 3 envelopes.
        for _ in 0..3 {
            publisher.send_envelope("trades", serde_json::json!({"i": 1})).await;
        }

        let mut buf = [0u8; 2048];
        let mut seqs = Vec::new();
        for _ in 0..3 {
            let n = tokio::time::timeout(Duration::from_secs(2), recv_socket.recv(&mut buf))
                .await
                .expect("timed out")
                .expect("recv failed");
            let v: serde_json::Value = serde_json::from_slice(&buf[..n]).unwrap();
            seqs.push(v["seq"].as_u64().unwrap());
        }
        assert_eq!(seqs, vec![0, 1, 2]);
    }

    #[test]
    fn test_multicast_trade_from_trade() {
        let trade = crate::types::Trade {
            coin: "SOL".to_string(),
            side: Side::Bid,
            px: "123.45".to_string(),
            sz: "10.0".to_string(),
            time: 1_700_000_001,
            hash: "0xdef".to_string(),
            tid: 99,
            users: [alloy::primitives::Address::ZERO, alloy::primitives::Address::ZERO],
        };
        let mc_trade = MulticastTrade::from_trade(&trade);
        assert_eq!(mc_trade.coin, "SOL");
        assert_eq!(mc_trade.side, Side::Bid);
        assert_eq!(mc_trade.px, "123.45");
        assert_eq!(mc_trade.sz, "10.0");
        assert_eq!(mc_trade.time, 1_700_000_001);
        assert_eq!(mc_trade.hash, "0xdef");
        assert_eq!(mc_trade.tid, 99);

        // Verify no users field in serialized JSON.
        let json = serde_json::to_string(&mc_trade).unwrap();
        assert!(!json.contains("users"));
    }
}
