#![allow(clippy::expect_used)]
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use log::{info, warn};
use tokio::net::UdpSocket;

use crate::instruments::{InstrumentRegistry, price_to_fixed, qty_to_fixed};
use crate::multicast::config::MulticastConfig;
use crate::protocol::constants::{
    AGGRESSOR_BUY, AGGRESSOR_SELL, CHANNEL_RESET_SIZE, END_OF_SESSION_SIZE, FLAG_SNAPSHOT, HEARTBEAT_SIZE, QUOTE_SIZE,
    TRADE_SIZE, UPDATE_FLAG_ASK_GONE, UPDATE_FLAG_ASK_UPDATED, UPDATE_FLAG_BID_GONE, UPDATE_FLAG_BID_UPDATED,
};
use crate::protocol::frame::{FrameBuilder, FrameError};
use crate::protocol::messages::{
    QuoteData, TradeData, encode_channel_reset, encode_end_of_session, encode_heartbeat, encode_quote, encode_trade,
};

pub(crate) struct MulticastPublisher {
    socket: UdpSocket,
    config: MulticastConfig,
    registry: InstrumentRegistry,
    seq: AtomicU64,
}

impl MulticastPublisher {
    pub(crate) fn new(socket: UdpSocket, config: MulticastConfig, registry: InstrumentRegistry) -> Self {
        Self { socket, config, registry, seq: AtomicU64::new(0) }
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn now_ns() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
    }

    async fn send_frame(&self, frame: &[u8]) {
        if let Err(err) = self.socket.send_to(frame, self.config.dest()).await {
            warn!("multicast: failed to send frame: {err}");
        }
    }

    async fn send_channel_reset(&self) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(CHANNEL_RESET_SIZE).expect("ChannelReset fits in empty frame");
        encode_channel_reset(buf, Self::now_ns());
        fb.commit_message();
        self.send_frame(fb.finalize()).await;
    }

    async fn send_heartbeat(&self) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(HEARTBEAT_SIZE).expect("Heartbeat fits in empty frame");
        encode_heartbeat(buf, 0, Self::now_ns());
        fb.commit_message();
        self.send_frame(fb.finalize()).await;
    }

    async fn send_end_of_session(&self) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(END_OF_SESSION_SIZE).expect("EndOfSession fits in empty frame");
        encode_end_of_session(buf, Self::now_ns());
        fb.commit_message();
        self.send_frame(fb.finalize()).await;
    }

    /// Encodes L2 snapshots as Quote messages, batching into frames.
    async fn publish_quotes(
        &self,
        snapshot_map: &HashMap<
            crate::order_book::Coin,
            HashMap<
                crate::listeners::order_book::L2SnapshotParams,
                crate::order_book::Snapshot<crate::types::inner::InnerLevel>,
            >,
        >,
        time: u64,
        is_snapshot: bool,
    ) {
        let flags = if is_snapshot { FLAG_SNAPSHOT } else { 0 };
        let source_timestamp_ns = time * 1_000_000; // HL time is ms

        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let default_params = crate::listeners::order_book::L2SnapshotParams::new(None, None);

        for (coin, params_map) in snapshot_map {
            let coin_name = coin.value();
            let Some(inst) = self.registry.get(&coin_name) else {
                continue;
            };

            let Some(snapshot) = params_map.get(&default_params) else {
                continue;
            };

            let levels = snapshot.truncate(1).export_inner_snapshot();
            let bids = &levels[0];
            let asks = &levels[1];

            let (bid_price, bid_qty, bid_n) = if let Some(level) = bids.first() {
                let px = price_to_fixed(level.px(), inst.price_exponent).unwrap_or(0);
                let qty = qty_to_fixed(level.sz(), inst.qty_exponent).unwrap_or(0);
                (px, qty, u16::try_from(level.n()).unwrap_or(u16::MAX))
            } else {
                (0, 0, 0)
            };

            let (ask_price, ask_qty, ask_n) = if let Some(level) = asks.first() {
                let px = price_to_fixed(level.px(), inst.price_exponent).unwrap_or(0);
                let qty = qty_to_fixed(level.sz(), inst.qty_exponent).unwrap_or(0);
                (px, qty, u16::try_from(level.n()).unwrap_or(u16::MAX))
            } else {
                (0, 0, 0)
            };

            let mut update_flags = 0u8;
            if bids.is_empty() {
                update_flags |= UPDATE_FLAG_BID_GONE;
            } else {
                update_flags |= UPDATE_FLAG_BID_UPDATED;
            }
            if asks.is_empty() {
                update_flags |= UPDATE_FLAG_ASK_GONE;
            } else {
                update_flags |= UPDATE_FLAG_ASK_UPDATED;
            }

            let quote = QuoteData {
                instrument_id: inst.instrument_id,
                source_id: self.config.source_id,
                update_flags,
                source_timestamp_ns,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                bid_source_count: bid_n,
                ask_source_count: ask_n,
            };

            match fb.message_buffer(QUOTE_SIZE) {
                Ok(buf) => {
                    encode_quote(buf, &quote, flags);
                    fb.commit_message();
                }
                Err(FrameError::ExceedsMtu { .. } | FrameError::MaxMessages) => {
                    if !fb.is_empty() {
                        self.send_frame(fb.finalize()).await;
                    }
                    fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
                    let buf = fb.message_buffer(QUOTE_SIZE).expect("Quote fits in empty frame");
                    encode_quote(buf, &quote, flags);
                    fb.commit_message();
                }
            }
        }

        if !fb.is_empty() {
            self.send_frame(fb.finalize()).await;
        }
    }

    /// Encodes fills as Trade messages, batching into frames.
    async fn publish_trades(&self, trades_by_coin: &HashMap<String, Vec<crate::types::Trade>>) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);

        for trades in trades_by_coin.values() {
            for trade in trades {
                let Some(inst) = self.registry.get(&trade.coin) else {
                    continue;
                };

                let aggressor_side = match trade.side {
                    crate::order_book::types::Side::Ask => AGGRESSOR_SELL,
                    crate::order_book::types::Side::Bid => AGGRESSOR_BUY,
                };

                let trade_data = TradeData {
                    instrument_id: inst.instrument_id,
                    source_id: self.config.source_id,
                    aggressor_side,
                    trade_flags: 0,
                    source_timestamp_ns: trade.time * 1_000_000,
                    trade_price: price_to_fixed(&trade.px, inst.price_exponent).unwrap_or(0),
                    trade_qty: qty_to_fixed(&trade.sz, inst.qty_exponent).unwrap_or(0),
                    trade_id: trade.tid,
                    cumulative_volume: 0,
                };

                match fb.message_buffer(TRADE_SIZE) {
                    Ok(buf) => {
                        encode_trade(buf, &trade_data, 0);
                        fb.commit_message();
                    }
                    Err(FrameError::ExceedsMtu { .. } | FrameError::MaxMessages) => {
                        if !fb.is_empty() {
                            self.send_frame(fb.finalize()).await;
                        }
                        fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
                        let buf = fb.message_buffer(TRADE_SIZE).expect("Trade fits in empty frame");
                        encode_trade(buf, &trade_data, 0);
                        fb.commit_message();
                    }
                }
            }
        }

        if !fb.is_empty() {
            self.send_frame(fb.finalize()).await;
        }
    }

    /// Main loop: subscribes to the broadcast channel and publishes binary frames.
    pub(crate) async fn run(
        &self,
        mut rx: tokio::sync::broadcast::Receiver<std::sync::Arc<crate::listeners::order_book::InternalMessage>>,
    ) {
        use crate::listeners::order_book::InternalMessage;

        info!(
            "multicast publisher started: group={} port={} mtu={} source_id={}",
            self.config.group_addr, self.config.port, self.config.mtu, self.config.source_id,
        );

        // Send ChannelReset on startup
        self.send_channel_reset().await;

        let mut snapshot_interval = tokio::time::interval(self.config.snapshot_interval);
        snapshot_interval.tick().await; // consume immediate first tick

        let mut heartbeat_interval = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat_interval.tick().await;

        // Cache the most recent snapshot message for periodic resends
        let mut cached_snapshot: Option<std::sync::Arc<InternalMessage>> = None;
        let mut had_activity = false;

        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(msg) => match msg.as_ref() {
                            InternalMessage::Snapshot { l2_snapshots, time } => {
                                let snapshot_map = l2_snapshots.as_ref();
                                cached_snapshot = Some(msg.clone());
                                self.publish_quotes(snapshot_map, *time, false).await;
                                had_activity = true;
                                heartbeat_interval.reset();
                            }
                            InternalMessage::Fills { batch } => {
                                let trades_by_coin = crate::servers::websocket_server::coin_to_trades(batch);
                                self.publish_trades(&trades_by_coin).await;
                                had_activity = true;
                                heartbeat_interval.reset();
                            }
                            InternalMessage::L4BookUpdates { .. } => {
                                // Ignored for multicast
                            }
                        },
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("multicast: broadcast receiver lagged by {n} messages");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            warn!("multicast: broadcast channel closed, sending EndOfSession");
                            self.send_end_of_session().await;
                            return;
                        }
                    }
                }
                _ = snapshot_interval.tick() => {
                    if let Some(ref cached) = cached_snapshot {
                        if let InternalMessage::Snapshot { l2_snapshots, time } = cached.as_ref() {
                            self.publish_quotes(l2_snapshots.as_ref(), *time, true).await;
                            had_activity = true;
                            heartbeat_interval.reset();
                        }
                    }
                }
                _ = heartbeat_interval.tick() => {
                    if !had_activity {
                        self.send_heartbeat().await;
                    }
                    had_activity = false;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::constants::{
        DEFAULT_MTU, FRAME_HEADER_SIZE, MAGIC_BYTES, MSG_TYPE_CHANNEL_RESET, MSG_TYPE_HEARTBEAT, SCHEMA_VERSION,
    };
    use std::net::Ipv4Addr;
    use std::time::Duration;

    fn test_config() -> MulticastConfig {
        MulticastConfig {
            group_addr: Ipv4Addr::LOCALHOST,
            port: 0,
            bind_addr: Ipv4Addr::LOCALHOST,
            snapshot_interval: Duration::from_secs(60),
            mtu: DEFAULT_MTU,
            source_id: 1,
            heartbeat_interval: Duration::from_secs(60),
            hl_api_url: String::new(),
        }
    }

    fn test_registry() -> InstrumentRegistry {
        let mut map = HashMap::new();
        map.insert(
            "BTC".to_string(),
            crate::instruments::InstrumentInfo {
                instrument_id: 0,
                price_exponent: -8,
                qty_exponent: -8,
                symbol: crate::instruments::make_symbol("BTC"),
            },
        );
        InstrumentRegistry::new(map)
    }

    #[tokio::test]
    async fn sends_channel_reset_on_startup() {
        let send_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();

        let mut config = test_config();
        config.port = recv_addr.port();

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());
        publisher.send_channel_reset().await;

        let mut buf = [0u8; 2048];
        let n = tokio::time::timeout(Duration::from_secs(2), recv_socket.recv(&mut buf))
            .await
            .expect("timed out")
            .expect("recv failed");

        let frame = &buf[..n];
        assert_eq!(&frame[0..2], &MAGIC_BYTES);
        assert_eq!(frame[2], SCHEMA_VERSION);
        assert_eq!(frame[20], 1); // msg_count
        assert_eq!(frame[FRAME_HEADER_SIZE], MSG_TYPE_CHANNEL_RESET);
        assert_eq!(frame[FRAME_HEADER_SIZE + 1], CHANNEL_RESET_SIZE as u8);
    }

    #[tokio::test]
    async fn heartbeat_sends_valid_frame() {
        let send_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();

        let mut config = test_config();
        config.port = recv_addr.port();

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());
        publisher.send_heartbeat().await;

        let mut buf = [0u8; 2048];
        let n = tokio::time::timeout(Duration::from_secs(2), recv_socket.recv(&mut buf))
            .await
            .expect("timed out")
            .expect("recv failed");

        let frame = &buf[..n];
        assert_eq!(&frame[0..2], &MAGIC_BYTES);
        assert_eq!(frame[FRAME_HEADER_SIZE], MSG_TYPE_HEARTBEAT);
        assert_eq!(frame[FRAME_HEADER_SIZE + 1], HEARTBEAT_SIZE as u8);
    }

    #[tokio::test]
    async fn sequence_numbers_increment() {
        let send_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();

        let mut config = test_config();
        config.port = recv_addr.port();

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());

        publisher.send_heartbeat().await;
        publisher.send_heartbeat().await;
        publisher.send_heartbeat().await;

        let mut buf = [0u8; 2048];
        let mut seqs = Vec::new();
        for _ in 0..3 {
            tokio::time::timeout(Duration::from_secs(2), recv_socket.recv(&mut buf))
                .await
                .expect("timed out")
                .expect("recv failed");
            let seq = u64::from_le_bytes(buf[4..12].try_into().unwrap());
            seqs.push(seq);
        }
        assert_eq!(seqs, vec![0, 1, 2]);
    }
}
