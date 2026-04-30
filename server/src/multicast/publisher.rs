#![allow(clippy::expect_used)]
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use log::{info, warn};
use tokio::net::UdpSocket;

use crate::instruments::{InstrumentInfo, SharedRegistry, price_to_fixed, qty_to_fixed};
use crate::multicast::config::MulticastConfig;
use crate::protocol::constants::{
    AGGRESSOR_BUY, AGGRESSOR_SELL, ASSET_CLASS_CRYPTO_SPOT, CHANNEL_RESET_SIZE, END_OF_SESSION_SIZE, FLAG_SNAPSHOT,
    HEARTBEAT_SIZE, INSTRUMENT_DEF_SIZE, MANIFEST_SUMMARY_SIZE, MARKET_MODEL_CLOB, PRICE_BOUND_UNBOUNDED, QUOTE_SIZE,
    SETTLE_TYPE_NA, TRADE_SIZE, UPDATE_FLAG_ASK_GONE, UPDATE_FLAG_ASK_UPDATED, UPDATE_FLAG_BID_GONE,
    UPDATE_FLAG_BID_UPDATED,
};
use crate::protocol::frame::{FrameBuilder, FrameError};
use crate::protocol::messages::{
    InstrumentDefinitionData, QuoteData, TradeData, encode_channel_reset, encode_end_of_session, encode_heartbeat,
    encode_instrument_definition, encode_manifest_summary, encode_quote, encode_trade,
};

/// Per-cycle snapshot of instruments for definition retransmission.
///
/// The publisher uses this to iterate through the active set one frame at a time,
/// resetting whenever `manifest_seq` changes.
struct DefinitionCycler {
    snapshot: Vec<(String, InstrumentInfo)>,
    pos: usize,
    seq_tag: u16,
}

impl DefinitionCycler {
    const fn empty() -> Self {
        Self { snapshot: Vec::new(), pos: 0, seq_tag: 0 }
    }

    fn reset(&mut self, new_snapshot: Vec<(String, InstrumentInfo)>, seq_tag: u16) {
        self.snapshot = new_snapshot;
        self.pos = 0;
        self.seq_tag = seq_tag;
    }

    fn is_cycle_complete(&self) -> bool {
        self.pos >= self.snapshot.len()
    }

    fn advance_to_start(&mut self) {
        self.pos = 0;
    }
}

pub(crate) struct MulticastPublisher {
    socket: UdpSocket,
    config: MulticastConfig,
    registry: SharedRegistry,
    seq: AtomicU64,
}

impl MulticastPublisher {
    pub(crate) fn new(socket: UdpSocket, config: MulticastConfig, registry: SharedRegistry) -> Self {
        Self { socket, config, registry, seq: AtomicU64::new(0) }
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    #[cfg(not(test))]
    const CATCHUP_THRESHOLD_MS: u64 = 500;

    #[cfg(test)]
    fn should_publish_lag(_lag_ms: u64) -> bool {
        true
    }

    #[cfg(not(test))]
    fn should_publish_lag(lag_ms: u64) -> bool {
        lag_ms <= Self::CATCHUP_THRESHOLD_MS
    }

    #[allow(clippy::cast_possible_truncation)]
    fn now_ns() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
    }

    #[allow(clippy::cast_possible_truncation)]
    fn now_ms() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
    }

    fn log_catchup_progress(kind: &str, lag_ms: u64, last_log_ms: &mut u64) {
        let now = Self::now_ms();
        if now.saturating_sub(*last_log_ms) >= 5_000 {
            let lag_s = lag_ms / 1_000;
            info!("multicast: catching up, suppressing {kind} (lag {lag_s}s)");
            *last_log_ms = now;
        }
    }

    async fn send_frame(&self, frame: &[u8], dest: SocketAddr) {
        if let Err(err) = self.socket.send_to(frame, dest).await {
            warn!("multicast: failed to send frame to {dest}: {err}");
        }
    }

    async fn send_channel_reset(&self, dest: SocketAddr) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(CHANNEL_RESET_SIZE).expect("ChannelReset fits in empty frame");
        encode_channel_reset(buf, Self::now_ns());
        fb.commit_message();
        self.send_frame(fb.finalize(), dest).await;
    }

    async fn send_heartbeat(&self) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(HEARTBEAT_SIZE).expect("Heartbeat fits in empty frame");
        encode_heartbeat(buf, 0, Self::now_ns());
        fb.commit_message();
        self.send_frame(fb.finalize(), self.config.dest()).await;
    }

    async fn send_end_of_session(&self) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(END_OF_SESSION_SIZE).expect("EndOfSession fits in empty frame");
        encode_end_of_session(buf, Self::now_ns());
        fb.commit_message();
        self.send_frame(fb.finalize(), self.config.dest()).await;
    }

    /// Sends a ManifestSummary on the refdata port.
    async fn send_manifest_summary(&self) {
        let (seq_tag, count) = {
            let guard = self.registry.load();
            (guard.manifest_seq, u32::try_from(guard.active.len()).unwrap_or(u32::MAX))
        };

        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(MANIFEST_SUMMARY_SIZE).expect("ManifestSummary fits in empty frame");
        encode_manifest_summary(buf, 0, seq_tag, count, Self::now_ns());
        fb.commit_message();
        self.send_frame(fb.finalize(), self.config.refdata_dest()).await;
    }

    /// Computes how many InstrumentDefinition messages we should emit per cycle tick.
    ///
    /// Spreads the full active set evenly across `definition_cycle`. Returns the
    /// per-tick count such that (count * ticks_per_cycle) covers all instruments.
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::cast_sign_loss)]
    fn defs_per_tick(&self, total: usize, tick_interval: std::time::Duration) -> usize {
        if total == 0 {
            return 0;
        }
        let cycle_secs = self.config.definition_cycle.as_secs_f64().max(0.001);
        let tick_secs = tick_interval.as_secs_f64().max(0.001);
        let ticks_per_cycle = (cycle_secs / tick_secs).max(1.0);
        let per_tick = (total as f64 / ticks_per_cycle).ceil() as usize;
        per_tick.max(1)
    }

    /// Emits InstrumentDefinitions for up to `count` entries starting at `cycler.pos`,
    /// packing them into MTU-sized frames on the refdata port.
    async fn publish_definitions_batch(&self, cycler: &mut DefinitionCycler, count: usize) {
        if cycler.snapshot.is_empty() || count == 0 {
            return;
        }

        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let mut emitted = 0usize;

        while emitted < count && cycler.pos < cycler.snapshot.len() {
            let (coin, info) = &cycler.snapshot[cycler.pos];
            let data = build_instrument_definition(coin, info, cycler.seq_tag);

            match fb.message_buffer(INSTRUMENT_DEF_SIZE) {
                Ok(buf) => {
                    encode_instrument_definition(buf, &data, 0);
                    fb.commit_message();
                    cycler.pos += 1;
                    emitted += 1;
                }
                Err(FrameError::ExceedsMtu { .. } | FrameError::MaxMessages) => {
                    if !fb.is_empty() {
                        self.send_frame(fb.finalize(), self.config.refdata_dest()).await;
                    }
                    fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
                    // Retry: a fresh frame is guaranteed to fit one message.
                    let buf = fb.message_buffer(INSTRUMENT_DEF_SIZE).expect("InstrumentDefinition fits in empty frame");
                    encode_instrument_definition(buf, &data, 0);
                    fb.commit_message();
                    cycler.pos += 1;
                    emitted += 1;
                }
            }
        }

        if !fb.is_empty() {
            self.send_frame(fb.finalize(), self.config.refdata_dest()).await;
        }
    }

    /// Reads the current registry snapshot and updates the cycler if manifest_seq changed.
    /// Returns `true` if the cycler was reset.
    fn sync_cycler(&self, cycler: &mut DefinitionCycler) -> bool {
        let guard = self.registry.load();
        if guard.manifest_seq != cycler.seq_tag {
            let snapshot: Vec<(String, InstrumentInfo)> =
                guard.active.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            let new_seq = guard.manifest_seq;
            drop(guard);
            cycler.reset(snapshot, new_seq);
            return true;
        }
        false
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

        // Snapshot-then-lookup against the lock-free ArcSwap. Holding the load
        // guard across the full batch is fine — it's just a ref-counted view of
        // an immutable RegistryState, and a concurrent refresh-task store will
        // simply publish a newer state we'll pick up on the next batch.
        let lookups: HashMap<String, InstrumentInfo> = {
            let guard = self.registry.load();
            snapshot_map
                .keys()
                .filter_map(|coin| {
                    let name = coin.value();
                    guard.active.get(&name).map(|info| (name, info.clone()))
                })
                .collect()
        };

        for (coin, params_map) in snapshot_map {
            let coin_name = coin.value();
            let Some(inst) = lookups.get(&coin_name) else {
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
                        self.send_frame(fb.finalize(), self.config.dest()).await;
                    }
                    fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
                    let buf = fb.message_buffer(QUOTE_SIZE).expect("Quote fits in empty frame");
                    encode_quote(buf, &quote, flags);
                    fb.commit_message();
                }
            }
        }

        if !fb.is_empty() {
            self.send_frame(fb.finalize(), self.config.dest()).await;
        }
    }

    /// Encodes fills as Trade messages, batching into frames.
    async fn publish_trades(&self, trades_by_coin: &HashMap<String, Vec<crate::types::Trade>>) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);

        let lookups: HashMap<String, InstrumentInfo> = {
            let guard = self.registry.load();
            trades_by_coin
                .keys()
                .filter_map(|coin| guard.active.get(coin).map(|info| (coin.clone(), info.clone())))
                .collect()
        };

        for (coin_name, trades) in trades_by_coin {
            let Some(inst) = lookups.get(coin_name) else {
                continue;
            };

            for trade in trades {
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
                            self.send_frame(fb.finalize(), self.config.dest()).await;
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
            self.send_frame(fb.finalize(), self.config.dest()).await;
        }
    }

    /// Main loop: subscribes to the broadcast channel and publishes binary frames.
    pub(crate) async fn run(
        &self,
        mut rx: tokio::sync::broadcast::Receiver<Arc<crate::listeners::order_book::InternalMessage>>,
    ) {
        use crate::listeners::order_book::InternalMessage;

        info!(
            "multicast publisher started: group={} marketdata_port={} refdata_port={} mtu={} source_id={}",
            self.config.group_addr, self.config.port, self.config.refdata_port, self.config.mtu, self.config.source_id,
        );

        // Send ChannelReset on both ports at startup.
        self.send_channel_reset(self.config.dest()).await;
        self.send_channel_reset(self.config.refdata_dest()).await;

        // Marketdata timers
        let mut snapshot_interval = tokio::time::interval(self.config.snapshot_interval);
        snapshot_interval.tick().await;

        let mut heartbeat_interval = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat_interval.tick().await;

        // Refdata timers
        let mut manifest_interval = tokio::time::interval(self.config.manifest_cadence);
        manifest_interval.tick().await;

        // Definition cycle tick: we want to emit one "batch" per tick so that frames
        // spread across the cycle. Tick rate = cycle / ceil(total / per_frame).
        // Use a fixed-ish tick (default: cycle / 20) so behavior is independent of registry size.
        let def_tick_interval = self.config.definition_cycle.div_f64(20.0).max(std::time::Duration::from_millis(500));
        let mut definition_interval = tokio::time::interval(def_tick_interval);
        definition_interval.tick().await;

        // Cache the most recent snapshot message for periodic marketdata resends.
        let mut cached_snapshot: Option<Arc<InternalMessage>> = None;
        let mut had_activity = false;
        let mut caught_up = false;
        let mut last_catchup_log_ms: u64 = 0;

        // Definition cycler state
        let mut cycler = DefinitionCycler::empty();

        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(msg) => match msg.as_ref() {
                            InternalMessage::Snapshot { l2_snapshots, time } => {
                                cached_snapshot = Some(msg.clone());
                                let lag_ms = Self::now_ms().saturating_sub(*time);
                                if Self::should_publish_lag(lag_ms) {
                                    if !caught_up {
                                        info!("multicast: caught up (lag {lag_ms}ms), publishing quotes");
                                        caught_up = true;
                                    }
                                    let snapshot_map = l2_snapshots.as_ref();
                                    self.publish_quotes(snapshot_map, *time, false).await;
                                    had_activity = true;
                                    heartbeat_interval.reset();
                                } else {
                                    Self::log_catchup_progress("quotes", lag_ms, &mut last_catchup_log_ms);
                                }
                            }
                            InternalMessage::Fills { batch } => {
                                let lag_ms = Self::now_ms().saturating_sub(batch.block_time());
                                if Self::should_publish_lag(lag_ms) {
                                    let trades_by_coin = crate::servers::websocket_server::coin_to_trades(batch);
                                    self.publish_trades(&trades_by_coin).await;
                                    had_activity = true;
                                    heartbeat_interval.reset();
                                }
                                // fills are high volume during catchup; progress
                                // is already logged by the snapshot path above
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
                    if caught_up {
                        if let Some(ref cached) = cached_snapshot
                            && let InternalMessage::Snapshot { l2_snapshots, time } = cached.as_ref() {
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
                _ = manifest_interval.tick() => {
                    // Sync cycler BEFORE sending the manifest, so they agree on seq.
                    let reset = self.sync_cycler(&mut cycler);
                    if reset {
                        info!(
                            "multicast: cycler reset to manifest_seq={} ({} instruments)",
                            cycler.seq_tag,
                            cycler.snapshot.len()
                        );
                    }
                    self.send_manifest_summary().await;
                }
                _ = definition_interval.tick() => {
                    self.sync_cycler(&mut cycler);
                    if cycler.snapshot.is_empty() {
                        continue;
                    }
                    if cycler.is_cycle_complete() {
                        cycler.advance_to_start();
                    }
                    let per_tick = self.defs_per_tick(cycler.snapshot.len(), def_tick_interval);
                    self.publish_definitions_batch(&mut cycler, per_tick).await;
                }
            }
        }
    }
}

/// Builds an `InstrumentDefinitionData` for the given instrument, stamped with `manifest_seq`.
fn build_instrument_definition(coin: &str, info: &InstrumentInfo, manifest_seq: u16) -> InstrumentDefinitionData {
    // Split a coin like "BTC-USDT" into leg1="BTC", leg2="USDT" if present.
    // For HL perps this is just the coin name on leg1.
    let (leg1_str, leg2_str) = split_legs(coin);
    let leg1 = make_leg(leg1_str);
    let leg2 = make_leg(leg2_str);

    InstrumentDefinitionData {
        instrument_id: info.instrument_id,
        symbol: info.symbol,
        leg1,
        leg2,
        asset_class: ASSET_CLASS_CRYPTO_SPOT,
        price_exponent: info.price_exponent,
        qty_exponent: info.qty_exponent,
        market_model: MARKET_MODEL_CLOB,
        tick_size: 0,
        lot_size: 0,
        contract_value: 0,
        expiry: 0,
        settle_type: SETTLE_TYPE_NA,
        price_bound: PRICE_BOUND_UNBOUNDED,
        manifest_seq,
    }
}

pub(crate) fn split_legs(coin: &str) -> (&str, &str) {
    if let Some((a, b)) = coin.split_once('/') {
        (a, b)
    } else if let Some((a, b)) = coin.split_once('-') {
        (a, b)
    } else {
        (coin, "")
    }
}

pub(crate) fn make_leg(name: &str) -> [u8; 8] {
    let mut buf = [0u8; 8];
    let bytes = name.as_bytes();
    let len = bytes.len().min(8);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instruments::{RegistryState, UniverseEntry, make_symbol};
    use crate::protocol::constants::{
        DEFAULT_MTU, FRAME_HEADER_SIZE, MAGIC_BYTES, MSG_TYPE_CHANNEL_RESET, MSG_TYPE_HEARTBEAT,
        MSG_TYPE_INSTRUMENT_DEF, MSG_TYPE_MANIFEST_SUMMARY, SCHEMA_VERSION,
    };
    use std::net::Ipv4Addr;
    use std::time::Duration;

    fn test_config() -> MulticastConfig {
        MulticastConfig {
            group_addr: Ipv4Addr::LOCALHOST,
            port: 0,
            refdata_port: 0,
            bind_addr: Ipv4Addr::LOCALHOST,
            snapshot_interval: Duration::from_secs(60),
            mtu: DEFAULT_MTU,
            source_id: 1,
            heartbeat_interval: Duration::from_secs(60),
            hl_api_url: String::new(),
            instruments_refresh_interval: Duration::from_secs(60),
            definition_cycle: Duration::from_secs(30),
            manifest_cadence: Duration::from_secs(1),
        }
    }

    fn test_registry() -> SharedRegistry {
        let universe = vec![
            UniverseEntry {
                instrument_id: 0,
                coin: "BTC".to_string(),
                is_delisted: false,
                info: InstrumentInfo {
                    instrument_id: 0,
                    price_exponent: -8,
                    qty_exponent: -8,
                    symbol: make_symbol("BTC"),
                },
            },
            UniverseEntry {
                instrument_id: 1,
                coin: "ETH".to_string(),
                is_delisted: false,
                info: InstrumentInfo {
                    instrument_id: 1,
                    price_exponent: -8,
                    qty_exponent: -8,
                    symbol: make_symbol("ETH"),
                },
            },
        ];
        crate::instruments::new_shared_registry(RegistryState::new(universe))
    }

    #[tokio::test]
    async fn sends_channel_reset_on_both_ports_at_startup() {
        let send_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let marketdata_recv = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let refdata_recv = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let mut config = test_config();
        config.port = marketdata_recv.local_addr().unwrap().port();
        config.refdata_port = refdata_recv.local_addr().unwrap().port();

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());

        publisher.send_channel_reset(publisher.config.dest()).await;
        publisher.send_channel_reset(publisher.config.refdata_dest()).await;

        let mut buf = [0u8; 2048];

        let n1 = tokio::time::timeout(Duration::from_secs(2), marketdata_recv.recv(&mut buf))
            .await
            .expect("timed out on marketdata port")
            .expect("hot recv failed");
        assert_eq!(&buf[0..2], &MAGIC_BYTES);
        assert_eq!(buf[2], SCHEMA_VERSION);
        assert_eq!(buf[FRAME_HEADER_SIZE], MSG_TYPE_CHANNEL_RESET);
        let _ = n1;

        let n2 = tokio::time::timeout(Duration::from_secs(2), refdata_recv.recv(&mut buf))
            .await
            .expect("timed out on refdata port")
            .expect("ref recv failed");
        assert_eq!(&buf[0..2], &MAGIC_BYTES);
        assert_eq!(buf[FRAME_HEADER_SIZE], MSG_TYPE_CHANNEL_RESET);
        let _ = n2;
    }

    #[tokio::test]
    async fn manifest_summary_carries_registry_state() {
        let send_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_port = recv_socket.local_addr().unwrap().port();

        let mut config = test_config();
        config.refdata_port = recv_port;

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());
        publisher.send_manifest_summary().await;

        let mut buf = [0u8; 2048];
        tokio::time::timeout(Duration::from_secs(2), recv_socket.recv(&mut buf))
            .await
            .expect("timed out")
            .expect("recv failed");

        assert_eq!(buf[FRAME_HEADER_SIZE], MSG_TYPE_MANIFEST_SUMMARY);
        let body_offset = FRAME_HEADER_SIZE + 4; // skip app msg header
        let seq = u16::from_le_bytes(buf[body_offset + 4..body_offset + 6].try_into().unwrap());
        let count = u32::from_le_bytes(buf[body_offset + 8..body_offset + 12].try_into().unwrap());
        assert_eq!(seq, 1);
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn definition_batch_emits_instruments() {
        let send_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_port = recv_socket.local_addr().unwrap().port();

        let mut config = test_config();
        config.refdata_port = recv_port;

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());
        let mut cycler = DefinitionCycler::empty();
        publisher.sync_cycler(&mut cycler);
        assert_eq!(cycler.snapshot.len(), 2);
        assert_eq!(cycler.seq_tag, 1);

        publisher.publish_definitions_batch(&mut cycler, 2).await;

        let mut buf = [0u8; 2048];
        tokio::time::timeout(Duration::from_secs(2), recv_socket.recv(&mut buf))
            .await
            .expect("timed out")
            .expect("recv failed");

        assert_eq!(buf[FRAME_HEADER_SIZE], MSG_TYPE_INSTRUMENT_DEF);
        assert_eq!(buf[20], 2); // msg_count in frame header: 2 definitions packed
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
        tokio::time::timeout(Duration::from_secs(2), recv_socket.recv(&mut buf))
            .await
            .expect("timed out")
            .expect("recv failed");

        assert_eq!(&buf[0..2], &MAGIC_BYTES);
        assert_eq!(buf[FRAME_HEADER_SIZE], MSG_TYPE_HEARTBEAT);
        assert_eq!(buf[FRAME_HEADER_SIZE + 1], HEARTBEAT_SIZE as u8);
    }

    #[test]
    fn split_legs_slash() {
        assert_eq!(split_legs("PURR/USDC"), ("PURR", "USDC"));
    }

    #[test]
    fn split_legs_dash() {
        assert_eq!(split_legs("BTC-USDT"), ("BTC", "USDT"));
    }

    #[test]
    fn split_legs_none() {
        assert_eq!(split_legs("BTC"), ("BTC", ""));
    }

    #[test]
    fn make_leg_padding() {
        let leg = make_leg("BTC");
        assert_eq!(&leg[..3], b"BTC");
        assert_eq!(&leg[3..], &[0u8; 5]);
    }

    #[test]
    fn make_leg_truncation() {
        let leg = make_leg("LONGLONGCOIN");
        assert_eq!(&leg, b"LONGLONG");
    }
}
