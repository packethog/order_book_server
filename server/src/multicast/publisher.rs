#![allow(clippy::expect_used)]
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
use crate::types::{Trade, node_data::NodeDataFill};

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

#[derive(Debug)]
struct PairedTobTrade {
    trade: Trade,
    block_time_ms: u64,
}

#[derive(Debug, Clone)]
struct PendingFill {
    fill: NodeDataFill,
    block_time_ms: u64,
    received_at_ms: u64,
    generation: u64,
}

#[derive(Debug, Clone, Copy)]
struct PendingFillKey {
    tid: u64,
    generation: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FillPairIntervalReport {
    paired: u64,
    orphan_dropped: u64,
    one_sided_token_alias: u64,
    malformed: u64,
    duplicate_replaced: u64,
    pending: usize,
}

#[derive(Debug)]
struct FillPairAccumulator {
    pending: HashMap<u64, PendingFill>,
    insertion_order: VecDeque<PendingFillKey>,
    next_generation: u64,
    paired_since_report: u64,
    orphan_dropped_since_report: u64,
    one_sided_token_alias_since_report: u64,
    malformed_since_report: u64,
    duplicate_replaced_since_report: u64,
    last_report_ms: u64,
}

impl FillPairAccumulator {
    const ORPHAN_TIMEOUT_MS: u64 = 5_000;
    const MAX_PENDING: usize = 100_000;
    const REPORT_INTERVAL_MS: u64 = 60_000;

    fn new(now_ms: u64) -> Self {
        crate::metrics::set_tob_fill_pair_pending(0);
        Self {
            pending: HashMap::new(),
            insertion_order: VecDeque::new(),
            next_generation: 1,
            paired_since_report: 0,
            orphan_dropped_since_report: 0,
            one_sided_token_alias_since_report: 0,
            malformed_since_report: 0,
            duplicate_replaced_since_report: 0,
            last_report_ms: now_ms,
        }
    }

    fn ingest_batch(
        &mut self,
        batch: &crate::types::node_data::Batch<NodeDataFill>,
        now_ms: u64,
    ) -> Vec<PairedTobTrade> {
        self.expire_orphans(now_ms);
        let mut paired = Vec::new();
        for fill in batch.events_ref() {
            if let Some(pair) = self.ingest_fill(fill.clone(), batch.block_time(), now_ms) {
                paired.push(pair);
            }
        }
        self.evict_to_capacity();
        crate::metrics::set_tob_fill_pair_pending(self.pending.len());
        paired
    }

    fn ingest_fill(&mut self, fill: NodeDataFill, block_time_ms: u64, now_ms: u64) -> Option<PairedTobTrade> {
        let tid = fill.1.tid;
        if let Some(existing) = self.pending.remove(&tid) {
            if existing.fill.1.side == fill.1.side {
                self.record_duplicate_replaced();
                self.insert_pending(fill, block_time_ms, now_ms);
                return None;
            }

            if !fills_are_pairable(&existing.fill, &fill) {
                self.record_malformed();
                return None;
            }

            let mut fills = HashMap::new();
            fills.insert(existing.fill.1.side, existing.fill);
            fills.insert(fill.1.side, fill);
            let Some(trade) = Trade::from_fills(fills) else {
                self.record_malformed();
                return None;
            };
            self.record_paired();
            return Some(PairedTobTrade { trade, block_time_ms: existing.block_time_ms.max(block_time_ms) });
        }

        self.insert_pending(fill, block_time_ms, now_ms);
        None
    }

    fn insert_pending(&mut self, fill: NodeDataFill, block_time_ms: u64, now_ms: u64) {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.saturating_add(1);
        let tid = fill.1.tid;
        self.pending.insert(tid, PendingFill { fill, block_time_ms, received_at_ms: now_ms, generation });
        self.insertion_order.push_back(PendingFillKey { tid, generation });
    }

    fn expire_orphans(&mut self, now_ms: u64) {
        loop {
            let Some(key) = self.insertion_order.front().copied() else {
                break;
            };
            let Some(pending) = self.pending.get(&key.tid) else {
                self.insertion_order.pop_front();
                continue;
            };
            if pending.generation != key.generation {
                self.insertion_order.pop_front();
                continue;
            }
            if now_ms.saturating_sub(pending.received_at_ms) < Self::ORPHAN_TIMEOUT_MS {
                break;
            }
            self.insertion_order.pop_front();
            if let Some(pending) = self.pending.remove(&key.tid) {
                self.record_unpaired_pending(&pending.fill);
            }
        }
    }

    fn evict_to_capacity(&mut self) {
        while self.pending.len() > Self::MAX_PENDING {
            let Some(key) = self.insertion_order.pop_front() else {
                break;
            };
            if self.pending.get(&key.tid).is_some_and(|pending| pending.generation == key.generation)
                && let Some(pending) = self.pending.remove(&key.tid)
            {
                self.record_unpaired_pending(&pending.fill);
            }
        }
    }

    fn report_if_due(&mut self, now_ms: u64) -> Option<FillPairIntervalReport> {
        if now_ms.saturating_sub(self.last_report_ms) < Self::REPORT_INTERVAL_MS {
            return None;
        }
        let report = FillPairIntervalReport {
            paired: self.paired_since_report,
            orphan_dropped: self.orphan_dropped_since_report,
            one_sided_token_alias: self.one_sided_token_alias_since_report,
            malformed: self.malformed_since_report,
            duplicate_replaced: self.duplicate_replaced_since_report,
            pending: self.pending.len(),
        };
        self.paired_since_report = 0;
        self.orphan_dropped_since_report = 0;
        self.one_sided_token_alias_since_report = 0;
        self.malformed_since_report = 0;
        self.duplicate_replaced_since_report = 0;
        self.last_report_ms = now_ms;
        if report.orphan_dropped > 0
            || report.one_sided_token_alias > 0
            || report.malformed > 0
            || report.duplicate_replaced > 0
        {
            Some(report)
        } else {
            None
        }
    }

    fn record_paired(&mut self) {
        self.paired_since_report += 1;
        crate::metrics::inc_tob_fill_pair("paired", 1);
    }

    fn record_orphan_dropped(&mut self) {
        self.orphan_dropped_since_report += 1;
        crate::metrics::inc_tob_fill_pair("orphan_dropped", 1);
    }

    fn record_unpaired_pending(&mut self, fill: &NodeDataFill) {
        if fill.1.coin.starts_with('#') {
            self.one_sided_token_alias_since_report += 1;
            crate::metrics::inc_tob_fill_pair("one_sided_token_alias", 1);
        } else {
            self.record_orphan_dropped();
        }
    }

    fn record_malformed(&mut self) {
        self.malformed_since_report += 1;
        crate::metrics::inc_tob_fill_pair("malformed", 1);
    }

    fn record_duplicate_replaced(&mut self) {
        self.duplicate_replaced_since_report += 1;
        crate::metrics::inc_tob_fill_pair("duplicate_replaced", 1);
    }
}

fn fills_are_pairable(left: &NodeDataFill, right: &NodeDataFill) -> bool {
    left.1.side != right.1.side
        && left.1.tid == right.1.tid
        && left.1.coin == right.1.coin
        && left.1.hash == right.1.hash
        && left.1.px == right.1.px
        && left.1.sz == right.1.sz
        && left.1.time == right.1.time
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TobSuppressedKind {
    Snapshot,
    Fill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TobHealthLevel {
    Info,
    Warn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TobHealthReport {
    level: TobHealthLevel,
    last_source_lag_ms: u64,
    source_lag_min_ms: u64,
    source_lag_avg_ms: u64,
    source_lag_max_ms: u64,
    queue_delay_min_ms: u64,
    queue_delay_avg_ms: u64,
    queue_delay_max_ms: u64,
    suppressed_snapshots: u64,
    suppressed_fills: u64,
    receiver_lag_events: u64,
    receiver_lagged_messages: u64,
    last_suppressed_snapshot: Option<TobSuppressedSnapshot>,
    last_suppressed_fill: Option<TobSuppressedFill>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TobSuppressedSnapshot {
    source: &'static str,
    height: u64,
    block_time_ms: u64,
    source_block_time_ms: u64,
    source_local_time_ms: u64,
    source_lag_ms: u64,
    snapshot_to_source_block_lag_ms: u64,
    validator_write_lag_ms: u64,
    listener_to_publisher_ms: u64,
    queue_delay_ms: u64,
    latest_status_height: Option<u64>,
    latest_diff_height: Option<u64>,
    latest_fill_height: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TobSuppressedFill {
    height: u64,
    block_time_ms: u64,
    local_time_ms: u64,
    source_lag_ms: u64,
    validator_write_lag_ms: u64,
    listener_to_publisher_ms: u64,
    queue_delay_ms: u64,
    event_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct IntervalStats {
    min: u64,
    max: u64,
    sum: u64,
    count: u64,
}

impl IntervalStats {
    const fn new() -> Self {
        Self { min: u64::MAX, max: 0, sum: 0, count: 0 }
    }

    fn record(&mut self, value: u64) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum = self.sum.saturating_add(value);
        self.count = self.count.saturating_add(1);
    }

    const fn snapshot(&self) -> (u64, u64, u64) {
        if self.count == 0 { (0, 0, 0) } else { (self.min, self.sum / self.count, self.max) }
    }

    fn reset(&mut self) {
        *self = Self::new();
    }
}

#[derive(Debug)]
struct TobPublisherHealth {
    last_source_lag_ms: Option<u64>,
    source_lag_stats: IntervalStats,
    queue_delay_stats: IntervalStats,
    suppressed_snapshots: u64,
    suppressed_fills: u64,
    receiver_lag_events: u64,
    receiver_lagged_messages: u64,
    last_suppressed_snapshot: Option<TobSuppressedSnapshot>,
    last_suppressed_fill: Option<TobSuppressedFill>,
    last_report_ms: u64,
}

impl TobPublisherHealth {
    const REPORT_INTERVAL_MS: u64 = 5_000;

    const fn new(now_ms: u64) -> Self {
        Self {
            last_source_lag_ms: None,
            source_lag_stats: IntervalStats::new(),
            queue_delay_stats: IntervalStats::new(),
            suppressed_snapshots: 0,
            suppressed_fills: 0,
            receiver_lag_events: 0,
            receiver_lagged_messages: 0,
            last_suppressed_snapshot: None,
            last_suppressed_fill: None,
            last_report_ms: now_ms,
        }
    }

    fn record_source_lag(&mut self, lag_ms: u64) {
        self.last_source_lag_ms = Some(lag_ms);
        self.source_lag_stats.record(lag_ms);
    }

    fn observe_publishable_lag(&mut self, lag_ms: u64) {
        self.record_source_lag(lag_ms);
    }

    fn record_queue_delay(&mut self, delay_ms: u64) {
        self.queue_delay_stats.record(delay_ms);
    }

    fn record_suppressed(
        &mut self,
        kind: TobSuppressedKind,
        lag_ms: u64,
        now_ms: u64,
        snapshot: Option<TobSuppressedSnapshot>,
        fill: Option<TobSuppressedFill>,
    ) -> Option<TobHealthReport> {
        self.record_source_lag(lag_ms);
        match kind {
            TobSuppressedKind::Snapshot => {
                self.suppressed_snapshots += 1;
                self.last_suppressed_snapshot = snapshot;
            }
            TobSuppressedKind::Fill => {
                self.suppressed_fills += 1;
                self.last_suppressed_fill = fill;
            }
        }
        self.report_if_due(now_ms)
    }

    fn record_receiver_lag(&mut self, lagged_messages: u64, now_ms: u64) -> Option<TobHealthReport> {
        self.receiver_lag_events += 1;
        self.receiver_lagged_messages += lagged_messages;
        self.report_if_due(now_ms)
    }

    fn report_if_due(&mut self, now_ms: u64) -> Option<TobHealthReport> {
        if now_ms.saturating_sub(self.last_report_ms) < Self::REPORT_INTERVAL_MS {
            return None;
        }
        self.take_report(now_ms)
    }

    fn take_report(&mut self, now_ms: u64) -> Option<TobHealthReport> {
        let has_suppression = self.suppressed_snapshots > 0 || self.suppressed_fills > 0;
        let has_receiver_lag = self.receiver_lag_events > 0 || self.receiver_lagged_messages > 0;
        if !has_suppression && !has_receiver_lag {
            self.last_report_ms = now_ms;
            return None;
        }

        let last_source_lag_ms = self.last_source_lag_ms.unwrap_or(0);
        let (source_lag_min_ms, source_lag_avg_ms, source_lag_max_ms) = self.source_lag_stats.snapshot();
        let (queue_delay_min_ms, queue_delay_avg_ms, queue_delay_max_ms) = self.queue_delay_stats.snapshot();
        let level = if has_receiver_lag && MulticastPublisher::should_warn_for_receiver_lag(last_source_lag_ms) {
            TobHealthLevel::Warn
        } else {
            TobHealthLevel::Info
        };
        let report = TobHealthReport {
            level,
            last_source_lag_ms,
            source_lag_min_ms,
            source_lag_avg_ms,
            source_lag_max_ms,
            queue_delay_min_ms,
            queue_delay_avg_ms,
            queue_delay_max_ms,
            suppressed_snapshots: self.suppressed_snapshots,
            suppressed_fills: self.suppressed_fills,
            receiver_lag_events: self.receiver_lag_events,
            receiver_lagged_messages: self.receiver_lagged_messages,
            last_suppressed_snapshot: self.last_suppressed_snapshot,
            last_suppressed_fill: self.last_suppressed_fill,
        };

        self.suppressed_snapshots = 0;
        self.suppressed_fills = 0;
        self.receiver_lag_events = 0;
        self.receiver_lagged_messages = 0;
        self.last_suppressed_snapshot = None;
        self.last_suppressed_fill = None;
        self.source_lag_stats.reset();
        self.queue_delay_stats.reset();
        self.last_report_ms = now_ms;
        Some(report)
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
    const CATCHUP_THRESHOLD_MS: u64 = 500;

    #[cfg(test)]
    fn should_publish_lag(_lag_ms: u64) -> bool {
        true
    }

    #[cfg(not(test))]
    fn should_publish_lag(lag_ms: u64) -> bool {
        lag_ms <= Self::CATCHUP_THRESHOLD_MS
    }

    fn should_warn_for_receiver_lag(lag_ms: u64) -> bool {
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

    fn log_tob_health(report: TobHealthReport) {
        match report.level {
            TobHealthLevel::Info => {
                info!(
                    "tob marketdata suppressed: source_lag_ms min/avg/max={}/{}/{} publisher_queue_ms min/avg/max={}/{}/{} suppressed_snapshots={} suppressed_fills={} receiver_lag_events={} receiver_lagged_messages={}",
                    report.source_lag_min_ms,
                    report.source_lag_avg_ms,
                    report.source_lag_max_ms,
                    report.queue_delay_min_ms,
                    report.queue_delay_avg_ms,
                    report.queue_delay_max_ms,
                    report.suppressed_snapshots,
                    report.suppressed_fills,
                    report.receiver_lag_events,
                    report.receiver_lagged_messages,
                );
                if let Some(snapshot) = report.last_suppressed_snapshot {
                    info!("{}", Self::format_suppressed_snapshot(snapshot));
                }
                if let Some(fill) = report.last_suppressed_fill {
                    info!("{}", Self::format_suppressed_fill(fill));
                }
            }
            TobHealthLevel::Warn => {
                warn!(
                    "tob receiver lagged while fresh: dropped_messages={} lag_events={} source_lag_ms={} source_lag_ms min/avg/max={}/{}/{} publisher_queue_ms min/avg/max={}/{}/{} suppressed_snapshots={} suppressed_fills={}",
                    report.receiver_lagged_messages,
                    report.receiver_lag_events,
                    report.last_source_lag_ms,
                    report.source_lag_min_ms,
                    report.source_lag_avg_ms,
                    report.source_lag_max_ms,
                    report.queue_delay_min_ms,
                    report.queue_delay_avg_ms,
                    report.queue_delay_max_ms,
                    report.suppressed_snapshots,
                    report.suppressed_fills,
                );
                if let Some(snapshot) = report.last_suppressed_snapshot {
                    warn!("{}", Self::format_suppressed_snapshot(snapshot));
                }
                if let Some(fill) = report.last_suppressed_fill {
                    warn!("{}", Self::format_suppressed_fill(fill));
                }
            }
        }
    }

    fn format_suppressed_snapshot(snapshot: TobSuppressedSnapshot) -> String {
        format!(
            "tob suppressed snapshot detail: source={} height={} block_time_ms={} minute_phase_ms={} source_block_time_ms={} source_local_time_ms={} source_lag_ms={} snapshot_to_source_block_lag_ms={} validator_write_lag_ms={} listener_to_publisher_ms={} publisher_queue_ms={} latest_heights statuses={:?} diffs={:?} fills={:?}",
            snapshot.source,
            snapshot.height,
            snapshot.block_time_ms,
            snapshot.block_time_ms % 60_000,
            snapshot.source_block_time_ms,
            snapshot.source_local_time_ms,
            snapshot.source_lag_ms,
            snapshot.snapshot_to_source_block_lag_ms,
            snapshot.validator_write_lag_ms,
            snapshot.listener_to_publisher_ms,
            snapshot.queue_delay_ms,
            snapshot.latest_status_height,
            snapshot.latest_diff_height,
            snapshot.latest_fill_height,
        )
    }

    fn format_suppressed_fill(fill: TobSuppressedFill) -> String {
        format!(
            "tob suppressed fill detail: height={} block_time_ms={} minute_phase_ms={} local_time_ms={} source_lag_ms={} validator_write_lag_ms={} listener_to_publisher_ms={} publisher_queue_ms={} event_count={}",
            fill.height,
            fill.block_time_ms,
            fill.block_time_ms % 60_000,
            fill.local_time_ms,
            fill.source_lag_ms,
            fill.validator_write_lag_ms,
            fill.listener_to_publisher_ms,
            fill.queue_delay_ms,
            fill.event_count,
        )
    }

    async fn send_frame(
        &self,
        frame: &[u8],
        dest: SocketAddr,
        channel: &'static str,
        packet_type: &'static str,
    ) -> bool {
        let start = Instant::now();
        let sent = if let Err(err) = self.socket.send_to(frame, dest).await {
            warn!("multicast: failed to send frame to {dest}: {err}");
            false
        } else {
            crate::metrics::inc_tob_packet(packet_type);
            true
        };
        crate::metrics::observe_tob_socket_send(channel, start.elapsed());
        sent
    }

    async fn send_channel_reset(&self, dest: SocketAddr, channel: &'static str) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(CHANNEL_RESET_SIZE).expect("ChannelReset fits in empty frame");
        encode_channel_reset(buf, Self::now_ns());
        fb.commit_message();
        let _sent = self.send_frame(fb.finalize(), dest, channel, "channel_reset").await;
    }

    async fn send_heartbeat(&self) -> bool {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(HEARTBEAT_SIZE).expect("Heartbeat fits in empty frame");
        encode_heartbeat(buf, 0, Self::now_ns());
        fb.commit_message();
        self.send_frame(fb.finalize(), self.config.dest(), "marketdata", "heartbeat").await
    }

    async fn send_end_of_session(&self) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(END_OF_SESSION_SIZE).expect("EndOfSession fits in empty frame");
        encode_end_of_session(buf, Self::now_ns());
        fb.commit_message();
        let _sent = self.send_frame(fb.finalize(), self.config.dest(), "marketdata", "end_of_session").await;
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
        self.send_frame(fb.finalize(), self.config.refdata_dest(), "refdata", "manifest_summary").await;
    }

    /// Computes how many InstrumentDefinition messages we should emit per cycle tick.
    ///
    /// Spreads the full active set evenly across `definition_cycle`. Returns the
    /// per-tick count such that (count * ticks_per_cycle) covers all instruments.
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::cast_sign_loss)]
    fn defs_per_tick(&self, total: usize, tick_interval: Duration) -> usize {
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
                        self.send_frame(fb.finalize(), self.config.refdata_dest(), "refdata", "instrument_definition")
                            .await;
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
            self.send_frame(fb.finalize(), self.config.refdata_dest(), "refdata", "instrument_definition").await;
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
    ) -> bool {
        let flags = if is_snapshot { FLAG_SNAPSHOT } else { 0 };
        let source_timestamp_ns = time * 1_000_000; // HL time is ms

        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let mut sent_any = false;
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
                        sent_any |= self.send_frame(fb.finalize(), self.config.dest(), "marketdata", "quote").await;
                    }
                    fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
                    let buf = fb.message_buffer(QUOTE_SIZE).expect("Quote fits in empty frame");
                    encode_quote(buf, &quote, flags);
                    fb.commit_message();
                }
            }
        }

        if !fb.is_empty() {
            sent_any |= self.send_frame(fb.finalize(), self.config.dest(), "marketdata", "quote").await;
        }
        sent_any
    }

    /// Encodes fills as Trade messages, batching into frames.
    async fn publish_trades(&self, trades_by_coin: &HashMap<String, Vec<Trade>>) -> bool {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let mut sent_any = false;

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
                            sent_any |= self.send_frame(fb.finalize(), self.config.dest(), "marketdata", "trade").await;
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
            sent_any |= self.send_frame(fb.finalize(), self.config.dest(), "marketdata", "trade").await;
        }
        sent_any
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
        self.send_channel_reset(self.config.dest(), "marketdata").await;
        self.send_channel_reset(self.config.refdata_dest(), "refdata").await;

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
        let def_tick_interval = self.config.definition_cycle.div_f64(20.0).max(Duration::from_millis(500));
        let mut definition_interval = tokio::time::interval(def_tick_interval);
        definition_interval.tick().await;

        // Cache the most recent snapshot message for periodic marketdata resends.
        let mut cached_snapshot: Option<Arc<InternalMessage>> = None;
        let mut had_activity = false;
        let mut caught_up = false;
        let mut health = TobPublisherHealth::new(Self::now_ms());
        let mut fill_pairs = FillPairAccumulator::new(Self::now_ms());

        // Definition cycler state
        let mut cycler = DefinitionCycler::empty();

        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(msg) => match msg.as_ref() {
                            InternalMessage::Snapshot {
                                l2_snapshots,
                                time,
                                height,
                                source,
                                source_block_time_ms,
                                source_local_time_ms,
                                latest_heights,
                                enqueued_at_ms,
                            } => {
                                cached_snapshot = Some(msg.clone());
                                let now_ms = Self::now_ms();
                                let queue_delay_ms = now_ms.saturating_sub(*enqueued_at_ms);
                                let listener_to_publisher_ms = now_ms.saturating_sub(*source_local_time_ms);
                                health.record_queue_delay(queue_delay_ms);
                                crate::metrics::observe_tob_queue_delay(
                                    "snapshot",
                                    Duration::from_millis(queue_delay_ms),
                                );
                                crate::metrics::observe_tob_snapshot_listener_to_publisher(
                                    source,
                                    Duration::from_millis(listener_to_publisher_ms),
                                );
                                let lag_ms = now_ms.saturating_sub(*time);
                                if Self::should_publish_lag(lag_ms) {
                                    health.observe_publishable_lag(lag_ms);
                                    crate::metrics::observe_tob_source_lag(
                                        "snapshot",
                                        "published",
                                        Duration::from_millis(lag_ms),
                                    );
                                    if !caught_up {
                                        info!("multicast: caught up (lag {lag_ms}ms), publishing quotes");
                                        caught_up = true;
                                    }
                                    let snapshot_map = l2_snapshots.as_ref();
                                    if self.publish_quotes(snapshot_map, *time, false).await {
                                        had_activity = true;
                                        heartbeat_interval.reset();
                                    }
                                } else {
                                    caught_up = false;
                                    crate::metrics::observe_tob_source_lag(
                                        "snapshot",
                                        "suppressed",
                                        Duration::from_millis(lag_ms),
                                    );
                                    crate::metrics::inc_tob_suppressed("snapshot");
                                    let snapshot = TobSuppressedSnapshot {
                                        source: *source,
                                        height: *height,
                                        block_time_ms: *time,
                                        source_block_time_ms: *source_block_time_ms,
                                        source_local_time_ms: *source_local_time_ms,
                                        source_lag_ms: lag_ms,
                                        snapshot_to_source_block_lag_ms: (*source_block_time_ms).saturating_sub(*time),
                                        validator_write_lag_ms: (*source_local_time_ms)
                                            .saturating_sub(*source_block_time_ms),
                                        listener_to_publisher_ms,
                                        queue_delay_ms,
                                        latest_status_height: latest_heights.statuses,
                                        latest_diff_height: latest_heights.diffs,
                                        latest_fill_height: latest_heights.fills,
                                    };
                                    if let Some(report) = health.record_suppressed(
                                        TobSuppressedKind::Snapshot,
                                        lag_ms,
                                        now_ms,
                                        Some(snapshot),
                                        None,
                                    ) {
                                        Self::log_tob_health(report);
                                    }
                                }
                            }
                            InternalMessage::Fills { batch, enqueued_at_ms, path } => {
                                let now_ms = Self::now_ms();
                                let queue_delay_ms = now_ms.saturating_sub(*enqueued_at_ms);
                                let listener_to_publisher_ms = now_ms.saturating_sub(batch.local_time_ms());
                                health.record_queue_delay(queue_delay_ms);
                                crate::metrics::observe_tob_queue_delay("fill", Duration::from_millis(queue_delay_ms));
                                crate::metrics::observe_tob_fill_listener_to_publisher(
                                    path,
                                    Duration::from_millis(listener_to_publisher_ms),
                                );
                                let row_lag_ms = now_ms.saturating_sub(batch.block_time());
                                if !Self::should_publish_lag(row_lag_ms) {
                                    let local_time_ms = batch.local_time_ms();
                                    let fill = TobSuppressedFill {
                                        height: batch.block_number(),
                                        block_time_ms: batch.block_time(),
                                        local_time_ms,
                                        source_lag_ms: row_lag_ms,
                                        validator_write_lag_ms: local_time_ms.saturating_sub(batch.block_time()),
                                        listener_to_publisher_ms,
                                        queue_delay_ms,
                                        event_count: batch.events_ref().len(),
                                    };
                                    crate::metrics::observe_tob_source_lag(
                                        "fill",
                                        "suppressed",
                                        Duration::from_millis(row_lag_ms),
                                    );
                                    crate::metrics::inc_tob_suppressed("fill");
                                    if let Some(report) = health.record_suppressed(
                                        TobSuppressedKind::Fill,
                                        row_lag_ms,
                                        now_ms,
                                        None,
                                        Some(fill),
                                    ) {
                                        Self::log_tob_health(report);
                                    }
                                }
                                let paired = fill_pairs.ingest_batch(batch, now_ms);
                                let mut publishable_trades = HashMap::<String, Vec<Trade>>::new();
                                for paired_trade in paired {
                                    let trade_lag_ms = now_ms.saturating_sub(paired_trade.block_time_ms);
                                    if Self::should_publish_lag(trade_lag_ms) {
                                        health.observe_publishable_lag(trade_lag_ms);
                                        crate::metrics::observe_tob_source_lag(
                                            "fill",
                                            "published",
                                            Duration::from_millis(trade_lag_ms),
                                        );
                                        publishable_trades
                                            .entry(paired_trade.trade.coin.clone())
                                            .or_default()
                                            .push(paired_trade.trade);
                                    }
                                }
                                if self.publish_trades(&publishable_trades).await {
                                    had_activity = true;
                                    heartbeat_interval.reset();
                                }
                                if let Some(report) = fill_pairs.report_if_due(now_ms) {
                                        if report.malformed > 0 || report.duplicate_replaced > 0 {
                                            warn!(
                                                "tob fill pairing: paired={} orphan_dropped={} one_sided_token_alias={} malformed={} duplicate_replaced={} pending={}",
                                                report.paired,
                                                report.orphan_dropped,
                                                report.one_sided_token_alias,
                                                report.malformed,
                                                report.duplicate_replaced,
                                                report.pending,
                                            );
                                        } else {
                                            info!(
                                                "tob fill pairing: paired={} orphan_dropped={} one_sided_token_alias={} malformed={} duplicate_replaced={} pending={}",
                                                report.paired,
                                                report.orphan_dropped,
                                                report.one_sided_token_alias,
                                                report.malformed,
                                                report.duplicate_replaced,
                                                report.pending,
                                            );
                                        }
                                }
                                // fills are high volume during catchup; progress
                                // is already logged by the snapshot path above
                            }
                            InternalMessage::L4BookUpdates { .. } => {
                                // Ignored for multicast
                            }
                        },
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            crate::metrics::inc_tob_receiver_lag("event", 1);
                            crate::metrics::inc_tob_receiver_lag("message", n);
                            if let Some(report) = health.record_receiver_lag(n, Self::now_ms()) {
                                Self::log_tob_health(report);
                            }
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
                            && let InternalMessage::Snapshot { l2_snapshots, time, .. } = cached.as_ref() {
                                if self.publish_quotes(l2_snapshots.as_ref(), *time, true).await {
                                    had_activity = true;
                                    heartbeat_interval.reset();
                                }
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
    use crate::listeners::order_book::InternalMessage;
    use crate::order_book::types::Side;
    use crate::protocol::constants::{
        DEFAULT_MTU, FRAME_HEADER_SIZE, MAGIC_BYTES, MSG_TYPE_CHANNEL_RESET, MSG_TYPE_HEARTBEAT,
        MSG_TYPE_INSTRUMENT_DEF, MSG_TYPE_MANIFEST_SUMMARY, MSG_TYPE_QUOTE, MSG_TYPE_TRADE, SCHEMA_VERSION,
    };
    use crate::types::{Fill, node_data::Batch, node_data::NodeDataFill};
    use alloy::primitives::Address;
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

    fn test_fill(coin: &str, side: Side, tid: u64) -> NodeDataFill {
        NodeDataFill(
            Address::new([0; 20]),
            Fill {
                coin: coin.to_string(),
                px: "100.0".to_string(),
                sz: "1.0".to_string(),
                side,
                time: 1_700_000_000_000,
                start_position: "0".to_string(),
                dir: "Open Long".to_string(),
                closed_pnl: "0".to_string(),
                hash: "0x0".to_string(),
                oid: 1,
                crossed: side == Side::Ask,
                fee: "0".to_string(),
                tid,
                fee_token: "USDC".to_string(),
                liquidation: None,
            },
        )
    }

    fn frame_msg_types(buf: &[u8], n: usize) -> Vec<u8> {
        if n < FRAME_HEADER_SIZE {
            return Vec::new();
        }
        let msg_count = buf[20] as usize;
        let mut offset = FRAME_HEADER_SIZE;
        let mut msg_types = Vec::new();
        for _ in 0..msg_count {
            if offset + 2 > n {
                break;
            }
            msg_types.push(buf[offset]);
            let msg_len = buf[offset + 1] as usize;
            if msg_len == 0 {
                break;
            }
            offset += msg_len;
        }
        msg_types
    }

    async fn collect_market_msg_types(socket: &UdpSocket, duration: Duration) -> Vec<u8> {
        let deadline = Instant::now() + duration;
        let mut msg_types = Vec::new();
        let mut buf = [0u8; 2048];
        loop {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            match tokio::time::timeout(remaining, socket.recv(&mut buf)).await {
                Ok(Ok(n)) => msg_types.extend(frame_msg_types(&buf, n)),
                _ => break,
            }
        }
        msg_types
    }

    async fn spawn_running_test_publisher(
        heartbeat_interval: Duration,
    ) -> (tokio::sync::broadcast::Sender<Arc<InternalMessage>>, tokio::task::JoinHandle<()>, UdpSocket) {
        let send_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let marketdata_recv = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let refdata_recv = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let mut config = test_config();
        config.port = marketdata_recv.local_addr().unwrap().port();
        config.refdata_port = refdata_recv.local_addr().unwrap().port();
        config.heartbeat_interval = heartbeat_interval;
        config.manifest_cadence = Duration::from_secs(60);
        config.definition_cycle = Duration::from_secs(60);

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());
        let (tx, _) = tokio::sync::broadcast::channel(128);
        let rx = tx.subscribe();
        let handle = tokio::spawn(async move {
            publisher.run(rx).await;
        });

        (tx, handle, marketdata_recv)
    }

    #[tokio::test]
    async fn sends_channel_reset_on_both_ports_at_startup() {
        let send_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let marketdata_recv = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let refdata_recv = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let mut config = test_config();
        config.port = marketdata_recv.local_addr().unwrap().port();
        config.refdata_port = refdata_recv.local_addr().unwrap().port();

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());

        publisher.send_channel_reset(publisher.config.dest(), "marketdata").await;
        publisher.send_channel_reset(publisher.config.refdata_dest(), "refdata").await;

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
        let send_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
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
        let send_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
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
        let send_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
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

    #[tokio::test]
    async fn empty_quote_batch_reports_no_marketdata_frame() {
        let send_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let mut config = test_config();
        config.port = recv_socket.local_addr().unwrap().port();

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());
        let sent = publisher.publish_quotes(&HashMap::new(), MulticastPublisher::now_ms(), false).await;

        assert!(!sent, "empty quote batch must not count as marketdata activity");
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                collect_market_msg_types(&recv_socket, Duration::from_millis(10))
            )
            .await
            .unwrap()
            .is_empty(),
            "empty quote batch should not emit marketdata packets",
        );
    }

    #[tokio::test]
    async fn unmapped_fresh_fills_do_not_starve_heartbeats() {
        let heartbeat_interval = Duration::from_millis(40);
        let (tx, handle, marketdata_recv) = spawn_running_test_publisher(heartbeat_interval).await;

        let startup = collect_market_msg_types(&marketdata_recv, Duration::from_millis(100)).await;
        assert!(startup.contains(&MSG_TYPE_CHANNEL_RESET), "publisher emits startup ChannelReset on mktdata");

        let send_until = Instant::now() + Duration::from_millis(160);
        let mut tid = 1;
        while Instant::now() < send_until {
            let batch = Batch::new_for_test(
                1,
                MulticastPublisher::now_ms(),
                vec![test_fill("UNKNOWN", Side::Ask, tid), test_fill("UNKNOWN", Side::Bid, tid)],
            );
            let _unused = tx.send(Arc::new(InternalMessage::Fills {
                batch,
                enqueued_at_ms: MulticastPublisher::now_ms(),
                path: "test",
            }));
            tid += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let msg_types = collect_market_msg_types(&marketdata_recv, Duration::from_millis(160)).await;
        handle.abort();

        assert!(
            msg_types.contains(&MSG_TYPE_HEARTBEAT),
            "fresh fills that produce no trade frame must leave mktdata idle so heartbeats continue",
        );
        assert!(!msg_types.contains(&MSG_TYPE_TRADE), "unmapped fills should not emit trade frames");
        assert!(!msg_types.contains(&MSG_TYPE_QUOTE), "fill-only traffic should not emit quote frames");
    }

    #[tokio::test]
    async fn fresh_trade_frame_resets_heartbeat_timer() {
        let heartbeat_interval = Duration::from_millis(80);
        let (tx, handle, marketdata_recv) = spawn_running_test_publisher(heartbeat_interval).await;

        let startup = collect_market_msg_types(&marketdata_recv, Duration::from_millis(100)).await;
        assert!(startup.contains(&MSG_TYPE_CHANNEL_RESET), "publisher emits startup ChannelReset on mktdata");

        let batch = Batch::new_for_test(
            1,
            MulticastPublisher::now_ms(),
            vec![test_fill("BTC", Side::Ask, 42), test_fill("BTC", Side::Bid, 42)],
        );
        let _unused = tx.send(Arc::new(InternalMessage::Fills {
            batch,
            enqueued_at_ms: MulticastPublisher::now_ms(),
            path: "test",
        }));

        let early = collect_market_msg_types(&marketdata_recv, Duration::from_millis(40)).await;
        assert!(early.contains(&MSG_TYPE_TRADE), "fresh mapped fills emit a trade frame");
        assert!(
            !early.contains(&MSG_TYPE_HEARTBEAT),
            "trade frame is mktdata activity and should reset heartbeat timer",
        );

        let later = collect_market_msg_types(&marketdata_recv, Duration::from_millis(140)).await;
        handle.abort();

        assert!(
            later.contains(&MSG_TYPE_HEARTBEAT),
            "heartbeat resumes after the marketdata path is idle for the heartbeat interval",
        );
    }

    #[tokio::test]
    async fn split_row_fills_emit_trade_frame() {
        let heartbeat_interval = Duration::from_millis(100);
        let (tx, handle, marketdata_recv) = spawn_running_test_publisher(heartbeat_interval).await;

        let startup = collect_market_msg_types(&marketdata_recv, Duration::from_millis(100)).await;
        assert!(startup.contains(&MSG_TYPE_CHANNEL_RESET), "publisher emits startup ChannelReset on mktdata");

        for fill in [test_fill("BTC", Side::Bid, 420), test_fill("BTC", Side::Ask, 420)] {
            let batch = Batch::new_for_test(1, MulticastPublisher::now_ms(), vec![fill]);
            let _unused = tx.send(Arc::new(InternalMessage::Fills {
                batch,
                enqueued_at_ms: MulticastPublisher::now_ms(),
                path: "test",
            }));
        }

        let msg_types = collect_market_msg_types(&marketdata_recv, Duration::from_millis(100)).await;
        handle.abort();

        assert!(msg_types.contains(&MSG_TYPE_TRADE), "split-row fills emit a trade frame");
    }

    fn fill_batch(block_number: u64, events: Vec<NodeDataFill>) -> Batch<NodeDataFill> {
        Batch::new_for_test(block_number, 1_700_000_000_000, events)
    }

    #[test]
    fn fill_pair_accumulator_pairs_same_row() {
        let mut pairs = FillPairAccumulator::new(1_000);
        let out = pairs
            .ingest_batch(&fill_batch(1, vec![test_fill("BTC", Side::Bid, 1), test_fill("BTC", Side::Ask, 1)]), 1_000);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].trade.tid, 1);
        assert!(pairs.pending.is_empty());
    }

    #[test]
    fn fill_pair_accumulator_pairs_split_rows_bid_then_ask() {
        let mut pairs = FillPairAccumulator::new(1_000);

        let first = pairs.ingest_batch(&fill_batch(1, vec![test_fill("BTC", Side::Bid, 2)]), 1_000);
        let second = pairs.ingest_batch(&fill_batch(1, vec![test_fill("BTC", Side::Ask, 2)]), 1_001);

        assert!(first.is_empty());
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].trade.tid, 2);
        assert!(pairs.pending.is_empty());
    }

    #[test]
    fn fill_pair_accumulator_pairs_split_rows_ask_then_bid() {
        let mut pairs = FillPairAccumulator::new(1_000);

        let first = pairs.ingest_batch(&fill_batch(1, vec![test_fill("BTC", Side::Ask, 3)]), 1_000);
        let second = pairs.ingest_batch(&fill_batch(1, vec![test_fill("BTC", Side::Bid, 3)]), 1_001);

        assert!(first.is_empty());
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].trade.tid, 3);
        assert!(pairs.pending.is_empty());
    }

    #[test]
    fn fill_pair_accumulator_replaces_duplicate_same_side() {
        let mut pairs = FillPairAccumulator::new(1_000);
        assert!(pairs.ingest_batch(&fill_batch(1, vec![test_fill("BTC", Side::Bid, 4)]), 1_000).is_empty());
        assert!(pairs.ingest_batch(&fill_batch(1, vec![test_fill("BTC", Side::Bid, 4)]), 1_001).is_empty());

        let report = pairs.report_if_due(61_001).expect("duplicate report");
        assert_eq!(report.duplicate_replaced, 1);
        assert_eq!(report.pending, 1);
    }

    #[test]
    fn fill_pair_accumulator_rejects_mismatched_pair() {
        let mut pairs = FillPairAccumulator::new(1_000);
        let mut ask = test_fill("BTC", Side::Ask, 5);
        ask.1.hash = "0x1".to_string();
        let mut bid = test_fill("BTC", Side::Bid, 5);
        bid.1.hash = "0x2".to_string();

        assert!(pairs.ingest_batch(&fill_batch(1, vec![ask]), 1_000).is_empty());
        assert!(pairs.ingest_batch(&fill_batch(1, vec![bid]), 1_001).is_empty());

        let report = pairs.report_if_due(61_001).expect("malformed report");
        assert_eq!(report.malformed, 1);
        assert_eq!(report.pending, 0);
    }

    #[test]
    fn fill_pair_accumulator_expires_orphans() {
        let mut pairs = FillPairAccumulator::new(1_000);
        assert!(pairs.ingest_batch(&fill_batch(1, vec![test_fill("BTC", Side::Bid, 6)]), 1_000).is_empty());

        pairs.expire_orphans(1_000 + FillPairAccumulator::ORPHAN_TIMEOUT_MS + 1);
        let report = pairs.report_if_due(61_001).expect("orphan report");

        assert_eq!(report.orphan_dropped, 1);
        assert_eq!(report.one_sided_token_alias, 0);
        assert_eq!(report.pending, 0);
    }

    #[test]
    fn fill_pair_accumulator_classifies_one_sided_token_aliases_separately() {
        let mut pairs = FillPairAccumulator::new(1_000);
        assert!(pairs.ingest_batch(&fill_batch(1, vec![test_fill("#350", Side::Bid, 7)]), 1_000).is_empty());

        pairs.expire_orphans(1_000 + FillPairAccumulator::ORPHAN_TIMEOUT_MS + 1);
        let report = pairs.report_if_due(61_001).expect("one-sided token alias report");

        assert_eq!(report.orphan_dropped, 0);
        assert_eq!(report.one_sided_token_alias, 1);
        assert_eq!(report.pending, 0);
    }

    #[test]
    fn fill_pair_accumulator_capacity_evicts_oldest() {
        let mut pairs = FillPairAccumulator::new(1_000);

        for tid in 0..=FillPairAccumulator::MAX_PENDING as u64 {
            let _paired = pairs.ingest_batch(&fill_batch(1, vec![test_fill("BTC", Side::Bid, tid)]), 1_000);
        }

        assert_eq!(pairs.pending.len(), FillPairAccumulator::MAX_PENDING);
        assert!(!pairs.pending.contains_key(&0), "oldest pending fill is evicted");
        let report = pairs.report_if_due(61_001).expect("capacity report");
        assert_eq!(report.orphan_dropped, 1);
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

    #[test]
    fn tob_health_reports_stale_suppression_as_info_and_resets() {
        let mut health = TobPublisherHealth::new(1_000);

        let snapshot = TobSuppressedSnapshot {
            source: "diffs",
            height: 42,
            block_time_ms: 61_234,
            source_block_time_ms: 62_000,
            source_local_time_ms: 62_250,
            source_lag_ms: 30_000,
            snapshot_to_source_block_lag_ms: 766,
            validator_write_lag_ms: 250,
            listener_to_publisher_ms: 1_000,
            queue_delay_ms: 4,
            latest_status_height: Some(43),
            latest_diff_height: Some(42),
            latest_fill_height: Some(41),
        };
        assert_eq!(health.record_suppressed(TobSuppressedKind::Snapshot, 30_000, 2_000, Some(snapshot), None), None);
        assert_eq!(health.record_receiver_lag(42, 3_000), None);

        let fill = TobSuppressedFill {
            height: 43,
            block_time_ms: 62_345,
            local_time_ms: 63_000,
            source_lag_ms: 31_000,
            validator_write_lag_ms: 655,
            listener_to_publisher_ms: 30_345,
            queue_delay_ms: 5,
            event_count: 2,
        };
        let report =
            health.record_suppressed(TobSuppressedKind::Fill, 31_000, 6_000, None, Some(fill)).expect("report is due");

        assert_eq!(report.level, TobHealthLevel::Info);
        assert_eq!(report.last_source_lag_ms, 31_000);
        assert_eq!(report.source_lag_min_ms, 30_000);
        assert_eq!(report.source_lag_avg_ms, 30_500);
        assert_eq!(report.source_lag_max_ms, 31_000);
        assert_eq!(report.suppressed_snapshots, 1);
        assert_eq!(report.suppressed_fills, 1);
        assert_eq!(report.receiver_lag_events, 1);
        assert_eq!(report.receiver_lagged_messages, 42);
        assert_eq!(report.last_suppressed_snapshot, Some(snapshot));
        assert_eq!(report.last_suppressed_fill, Some(fill));
        assert!(MulticastPublisher::format_suppressed_snapshot(snapshot).contains("minute_phase_ms=1234"));
        assert!(MulticastPublisher::format_suppressed_snapshot(snapshot).contains("listener_to_publisher_ms=1000"));
        assert!(MulticastPublisher::format_suppressed_fill(fill).contains("event_count=2"));

        assert_eq!(health.take_report(11_000), None);
    }

    #[test]
    fn tob_health_warns_when_receiver_lags_while_source_is_fresh() {
        let mut health = TobPublisherHealth::new(1_000);
        health.observe_publishable_lag(100);
        health.record_queue_delay(2);
        health.record_queue_delay(4);

        let report = health.record_receiver_lag(7, 6_000).expect("report is due");

        assert_eq!(report.level, TobHealthLevel::Warn);
        assert_eq!(report.last_source_lag_ms, 100);
        assert_eq!(report.queue_delay_min_ms, 2);
        assert_eq!(report.queue_delay_avg_ms, 3);
        assert_eq!(report.queue_delay_max_ms, 4);
        assert_eq!(report.suppressed_snapshots, 0);
        assert_eq!(report.suppressed_fills, 0);
        assert_eq!(report.receiver_lag_events, 1);
        assert_eq!(report.receiver_lagged_messages, 7);
    }

    #[test]
    fn tob_health_coalesces_receiver_lag_events() {
        let mut health = TobPublisherHealth::new(1_000);
        health.observe_publishable_lag(10_000);

        assert_eq!(health.record_receiver_lag(3, 2_000), None);
        assert_eq!(health.record_receiver_lag(5, 3_000), None);

        let report = health.record_receiver_lag(11, 6_000).expect("report is due");

        assert_eq!(report.level, TobHealthLevel::Info);
        assert_eq!(report.last_source_lag_ms, 10_000);
        assert_eq!(report.receiver_lag_events, 3);
        assert_eq!(report.receiver_lagged_messages, 19);
    }
}
