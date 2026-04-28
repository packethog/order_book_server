//! DZ Depth-of-Book multicast emitter.
//!
//! Receives `DobEvent`s from the L4 apply step via a bounded MPSC, packs them into
//! DoB frames per the wire spec, and sends on the mktdata / refdata sockets.
//! Phase 1 covers mktdata + refdata; the snapshot port is wired in phase 2.

use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::interval;

use crate::multicast::publisher::{split_legs, make_leg};

use crate::protocol::dob::constants::{
    BATCH_BOUNDARY_SIZE, HEARTBEAT_SIZE, INSTRUMENT_RESET_SIZE,
    ORDER_ADD_SIZE, ORDER_CANCEL_SIZE, ORDER_EXECUTE_SIZE,
    SNAPSHOT_BEGIN_SIZE, SNAPSHOT_END_SIZE, SNAPSHOT_ORDER_SIZE,
    SIDE_ASK, SIDE_BID,
};
use crate::protocol::dob::frame::DobFrameBuilder;
use crate::protocol::dob::messages::{
    BatchBoundary, InstrumentReset, OrderAdd, OrderCancel, OrderExecute,
    SnapshotBegin, SnapshotEnd, SnapshotOrder, encode_batch_boundary, encode_heartbeat,
    encode_instrument_reset, encode_order_add, encode_order_cancel, encode_order_execute,
    encode_snapshot_begin, encode_snapshot_end, encode_snapshot_order,
};

/// Events produced by the L4 apply step, consumed by the DoB emitter.
#[derive(Debug, Clone)]
pub enum DobEvent {
    OrderAdd(OrderAdd),
    OrderCancel(OrderCancel),
    OrderExecute(OrderExecute),
    BatchBoundary(BatchBoundary),
    /// Per-instrument resync signal emitted by `apply_recovery` when the
    /// publisher detects that its book state is inconsistent with the source.
    /// Subscribers discard state for `instrument_id` and await a snapshot with
    /// `Anchor Seq == new_anchor_seq` on the snapshot port.
    InstrumentReset(InstrumentReset),
    /// Request a `Heartbeat` emission on mktdata (driven by the quiet-period timer).
    HeartbeatTick,
    /// Signals the emitter to flush and emit `EndOfSession` on shutdown.
    Shutdown,
}

/// Sender half handed to the L4 apply step and control tasks.
pub type DobEventSender = mpsc::Sender<DobEvent>;

/// Receiver half consumed by the emitter task.
pub type DobEventReceiver = mpsc::Receiver<DobEvent>;

/// Construct the bounded MPSC used between L4 apply and the DoB emitter.
/// Bound is a configuration knob; overflow drops and bumps `Reset Count`.
#[must_use]
pub fn channel(bound: usize) -> (DobEventSender, DobEventReceiver) {
    mpsc::channel(bound)
}

// ---------------------------------------------------------------------------
// DoB Snapshot scheduler scaffolding — Task 6
// ---------------------------------------------------------------------------

/// Snapshot stream configuration.
#[derive(Debug, Clone)]
pub struct DobSnapshotConfig {
    pub group_addr: Ipv4Addr,
    pub port: u16,
    pub bind_addr: Ipv4Addr,
    pub channel_id: u8,
    pub mtu: u16,
    /// Target round-robin cycle duration. Scheduler paces emission to hit
    /// this; if the active set grows, cycles will simply take longer.
    pub round_duration: Duration,
}

/// Snapshot requests are either routine round-robin slots (synthesised by
/// the scheduler itself) or priority requests injected from `apply_recovery`.
#[derive(Debug, Clone)]
pub enum DobSnapshotRequest {
    /// Out-of-cycle priority snapshot for an instrument that just received
    /// an `InstrumentReset`. The scheduler emits this at the head of its
    /// queue, with `Anchor Seq == anchor_seq`.
    Priority { instrument_id: u32, anchor_seq: u64 },
}

pub type DobSnapshotRequestSender = mpsc::Sender<DobSnapshotRequest>;
pub type DobSnapshotRequestReceiver = mpsc::Receiver<DobSnapshotRequest>;

#[must_use]
pub fn snapshot_request_channel(
    bound: usize,
) -> (DobSnapshotRequestSender, DobSnapshotRequestReceiver) {
    mpsc::channel(bound)
}

/// Shared `mktdata`-port sequence number, read by the snapshot emitter at
/// per-instrument lock-clone time and by `apply_recovery` when reserving a
/// `new_anchor_seq` for an `InstrumentReset`. Atomic ordering: relaxed reads
/// suffice — the value is monotonic and the snapshot port carries the
/// resulting `anchor_seq` along with the data, so a slightly stale read is
/// harmless (subscribers reconcile via the carried `anchor_seq`).
pub type SharedMktdataSeq = Arc<AtomicU64>;

#[derive(Debug, Clone)]
pub struct DobMktdataConfig {
    pub group_addr: Ipv4Addr,
    pub port: u16,
    pub bind_addr: Ipv4Addr,
    pub channel_id: u8,
    pub mtu: u16,
    pub heartbeat_interval: Duration,
}

pub struct DobEmitter {
    config: DobMktdataConfig,
    socket: UdpSocket,
    /// Shared frame seq with `apply_recovery`. The recovery path reserves
    /// `new_anchor_seq` via `fetch_add` on this same atomic so that
    /// `InstrumentReset.new_anchor_seq` always points to a future mktdata
    /// frame seq, never colliding with what the emitter is about to write.
    seq: SharedMktdataSeq,
    reset_count: u8,
    current_frame: Option<DobFrameBuilder>,
}

impl DobEmitter {
    /// Convenience constructor for tests and standalone use; allocates a
    /// fresh, unshared seq counter. Production code should use
    /// `bind_with_seq` so the recovery path can share the same atomic.
    pub async fn bind(config: DobMktdataConfig) -> std::io::Result<Self> {
        Self::bind_with_seq(config, Arc::new(AtomicU64::new(0))).await
    }

    /// Binds the mktdata socket using a caller-provided shared seq atomic.
    /// The same `Arc<AtomicU64>` MUST be passed to the snapshot scheduler
    /// (read via `load`) and the `DobReplayTaps` (which `fetch_add`s when
    /// reserving an `InstrumentReset` anchor) so seq numbers stay coherent
    /// across the three call sites.
    pub async fn bind_with_seq(
        config: DobMktdataConfig,
        seq: SharedMktdataSeq,
    ) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(SocketAddrV4::new(config.bind_addr, 0)).await?;
        socket.set_multicast_loop_v4(false)?;
        socket.set_multicast_ttl_v4(64)?;
        // Connect so send() pushes to the configured multicast group:port
        socket.connect(SocketAddrV4::new(config.group_addr, config.port)).await?;
        Ok(Self {
            config,
            socket,
            seq,
            reset_count: 0,
            current_frame: None,
        })
    }

    fn next_seq(&mut self) -> u64 {
        // Relaxed: the atomic carries no other published memory; monotonicity
        // comes from fetch_add itself. Mirrors the read in the snapshot
        // scheduler and the reserve-on-recovery path in apply_recovery.
        let prev = self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        prev.wrapping_add(1)
    }

    fn start_frame(&mut self) -> &mut DobFrameBuilder {
        let seq = self.next_seq();
        let ts = now_ns();
        let fb = DobFrameBuilder::new(
            self.config.channel_id, seq, ts, self.reset_count, self.config.mtu,
        );
        self.current_frame = Some(fb);
        self.current_frame.as_mut().unwrap()
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        if let Some(mut fb) = self.current_frame.take() {
            if !fb.is_empty() {
                let frame = fb.finalize();
                self.socket.send(frame).await?;
            }
        }
        Ok(())
    }

    /// Write one event into the current frame; flush and open a new frame
    /// when the event wouldn't fit.
    async fn append(&mut self, event: &DobEvent) -> std::io::Result<()> {
        let size = event_size(event);
        if size == 0 {
            return Ok(()); // HeartbeatTick and Shutdown don't produce bytes here
        }

        // Ensure we have a frame, and that the event fits.
        if self.current_frame.as_ref().map_or(true, |fb| fb.remaining() < size) {
            self.flush().await?;
            self.start_frame();
        }

        let fb = self.current_frame.as_mut().unwrap();
        let buf = fb.message_buffer(size).expect("size checked above");
        match event {
            DobEvent::OrderAdd(msg) => encode_order_add(buf, msg),
            DobEvent::OrderCancel(msg) => encode_order_cancel(buf, msg),
            DobEvent::OrderExecute(msg) => encode_order_execute(buf, msg),
            DobEvent::BatchBoundary(msg) => encode_batch_boundary(buf, msg),
            DobEvent::InstrumentReset(msg) => encode_instrument_reset(buf, msg),
            _ => unreachable!("size filter should have returned"),
        }
        fb.commit_message();
        Ok(())
    }
}

fn event_size(event: &DobEvent) -> usize {
    match event {
        DobEvent::OrderAdd(_) => ORDER_ADD_SIZE,
        DobEvent::OrderCancel(_) => ORDER_CANCEL_SIZE,
        DobEvent::OrderExecute(_) => ORDER_EXECUTE_SIZE,
        DobEvent::BatchBoundary(_) => BATCH_BOUNDARY_SIZE,
        DobEvent::InstrumentReset(_) => INSTRUMENT_RESET_SIZE,
        DobEvent::HeartbeatTick | DobEvent::Shutdown => 0,
    }
}

/// Runs the emitter loop until `rx` is closed or a `Shutdown` event is received.
pub async fn run_dob_emitter(
    mut emitter: DobEmitter,
    mut rx: DobEventReceiver,
) -> std::io::Result<()> {
    let mut heartbeat = interval(emitter.config.heartbeat_interval);
    heartbeat.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            event = rx.recv() => {
                let Some(event) = event else {
                    // All senders dropped — clean exit without EndOfSession (sender-side
                    // crash/restart is not a graceful shutdown).
                    return Ok(());
                };
                if matches!(event, DobEvent::Shutdown) {
                    // Emit EndOfSession on mktdata before exiting so subscribers
                    // see a clean session close rather than a silent timeout.
                    use crate::protocol::dob::constants::END_OF_SESSION_SIZE;
                    if emitter.current_frame.as_ref().map_or(true, |fb| fb.remaining() < END_OF_SESSION_SIZE) {
                        emitter.flush().await?;
                        emitter.start_frame();
                    }
                    let fb = emitter.current_frame.as_mut().unwrap();
                    let buf = fb.message_buffer(END_OF_SESSION_SIZE).expect("size checked");
                    crate::protocol::dob::messages::encode_end_of_session(buf, now_ns());
                    fb.commit_message();
                    emitter.flush().await?;
                    return Ok(());
                }
                if matches!(event, DobEvent::HeartbeatTick) {
                    // Writing a Heartbeat into the current frame, flushing first if needed.
                    if emitter.current_frame.as_ref().map_or(true, |fb| fb.remaining() < HEARTBEAT_SIZE) {
                        emitter.flush().await?;
                        emitter.start_frame();
                    }
                    let fb = emitter.current_frame.as_mut().unwrap();
                    let buf = fb.message_buffer(HEARTBEAT_SIZE).expect("size checked");
                    encode_heartbeat(buf, emitter.config.channel_id, now_ns());
                    fb.commit_message();
                    emitter.flush().await?;
                    continue;
                }
                emitter.append(&event).await?;
            }
            _ = heartbeat.tick() => {
                // Opportunistic flush if we have pending messages; the quiet-period
                // heartbeat is emitted explicitly via HeartbeatTick, so this branch
                // just keeps the MTU-sized frame from sitting idle.
                emitter.flush().await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn channel_round_trip() {
        let (tx, mut rx) = channel(4);
        let msg = DobEvent::HeartbeatTick;
        tx.send(msg.clone()).await.unwrap();
        let received = rx.recv().await.unwrap();
        assert!(matches!(received, DobEvent::HeartbeatTick));
    }
}

// ---------------------------------------------------------------------------
// DoB Snapshot emitter & scheduler — Task 7
// ---------------------------------------------------------------------------

/// Emits `SnapshotBegin` / `SnapshotOrder` / `SnapshotEnd` triads on the
/// snapshot multicast port. One emitter is shared by all instruments — each
/// `emit_snapshot` call produces a complete, self-contained snapshot group
/// for a single instrument.
pub(crate) struct DobSnapshotEmitter {
    config: DobSnapshotConfig,
    socket: UdpSocket,
    /// Snapshot-port frame seq, independent of the mktdata-port `seq`. The
    /// two streams are correlated only via `anchor_seq` carried in
    /// `SnapshotBegin`/`SnapshotEnd`.
    seq: u64,
    reset_count: u8,
    snapshot_id_counter: u32,
}

impl DobSnapshotEmitter {
    /// Binds the snapshot UDP socket. Mirrors `DobEmitter::bind`.
    pub(crate) async fn bind(config: DobSnapshotConfig) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(SocketAddrV4::new(config.bind_addr, 0)).await?;
        socket.set_multicast_loop_v4(false)?;
        socket.set_multicast_ttl_v4(64)?;
        socket.connect(SocketAddrV4::new(config.group_addr, config.port)).await?;
        Ok(Self {
            config,
            socket,
            seq: 0,
            reset_count: 0,
            snapshot_id_counter: 0,
        })
    }

    fn next_seq(&mut self) -> u64 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
    }

    fn next_snapshot_id(&mut self) -> u32 {
        self.snapshot_id_counter = self.snapshot_id_counter.wrapping_add(1);
        self.snapshot_id_counter
    }

    /// Emits one full snapshot triad for `instrument_id`:
    /// - `SnapshotBegin` in its own frame.
    /// - Zero or more `SnapshotOrder` frames, packed up to `config.mtu`.
    /// - `SnapshotEnd` in its own frame.
    ///
    /// `anchor_seq` is the mktdata-port sequence number at the time the
    /// snapshot was anchored; subscribers reconcile the snapshot stream with
    /// the delta stream against this value. `last_instrument_seq` is the most
    /// recently emitted per-instrument seq for `instrument_id` (or 0 if none).
    /// `qty_exponent` is the venue's per-instrument exponent used to scale
    /// each `SnapshotOrder.quantity` from internal `Sz` (10^8 fixed) to wire.
    /// The empty-orders case is supported and emits Begin + End with no
    /// `SnapshotOrder` frames in between.
    pub(crate) async fn emit_snapshot(
        &mut self,
        instrument_id: u32,
        anchor_seq: u64,
        last_instrument_seq: u32,
        qty_exponent: i8,
        orders: Vec<crate::types::inner::InnerL4Order>,
    ) -> std::io::Result<()> {
        let snapshot_id = self.next_snapshot_id();
        let total_orders = u32::try_from(orders.len()).unwrap_or(u32::MAX);

        // 1. SnapshotBegin in its own frame.
        {
            let begin = SnapshotBegin {
                instrument_id,
                anchor_seq,
                total_orders,
                snapshot_id,
                last_instrument_seq,
                timestamp_ns: now_ns(),
            };
            let mut fb = DobFrameBuilder::new(
                self.config.channel_id,
                self.next_seq(),
                now_ns(),
                self.reset_count,
                self.config.mtu,
            );
            let buf = fb
                .message_buffer(SNAPSHOT_BEGIN_SIZE)
                .expect("SnapshotBegin fits in a fresh frame");
            encode_snapshot_begin(buf, &begin);
            fb.commit_message();
            let frame = fb.finalize();
            self.socket.send(frame).await?;
        }

        // 2. Pack SnapshotOrder messages into MTU-sized frames.
        let mut fb_opt: Option<DobFrameBuilder> = None;
        for order in &orders {
            // Open a frame on demand.
            if fb_opt.as_ref().map_or(true, |fb| fb.remaining() < SNAPSHOT_ORDER_SIZE) {
                if let Some(mut fb) = fb_opt.take() {
                    if !fb.is_empty() {
                        let frame = fb.finalize();
                        self.socket.send(frame).await?;
                    }
                }
                fb_opt = Some(DobFrameBuilder::new(
                    self.config.channel_id,
                    self.next_seq(),
                    now_ns(),
                    self.reset_count,
                    self.config.mtu,
                ));
            }
            let fb = fb_opt.as_mut().expect("opened above");
            let buf = fb
                .message_buffer(SNAPSHOT_ORDER_SIZE)
                .expect("SnapshotOrder fits, just-checked remaining()");
            let side = match order.side {
                crate::order_book::Side::Bid => SIDE_BID,
                crate::order_book::Side::Ask => SIDE_ASK,
            };
            // `InnerL4Order.timestamp` is an entry timestamp in milliseconds
            // (set from the order-status row's wall-clock time). Convert to
            // ns to match the wire field. This is the right field per the
            // Task 7 brief's decision #5; using apply-time would be a
            // different (misleading) signal.
            let enter_timestamp_ns = order.timestamp.saturating_mul(1_000_000);
            let msg = SnapshotOrder {
                snapshot_id,
                order_id: order.oid,
                side,
                order_flags: 0, // Phase 1 leaves these at 0; mirrors OrderAdd in dob_tap.
                enter_timestamp_ns,
                price: order.limit_px.value() as i64,
                quantity: crate::order_book::sz_to_fixed(order.sz, qty_exponent),
            };
            encode_snapshot_order(buf, &msg);
            fb.commit_message();
        }
        // Flush any pending SnapshotOrder frame.
        if let Some(mut fb) = fb_opt.take() {
            if !fb.is_empty() {
                let frame = fb.finalize();
                self.socket.send(frame).await?;
            }
        }

        // 3. SnapshotEnd in its own frame.
        {
            let end = SnapshotEnd {
                instrument_id,
                anchor_seq,
                snapshot_id,
            };
            let mut fb = DobFrameBuilder::new(
                self.config.channel_id,
                self.next_seq(),
                now_ns(),
                self.reset_count,
                self.config.mtu,
            );
            let buf = fb
                .message_buffer(SNAPSHOT_END_SIZE)
                .expect("SnapshotEnd fits in a fresh frame");
            encode_snapshot_end(buf, &end);
            fb.commit_message();
            let frame = fb.finalize();
            self.socket.send(frame).await?;
        }

        Ok(())
    }
}

/// Spawns the snapshot emitter task: round-robin across active instruments
/// with a priority queue head that preempts BETWEEN slots (not mid-slot or
/// mid-sleep). Recovery snapshots queued via `priority_rx` typically wait
/// at most one slot's worth of `round_duration / active.len()` before
/// being serviced. With a single active instrument the wait can extend up
/// to the full `round_duration`.
///
/// On each iteration:
/// 1. Drain any pending `DobSnapshotRequest::Priority` requests.
/// 2. If priority work is queued, emit one priority snapshot and loop.
/// 3. Otherwise iterate the active instrument set once, emitting one snapshot
///    per slot and pacing with `config.round_duration / active.len()`.
///    Between slots, re-check the priority queue so a recovery snapshot does
///    not have to wait a full cycle.
///
/// Returns Ok(()) when the priority channel is closed.
pub(crate) async fn run_dob_snapshot_task(
    mut emitter: DobSnapshotEmitter,
    listener: Arc<tokio::sync::Mutex<crate::listeners::order_book::OrderBookListener>>,
    seq_counter: crate::listeners::order_book::dob_tap::SharedSeqCounter,
    registry: crate::instruments::SharedRegistry,
    mktdata_seq: SharedMktdataSeq,
    mut priority_rx: DobSnapshotRequestReceiver,
) -> std::io::Result<()> {
    use std::collections::VecDeque;
    use std::sync::atomic::Ordering;
    use tokio::sync::mpsc::error::TryRecvError;

    let mut priority_queue: VecDeque<DobSnapshotRequest> = VecDeque::new();

    loop {
        // 1. Drain any pending priority requests (non-blocking).
        loop {
            match priority_rx.try_recv() {
                Ok(req) => priority_queue.push_back(req),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }

        // 2. Service one priority request per loop iteration.
        if let Some(DobSnapshotRequest::Priority { instrument_id, anchor_seq }) =
            priority_queue.pop_front()
        {
            let Some((coin, qty_exponent)) =
                lookup_coin_for_instrument(&registry, instrument_id)
            else {
                log::warn!(
                    "dob_snapshot: priority request for unknown instrument_id {instrument_id}, skipping"
                );
                continue;
            };
            // Read last_instrument_seq BEFORE cloning orders. Between this read and
            // the clone, more deltas may be applied (apply tap holds neither lock).
            // last_instrument_seq <= snapshot view is safe — subscribers may replay a
            // few deltas already reflected in the snapshot. The reverse ordering
            // would let them skip deltas, which is unsafe.
            let last_instrument_seq = seq_counter
                .lock()
                .expect("seq mutex poisoned")
                .last(instrument_id);
            let orders = listener
                .lock()
                .await
                .clone_coin_orders(&coin)
                .unwrap_or_default();
            emitter
                .emit_snapshot(instrument_id, anchor_seq, last_instrument_seq, qty_exponent, orders)
                .await?;
            continue;
        }

        // 3. No priority work: do one round-robin slot per active instrument.
        let active = list_active_instruments(&registry);
        if active.is_empty() {
            // Nothing to do — sleep briefly before re-checking.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        let active_len = u32::try_from(active.len()).unwrap_or(u32::MAX).max(1);
        let slot_budget = emitter.config.round_duration / active_len;

        for (instrument_id, coin, qty_exponent) in active {
            // Re-check the priority queue between slots so a recovery snapshot
            // does not have to wait a full cycle.
            loop {
                match priority_rx.try_recv() {
                    Ok(req) => priority_queue.push_back(req),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Ok(()),
                }
            }
            if !priority_queue.is_empty() {
                // Break out of the round-robin so the outer loop services it.
                break;
            }

            let anchor_seq = mktdata_seq.load(Ordering::Relaxed);
            // Read last_instrument_seq BEFORE cloning orders. Between this read and
            // the clone, more deltas may be applied (apply tap holds neither lock).
            // last_instrument_seq <= snapshot view is safe — subscribers may replay a
            // few deltas already reflected in the snapshot. The reverse ordering
            // would let them skip deltas, which is unsafe.
            let last_instrument_seq = seq_counter
                .lock()
                .expect("seq mutex poisoned")
                .last(instrument_id);
            let orders = listener
                .lock()
                .await
                .clone_coin_orders(&coin)
                .unwrap_or_default();

            let emit_start = std::time::Instant::now();
            emitter
                .emit_snapshot(instrument_id, anchor_seq, last_instrument_seq, qty_exponent, orders)
                .await?;
            let elapsed = emit_start.elapsed();
            if elapsed < slot_budget {
                tokio::time::sleep(slot_budget - elapsed).await;
            }
        }
    }
}

/// Linear-scan lookup of `instrument_id -> (Coin, qty_exponent)` against the
/// active instrument set. Used by the priority path; cost is fine because
/// priority requests are rare (only on validation mismatch via `apply_recovery`).
fn lookup_coin_for_instrument(
    registry: &crate::instruments::SharedRegistry,
    instrument_id: u32,
) -> Option<(crate::order_book::Coin, i8)> {
    let state = registry.load();
    state
        .active
        .iter()
        .find(|(_coin, info)| info.instrument_id == instrument_id)
        .map(|(coin, info)| (crate::order_book::Coin::new(coin), info.qty_exponent))
}

/// Snapshot of the active instrument set as `(instrument_id, Coin, qty_exponent)`
/// triples. The scheduler iterates this once per round; if the set changes
/// mid-cycle we'll pick up the change on the next round.
fn list_active_instruments(
    registry: &crate::instruments::SharedRegistry,
) -> Vec<(u32, crate::order_book::Coin, i8)> {
    let state = registry.load();
    state
        .active
        .iter()
        .map(|(coin, info)| (info.instrument_id, crate::order_book::Coin::new(coin), info.qty_exponent))
        .collect()
}

// ---------------------------------------------------------------------------
// DoB Refdata task — Task 13
// ---------------------------------------------------------------------------

/// Configuration for the DoB refdata multicast task.
#[derive(Debug, Clone)]
pub struct DobRefdataConfig {
    pub group_addr: Ipv4Addr,
    pub port: u16,
    pub bind_addr: Ipv4Addr,
    pub channel_id: u8,
    pub mtu: u16,
    /// How long a full InstrumentDefinition retransmission cycle takes.
    pub definition_cycle: Duration,
    /// How often to emit a ManifestSummary heartbeat.
    pub manifest_cadence: Duration,
}

/// Runs the DoB refdata emitter loop.
///
/// Periodically emits:
/// - `InstrumentDefinition` retransmissions (one full cycle per `definition_cycle`).
/// - `ManifestSummary` heartbeats (one per `manifest_cadence`).
///
/// Both streams are packed into DoB frames and sent to the configured multicast group/port.
/// Runs until the task is cancelled (or an I/O error occurs).
pub async fn run_dob_refdata_task(
    config: DobRefdataConfig,
    registry: crate::instruments::InstrumentRegistry,
) -> std::io::Result<()> {
    // Bind the UDP socket and point it at the multicast group.
    let socket = UdpSocket::bind(SocketAddrV4::new(config.bind_addr, 0)).await?;
    socket.set_multicast_loop_v4(false)?;
    socket.set_multicast_ttl_v4(64)?;
    socket.connect(SocketAddrV4::new(config.group_addr, config.port)).await?;

    let mut seq: u64 = 0;
    let reset_count: u8 = 0;

    // Helper: advance and return the next sequence number.
    let mut next_seq = || {
        seq = seq.wrapping_add(1);
        seq
    };

    let mut definition_ticker = interval(config.definition_cycle);
    definition_ticker.tick().await; // consume the immediate first tick

    let mut manifest_ticker = interval(config.manifest_cadence);
    manifest_ticker.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            _ = definition_ticker.tick() => {
                // Snapshot the active instruments lock-free via ArcSwap.
                let (snapshot, manifest_seq) = {
                    let guard = registry.shared();
                    let state = guard.load();
                    let snap: Vec<(String, crate::instruments::InstrumentInfo)> =
                        state.active.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    let seq_tag = state.manifest_seq;
                    (snap, seq_tag)
                };

                if snapshot.is_empty() {
                    continue;
                }

                let now = now_ns();
                let mut fb = DobFrameBuilder::new(
                    config.channel_id, next_seq(), now, reset_count, config.mtu,
                );

                for (coin, info) in &snapshot {
                    let data = build_refdata_instrument_definition(coin, info, manifest_seq);
                    match fb.message_buffer(crate::protocol::dob::constants::INSTRUMENT_DEF_SIZE) {
                        Ok(buf) => {
                            crate::protocol::dob::messages::encode_instrument_definition(buf, &data, 0);
                            fb.commit_message();
                        }
                        Err(_) => {
                            // Frame full — flush and open a new one.
                            if !fb.is_empty() {
                                let frame = fb.finalize();
                                socket.send(frame).await?;
                            }
                            let now2 = now_ns();
                            fb = DobFrameBuilder::new(
                                config.channel_id, next_seq(), now2, reset_count, config.mtu,
                            );
                            let buf = fb
                                .message_buffer(crate::protocol::dob::constants::INSTRUMENT_DEF_SIZE)
                                .expect("InstrumentDefinition fits in a fresh frame");
                            crate::protocol::dob::messages::encode_instrument_definition(buf, &data, 0);
                            fb.commit_message();
                        }
                    }
                }

                // Flush any partial final frame.
                if !fb.is_empty() {
                    let frame = fb.finalize();
                    socket.send(frame).await?;
                }
            }

            _ = manifest_ticker.tick() => {
                // Collect stats lock-free.
                let (manifest_seq, instrument_count) = {
                    let guard = registry.shared();
                    let state = guard.load();
                    let seq_tag = state.manifest_seq;
                    let count = u32::try_from(state.active.len()).unwrap_or(u32::MAX);
                    (seq_tag, count)
                };

                let now = now_ns();
                let mut fb = DobFrameBuilder::new(
                    config.channel_id, next_seq(), now, reset_count, config.mtu,
                );
                let buf = fb
                    .message_buffer(crate::protocol::dob::constants::MANIFEST_SUMMARY_SIZE)
                    .expect("ManifestSummary fits in a fresh frame");
                crate::protocol::dob::messages::encode_manifest_summary(
                    buf,
                    config.channel_id,
                    manifest_seq,
                    instrument_count,
                    now,
                );
                fb.commit_message();
                let frame = fb.finalize();
                socket.send(frame).await?;
            }
        }
    }
}

/// Builds an `InstrumentDefinitionData` for the given instrument.
/// Mirrors `build_instrument_definition` in `multicast/publisher.rs` but is
/// local to the DoB refdata task so we don't depend on a private function.
fn build_refdata_instrument_definition(
    coin: &str,
    info: &crate::instruments::InstrumentInfo,
    manifest_seq: u16,
) -> crate::protocol::messages::InstrumentDefinitionData {
    use crate::protocol::constants::{
        ASSET_CLASS_CRYPTO_SPOT, MARKET_MODEL_CLOB, PRICE_BOUND_UNBOUNDED, SETTLE_TYPE_NA,
    };

    let (leg1_str, leg2_str) = split_legs(coin);
    let leg1 = make_leg(leg1_str);
    let leg2 = make_leg(leg2_str);

    crate::protocol::messages::InstrumentDefinitionData {
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

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod refdata_tests {
    use super::*;
    use crate::instruments::{InstrumentInfo, InstrumentRegistry, RegistryState, UniverseEntry, make_symbol};
    use std::net::Ipv4Addr;

    fn test_registry() -> InstrumentRegistry {
        let universe = vec![
            UniverseEntry {
                instrument_id: 0,
                coin: "BTC".to_string(),
                is_delisted: false,
                info: InstrumentInfo {
                    instrument_id: 0,
                    price_exponent: -1,
                    qty_exponent: -5,
                    symbol: make_symbol("BTC"),
                },
            },
            UniverseEntry {
                instrument_id: 1,
                coin: "ETH".to_string(),
                is_delisted: false,
                info: InstrumentInfo {
                    instrument_id: 1,
                    price_exponent: -2,
                    qty_exponent: -4,
                    symbol: make_symbol("ETH"),
                },
            },
        ];
        InstrumentRegistry::new(RegistryState::new(universe))
    }

    async fn bind_receiver() -> (UdpSocket, SocketAddrV4) {
        let sock = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = match sock.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        (sock, addr)
    }

    #[tokio::test]
    async fn refdata_emits_on_wire() {
        use crate::protocol::dob::constants::DEFAULT_MTU;

        let (recv, recv_addr) = bind_receiver().await;
        let registry = test_registry();

        let config = DobRefdataConfig {
            group_addr: *recv_addr.ip(),
            port: recv_addr.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: 2,
            mtu: DEFAULT_MTU,
            // Long cycle so we only see the ManifestSummary in the test window.
            definition_cycle: Duration::from_secs(60),
            manifest_cadence: Duration::from_millis(50),
        };

        let task = tokio::spawn(run_dob_refdata_task(config, registry));

        let mut buf = [0u8; 2048];
        let n = tokio::time::timeout(Duration::from_secs(2), recv.recv(&mut buf))
            .await
            .expect("timed out waiting for refdata packet")
            .expect("recv error");

        // Every DoB frame starts with magic [0x44, 0x44].
        assert_eq!(&buf[0..2], &[0x44, 0x44], "DoB magic on wire");

        // The first app message type byte is at offset FRAME_HEADER_SIZE.
        // Accept either InstrumentDefinition (0x02) or ManifestSummary (0x07).
        use crate::protocol::dob::constants::{
            FRAME_HEADER_SIZE, MSG_TYPE_INSTRUMENT_DEF, MSG_TYPE_MANIFEST_SUMMARY,
        };
        assert!(n >= FRAME_HEADER_SIZE + 1, "frame too short to contain a message type");
        let msg_type = buf[FRAME_HEADER_SIZE];
        assert!(
            msg_type == MSG_TYPE_INSTRUMENT_DEF || msg_type == MSG_TYPE_MANIFEST_SUMMARY,
            "unexpected message type byte: 0x{msg_type:02X}",
        );

        task.abort();
    }
}

#[cfg(test)]
mod emitter_tests {
    use super::*;
    use crate::protocol::dob::constants::{DEFAULT_MTU, FRAME_HEADER_SIZE};

    fn sample_order_add(seq: u32) -> OrderAdd {
        OrderAdd {
            instrument_id: 42, source_id: 1, side: 0, order_flags: 0,
            per_instrument_seq: seq, order_id: seq as u64,
            enter_timestamp_ns: 1_700_000_000_000_000_000,
            price: 100, quantity: 1,
        }
    }

    async fn bind_receiver() -> (UdpSocket, SocketAddrV4) {
        let sock = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = match sock.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        (sock, addr)
    }

    #[tokio::test]
    async fn single_order_add_roundtrip_via_udp() {
        let (recv, recv_addr) = bind_receiver().await;
        let config = DobMktdataConfig {
            group_addr: *recv_addr.ip(),
            port: recv_addr.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: 3,
            mtu: DEFAULT_MTU,
            heartbeat_interval: Duration::from_secs(60),
        };
        let mut emitter = DobEmitter::bind(config).await.unwrap();
        emitter.start_frame();
        let buf = emitter.current_frame.as_mut().unwrap().message_buffer(ORDER_ADD_SIZE).unwrap();
        encode_order_add(buf, &sample_order_add(1));
        emitter.current_frame.as_mut().unwrap().commit_message();
        emitter.flush().await.unwrap();

        let mut rxbuf = [0u8; 1500];
        let (n, _) = tokio::time::timeout(Duration::from_secs(1), recv.recv_from(&mut rxbuf))
            .await.unwrap().unwrap();
        assert_eq!(n, FRAME_HEADER_SIZE + ORDER_ADD_SIZE);
        assert_eq!(&rxbuf[0..2], &[0x44, 0x44], "DoB magic on wire");
        assert_eq!(rxbuf[FRAME_HEADER_SIZE], 0x10, "OrderAdd type");
    }
}

#[cfg(test)]
mod snapshot_anchor_tests {
    //! Anchor-seq integrity test: `SnapshotBegin` must carry the `anchor_seq`
    //! from the priority request and the per-instrument seq's `last()` value
    //! at the moment the per-coin clone is taken.
    //!
    //! This is the Phase 2 task 10 anchor-seq test from the plan
    //! (`docs/superpowers/plans/2026-04-25-depth-of-book-publisher-phase-2.md`).
    //! The plan template directed the test to `server/tests/dob_snapshot_anchor_seq.rs`,
    //! but the integration-test target can only see `pub` items, and
    //! exposing `OrderBookListener`, `Snapshots<InnerL4Order>`, `InnerL4Order`,
    //! `DobSnapshotEmitter`, and `run_dob_snapshot_task` solely for an
    //! external test fixture would expand the public API far beyond what's
    //! warranted. Keeping the test as a unit test here exercises the same
    //! priority-path code (`run_dob_snapshot_task` → `emit_snapshot` →
    //! `encode_snapshot_begin`) and asserts the exact same invariants.
    use super::*;
    use crate::listeners::order_book::OrderBookListener;
    use crate::order_book::PerInstrumentSeqCounter;
    use crate::protocol::dob::constants::{
        DEFAULT_MTU, FRAME_HEADER_SIZE, MSG_TYPE_SNAPSHOT_BEGIN,
    };
    use crate::test_fixtures::{build_one_coin_snapshot, build_registry_with_one_instrument};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::net::UdpSocket;

    #[tokio::test]
    async fn snapshot_anchor_matches_request_and_last_instrument_seq() {
        // 1. Bind a collector socket on a random loopback port. This stands
        //    in for the snapshot-port subscriber.
        let collector =
            UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let collector_addr = match collector.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };

        // 2. Pre-populate shared state with known sentinel values. The mktdata
        //    seq is set to 99 (NOT 42) so we can verify the priority path
        //    threads the request's anchor_seq=42 through to the wire — not the
        //    live mktdata_seq value (which the round-robin path would use).
        let mktdata_seq = Arc::new(AtomicU64::new(0));
        mktdata_seq.store(99, Ordering::Relaxed);

        let seq_counter = Arc::new(std::sync::Mutex::new(PerInstrumentSeqCounter::new()));
        {
            let mut g = seq_counter.lock().unwrap();
            for _ in 0..7 {
                g.next(0);
            }
            assert_eq!(g.last(0), 7, "fixture pre-populated last(0) to 7");
        }

        // 3. Build a listener pre-seeded with one coin's resting orders.
        let coin_str = "BTC";
        let listener = OrderBookListener::for_test_with_snapshot(
            build_one_coin_snapshot(coin_str, 3, /* oid_offset = */ 0),
            /* height = */ 1,
        );
        let listener = Arc::new(tokio::sync::Mutex::new(listener));

        // 4. Bind the snapshot emitter at the collector address.
        //    60s round_duration => 60s per-slot pacing sleep (one instrument).
        //    This guarantees the round-robin path cannot complete an emission
        //    within the test's 2-second recv timeout, so the assertions below
        //    can only see the priority-path frame.
        let emitter = DobSnapshotEmitter::bind(DobSnapshotConfig {
            group_addr: *collector_addr.ip(),
            port: collector_addr.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: 0,
            mtu: DEFAULT_MTU,
            round_duration: Duration::from_secs(60),
        })
        .await
        .unwrap();

        // 5. Build the registry with one instrument (id=0 for BTC).
        let registry = build_registry_with_one_instrument(coin_str, 0);

        // 6. Send the priority request BEFORE spawning the task so the test
        //    doesn't rely on tokio's current-thread-runtime scheduler ordering.
        //    The channel buffers the request; the task picks it up on its
        //    first try_recv.
        let (req_tx, req_rx) = snapshot_request_channel(8);
        req_tx
            .send(DobSnapshotRequest::Priority { instrument_id: 0, anchor_seq: 42 })
            .await
            .unwrap();

        // 7. Spawn the snapshot task. Its first iteration will see the
        //    already-buffered Priority request and preempt any round-robin
        //    work.
        let handle = tokio::spawn(run_dob_snapshot_task(
            emitter,
            listener.clone(),
            seq_counter.clone(),
            registry.clone(),
            mktdata_seq.clone(),
            req_rx,
        ));

        // 8. Read the first datagram off the collector (timeout-bounded).
        let mut buf = [0u8; 2048];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), collector.recv_from(&mut buf))
            .await
            .expect("timed out waiting for SnapshotBegin frame")
            .expect("recv error");

        // 9. Validate frame magic and the SnapshotBegin app message.
        //    Frame header at [0..FRAME_HEADER_SIZE]; SnapshotBegin starts at
        //    FRAME_HEADER_SIZE. SnapshotBegin layout (36 bytes):
        //       msg_type(1) + flags(1) + reserved(2)   = app header (4)
        //       instrument_id(4)                       offset 4..8
        //       anchor_seq(8)                          offset 8..16
        //       total_orders(4)                        offset 16..20
        //       snapshot_id(4)                         offset 20..24
        //       last_instrument_seq(4)                 offset 24..28
        //       timestamp(8)                           offset 28..36
        assert!(
            n >= FRAME_HEADER_SIZE + SNAPSHOT_BEGIN_SIZE,
            "expected at least one full SnapshotBegin in the datagram (got {n} bytes)",
        );
        assert_eq!(&buf[0..2], &[0x44, 0x44], "DoB magic on wire");
        assert_eq!(
            buf[FRAME_HEADER_SIZE], MSG_TYPE_SNAPSHOT_BEGIN,
            "first message must be SnapshotBegin",
        );

        let body = &buf[FRAME_HEADER_SIZE..];
        let instrument_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
        let anchor_seq = u64::from_le_bytes(body[8..16].try_into().unwrap());
        // 24..28 is last_instrument_seq (NOT 20..24 — that's snapshot_id).
        let last_instrument_seq = u32::from_le_bytes(body[24..28].try_into().unwrap());

        assert_eq!(instrument_id, 0, "instrument_id from request");
        assert_eq!(
            anchor_seq, 42,
            "anchor_seq must come from the priority request, not mktdata_seq.load()",
        );
        assert_eq!(
            last_instrument_seq, 7,
            "last_instrument_seq must equal seq_counter.last(0) at clone time",
        );

        handle.abort();
    }
}

#[cfg(test)]
mod snapshot_qty_scaling_tests {
    //! Verifies `SnapshotOrder.quantity` is scaled to the venue's
    //! `qty_exponent` (issue #10). Internal `Sz` carries quantity at the
    //! publisher's fixed 10^8 scale; the wire field must be
    //! `qty * 10^-qty_exponent` (i.e. `Sz::value() / 10^(8 + qty_exponent)`).
    use super::*;
    use crate::order_book::{Coin, Px, Side, Sz};
    use crate::protocol::dob::constants::{
        DEFAULT_MTU, FRAME_HEADER_SIZE, MSG_TYPE_SNAPSHOT_ORDER, SNAPSHOT_ORDER_SIZE,
    };
    use alloy::primitives::Address;
    use tokio::net::UdpSocket;

    fn make_inner_order(coin: &Coin, oid: u64, side: Side, px: &str, sz: &str) -> crate::types::inner::InnerL4Order {
        crate::types::inner::InnerL4Order {
            user: Address::new([0; 20]),
            coin: coin.clone(),
            side,
            limit_px: Px::parse_from_str(px).unwrap(),
            sz: Sz::parse_from_str(sz).unwrap(),
            oid,
            timestamp: 0,
            trigger_condition: String::new(),
            is_trigger: false,
            trigger_px: String::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: String::new(),
            tif: None,
            cloid: None,
        }
    }

    /// Walks the captured datagrams and returns the `quantity` field of the
    /// first `SnapshotOrder` found. Each datagram is one DoB frame; a single
    /// frame may pack multiple SnapshotOrder messages, but the first message
    /// in the frame is sufficient to verify scaling.
    fn first_snapshot_order_quantity(frames: &[Vec<u8>]) -> Option<u64> {
        for frame in frames {
            if frame.len() < FRAME_HEADER_SIZE + SNAPSHOT_ORDER_SIZE {
                continue;
            }
            let body = &frame[FRAME_HEADER_SIZE..];
            if body[0] != MSG_TYPE_SNAPSHOT_ORDER {
                continue;
            }
            // SnapshotOrder body offsets: quantity at 36..44 (see encode_snapshot_order).
            return Some(u64::from_le_bytes(body[36..44].try_into().ok()?));
        }
        None
    }

    #[tokio::test]
    async fn snapshot_order_quantity_scales_to_qty_exponent() {
        // 1. Bind collector and the snapshot emitter pointing at it.
        let collector = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let collector_addr = match collector.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        let mut emitter = DobSnapshotEmitter::bind(DobSnapshotConfig {
            group_addr: *collector_addr.ip(),
            port: collector_addr.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: 0,
            mtu: DEFAULT_MTU,
            round_duration: Duration::from_secs(60),
        })
        .await
        .unwrap();

        // 2. One order with sz="1.234" → Sz::value() == 1.234 * 1e8 == 123_400_000.
        //    With qty_exponent=-3, wire quantity must be 1234.
        let coin = Coin::new("BTC");
        let orders = vec![make_inner_order(&coin, 42, Side::Bid, "100", "1.234")];

        // 3. Emit one snapshot triad.
        emitter
            .emit_snapshot(
                /* instrument_id = */ 0,
                /* anchor_seq = */ 0,
                /* last_instrument_seq = */ 0,
                /* qty_exponent = */ -3,
                orders,
            )
            .await
            .unwrap();

        // 4. Drain frames (Begin, Orders, End all emit immediately above).
        let mut frames: Vec<Vec<u8>> = Vec::new();
        loop {
            let mut buf = [0u8; 4096];
            match tokio::time::timeout(Duration::from_millis(200), collector.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) => frames.push(buf[..n].to_vec()),
                _ => break,
            }
        }

        // 5. Verify the SnapshotOrder.quantity was scaled to qty_exponent.
        let qty = first_snapshot_order_quantity(&frames)
            .expect("captured at least one SnapshotOrder frame");
        assert_eq!(
            qty, 1234,
            "SnapshotOrder.quantity must equal 1234 for sz=1.234 at qty_exponent=-3 (got {})",
            qty,
        );
    }

    #[tokio::test]
    async fn snapshot_order_quantity_at_qty_exponent_zero() {
        // qty_exponent=0 case (e.g. instrument 2Z from issue #10): wire must
        // be the integer count, not Sz::value() (which is qty * 1e8).
        let collector = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let collector_addr = match collector.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        let mut emitter = DobSnapshotEmitter::bind(DobSnapshotConfig {
            group_addr: *collector_addr.ip(),
            port: collector_addr.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: 0,
            mtu: DEFAULT_MTU,
            round_duration: Duration::from_secs(60),
        })
        .await
        .unwrap();

        let coin = Coin::new("2Z");
        let orders = vec![make_inner_order(&coin, 42, Side::Bid, "100", "2921")];

        emitter.emit_snapshot(0, 0, 0, 0, orders).await.unwrap();

        let mut frames: Vec<Vec<u8>> = Vec::new();
        loop {
            let mut buf = [0u8; 4096];
            match tokio::time::timeout(Duration::from_millis(200), collector.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) => frames.push(buf[..n].to_vec()),
                _ => break,
            }
        }

        let qty = first_snapshot_order_quantity(&frames)
            .expect("captured at least one SnapshotOrder frame");
        assert_eq!(qty, 2921);
    }
}
