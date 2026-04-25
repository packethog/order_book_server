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
};
use crate::protocol::dob::frame::DobFrameBuilder;
use crate::protocol::dob::messages::{
    BatchBoundary, InstrumentReset, OrderAdd, OrderCancel, OrderExecute,
    encode_batch_boundary, encode_heartbeat, encode_instrument_reset, encode_order_add,
    encode_order_cancel, encode_order_execute,
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
    seq: u64,
    reset_count: u8,
    current_frame: Option<DobFrameBuilder>,
}

impl DobEmitter {
    pub async fn bind(config: DobMktdataConfig) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(SocketAddrV4::new(config.bind_addr, 0)).await?;
        socket.set_multicast_loop_v4(false)?;
        socket.set_multicast_ttl_v4(1)?;
        // Connect so send() pushes to the configured multicast group:port
        socket.connect(SocketAddrV4::new(config.group_addr, config.port)).await?;
        Ok(Self {
            config,
            socket,
            seq: 0,
            reset_count: 0,
            current_frame: None,
        })
    }

    fn next_seq(&mut self) -> u64 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
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
    socket.set_multicast_ttl_v4(1)?;
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
                // Take a single read-lock snapshot of all active instruments.
                let (snapshot, manifest_seq) = {
                    let guard = registry.shared();
                    let state = guard.read().await;
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
                // Collect stats under a single short read-lock.
                let (manifest_seq, instrument_count) = {
                    let guard = registry.shared();
                    let state = guard.read().await;
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
