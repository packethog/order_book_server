#![cfg(test)]
//! TOB-vs-DoB best bid/ask parity test (synthetic in-process driver, v0).
//!
//! Drives a real `OrderBookListener` through a hand-coded sequence of L4
//! events that exercise add / partial-execute / full-execute / cancel.
//! Both emission paths are observed:
//!
//!   - DoB deltas: a `DobApplyTap` is attached to the listener, feeding a
//!     bounded mpsc into `run_dob_emitter`, which writes UDP frames to a
//!     loopback `UdpSocket` collector. The collected frames are decoded by
//!     `ReferenceSubscriber` (Phase 2 Task 12) into a per-instrument L2
//!     book.
//!
//!   - TOB Quote: derived in-process from `listener.l2_snapshots_for_test()`,
//!     using the same `(level.px(), level.sz(), level.n())` extraction that
//!     `MulticastPublisher::publish_quotes` performs. The price/qty are
//!     converted with `price_to_fixed` / `qty_to_fixed` so the on-the-wire
//!     fixed-point matches what production would emit. We do NOT spin up
//!     the actual websocket-server publisher loop — the L2 derivation is
//!     the load-bearing piece; the publisher just byte-packs it.
//!
//! ## Scope (v0)
//!
//! - End-state parity only: after the full event sequence, the TOB Quote's
//!   `(bid_price, ask_price)` must equal the subscriber's `(best_bid().px,
//!   best_ask().px)` for the test instrument.
//! - No snapshot stream wired: the subscriber rebuilds from deltas only.
//!   This is sound because the listener starts from a known snapshot (the
//!   subscriber starts empty) and the only deltas in flight are the ones
//!   the test feeds. We skip the snapshot port complexity entirely.
//! - No mid-stream Quote-vs-DoB comparison; if the end state diverges, the
//!   assertion will catch it. Per-step Quote comparison is deferred to v1
//!   when captured market-data fixtures land.
//!
//! ## Why a synthetic driver
//!
//! Phase 1 never landed a fixture-replay harness, so we drive the listener
//! with hand-constructed `Batch<NodeDataOrderStatus>` /
//! `Batch<NodeDataOrderDiff>` pairs. The parity property is structural
//! ("TOB and DoB derive from the same apply step"), so synthetic events
//! that walk through realistic state transitions are sufficient.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use alloy::primitives::Address;
use chrono::NaiveDateTime;
use tokio::net::UdpSocket;

use crate::instruments::{
    InstrumentInfo, RegistryState, UniverseEntry, make_symbol, price_to_fixed, qty_to_fixed,
};
use crate::listeners::order_book::{L2SnapshotParams, OrderBookListener};
use crate::listeners::order_book::dob_tap::{DobApplyTap, SharedSeqCounter};
use crate::multicast::dob::{
    DobEmitter, DobMktdataConfig, SharedMktdataSeq, channel, run_dob_emitter,
};
use crate::order_book::{Coin, OrderBook, PerInstrumentSeqCounter, Px, Side, Snapshot, Sz};
use crate::order_book::multi_book::Snapshots;
use crate::protocol::dob::constants::DEFAULT_MTU;
use crate::test_subscriber::ReferenceSubscriber;
use crate::types::inner::InnerL4Order;
use crate::types::node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus};
use crate::types::{L4Order, OrderDiff};

/// Use exponent -8 so that fixed-point scales of TOB and DoB align with the
/// internal `Px`/`Sz` representation (which is `value * 1e8`). With -8,
/// `price_to_fixed("100", -8) == 10_000_000_000 == Px("100").value()`, so a
/// price of 100 in human form survives both encoding paths as the same
/// `i64` on the wire.
const PRICE_EXPONENT: i8 = -8;
const QTY_EXPONENT: i8 = -8;
const TEST_INSTRUMENT_ID: u32 = 0;
const TEST_COIN: &str = "BTC";

/// Wraps `block_time_ms` (UTC ms) into a `NaiveDateTime` — the type both
/// `Batch.block_time` and `NodeDataOrderStatus.time` use for serde.
fn dt_from_ms(block_time_ms: u64) -> NaiveDateTime {
    let secs = (block_time_ms / 1_000) as i64;
    let nsecs = ((block_time_ms % 1_000) * 1_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs)
        .expect("valid timestamp")
        .naive_utc()
}

/// Build an `InnerL4Order` via the `parse_from_str` path so its `Px`/`Sz`
/// scale matches what `apply_updates` produces from synthetic JSON. Used to
/// pre-seed the listener's initial snapshot.
fn make_inner_order_parsed(coin: &Coin, oid: u64, side: Side, px: &str, sz: &str) -> InnerL4Order {
    InnerL4Order {
        user: Address::new([0; 20]),
        coin: coin.clone(),
        side,
        limit_px: Px::parse_from_str(px).expect("valid px"),
        sz: Sz::parse_from_str(sz).expect("valid sz"),
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

/// Pre-seed the snapshot with one bid and one ask so `for_test_with_snapshot`
/// produces a non-empty universe (the listener's snapshot path expects at
/// least one resting order to populate). We immediately cancel both in the
/// first batch so the assertion stays clean.
fn seeded_initial_snapshot() -> Snapshots<InnerL4Order> {
    let coin = Coin::new(TEST_COIN);
    let mut book: OrderBook<InnerL4Order> = OrderBook::new();
    book.add_order(make_inner_order_parsed(&coin, 9_000, Side::Bid, "1", "1"));
    book.add_order(make_inner_order_parsed(&coin, 9_001, Side::Ask, "10000", "1"));
    let mut map: HashMap<Coin, Snapshot<InnerL4Order>> = HashMap::new();
    map.insert(coin, book.to_snapshot());
    Snapshots::new(map)
}

/// Build a `(NodeDataOrderStatus, NodeDataOrderDiff)` pair representing
/// "open + book diff: New" for an order that just rests on the book.
fn add_event(
    block_time_ms: u64,
    side: Side,
    oid: u64,
    px: &str,
    sz: &str,
) -> (NodeDataOrderStatus, NodeDataOrderDiff) {
    let user = Address::new([0; 20]);
    let l4 = L4Order {
        user: Some(user),
        coin: TEST_COIN.to_string(),
        side,
        limit_px: px.to_string(),
        sz: sz.to_string(),
        oid,
        timestamp: block_time_ms,
        trigger_condition: String::new(),
        is_trigger: false,
        trigger_px: String::new(),
        is_position_tpsl: false,
        reduce_only: false,
        order_type: String::new(),
        tif: None,
        cloid: None,
    };
    let status = NodeDataOrderStatus {
        time: dt_from_ms(block_time_ms),
        user,
        status: "open".to_string(),
        order: l4,
    };
    let diff = NodeDataOrderDiff::new_for_test(
        user,
        oid,
        px.to_string(),
        TEST_COIN.to_string(),
        OrderDiff::New { sz: sz.to_string() },
    );
    (status, diff)
}

/// Diff-only event: an order's resting size shrinks from `orig_sz` to
/// `new_sz` (partial or full execute). The state.rs apply path only needs
/// the diff for an Update — no order_status is required.
fn execute_diff(oid: u64, px: &str, orig_sz: &str, new_sz: &str) -> NodeDataOrderDiff {
    NodeDataOrderDiff::new_for_test(
        Address::new([0; 20]),
        oid,
        px.to_string(),
        TEST_COIN.to_string(),
        OrderDiff::Update { orig_sz: orig_sz.to_string(), new_sz: new_sz.to_string() },
    )
}

/// Diff-only event: an order is cancelled.
fn cancel_diff(oid: u64, px: &str) -> NodeDataOrderDiff {
    NodeDataOrderDiff::new_for_test(
        Address::new([0; 20]),
        oid,
        px.to_string(),
        TEST_COIN.to_string(),
        OrderDiff::Remove,
    )
}

/// Build a registry containing a single instrument matching `TEST_COIN` /
/// `TEST_INSTRUMENT_ID`, with the price/qty exponents the parity test
/// requires.
fn build_test_registry() -> RegistryState {
    RegistryState::new(vec![UniverseEntry {
        instrument_id: TEST_INSTRUMENT_ID,
        coin: TEST_COIN.to_string(),
        is_delisted: false,
        info: InstrumentInfo {
            instrument_id: TEST_INSTRUMENT_ID,
            price_exponent: PRICE_EXPONENT,
            qty_exponent: QTY_EXPONENT,
            symbol: make_symbol(TEST_COIN),
        },
    }])
}

/// Mimics the inner of `MulticastPublisher::publish_quotes` for one
/// instrument: takes the listener's `L2Snapshots`, looks up the default
/// `L2SnapshotParams`, truncates to depth=1, and produces fixed-point
/// `(bid_price, ask_price)` exactly as the publisher would emit them.
fn derive_tob_quote_prices(
    snapshot_map: &HashMap<
        crate::order_book::Coin,
        HashMap<L2SnapshotParams, crate::order_book::Snapshot<crate::types::inner::InnerLevel>>,
    >,
    inst: &InstrumentInfo,
) -> Option<(i64, i64)> {
    let coin = Coin::new(TEST_COIN);
    let params_map = snapshot_map.get(&coin)?;
    let default_params = L2SnapshotParams::new(None, None);
    let snapshot = params_map.get(&default_params)?;
    let levels = snapshot.truncate(1).export_inner_snapshot();
    let bids = &levels[0];
    let asks = &levels[1];
    let bid_price = bids
        .first()
        .and_then(|level| price_to_fixed(level.px(), inst.price_exponent))
        .unwrap_or(0);
    let ask_price = asks
        .first()
        .and_then(|level| price_to_fixed(level.px(), inst.price_exponent))
        .unwrap_or(0);
    let _bid_qty = bids
        .first()
        .and_then(|level| qty_to_fixed(level.sz(), inst.qty_exponent));
    let _ask_qty = asks
        .first()
        .and_then(|level| qty_to_fixed(level.sz(), inst.qty_exponent));
    Some((bid_price, ask_price))
}

/// Drains the UDP collector for `timeout`. Each datagram is one DoB frame.
async fn drain_collector(sock: &UdpSocket, timeout: Duration) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    loop {
        let mut buf = [0u8; 4096];
        match tokio::time::timeout(timeout, sock.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => frames.push(buf[..n].to_vec()),
            // Timeout or recv error — assume the burst is complete.
            _ => break,
        }
    }
    frames
}

#[tokio::test]
async fn tob_dob_best_bid_ask_parity_at_end_state() {
    // 1. Bind a UDP collector that the DoB emitter will multicast-loop into.
    let dob_collector = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let dob_addr = match dob_collector.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };

    // 2. Construct shared state (mktdata seq + per-instrument seq).
    let mktdata_seq: SharedMktdataSeq = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let seq_counter: SharedSeqCounter = Arc::new(StdMutex::new(PerInstrumentSeqCounter::new()));

    // 3. Build the listener, pre-seed with a non-empty snapshot, and attach
    //    a DoB tap. The snapshot has two seed orders we'll cancel in the
    //    first event batch so they don't pollute end-state assertions.
    let mut listener = OrderBookListener::for_test_with_snapshot(seeded_initial_snapshot(), 1);

    // 4. Wire the DoB emitter pipeline: tap -> mpsc -> run_dob_emitter -> UDP.
    let (event_tx, event_rx) = channel(256);
    let mkt_emitter = DobEmitter::bind_with_seq(
        DobMktdataConfig {
            group_addr: *dob_addr.ip(),
            port: dob_addr.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: 0,
            mtu: DEFAULT_MTU,
            // Long heartbeat — we don't want stray heartbeats interleaving
            // with our deltas. Heartbeats only fire on explicit
            // DobEvent::HeartbeatTick, so the timer here only flushes; the
            // collector will not see heartbeat bytes.
            heartbeat_interval: Duration::from_secs(60),
        },
        mktdata_seq.clone(),
    )
    .await
    .unwrap();
    let emitter_handle = tokio::spawn(run_dob_emitter(mkt_emitter, event_rx));

    let tap = DobApplyTap::new(
        event_tx.clone(),
        /* source_id = */ 1,
        /* channel_id = */ 0,
        seq_counter.clone(),
        Box::new(|c: &Coin| {
            if c.value() == TEST_COIN { Some(TEST_INSTRUMENT_ID) } else { None }
        }),
    );
    listener.set_dob_tap(tap);

    // 5. Drive the listener through ~10 hand-coded events covering:
    //      - cancel of seed bid (prep)
    //      - cancel of seed ask (prep)
    //      - 4 adds (2 bids, 2 asks)
    //      - partial execute of best bid
    //      - full execute of best ask
    //      - cancel of a non-top resting order
    //      - add a new best ask
    //      - one more cancel for variety
    //
    // Each call increments block height by 1 (state.apply_updates expects
    // height = self.height + 1). Initial height was 1, so we start at 2.
    //
    // Layout reminder: an "add" event needs both a status (status="open")
    // and a diff (OrderDiff::New). Updates and Removes need the diff alone.

    // Block 2: cancel seed orders (oid 9000 bid, 9001 ask).
    let stmts: Vec<NodeDataOrderStatus> = vec![];
    let diffs = vec![cancel_diff(9_000, "1"), cancel_diff(9_001, "10000")];
    listener
        .apply_test_batch(
            Batch::new_for_test(2, 1_700_000_002_000, stmts),
            Batch::new_for_test(2, 1_700_000_002_000, diffs),
        )
        .expect("seed-cancel batch applies");

    // Block 3: add four resting orders.
    //   bid 100 oid=101 sz=5
    //   bid  99 oid=102 sz=4   (worse bid)
    //   ask 110 oid=201 sz=3
    //   ask 111 oid=202 sz=2   (worse ask)
    let mut stmts = Vec::new();
    let mut diffs = Vec::new();
    for (side, oid, px, sz) in [
        (Side::Bid, 101_u64, "100", "5"),
        (Side::Bid, 102_u64, "99", "4"),
        (Side::Ask, 201_u64, "110", "3"),
        (Side::Ask, 202_u64, "111", "2"),
    ] {
        let (s, d) = add_event(1_700_000_003_000, side, oid, px, sz);
        stmts.push(s);
        diffs.push(d);
    }
    listener
        .apply_test_batch(
            Batch::new_for_test(3, 1_700_000_003_000, stmts),
            Batch::new_for_test(3, 1_700_000_003_000, diffs),
        )
        .expect("four-add batch applies");

    // Block 4: partial execute of the best bid (oid=101 5 -> 2). Best bid
    // qty shrinks but price stays at 100. (Note: a single-event batch must
    // stay a single event — state.apply_updates only opens a BatchBoundary
    // for >= 2 events.)
    listener
        .apply_test_batch(
            Batch::new_for_test(4, 1_700_000_004_000, vec![]),
            Batch::new_for_test(
                4,
                1_700_000_004_000,
                vec![execute_diff(101, "100", "5", "2")],
            ),
        )
        .expect("partial-execute batch applies");

    // Block 5: full execute of the best ask (oid=201 3 -> 0). Best ask
    // becomes 111 (oid=202).
    listener
        .apply_test_batch(
            Batch::new_for_test(5, 1_700_000_005_000, vec![]),
            Batch::new_for_test(
                5,
                1_700_000_005_000,
                vec![execute_diff(201, "110", "3", "0")],
            ),
        )
        .expect("full-execute batch applies");

    // Block 6: cancel the worse-bid (oid=102 at 99). Best bid stays at 100.
    listener
        .apply_test_batch(
            Batch::new_for_test(6, 1_700_000_006_000, vec![]),
            Batch::new_for_test(6, 1_700_000_006_000, vec![cancel_diff(102, "99")]),
        )
        .expect("cancel-non-top batch applies");

    // Block 7: add a NEW best ask (oid=203 at 109). Now best ask = 109.
    let (s, d) = add_event(1_700_000_007_000, Side::Ask, 203, "109", "7");
    listener
        .apply_test_batch(
            Batch::new_for_test(7, 1_700_000_007_000, vec![s]),
            Batch::new_for_test(7, 1_700_000_007_000, vec![d]),
        )
        .expect("new-best-ask batch applies");

    // Block 8: cancel oid=202 (the previously-worst ask at 111). Best ask
    // stays at 109. End state: best bid 100 (qty 2), best ask 109 (qty 7).
    listener
        .apply_test_batch(
            Batch::new_for_test(8, 1_700_000_008_000, vec![]),
            Batch::new_for_test(8, 1_700_000_008_000, vec![cancel_diff(202, "111")]),
        )
        .expect("final-cancel batch applies");

    // 6. Derive what the TOB publisher would emit. We pull the L2 snapshot
    //    directly off the listener — same call publish_quotes makes — and
    //    convert via price_to_fixed/qty_to_fixed exactly as the publisher
    //    does. The actual websocket-server publisher loop is intentionally
    //    NOT spun up: the conversion is the load-bearing step, and decoupling
    //    keeps the test fast and deterministic.
    let registry = build_test_registry();
    let inst = registry.active.get(TEST_COIN).expect("instrument registered").clone();
    let (_time, l2_snapshots) = listener.l2_snapshots_for_test().expect("snapshot ready");
    let (tob_bid_price, tob_ask_price) =
        derive_tob_quote_prices(l2_snapshots.as_ref(), &inst).expect("instrument in snapshot");

    // 7. Tear down the emitter so its loop drains and frames hit the wire,
    //    then drain the collector. The Shutdown event triggers a final
    //    flush + EndOfSession (the `Senders dropped` path of run_dob_emitter
    //    deliberately does NOT flush — see its "All senders dropped — clean
    //    exit without EndOfSession" branch — so we must send Shutdown here
    //    or the in-flight frame would be lost.).
    use crate::multicast::dob::DobEvent;
    event_tx.send(DobEvent::Shutdown).await.unwrap();
    drop(event_tx);
    let _unused = tokio::time::timeout(Duration::from_millis(500), emitter_handle).await;

    let frames = drain_collector(&dob_collector, Duration::from_millis(500)).await;
    assert!(
        !frames.is_empty(),
        "DoB collector got no frames — emitter pipeline did not fire",
    );

    // 8. Decode every frame into the reference subscriber and read its
    //    current best bid/ask.
    let mut subscriber = ReferenceSubscriber::new();
    for frame in &frames {
        subscriber.apply_mktdata_frame(frame);
    }
    let book = subscriber
        .books
        .get(&TEST_INSTRUMENT_ID)
        .expect("subscriber rebuilt a book for the test instrument");
    let dob_best_bid = book.best_bid().expect("subscriber has a best bid at end-state");
    let dob_best_ask = book.best_ask().expect("subscriber has a best ask at end-state");

    // 9. Compare. TOB and DoB MUST agree on best bid/ask price.
    assert_eq!(
        tob_bid_price, dob_best_bid.0,
        "TOB best bid ({}) != DoB best bid ({}) — apply-step parity violation",
        tob_bid_price, dob_best_bid.0,
    );
    assert_eq!(
        tob_ask_price, dob_best_ask.0,
        "TOB best ask ({}) != DoB best ask ({}) — apply-step parity violation",
        tob_ask_price, dob_best_ask.0,
    );

    // Sanity: the test sequence ends with bid=100, ask=109. With
    // PRICE_EXPONENT=-8 these become 1e10 and 1.09e10 respectively.
    assert_eq!(tob_bid_price, 100 * 100_000_000);
    assert_eq!(tob_ask_price, 109 * 100_000_000);
}
