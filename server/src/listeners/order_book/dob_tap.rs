//! Tap that emits `DobEvent`s at the L4 apply step.
//!
//! `DobApplyTap` is attached to an `OrderBookState` instance and called after
//! each successful apply (add_order, modify_sz, cancel_order). It resolves the
//! coin to a numeric instrument_id via a sync closure, advances a per-instrument
//! sequence counter, and sends the appropriate `DobEvent` on a bounded MPSC.
//! On channel-full it logs a warning and drops the event (no back-pressure into
//! the apply path).

use crate::{
    multicast::dob::{DobEvent, DobEventSender},
    order_book::{Coin, Oid, Px, Side, Sz, per_instrument_seq::PerInstrumentSeqCounter, sz_to_fixed},
    protocol::dob::{
        constants::{AGGRESSOR_UNKNOWN, CANCEL_REASON_UNKNOWN, SIDE_ASK, SIDE_BID},
        messages::{BatchBoundary, OrderAdd, OrderCancel, OrderExecute},
    },
    types::inner::InnerL4Order,
};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::error::TrySendError;

/// Shared per-instrument sequence counter, held jointly by the L4 apply tap
/// and the snapshot emitter task. The counter is bumped on every emitted
/// delta event by `DobApplyTap`, and read (without bumping) by the snapshot
/// emitter when populating `SnapshotBegin.last_instrument_seq`.
///
/// A `std::sync::Mutex` is used (not `tokio::sync::Mutex`) because the apply
/// path is synchronous code running on a tokio worker thread; calling
/// `tokio::sync::Mutex::blocking_lock` from there would panic. Lock holds on
/// either side are very brief (a hashmap insert / lookup) and never bracket
/// an `.await`, so a sync mutex is the correct primitive.
pub(crate) type SharedSeqCounter = Arc<Mutex<PerInstrumentSeqCounter>>;

/// Resolver returning the on-wire `(instrument_id, qty_exponent)` for a coin,
/// or `None` if the coin is not in the active registry. The `qty_exponent` is
/// required so emitted quantities can be scaled to the venue's per-instrument
/// fixed-point representation (the publisher's internal `Sz` is at 10^8).
pub(crate) type CoinResolver = Box<dyn Fn(&Coin) -> Option<(u32, i8)> + Send + Sync>;

pub(crate) struct DobApplyTap {
    sender: DobEventSender,
    seq: SharedSeqCounter,
    source_id: u16,
    channel_id: u8,
    coin_resolver: CoinResolver,
}

impl DobApplyTap {
    /// Creates a new tap.
    ///
    /// The `seq` counter is shared with the DoB snapshot emitter task so the
    /// emitter can read the last-emitted per-instrument seq when building
    /// `SnapshotBegin.last_instrument_seq`.
    pub(crate) fn new(
        sender: DobEventSender,
        source_id: u16,
        channel_id: u8,
        seq: SharedSeqCounter,
        coin_resolver: CoinResolver,
    ) -> Self {
        Self { sender, seq, source_id, channel_id, coin_resolver }
    }

    /// Emits an `OrderAdd` event for a newly resting order.
    pub(crate) fn emit_order_add(&mut self, coin: &Coin, inner_order: &InnerL4Order, timestamp_ns: u64) {
        let Some((instrument_id, qty_exponent)) = (self.coin_resolver)(coin) else {
            log::warn!("dob_tap: unknown coin '{}' — skipping OrderAdd", coin.value());
            return;
        };
        let per_instrument_seq = self.seq.lock().expect("seq mutex poisoned").next(instrument_id);
        let side = match inner_order.side {
            Side::Bid => SIDE_BID,
            Side::Ask => SIDE_ASK,
        };
        let event = DobEvent::OrderAdd(OrderAdd {
            instrument_id,
            source_id: self.source_id,
            side,
            order_flags: 0, // Phase 1: no flag derivation
            per_instrument_seq,
            order_id: inner_order.oid,
            enter_timestamp_ns: timestamp_ns,
            price: inner_order.limit_px.value() as i64,
            quantity: sz_to_fixed(inner_order.sz, qty_exponent),
        });
        self.try_send(event, "OrderAdd");
    }

    /// Emits an `OrderCancel` event when an order is removed from the book.
    pub(crate) fn emit_order_cancel(&mut self, coin: &Coin, oid: Oid, timestamp_ns: u64) {
        let Some((instrument_id, _qty_exponent)) = (self.coin_resolver)(coin) else {
            log::warn!("dob_tap: unknown coin '{}' — skipping OrderCancel", coin.value());
            return;
        };
        let per_instrument_seq = self.seq.lock().expect("seq mutex poisoned").next(instrument_id);
        let event = DobEvent::OrderCancel(OrderCancel {
            instrument_id,
            source_id: self.source_id,
            reason: CANCEL_REASON_UNKNOWN, // Phase 1: cannot distinguish cancel vs full-fill
            per_instrument_seq,
            order_id: oid.into_inner(),
            timestamp_ns,
        });
        self.try_send(event, "OrderCancel");
    }

    /// Emits an `OrderExecute` event when an order's size is reduced by a fill.
    pub(crate) fn emit_order_execute(
        &mut self,
        coin: &Coin,
        oid: Oid,
        exec_price: Px,
        exec_quantity: Sz,
        timestamp_ns: u64,
    ) {
        let Some((instrument_id, qty_exponent)) = (self.coin_resolver)(coin) else {
            log::warn!("dob_tap: unknown coin '{}' — skipping OrderExecute", coin.value());
            return;
        };
        let per_instrument_seq = self.seq.lock().expect("seq mutex poisoned").next(instrument_id);
        let event = DobEvent::OrderExecute(OrderExecute {
            instrument_id,
            source_id: self.source_id,
            aggressor_side: AGGRESSOR_UNKNOWN, // Phase 1: fills listener wiring is out of scope
            exec_flags: 0,
            per_instrument_seq,
            order_id: oid.into_inner(),
            trade_id: 0, // Phase 1: no trade_id available from diff alone
            timestamp_ns,
            exec_price: exec_price.value() as i64,
            exec_quantity: sz_to_fixed(exec_quantity, qty_exponent),
        });
        self.try_send(event, "OrderExecute");
    }

    /// Emits a `BatchBoundary` event to mark the open (phase=0) or close (phase=1)
    /// of a multi-event node block. `batch_id` is the block height.
    pub(crate) fn emit_batch_boundary(&mut self, phase: u8, batch_id: u64, timestamp_ns: u64) {
        let _ = timestamp_ns; // BatchBoundary wire format has no timestamp field
        let msg = BatchBoundary { channel_id: self.channel_id, phase, batch_id };
        self.try_send(DobEvent::BatchBoundary(msg), "BatchBoundary");
    }

    fn try_send(&self, event: DobEvent, label: &'static str) {
        match self.sender.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                log::warn!("dob_tap: channel full, dropping {label}");
            }
            Err(TrySendError::Closed(_)) => {
                log::error!("dob_tap: emitter channel closed, dropping {label}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        multicast::dob::{DobEvent, channel},
        order_book::{Coin, Oid, Px, Side, Sz},
    };

    fn btc_coin() -> Coin {
        Coin::new("BTC")
    }

    /// Default resolver: BTC -> (id=0, qty_exponent=-8). The -8 case is the
    /// no-op for `sz_to_fixed`, so existing tests that assert on `Sz::value()`
    /// (e.g. 50_000_000 from a parsed `Sz`) continue to hold.
    fn btc_resolver() -> CoinResolver {
        Box::new(|coin: &Coin| if coin.value() == "BTC" { Some((0, -8)) } else { None })
    }

    /// Resolver with a non-trivial qty_exponent for testing wire scaling.
    fn btc_resolver_with_qty_exponent(qty_exponent: i8) -> CoinResolver {
        Box::new(move |coin: &Coin| if coin.value() == "BTC" { Some((0, qty_exponent)) } else { None })
    }

    fn make_inner_order(oid: u64, side: Side, limit_px: u64, sz: u64) -> InnerL4Order {
        use alloy::primitives::Address;
        InnerL4Order {
            user: Address::new([0; 20]),
            coin: btc_coin(),
            side,
            limit_px: Px::new(limit_px),
            sz: Sz::new(sz),
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

    #[tokio::test]
    async fn emit_order_add_sends_order_add_event() {
        let (tx, mut rx) = channel(4);
        let seq = Arc::new(Mutex::new(PerInstrumentSeqCounter::new()));
        let mut tap = DobApplyTap::new(tx, 1, 0, seq, btc_resolver());
        let order = make_inner_order(42, Side::Bid, 100_000_000_000, 50_000_000);

        tap.emit_order_add(&btc_coin(), &order, 1_700_000_000_000_000_000);

        let event = rx.recv().await.unwrap();
        match event {
            DobEvent::OrderAdd(msg) => {
                assert_eq!(msg.instrument_id, 0);
                assert_eq!(msg.source_id, 1);
                assert_eq!(msg.side, SIDE_BID);
                assert_eq!(msg.order_flags, 0);
                assert_eq!(msg.per_instrument_seq, 1);
                assert_eq!(msg.order_id, 42);
                assert_eq!(msg.enter_timestamp_ns, 1_700_000_000_000_000_000);
                assert_eq!(msg.price, 100_000_000_000);
                assert_eq!(msg.quantity, 50_000_000);
            }
            other => panic!("expected OrderAdd, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_order_cancel_sends_order_cancel_event() {
        let (tx, mut rx) = channel(4);
        let seq = Arc::new(Mutex::new(PerInstrumentSeqCounter::new()));
        let mut tap = DobApplyTap::new(tx, 2, 0, seq, btc_resolver());
        let oid = Oid::new(99);

        tap.emit_order_cancel(&btc_coin(), oid, 1_700_000_000_000_000_001);

        let event = rx.recv().await.unwrap();
        match event {
            DobEvent::OrderCancel(msg) => {
                assert_eq!(msg.instrument_id, 0);
                assert_eq!(msg.source_id, 2);
                assert_eq!(msg.reason, CANCEL_REASON_UNKNOWN);
                assert_eq!(msg.per_instrument_seq, 1);
                assert_eq!(msg.order_id, 99);
                assert_eq!(msg.timestamp_ns, 1_700_000_000_000_000_001);
            }
            other => panic!("expected OrderCancel, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn emit_order_execute_sends_order_execute_event() {
        let (tx, mut rx) = channel(4);
        let seq = Arc::new(Mutex::new(PerInstrumentSeqCounter::new()));
        let mut tap = DobApplyTap::new(tx, 3, 0, seq, btc_resolver());
        let oid = Oid::new(77);
        let exec_price = Px::new(200_000_000_000);
        let exec_quantity = Sz::new(10_000_000);

        tap.emit_order_execute(&btc_coin(), oid, exec_price, exec_quantity, 1_700_000_000_000_000_002);

        let event = rx.recv().await.unwrap();
        match event {
            DobEvent::OrderExecute(msg) => {
                assert_eq!(msg.instrument_id, 0);
                assert_eq!(msg.source_id, 3);
                assert_eq!(msg.aggressor_side, AGGRESSOR_UNKNOWN);
                assert_eq!(msg.exec_flags, 0);
                assert_eq!(msg.per_instrument_seq, 1);
                assert_eq!(msg.order_id, 77);
                assert_eq!(msg.trade_id, 0);
                assert_eq!(msg.timestamp_ns, 1_700_000_000_000_000_002);
                assert_eq!(msg.exec_price, 200_000_000_000);
                assert_eq!(msg.exec_quantity, 10_000_000);
            }
            other => panic!("expected OrderExecute, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn per_instrument_seq_advances_across_events() {
        let (tx, mut rx) = channel(16);
        let seq = Arc::new(Mutex::new(PerInstrumentSeqCounter::new()));
        let mut tap = DobApplyTap::new(tx, 1, 0, seq, btc_resolver());
        let coin = btc_coin();

        // Three different event types for instrument 0 (BTC)
        let order = make_inner_order(1, Side::Ask, 100_000_000_000, 5_000_000);
        tap.emit_order_add(&coin, &order, 1_000);
        tap.emit_order_cancel(&coin, Oid::new(2), 2_000);
        tap.emit_order_execute(&coin, Oid::new(3), Px::new(100_000_000_000), Sz::new(1_000_000), 3_000);

        for expected_seq in 1u32..=3 {
            let event = rx.recv().await.unwrap();
            let seq = match event {
                DobEvent::OrderAdd(m) => m.per_instrument_seq,
                DobEvent::OrderCancel(m) => m.per_instrument_seq,
                DobEvent::OrderExecute(m) => m.per_instrument_seq,
                other => panic!("unexpected event: {other:?}"),
            };
            assert_eq!(seq, expected_seq, "expected seq {expected_seq}, got {seq}");
        }
    }

    #[tokio::test]
    async fn unknown_coin_skips_without_send() {
        let (tx, mut rx) = channel(4);
        let seq = Arc::new(Mutex::new(PerInstrumentSeqCounter::new()));
        let mut tap = DobApplyTap::new(tx, 1, 0, seq, btc_resolver());
        let unknown_coin = Coin::new("ETH");
        let order = make_inner_order(1, Side::Bid, 100, 10);

        tap.emit_order_add(&unknown_coin, &order, 1_000);

        // Channel should be empty — nothing was sent
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn emit_batch_boundary_sends_event() {
        let (tx, mut rx) = channel(4);
        let seq = Arc::new(Mutex::new(PerInstrumentSeqCounter::new()));
        let mut tap = DobApplyTap::new(tx, /* source_id */ 1, /* channel_id */ 7, seq, btc_resolver());
        tap.emit_batch_boundary(0, 999, 1_700_000_000_000_000_000);
        match rx.recv().await.unwrap() {
            DobEvent::BatchBoundary(msg) => {
                assert_eq!(msg.channel_id, 7);
                assert_eq!(msg.phase, 0);
                assert_eq!(msg.batch_id, 999);
            }
            other => panic!("expected BatchBoundary, got {:?}", other),
        }
    }

    /// `Sz::parse_from_str("0.5")` is `0.5 * 1e8 = 50_000_000` internally.
    /// With `qty_exponent = -3`, the wire representation is `0.5 * 10^3 = 500`.
    #[tokio::test]
    async fn emit_order_add_scales_quantity_to_qty_exponent() {
        let (tx, mut rx) = channel(4);
        let seq = Arc::new(Mutex::new(PerInstrumentSeqCounter::new()));
        let mut tap = DobApplyTap::new(tx, 1, 0, seq, btc_resolver_with_qty_exponent(-3));
        let order = make_inner_order(
            42,
            Side::Bid,
            Px::parse_from_str("100").unwrap().value(),
            Sz::parse_from_str("0.5").unwrap().value(),
        );

        tap.emit_order_add(&btc_coin(), &order, 1_000);

        match rx.recv().await.unwrap() {
            DobEvent::OrderAdd(msg) => {
                assert_eq!(
                    msg.quantity, 500,
                    "quantity must be scaled to qty_exponent=-3 (expected 500, got {})",
                    msg.quantity,
                );
            }
            other => panic!("expected OrderAdd, got {other:?}"),
        }
    }

    /// `qty_exponent = 0` (e.g. instrument `2Z` from issue #10) — wire qty must
    /// be the integer count (`Sz::value() / 1e8`), not the raw 10^8 internal.
    #[tokio::test]
    async fn emit_order_add_scales_quantity_at_qty_exponent_zero() {
        let (tx, mut rx) = channel(4);
        let seq = Arc::new(Mutex::new(PerInstrumentSeqCounter::new()));
        let mut tap = DobApplyTap::new(tx, 1, 0, seq, btc_resolver_with_qty_exponent(0));
        let order = make_inner_order(
            42,
            Side::Bid,
            Px::parse_from_str("100").unwrap().value(),
            Sz::parse_from_str("2921").unwrap().value(),
        );

        tap.emit_order_add(&btc_coin(), &order, 1_000);

        match rx.recv().await.unwrap() {
            DobEvent::OrderAdd(msg) => {
                assert_eq!(
                    msg.quantity, 2921,
                    "quantity must be scaled to qty_exponent=0 (expected 2921, got {})",
                    msg.quantity,
                );
            }
            other => panic!("expected OrderAdd, got {other:?}"),
        }
    }

    /// `OrderExecute.exec_quantity` must follow the same scaling rule.
    #[tokio::test]
    async fn emit_order_execute_scales_exec_quantity_to_qty_exponent() {
        let (tx, mut rx) = channel(4);
        let seq = Arc::new(Mutex::new(PerInstrumentSeqCounter::new()));
        let mut tap = DobApplyTap::new(tx, 1, 0, seq, btc_resolver_with_qty_exponent(-3));
        let exec_quantity = Sz::parse_from_str("1.234").unwrap();

        tap.emit_order_execute(&btc_coin(), Oid::new(77), Px::parse_from_str("100").unwrap(), exec_quantity, 2_000);

        match rx.recv().await.unwrap() {
            DobEvent::OrderExecute(msg) => {
                assert_eq!(
                    msg.exec_quantity, 1234,
                    "exec_quantity must be scaled to qty_exponent=-3 (expected 1234, got {})",
                    msg.exec_quantity,
                );
            }
            other => panic!("expected OrderExecute, got {other:?}"),
        }
    }
}
