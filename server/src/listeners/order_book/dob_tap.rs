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
    order_book::{
        Coin, Oid, Px, Side, Sz,
        per_instrument_seq::PerInstrumentSeqCounter,
    },
    protocol::dob::{
        constants::{AGGRESSOR_UNKNOWN, CANCEL_REASON_UNKNOWN, SIDE_ASK, SIDE_BID},
        messages::{OrderAdd, OrderCancel, OrderExecute},
    },
    types::inner::InnerL4Order,
};
use tokio::sync::mpsc::error::TrySendError;

pub(crate) struct DobApplyTap {
    sender: DobEventSender,
    seq: PerInstrumentSeqCounter,
    source_id: u16,
    coin_resolver: Box<dyn Fn(&Coin) -> Option<u32> + Send + Sync>,
}

impl DobApplyTap {
    /// Creates a new tap.
    pub(crate) fn new(
        sender: DobEventSender,
        source_id: u16,
        coin_resolver: Box<dyn Fn(&Coin) -> Option<u32> + Send + Sync>,
    ) -> Self {
        Self { sender, seq: PerInstrumentSeqCounter::new(), source_id, coin_resolver }
    }

    /// Emits an `OrderAdd` event for a newly resting order.
    pub(crate) fn emit_order_add(&mut self, coin: &Coin, inner_order: &InnerL4Order, timestamp_ns: u64) {
        let Some(instrument_id) = (self.coin_resolver)(coin) else {
            log::warn!("dob_tap: unknown coin '{}' — skipping OrderAdd", coin.value());
            return;
        };
        let per_instrument_seq = self.seq.next(instrument_id);
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
            quantity: inner_order.sz.value(),
        });
        self.try_send(event, "OrderAdd");
    }

    /// Emits an `OrderCancel` event when an order is removed from the book.
    pub(crate) fn emit_order_cancel(&mut self, coin: &Coin, oid: Oid, timestamp_ns: u64) {
        let Some(instrument_id) = (self.coin_resolver)(coin) else {
            log::warn!("dob_tap: unknown coin '{}' — skipping OrderCancel", coin.value());
            return;
        };
        let per_instrument_seq = self.seq.next(instrument_id);
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
        let Some(instrument_id) = (self.coin_resolver)(coin) else {
            log::warn!("dob_tap: unknown coin '{}' — skipping OrderExecute", coin.value());
            return;
        };
        let per_instrument_seq = self.seq.next(instrument_id);
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
            exec_quantity: exec_quantity.value(),
        });
        self.try_send(event, "OrderExecute");
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

    fn btc_resolver() -> Box<dyn Fn(&Coin) -> Option<u32> + Send + Sync> {
        Box::new(|coin: &Coin| if coin.value() == "BTC" { Some(0) } else { None })
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
        let mut tap = DobApplyTap::new(tx, 1, btc_resolver());
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
        let mut tap = DobApplyTap::new(tx, 2, btc_resolver());
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
        let mut tap = DobApplyTap::new(tx, 3, btc_resolver());
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
        let mut tap = DobApplyTap::new(tx, 1, btc_resolver());
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
        let mut tap = DobApplyTap::new(tx, 1, btc_resolver());
        let unknown_coin = Coin::new("ETH");
        let order = make_inner_order(1, Side::Bid, 100, 10);

        tap.emit_order_add(&unknown_coin, &order, 1_000);

        // Channel should be empty — nothing was sent
        assert!(rx.try_recv().is_err());
    }
}
