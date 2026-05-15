#![cfg(test)]
//! Minimal DoB reference subscriber for the parity test.
//!
//! Decodes DoB frames (mktdata + snapshot ports), tracks active snapshot
//! context per instrument, and rebuilds per-instrument L2 (best bid/ask).
//! Happy-path only — no gap-recovery polish, no out-of-order tolerance,
//! no multi-channel support.
//!
//! Used solely by `tests/dob_tob_parity.rs`. NOT a deliverable product.

use std::collections::BTreeMap;

use crate::protocol::dob::constants::{
    FRAME_HEADER_SIZE, MSG_TYPE_BATCH_BOUNDARY, MSG_TYPE_END_OF_SESSION, MSG_TYPE_HEARTBEAT, MSG_TYPE_INSTRUMENT_DEF,
    MSG_TYPE_INSTRUMENT_RESET, MSG_TYPE_MANIFEST_SUMMARY, MSG_TYPE_ORDER_ADD, MSG_TYPE_ORDER_CANCEL,
    MSG_TYPE_ORDER_EXECUTE, MSG_TYPE_SNAPSHOT_BEGIN, MSG_TYPE_SNAPSHOT_END, MSG_TYPE_SNAPSHOT_ORDER, MSG_TYPE_TRADE,
    SIDE_BID,
};

#[derive(Debug, Default, Clone)]
pub(crate) struct InstrumentL2 {
    /// price -> total resting quantity at that level
    pub(crate) bids: BTreeMap<i64, u64>,
    pub(crate) asks: BTreeMap<i64, u64>,
}

impl InstrumentL2 {
    pub(crate) fn best_bid(&self) -> Option<(i64, u64)> {
        self.bids.iter().next_back().map(|(&p, &q)| (p, q))
    }
    pub(crate) fn best_ask(&self) -> Option<(i64, u64)> {
        self.asks.iter().next().map(|(&p, &q)| (p, q))
    }
}

#[derive(Default)]
pub(crate) struct ReferenceSubscriber {
    /// Per-instrument L2 books, keyed by instrument_id.
    pub(crate) books: BTreeMap<u32, InstrumentL2>,
    /// order_id -> (instrument_id, side, price, qty). Tracks resting orders so
    /// OrderCancel and OrderExecute can update the right level.
    orders: BTreeMap<u64, (u32, u8, i64, u64)>,
    /// Active snapshot group context: instrument_id -> snapshot_id.
    /// SnapshotOrder messages reference this to know which instrument to apply to.
    active_snapshot: BTreeMap<u32, u32>,
}

impl ReferenceSubscriber {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Apply all application messages from one DoB mktdata frame.
    pub(crate) fn apply_mktdata_frame(&mut self, frame: &[u8]) {
        if frame.len() < FRAME_HEADER_SIZE {
            return;
        }
        let mut offset = FRAME_HEADER_SIZE;
        while offset + 4 <= frame.len() {
            let msg_type = frame[offset];
            let msg_len = frame[offset + 1] as usize;
            if msg_len == 0 || offset + msg_len > frame.len() {
                break;
            }
            let body = &frame[offset..offset + msg_len];
            match msg_type {
                MSG_TYPE_ORDER_ADD => self.apply_order_add(body),
                MSG_TYPE_ORDER_CANCEL => self.apply_order_cancel(body),
                MSG_TYPE_ORDER_EXECUTE => self.apply_order_execute(body),
                MSG_TYPE_INSTRUMENT_RESET => self.apply_instrument_reset(body),
                // Ignored on mktdata for parity purposes:
                MSG_TYPE_BATCH_BOUNDARY
                | MSG_TYPE_HEARTBEAT
                | MSG_TYPE_INSTRUMENT_DEF
                | MSG_TYPE_TRADE
                | MSG_TYPE_END_OF_SESSION
                | MSG_TYPE_MANIFEST_SUMMARY => {}
                _ => {}
            }
            offset += msg_len;
        }
    }

    /// Apply all application messages from one DoB snapshot frame.
    pub(crate) fn apply_snapshot_frame(&mut self, frame: &[u8]) {
        if frame.len() < FRAME_HEADER_SIZE {
            return;
        }
        let mut offset = FRAME_HEADER_SIZE;
        while offset + 4 <= frame.len() {
            let msg_type = frame[offset];
            let msg_len = frame[offset + 1] as usize;
            if msg_len == 0 || offset + msg_len > frame.len() {
                break;
            }
            let body = &frame[offset..offset + msg_len];
            match msg_type {
                MSG_TYPE_SNAPSHOT_BEGIN => self.apply_snapshot_begin(body),
                MSG_TYPE_SNAPSHOT_ORDER => self.apply_snapshot_order(body),
                MSG_TYPE_SNAPSHOT_END => self.apply_snapshot_end(body),
                _ => {}
            }
            offset += msg_len;
        }
    }

    fn apply_order_add(&mut self, body: &[u8]) {
        let instrument_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
        let side = body[10];
        let order_id = u64::from_le_bytes(body[16..24].try_into().unwrap());
        let price = i64::from_le_bytes(body[32..40].try_into().unwrap());
        let qty = u64::from_le_bytes(body[40..48].try_into().unwrap());
        self.add_resting_order(instrument_id, order_id, side, price, qty);
    }

    fn apply_order_cancel(&mut self, body: &[u8]) {
        let order_id = u64::from_le_bytes(body[16..24].try_into().unwrap());
        if let Some((instrument_id, side, price, qty)) = self.orders.remove(&order_id) {
            self.remove_resting_qty(instrument_id, side, price, qty);
        }
    }

    fn apply_order_execute(&mut self, body: &[u8]) {
        let order_id = u64::from_le_bytes(body[16..24].try_into().unwrap());
        let exec_qty = u64::from_le_bytes(body[48..56].try_into().unwrap());
        // Read the order's current state, update the level, possibly remove the order.
        let order_state = self.orders.get(&order_id).copied();
        let Some((instrument_id, side, price, qty)) = order_state else {
            return;
        };
        let new_qty = qty.saturating_sub(exec_qty);
        self.remove_resting_qty(instrument_id, side, price, exec_qty.min(qty));
        if new_qty == 0 {
            self.orders.remove(&order_id);
        } else {
            self.orders.insert(order_id, (instrument_id, side, price, new_qty));
        }
    }

    fn apply_instrument_reset(&mut self, body: &[u8]) {
        let instrument_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
        // Discard everything for this instrument. Snapshot will repopulate.
        self.books.remove(&instrument_id);
        self.orders.retain(|_oid, (instr, _, _, _)| *instr != instrument_id);
        self.active_snapshot.remove(&instrument_id);
    }

    fn apply_snapshot_begin(&mut self, body: &[u8]) {
        let instrument_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
        let snapshot_id = u32::from_le_bytes(body[20..24].try_into().unwrap());
        // Start a fresh book for this instrument.
        self.books.insert(instrument_id, InstrumentL2::default());
        self.orders.retain(|_oid, (instr, _, _, _)| *instr != instrument_id);
        self.active_snapshot.insert(instrument_id, snapshot_id);
    }

    fn apply_snapshot_order(&mut self, body: &[u8]) {
        let snapshot_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
        // Find the instrument whose active snapshot matches this snapshot_id.
        let instrument_id = self.active_snapshot.iter().find(|&(_, &sid)| sid == snapshot_id).map(|(&iid, _)| iid);
        let Some(instrument_id) = instrument_id else {
            return;
        };
        let order_id = u64::from_le_bytes(body[8..16].try_into().unwrap());
        let side = body[16];
        let price = i64::from_le_bytes(body[28..36].try_into().unwrap());
        let qty = u64::from_le_bytes(body[36..44].try_into().unwrap());
        self.add_resting_order(instrument_id, order_id, side, price, qty);
    }

    fn apply_snapshot_end(&mut self, body: &[u8]) {
        let instrument_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
        let snapshot_id = u32::from_le_bytes(body[16..20].try_into().unwrap());
        // Verify the End matches the active Begin's snapshot_id; if so, clear.
        if self.active_snapshot.get(&instrument_id) == Some(&snapshot_id) {
            self.active_snapshot.remove(&instrument_id);
        }
    }

    fn add_resting_order(&mut self, instrument_id: u32, order_id: u64, side: u8, price: i64, qty: u64) {
        if qty == 0 {
            return;
        }
        self.orders.insert(order_id, (instrument_id, side, price, qty));
        let book = self.books.entry(instrument_id).or_default();
        let level = if side == SIDE_BID { &mut book.bids } else { &mut book.asks };
        *level.entry(price).or_insert(0) += qty;
    }

    fn remove_resting_qty(&mut self, instrument_id: u32, side: u8, price: i64, qty: u64) {
        let Some(book) = self.books.get_mut(&instrument_id) else {
            return;
        };
        let level = if side == SIDE_BID { &mut book.bids } else { &mut book.asks };
        if let Some(total) = level.get_mut(&price) {
            *total = total.saturating_sub(qty);
            if *total == 0 {
                level.remove(&price);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::dob::constants::{
        DEFAULT_MTU, ORDER_ADD_SIZE, ORDER_CANCEL_SIZE, ORDER_EXECUTE_SIZE, SIDE_ASK, SIDE_BID, SNAPSHOT_BEGIN_SIZE,
        SNAPSHOT_END_SIZE, SNAPSHOT_ORDER_SIZE,
    };
    use crate::protocol::dob::frame::DobFrameBuilder;
    use crate::protocol::dob::messages::{
        OrderAdd, OrderCancel, OrderExecute, SnapshotBegin, SnapshotEnd, SnapshotOrder, encode_order_add,
        encode_order_cancel, encode_order_execute, encode_snapshot_begin, encode_snapshot_end, encode_snapshot_order,
    };

    /// Build a single-message mktdata frame containing the given OrderAdd.
    fn frame_order_add(msg: &OrderAdd) -> Vec<u8> {
        let mut fb = DobFrameBuilder::new(0, 1, 0, 0, DEFAULT_MTU);
        let buf = fb.message_buffer(ORDER_ADD_SIZE).unwrap();
        encode_order_add(buf, msg);
        fb.commit_message();
        fb.finalize().to_vec()
    }

    fn frame_order_cancel(msg: &OrderCancel) -> Vec<u8> {
        let mut fb = DobFrameBuilder::new(0, 2, 0, 0, DEFAULT_MTU);
        let buf = fb.message_buffer(ORDER_CANCEL_SIZE).unwrap();
        encode_order_cancel(buf, msg);
        fb.commit_message();
        fb.finalize().to_vec()
    }

    fn frame_order_execute(msg: &OrderExecute) -> Vec<u8> {
        let mut fb = DobFrameBuilder::new(0, 3, 0, 0, DEFAULT_MTU);
        let buf = fb.message_buffer(ORDER_EXECUTE_SIZE).unwrap();
        encode_order_execute(buf, msg);
        fb.commit_message();
        fb.finalize().to_vec()
    }

    /// Build a single snapshot frame containing Begin + N Orders + End.
    fn frame_snapshot(begin: &SnapshotBegin, orders: &[SnapshotOrder], end: &SnapshotEnd) -> Vec<u8> {
        let mut fb = DobFrameBuilder::new(1, 1, 0, 0, DEFAULT_MTU);
        {
            let buf = fb.message_buffer(SNAPSHOT_BEGIN_SIZE).unwrap();
            encode_snapshot_begin(buf, begin);
            fb.commit_message();
        }
        for so in orders {
            let buf = fb.message_buffer(SNAPSHOT_ORDER_SIZE).unwrap();
            encode_snapshot_order(buf, so);
            fb.commit_message();
        }
        {
            let buf = fb.message_buffer(SNAPSHOT_END_SIZE).unwrap();
            encode_snapshot_end(buf, end);
            fb.commit_message();
        }
        fb.finalize().to_vec()
    }

    #[test]
    fn order_add_populates_book() {
        let mut sub = ReferenceSubscriber::new();
        let frame = frame_order_add(&OrderAdd {
            instrument_id: 42,
            source_id: 1,
            side: SIDE_BID,
            order_flags: 0,
            per_instrument_seq: 1,
            order_id: 100,
            enter_timestamp_ns: 0,
            price: 1_000,
            quantity: 5,
        });
        sub.apply_mktdata_frame(&frame);
        let book = sub.books.get(&42).expect("book exists");
        assert_eq!(book.best_bid(), Some((1_000, 5)));
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn order_cancel_removes_resting_order() {
        let mut sub = ReferenceSubscriber::new();
        sub.apply_mktdata_frame(&frame_order_add(&OrderAdd {
            instrument_id: 42,
            source_id: 1,
            side: SIDE_ASK,
            order_flags: 0,
            per_instrument_seq: 1,
            order_id: 200,
            enter_timestamp_ns: 0,
            price: 2_000,
            quantity: 7,
        }));
        sub.apply_mktdata_frame(&frame_order_cancel(&OrderCancel {
            instrument_id: 42,
            source_id: 1,
            reason: 1,
            per_instrument_seq: 2,
            order_id: 200,
            timestamp_ns: 0,
        }));
        let book = sub.books.get(&42).expect("book exists");
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn order_execute_partial_then_full() {
        let mut sub = ReferenceSubscriber::new();
        sub.apply_mktdata_frame(&frame_order_add(&OrderAdd {
            instrument_id: 42,
            source_id: 1,
            side: SIDE_BID,
            order_flags: 0,
            per_instrument_seq: 1,
            order_id: 300,
            enter_timestamp_ns: 0,
            price: 1_500,
            quantity: 10,
        }));
        // Partial execute of 3.
        sub.apply_mktdata_frame(&frame_order_execute(&OrderExecute {
            instrument_id: 42,
            source_id: 1,
            aggressor_side: 2,
            exec_flags: 0,
            per_instrument_seq: 2,
            order_id: 300,
            trade_id: 1,
            timestamp_ns: 0,
            exec_price: 1_500,
            exec_quantity: 3,
        }));
        let book = sub.books.get(&42).expect("book exists");
        assert_eq!(book.best_bid(), Some((1_500, 7)));
        // Full execute of remaining 7.
        sub.apply_mktdata_frame(&frame_order_execute(&OrderExecute {
            instrument_id: 42,
            source_id: 1,
            aggressor_side: 2,
            exec_flags: 0,
            per_instrument_seq: 3,
            order_id: 300,
            trade_id: 2,
            timestamp_ns: 0,
            exec_price: 1_500,
            exec_quantity: 7,
        }));
        let book = sub.books.get(&42).expect("book exists");
        assert_eq!(book.best_bid(), None);
    }

    #[test]
    fn snapshot_round_trip_rebuilds_book() {
        let mut sub = ReferenceSubscriber::new();
        let begin = SnapshotBegin {
            instrument_id: 42,
            anchor_seq: 100,
            total_orders: 3,
            snapshot_id: 7,
            last_instrument_seq: 99,
            timestamp_ns: 0,
        };
        let end = SnapshotEnd { instrument_id: 42, anchor_seq: 100, snapshot_id: 7 };
        let orders = vec![
            SnapshotOrder {
                snapshot_id: 7,
                order_id: 1001,
                side: SIDE_BID,
                order_flags: 0,
                enter_timestamp_ns: 0,
                price: 99,
                quantity: 10,
            },
            SnapshotOrder {
                snapshot_id: 7,
                order_id: 1002,
                side: SIDE_BID,
                order_flags: 0,
                enter_timestamp_ns: 0,
                price: 100,
                quantity: 5,
            },
            SnapshotOrder {
                snapshot_id: 7,
                order_id: 2001,
                side: SIDE_ASK,
                order_flags: 0,
                enter_timestamp_ns: 0,
                price: 200,
                quantity: 3,
            },
        ];
        let frame = frame_snapshot(&begin, &orders, &end);
        sub.apply_snapshot_frame(&frame);
        let book = sub.books.get(&42).expect("book exists");
        assert_eq!(book.best_bid(), Some((100, 5)));
        assert_eq!(book.best_ask(), Some((200, 3)));
    }

    #[test]
    fn instrument_reset_clears_book() {
        use crate::protocol::dob::constants::INSTRUMENT_RESET_SIZE;
        use crate::protocol::dob::messages::{InstrumentReset, encode_instrument_reset};

        let mut sub = ReferenceSubscriber::new();
        sub.apply_mktdata_frame(&frame_order_add(&OrderAdd {
            instrument_id: 42,
            source_id: 1,
            side: SIDE_BID,
            order_flags: 0,
            per_instrument_seq: 1,
            order_id: 500,
            enter_timestamp_ns: 0,
            price: 1_000,
            quantity: 5,
        }));
        let mut fb = DobFrameBuilder::new(0, 4, 0, 0, DEFAULT_MTU);
        let buf = fb.message_buffer(INSTRUMENT_RESET_SIZE).unwrap();
        encode_instrument_reset(
            buf,
            &InstrumentReset { instrument_id: 42, reason: 1, new_anchor_seq: 200, timestamp_ns: 0 },
        );
        fb.commit_message();
        let frame = fb.finalize().to_vec();
        sub.apply_mktdata_frame(&frame);
        assert!(sub.books.get(&42).is_none(), "book cleared by reset");
    }

    #[test]
    fn snapshot_order_with_unknown_id_is_ignored() {
        // SnapshotOrder arriving with no matching active snapshot must not crash
        // and must not corrupt the book.
        let mut sub = ReferenceSubscriber::new();
        let mut fb = DobFrameBuilder::new(1, 1, 0, 0, DEFAULT_MTU);
        let buf = fb.message_buffer(SNAPSHOT_ORDER_SIZE).unwrap();
        encode_snapshot_order(
            buf,
            &SnapshotOrder {
                snapshot_id: 999,
                order_id: 1,
                side: SIDE_BID,
                order_flags: 0,
                enter_timestamp_ns: 0,
                price: 100,
                quantity: 1,
            },
        );
        fb.commit_message();
        let frame = fb.finalize().to_vec();
        sub.apply_snapshot_frame(&frame);
        assert!(sub.books.is_empty());
    }
}
