//! DZ-DoB message encoders.
//!
//! Messages shared byte-for-byte with TOB (Heartbeat, InstrumentDefinition, Trade,
//! EndOfSession, ManifestSummary) are re-exported from `crate::protocol::messages`
//! so DoB call sites have a single import path.

pub use crate::protocol::messages::{
    encode_end_of_session, encode_heartbeat, encode_instrument_definition, encode_manifest_summary, encode_trade,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::dob::constants::{HEARTBEAT_SIZE, MSG_TYPE_HEARTBEAT};

    #[test]
    fn reexported_heartbeat_writes_expected_header() {
        let mut buf = [0u8; HEARTBEAT_SIZE];
        encode_heartbeat(&mut buf, 3, 1_700_000_000_000_000_000);
        assert_eq!(buf[0], MSG_TYPE_HEARTBEAT, "type byte");
        assert_eq!(buf[1] as usize, HEARTBEAT_SIZE, "length byte");
        assert_eq!(buf[4], 3, "channel id");
        assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 1_700_000_000_000_000_000, "timestamp");
    }
}

/// Payload for the OrderAdd (0x10) message.
#[derive(Debug, Clone, Copy)]
pub struct OrderAdd {
    pub instrument_id: u32,
    pub source_id: u16,
    pub side: u8,
    pub order_flags: u8,
    pub per_instrument_seq: u32,
    pub order_id: u64,
    pub enter_timestamp_ns: u64,
    pub price: i64,
    pub quantity: u64,
}

/// Encodes an OrderAdd into `out`. `out` must be exactly `ORDER_ADD_SIZE` bytes.
pub fn encode_order_add(out: &mut [u8], msg: &OrderAdd) {
    use crate::protocol::dob::constants::{MSG_TYPE_ORDER_ADD, ORDER_ADD_SIZE};
    assert_eq!(out.len(), ORDER_ADD_SIZE, "OrderAdd buffer size mismatch");

    out[0] = MSG_TYPE_ORDER_ADD;
    out[1] = ORDER_ADD_SIZE as u8;
    out[2..4].copy_from_slice(&0u16.to_le_bytes()); // flags = 0 (incremental)
    out[4..8].copy_from_slice(&msg.instrument_id.to_le_bytes());
    out[8..10].copy_from_slice(&msg.source_id.to_le_bytes());
    out[10] = msg.side;
    out[11] = msg.order_flags;
    out[12..16].copy_from_slice(&msg.per_instrument_seq.to_le_bytes());
    out[16..24].copy_from_slice(&msg.order_id.to_le_bytes());
    out[24..32].copy_from_slice(&msg.enter_timestamp_ns.to_le_bytes());
    out[32..40].copy_from_slice(&msg.price.to_le_bytes());
    out[40..48].copy_from_slice(&msg.quantity.to_le_bytes());
    out[48..52].copy_from_slice(&[0u8; 4]); // reserved
}

#[cfg(test)]
mod order_add_tests {
    use super::*;
    use crate::protocol::dob::constants::{MSG_TYPE_ORDER_ADD, ORDER_ADD_SIZE, ORDER_FLAG_POST_ONLY, SIDE_BID};

    #[test]
    fn round_trip_order_add() {
        let msg = OrderAdd {
            instrument_id: 42,
            source_id: 1,
            side: SIDE_BID,
            order_flags: ORDER_FLAG_POST_ONLY,
            per_instrument_seq: 12345,
            order_id: 0xDEAD_BEEF_CAFE_BABE,
            enter_timestamp_ns: 1_700_000_000_000_000_000,
            price: -12_345_678,
            quantity: 98_765_432,
        };
        let mut buf = [0u8; ORDER_ADD_SIZE];
        encode_order_add(&mut buf, &msg);

        assert_eq!(buf[0], MSG_TYPE_ORDER_ADD);
        assert_eq!(buf[1], ORDER_ADD_SIZE as u8);
        assert_eq!(u16::from_le_bytes(buf[2..4].try_into().unwrap()), 0, "flags");
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 42);
        assert_eq!(u16::from_le_bytes(buf[8..10].try_into().unwrap()), 1);
        assert_eq!(buf[10], SIDE_BID);
        assert_eq!(buf[11], ORDER_FLAG_POST_ONLY);
        assert_eq!(u32::from_le_bytes(buf[12..16].try_into().unwrap()), 12345);
        assert_eq!(u64::from_le_bytes(buf[16..24].try_into().unwrap()), 0xDEAD_BEEF_CAFE_BABE);
        assert_eq!(u64::from_le_bytes(buf[24..32].try_into().unwrap()), 1_700_000_000_000_000_000);
        assert_eq!(i64::from_le_bytes(buf[32..40].try_into().unwrap()), -12_345_678);
        assert_eq!(u64::from_le_bytes(buf[40..48].try_into().unwrap()), 98_765_432);
        assert_eq!(&buf[48..52], &[0, 0, 0, 0], "reserved");
    }

    #[test]
    #[should_panic(expected = "buffer size mismatch")]
    fn wrong_buffer_size_panics() {
        let mut buf = [0u8; 10];
        let msg = OrderAdd {
            instrument_id: 0,
            source_id: 0,
            side: 0,
            order_flags: 0,
            per_instrument_seq: 0,
            order_id: 0,
            enter_timestamp_ns: 0,
            price: 0,
            quantity: 0,
        };
        encode_order_add(&mut buf, &msg);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OrderCancel {
    pub instrument_id: u32,
    pub source_id: u16,
    pub reason: u8,
    pub per_instrument_seq: u32,
    pub order_id: u64,
    pub timestamp_ns: u64,
}

pub fn encode_order_cancel(out: &mut [u8], msg: &OrderCancel) {
    use crate::protocol::dob::constants::{MSG_TYPE_ORDER_CANCEL, ORDER_CANCEL_SIZE};
    assert_eq!(out.len(), ORDER_CANCEL_SIZE, "OrderCancel buffer size mismatch");

    out[0] = MSG_TYPE_ORDER_CANCEL;
    out[1] = ORDER_CANCEL_SIZE as u8;
    out[2..4].copy_from_slice(&0u16.to_le_bytes());
    out[4..8].copy_from_slice(&msg.instrument_id.to_le_bytes());
    out[8..10].copy_from_slice(&msg.source_id.to_le_bytes());
    out[10] = msg.reason;
    out[11] = 0; // reserved
    out[12..16].copy_from_slice(&msg.per_instrument_seq.to_le_bytes());
    out[16..24].copy_from_slice(&msg.order_id.to_le_bytes());
    out[24..32].copy_from_slice(&msg.timestamp_ns.to_le_bytes());
}

#[cfg(test)]
mod order_cancel_tests {
    use super::*;
    use crate::protocol::dob::constants::{CANCEL_REASON_USER_CANCEL, MSG_TYPE_ORDER_CANCEL, ORDER_CANCEL_SIZE};

    #[test]
    fn round_trip_order_cancel() {
        let msg = OrderCancel {
            instrument_id: 7,
            source_id: 1,
            reason: CANCEL_REASON_USER_CANCEL,
            per_instrument_seq: 999,
            order_id: 0x1234_5678_9ABC_DEF0,
            timestamp_ns: 1_700_000_000_000_000_000,
        };
        let mut buf = [0u8; ORDER_CANCEL_SIZE];
        encode_order_cancel(&mut buf, &msg);

        assert_eq!(buf[0], MSG_TYPE_ORDER_CANCEL);
        assert_eq!(buf[1], ORDER_CANCEL_SIZE as u8);
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 7);
        assert_eq!(u16::from_le_bytes(buf[8..10].try_into().unwrap()), 1);
        assert_eq!(buf[10], CANCEL_REASON_USER_CANCEL);
        assert_eq!(buf[11], 0, "reserved");
        assert_eq!(u32::from_le_bytes(buf[12..16].try_into().unwrap()), 999);
        assert_eq!(u64::from_le_bytes(buf[16..24].try_into().unwrap()), 0x1234_5678_9ABC_DEF0);
        assert_eq!(u64::from_le_bytes(buf[24..32].try_into().unwrap()), 1_700_000_000_000_000_000);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OrderExecute {
    pub instrument_id: u32,
    pub source_id: u16,
    pub aggressor_side: u8,
    pub exec_flags: u8,
    pub per_instrument_seq: u32,
    pub order_id: u64,
    pub trade_id: u64,
    pub timestamp_ns: u64,
    pub exec_price: i64,
    pub exec_quantity: u64,
}

pub fn encode_order_execute(out: &mut [u8], msg: &OrderExecute) {
    use crate::protocol::dob::constants::{MSG_TYPE_ORDER_EXECUTE, ORDER_EXECUTE_SIZE};
    assert_eq!(out.len(), ORDER_EXECUTE_SIZE, "OrderExecute buffer size mismatch");

    out[0] = MSG_TYPE_ORDER_EXECUTE;
    out[1] = ORDER_EXECUTE_SIZE as u8;
    out[2..4].copy_from_slice(&0u16.to_le_bytes());
    out[4..8].copy_from_slice(&msg.instrument_id.to_le_bytes());
    out[8..10].copy_from_slice(&msg.source_id.to_le_bytes());
    out[10] = msg.aggressor_side;
    out[11] = msg.exec_flags;
    out[12..16].copy_from_slice(&msg.per_instrument_seq.to_le_bytes());
    out[16..24].copy_from_slice(&msg.order_id.to_le_bytes());
    out[24..32].copy_from_slice(&msg.trade_id.to_le_bytes());
    out[32..40].copy_from_slice(&msg.timestamp_ns.to_le_bytes());
    out[40..48].copy_from_slice(&msg.exec_price.to_le_bytes());
    out[48..56].copy_from_slice(&msg.exec_quantity.to_le_bytes());
}

#[cfg(test)]
mod order_execute_tests {
    use super::*;
    use crate::protocol::dob::constants::{AGGRESSOR_BUY, MSG_TYPE_ORDER_EXECUTE, ORDER_EXECUTE_SIZE};

    #[test]
    fn round_trip_order_execute() {
        let msg = OrderExecute {
            instrument_id: 42,
            source_id: 1,
            aggressor_side: AGGRESSOR_BUY,
            exec_flags: 0,
            per_instrument_seq: 777,
            order_id: 0xAAAA_BBBB_CCCC_DDDD,
            trade_id: 0x1111_2222_3333_4444,
            timestamp_ns: 1_700_000_000_000_000_000,
            exec_price: 12_345_678,
            exec_quantity: 999_999,
        };
        let mut buf = [0u8; ORDER_EXECUTE_SIZE];
        encode_order_execute(&mut buf, &msg);

        assert_eq!(buf[0], MSG_TYPE_ORDER_EXECUTE);
        assert_eq!(buf[1], ORDER_EXECUTE_SIZE as u8);
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 42);
        assert_eq!(buf[10], AGGRESSOR_BUY);
        assert_eq!(buf[11], 0);
        assert_eq!(u32::from_le_bytes(buf[12..16].try_into().unwrap()), 777);
        assert_eq!(u64::from_le_bytes(buf[16..24].try_into().unwrap()), 0xAAAA_BBBB_CCCC_DDDD);
        assert_eq!(u64::from_le_bytes(buf[24..32].try_into().unwrap()), 0x1111_2222_3333_4444);
        assert_eq!(i64::from_le_bytes(buf[40..48].try_into().unwrap()), 12_345_678);
        assert_eq!(u64::from_le_bytes(buf[48..56].try_into().unwrap()), 999_999);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BatchBoundary {
    pub channel_id: u8,
    pub phase: u8, // 0 = open, 1 = close
    pub batch_id: u64,
}

pub fn encode_batch_boundary(out: &mut [u8], msg: &BatchBoundary) {
    use crate::protocol::dob::constants::{BATCH_BOUNDARY_SIZE, MSG_TYPE_BATCH_BOUNDARY};
    assert_eq!(out.len(), BATCH_BOUNDARY_SIZE, "BatchBoundary buffer size mismatch");

    out[0] = MSG_TYPE_BATCH_BOUNDARY;
    out[1] = BATCH_BOUNDARY_SIZE as u8;
    out[2..4].copy_from_slice(&0u16.to_le_bytes());
    out[4] = msg.channel_id;
    out[5] = msg.phase;
    out[6..8].copy_from_slice(&[0u8; 2]); // reserved
    out[8..16].copy_from_slice(&msg.batch_id.to_le_bytes());
}

#[cfg(test)]
mod batch_boundary_tests {
    use super::*;
    use crate::protocol::dob::constants::{BATCH_BOUNDARY_SIZE, MSG_TYPE_BATCH_BOUNDARY};

    #[test]
    fn round_trip_batch_boundary_open() {
        let msg = BatchBoundary { channel_id: 3, phase: 0, batch_id: 999_888_777 };
        let mut buf = [0u8; BATCH_BOUNDARY_SIZE];
        encode_batch_boundary(&mut buf, &msg);

        assert_eq!(buf[0], MSG_TYPE_BATCH_BOUNDARY);
        assert_eq!(buf[1], BATCH_BOUNDARY_SIZE as u8);
        assert_eq!(buf[4], 3);
        assert_eq!(buf[5], 0);
        assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 999_888_777);
    }

    #[test]
    fn round_trip_batch_boundary_close() {
        let msg = BatchBoundary { channel_id: 3, phase: 1, batch_id: 999_888_777 };
        let mut buf = [0u8; BATCH_BOUNDARY_SIZE];
        encode_batch_boundary(&mut buf, &msg);
        assert_eq!(buf[5], 1, "close phase");
    }
}

/// 0x14 InstrumentReset — publisher-side per-coin resync signal on mktdata.
/// Subscribers discard state for `instrument_id`, discard buffered deltas with
/// `mktdata_seq <= new_anchor_seq`, and await a snapshot with
/// `Anchor Seq == new_anchor_seq` on the snapshot port.
#[derive(Debug, Clone, Copy)]
pub struct InstrumentReset {
    pub instrument_id: u32,
    pub reason: u8,
    pub new_anchor_seq: u64,
    pub timestamp_ns: u64,
}

pub fn encode_instrument_reset(out: &mut [u8], msg: &InstrumentReset) {
    use crate::protocol::dob::constants::{INSTRUMENT_RESET_SIZE, MSG_TYPE_INSTRUMENT_RESET};
    assert_eq!(out.len(), INSTRUMENT_RESET_SIZE, "InstrumentReset buffer size mismatch");

    out[0] = MSG_TYPE_INSTRUMENT_RESET;
    out[1] = INSTRUMENT_RESET_SIZE as u8;
    out[2..4].copy_from_slice(&0u16.to_le_bytes()); // flags = 0
    out[4..8].copy_from_slice(&msg.instrument_id.to_le_bytes());
    out[8] = msg.reason;
    out[9..12].copy_from_slice(&[0u8; 3]); // reserved
    out[12..20].copy_from_slice(&msg.new_anchor_seq.to_le_bytes());
    out[20..28].copy_from_slice(&msg.timestamp_ns.to_le_bytes());
}

#[cfg(test)]
mod instrument_reset_tests {
    use super::*;
    use crate::protocol::dob::constants::{
        INSTRUMENT_RESET_SIZE, MSG_TYPE_INSTRUMENT_RESET, RESET_REASON_PUBLISHER_INCONSISTENCY,
    };

    #[test]
    fn round_trip_instrument_reset() {
        let msg = InstrumentReset {
            instrument_id: 42,
            reason: RESET_REASON_PUBLISHER_INCONSISTENCY,
            new_anchor_seq: 0x1122_3344_5566_7788,
            timestamp_ns: 1_700_000_000_000_000_000,
        };
        let mut buf = [0u8; INSTRUMENT_RESET_SIZE];
        encode_instrument_reset(&mut buf, &msg);

        assert_eq!(buf[0], MSG_TYPE_INSTRUMENT_RESET);
        assert_eq!(buf[1] as usize, INSTRUMENT_RESET_SIZE);
        assert_eq!(u16::from_le_bytes(buf[2..4].try_into().unwrap()), 0, "flags");
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 42);
        assert_eq!(buf[8], RESET_REASON_PUBLISHER_INCONSISTENCY);
        assert_eq!(&buf[9..12], &[0, 0, 0], "reserved");
        assert_eq!(u64::from_le_bytes(buf[12..20].try_into().unwrap()), 0x1122_3344_5566_7788);
        assert_eq!(u64::from_le_bytes(buf[20..28].try_into().unwrap()), 1_700_000_000_000_000_000);
    }
}

/// 0x20 SnapshotBegin — start of a per-instrument snapshot group on the
/// `snapshot` port. Anchored to a `mktdata`-port sequence number so subscribers
/// can reconcile the snapshot stream with the delta stream.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotBegin {
    pub instrument_id: u32,
    pub anchor_seq: u64,
    pub total_orders: u32,
    pub snapshot_id: u32,
    pub last_instrument_seq: u32,
    pub timestamp_ns: u64,
}

pub fn encode_snapshot_begin(out: &mut [u8], msg: &SnapshotBegin) {
    use crate::protocol::dob::constants::{FLAG_SNAPSHOT, MSG_TYPE_SNAPSHOT_BEGIN, SNAPSHOT_BEGIN_SIZE};
    assert_eq!(out.len(), SNAPSHOT_BEGIN_SIZE, "SnapshotBegin buffer size mismatch");

    out[0] = MSG_TYPE_SNAPSHOT_BEGIN;
    out[1] = SNAPSHOT_BEGIN_SIZE as u8;
    out[2..4].copy_from_slice(&FLAG_SNAPSHOT.to_le_bytes());
    out[4..8].copy_from_slice(&msg.instrument_id.to_le_bytes());
    out[8..16].copy_from_slice(&msg.anchor_seq.to_le_bytes());
    out[16..20].copy_from_slice(&msg.total_orders.to_le_bytes());
    out[20..24].copy_from_slice(&msg.snapshot_id.to_le_bytes());
    out[24..28].copy_from_slice(&msg.last_instrument_seq.to_le_bytes());
    out[28..36].copy_from_slice(&msg.timestamp_ns.to_le_bytes());
}

#[cfg(test)]
mod snapshot_begin_tests {
    use super::*;
    use crate::protocol::dob::constants::{FLAG_SNAPSHOT, MSG_TYPE_SNAPSHOT_BEGIN, SNAPSHOT_BEGIN_SIZE};

    #[test]
    fn round_trip_snapshot_begin() {
        let msg = SnapshotBegin {
            instrument_id: 0xAABB_CCDD,
            anchor_seq: 0x1122_3344_5566_7788,
            total_orders: 0x0001_0203,
            snapshot_id: 0x0405_0607,
            last_instrument_seq: 0x0809_0A0B,
            timestamp_ns: 1_700_000_000_000_000_001,
        };
        let mut buf = [0u8; SNAPSHOT_BEGIN_SIZE];
        encode_snapshot_begin(&mut buf, &msg);

        assert_eq!(buf[0], MSG_TYPE_SNAPSHOT_BEGIN);
        assert_eq!(buf[1] as usize, SNAPSHOT_BEGIN_SIZE);
        assert_eq!(&buf[2..4], &FLAG_SNAPSHOT.to_le_bytes(), "flags");
        assert_eq!(&buf[2..4], &[0x01, 0x00], "flags bytes");
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 0xAABB_CCDD);
        assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 0x1122_3344_5566_7788);
        assert_eq!(u32::from_le_bytes(buf[16..20].try_into().unwrap()), 0x0001_0203);
        assert_eq!(u32::from_le_bytes(buf[20..24].try_into().unwrap()), 0x0405_0607);
        assert_eq!(u32::from_le_bytes(buf[24..28].try_into().unwrap()), 0x0809_0A0B);
        assert_eq!(u64::from_le_bytes(buf[28..36].try_into().unwrap()), 1_700_000_000_000_000_001);
    }
}

/// 0x21 SnapshotOrder — one resting order in a snapshot. Same per-order
/// fields as `OrderAdd` but no `Per-Instrument Seq` (snapshot is a state
/// dump, not a delta) and no `Instrument ID` (implied by the containing
/// `SnapshotBegin`). Carries the `snapshot_id` so subscribers can discard
/// orders that don't belong to the currently-open snapshot.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotOrder {
    pub snapshot_id: u32,
    pub order_id: u64,
    pub side: u8,
    pub order_flags: u8,
    pub enter_timestamp_ns: u64,
    pub price: i64,
    pub quantity: u64,
}

pub fn encode_snapshot_order(out: &mut [u8], msg: &SnapshotOrder) {
    use crate::protocol::dob::constants::{FLAG_SNAPSHOT, MSG_TYPE_SNAPSHOT_ORDER, SNAPSHOT_ORDER_SIZE};
    assert_eq!(out.len(), SNAPSHOT_ORDER_SIZE, "SnapshotOrder buffer size mismatch");

    out[0] = MSG_TYPE_SNAPSHOT_ORDER;
    out[1] = SNAPSHOT_ORDER_SIZE as u8;
    out[2..4].copy_from_slice(&FLAG_SNAPSHOT.to_le_bytes());
    out[4..8].copy_from_slice(&msg.snapshot_id.to_le_bytes());
    out[8..16].copy_from_slice(&msg.order_id.to_le_bytes());
    out[16] = msg.side;
    out[17] = msg.order_flags;
    out[18..20].copy_from_slice(&[0u8; 2]); // reserved
    out[20..28].copy_from_slice(&msg.enter_timestamp_ns.to_le_bytes());
    out[28..36].copy_from_slice(&msg.price.to_le_bytes());
    out[36..44].copy_from_slice(&msg.quantity.to_le_bytes());
}

#[cfg(test)]
mod snapshot_order_tests {
    use super::*;
    use crate::protocol::dob::constants::{
        FLAG_SNAPSHOT, MSG_TYPE_SNAPSHOT_ORDER, ORDER_FLAG_POST_ONLY, SIDE_ASK, SNAPSHOT_ORDER_SIZE,
    };

    #[test]
    fn round_trip_snapshot_order() {
        let msg = SnapshotOrder {
            snapshot_id: 0x0405_0607,
            order_id: 0xDEAD_BEEF_CAFE_BABE,
            side: SIDE_ASK,
            order_flags: ORDER_FLAG_POST_ONLY,
            enter_timestamp_ns: 1_700_000_000_000_000_001,
            price: -12_345_678,
            quantity: 98_765_432,
        };
        let mut buf = [0u8; SNAPSHOT_ORDER_SIZE];
        encode_snapshot_order(&mut buf, &msg);

        assert_eq!(buf[0], MSG_TYPE_SNAPSHOT_ORDER, "type");
        assert_eq!(buf[1] as usize, SNAPSHOT_ORDER_SIZE, "length");
        assert_eq!(&buf[2..4], &FLAG_SNAPSHOT.to_le_bytes(), "flags");
        assert_eq!(&buf[2..4], &[0x01, 0x00], "flags bytes");
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 0x0405_0607, "snapshot_id");
        assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 0xDEAD_BEEF_CAFE_BABE, "order_id");
        assert_eq!(buf[16], SIDE_ASK, "side");
        assert_eq!(buf[17], ORDER_FLAG_POST_ONLY, "order_flags");
        assert_eq!(&buf[18..20], &[0, 0], "reserved");
        assert_eq!(
            u64::from_le_bytes(buf[20..28].try_into().unwrap()),
            1_700_000_000_000_000_001,
            "enter_timestamp_ns"
        );
        assert_eq!(
            i64::from_le_bytes(buf[28..36].try_into().unwrap()),
            -12_345_678,
            "price (negative, exercises i64 sign)"
        );
        assert_eq!(u64::from_le_bytes(buf[36..44].try_into().unwrap()), 98_765_432, "quantity");
    }
}

/// 0x22 SnapshotEnd — close of a per-instrument snapshot group on the
/// `snapshot` port. Carries the same `instrument_id`, `anchor_seq`, and
/// `snapshot_id` as the bracketing `SnapshotBegin` so subscribers can
/// confirm the pair before applying the contained `SnapshotOrder` messages.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotEnd {
    pub instrument_id: u32,
    pub anchor_seq: u64,
    pub snapshot_id: u32,
}

pub fn encode_snapshot_end(out: &mut [u8], msg: &SnapshotEnd) {
    use crate::protocol::dob::constants::{FLAG_SNAPSHOT, MSG_TYPE_SNAPSHOT_END, SNAPSHOT_END_SIZE};
    assert_eq!(out.len(), SNAPSHOT_END_SIZE, "SnapshotEnd buffer size mismatch");

    out[0] = MSG_TYPE_SNAPSHOT_END;
    out[1] = SNAPSHOT_END_SIZE as u8;
    out[2..4].copy_from_slice(&FLAG_SNAPSHOT.to_le_bytes());
    out[4..8].copy_from_slice(&msg.instrument_id.to_le_bytes());
    out[8..16].copy_from_slice(&msg.anchor_seq.to_le_bytes());
    out[16..20].copy_from_slice(&msg.snapshot_id.to_le_bytes());
}

#[cfg(test)]
mod snapshot_end_tests {
    use super::*;
    use crate::protocol::dob::constants::{FLAG_SNAPSHOT, MSG_TYPE_SNAPSHOT_END, SNAPSHOT_END_SIZE};

    #[test]
    fn round_trip_snapshot_end() {
        let msg =
            SnapshotEnd { instrument_id: 0xAABB_CCDD, anchor_seq: 0x1122_3344_5566_7788, snapshot_id: 0x0405_0607 };
        let mut buf = [0u8; SNAPSHOT_END_SIZE];
        encode_snapshot_end(&mut buf, &msg);

        assert_eq!(buf[0], MSG_TYPE_SNAPSHOT_END, "type");
        assert_eq!(buf[1] as usize, SNAPSHOT_END_SIZE, "length");
        assert_eq!(&buf[2..4], &FLAG_SNAPSHOT.to_le_bytes(), "flags");
        assert_eq!(&buf[2..4], &[0x01, 0x00], "flags bytes");
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 0xAABB_CCDD, "instrument_id");
        assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 0x1122_3344_5566_7788, "anchor_seq");
        assert_eq!(u32::from_le_bytes(buf[16..20].try_into().unwrap()), 0x0405_0607, "snapshot_id");
    }
}
