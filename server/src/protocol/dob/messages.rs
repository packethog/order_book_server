//! DZ-DoB message encoders.
//!
//! Messages shared byte-for-byte with TOB (Heartbeat, InstrumentDefinition, Trade,
//! EndOfSession, ManifestSummary) are re-exported from `crate::protocol::messages`
//! so DoB call sites have a single import path.

pub use crate::protocol::messages::{
    encode_end_of_session,
    encode_heartbeat,
    encode_instrument_definition,
    encode_manifest_summary,
    encode_trade,
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
        assert_eq!(
            u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            1_700_000_000_000_000_000,
            "timestamp"
        );
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
    use crate::protocol::dob::constants::{
        MSG_TYPE_ORDER_ADD, ORDER_ADD_SIZE, ORDER_FLAG_POST_ONLY, SIDE_BID,
    };

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
            instrument_id: 0, source_id: 0, side: 0, order_flags: 0,
            per_instrument_seq: 0, order_id: 0, enter_timestamp_ns: 0,
            price: 0, quantity: 0,
        };
        encode_order_add(&mut buf, &msg);
    }
}
