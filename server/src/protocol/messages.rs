use crate::protocol::constants::*;

/// Data needed to encode a Quote message.
pub struct QuoteData {
    pub instrument_id: u32,
    pub source_id: u16,
    pub update_flags: u8,
    pub source_timestamp_ns: u64,
    pub bid_price: i64,
    pub bid_qty: u64,
    pub ask_price: i64,
    pub ask_qty: u64,
    pub bid_source_count: u16,
    pub ask_source_count: u16,
}

/// Data needed to encode a Trade message.
pub struct TradeData {
    pub instrument_id: u32,
    pub source_id: u16,
    pub aggressor_side: u8,
    pub trade_flags: u8,
    pub source_timestamp_ns: u64,
    pub trade_price: i64,
    pub trade_qty: u64,
    pub trade_id: u64,
    pub cumulative_volume: u64,
}

/// Writes a Quote message (60 bytes) into the provided buffer.
/// The buffer MUST be exactly `QUOTE_SIZE` bytes.
pub fn encode_quote(buf: &mut [u8], data: &QuoteData, flags: u16) {
    debug_assert_eq!(buf.len(), QUOTE_SIZE);
    // App message header
    buf[0] = MSG_TYPE_QUOTE;
    buf[1] = QUOTE_SIZE as u8;
    buf[2..4].copy_from_slice(&flags.to_le_bytes());
    // Body
    buf[4..8].copy_from_slice(&data.instrument_id.to_le_bytes());
    buf[8..10].copy_from_slice(&data.source_id.to_le_bytes());
    buf[10] = data.update_flags;
    buf[11] = 0; // reserved
    buf[12..20].copy_from_slice(&data.source_timestamp_ns.to_le_bytes());
    buf[20..28].copy_from_slice(&data.bid_price.to_le_bytes());
    buf[28..36].copy_from_slice(&data.bid_qty.to_le_bytes());
    buf[36..44].copy_from_slice(&data.ask_price.to_le_bytes());
    buf[44..52].copy_from_slice(&data.ask_qty.to_le_bytes());
    buf[52..54].copy_from_slice(&data.bid_source_count.to_le_bytes());
    buf[54..56].copy_from_slice(&data.ask_source_count.to_le_bytes());
    buf[56..60].copy_from_slice(&[0; 4]); // reserved
}

/// Writes a Trade message (52 bytes) into the provided buffer.
/// The buffer MUST be exactly `TRADE_SIZE` bytes.
pub fn encode_trade(buf: &mut [u8], data: &TradeData, flags: u16) {
    debug_assert_eq!(buf.len(), TRADE_SIZE);
    // App message header
    buf[0] = MSG_TYPE_TRADE;
    buf[1] = TRADE_SIZE as u8;
    buf[2..4].copy_from_slice(&flags.to_le_bytes());
    // Body
    buf[4..8].copy_from_slice(&data.instrument_id.to_le_bytes());
    buf[8..10].copy_from_slice(&data.source_id.to_le_bytes());
    buf[10] = data.aggressor_side;
    buf[11] = data.trade_flags;
    buf[12..20].copy_from_slice(&data.source_timestamp_ns.to_le_bytes());
    buf[20..28].copy_from_slice(&data.trade_price.to_le_bytes());
    buf[28..36].copy_from_slice(&data.trade_qty.to_le_bytes());
    buf[36..44].copy_from_slice(&data.trade_id.to_le_bytes());
    buf[44..52].copy_from_slice(&data.cumulative_volume.to_le_bytes());
}

/// Writes a Heartbeat message (16 bytes) into the provided buffer.
/// The buffer MUST be exactly `HEARTBEAT_SIZE` bytes.
pub fn encode_heartbeat(buf: &mut [u8], channel_id: u8, timestamp_ns: u64) {
    debug_assert_eq!(buf.len(), HEARTBEAT_SIZE);
    buf[0] = MSG_TYPE_HEARTBEAT;
    buf[1] = HEARTBEAT_SIZE as u8;
    buf[2..4].copy_from_slice(&0u16.to_le_bytes()); // flags
    buf[4] = channel_id;
    buf[5..8].copy_from_slice(&[0; 3]); // reserved
    buf[8..16].copy_from_slice(&timestamp_ns.to_le_bytes());
}

/// Writes a ChannelReset message (12 bytes) into the provided buffer.
/// The buffer MUST be exactly `CHANNEL_RESET_SIZE` bytes.
pub fn encode_channel_reset(buf: &mut [u8], timestamp_ns: u64) {
    debug_assert_eq!(buf.len(), CHANNEL_RESET_SIZE);
    buf[0] = MSG_TYPE_CHANNEL_RESET;
    buf[1] = CHANNEL_RESET_SIZE as u8;
    buf[2..4].copy_from_slice(&0u16.to_le_bytes()); // flags
    buf[4..12].copy_from_slice(&timestamp_ns.to_le_bytes());
}

/// Writes an EndOfSession message (12 bytes) into the provided buffer.
/// The buffer MUST be exactly `END_OF_SESSION_SIZE` bytes.
pub fn encode_end_of_session(buf: &mut [u8], timestamp_ns: u64) {
    debug_assert_eq!(buf.len(), END_OF_SESSION_SIZE);
    buf[0] = MSG_TYPE_END_OF_SESSION;
    buf[1] = END_OF_SESSION_SIZE as u8;
    buf[2..4].copy_from_slice(&0u16.to_le_bytes()); // flags
    buf[4..12].copy_from_slice(&timestamp_ns.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_layout_matches_spec() {
        let data = QuoteData {
            instrument_id: 7,
            source_id: 1,
            update_flags: UPDATE_FLAG_BID_UPDATED | UPDATE_FLAG_ASK_UPDATED,
            source_timestamp_ns: 1_700_000_000_000_000_000,
            bid_price: 1_062_170,
            bid_qty: 100,
            ask_price: 1_062_330,
            ask_qty: 267,
            bid_source_count: 1,
            ask_source_count: 3,
        };
        let mut buf = [0u8; QUOTE_SIZE];
        encode_quote(&mut buf, &data, 0);

        assert_eq!(buf[0], MSG_TYPE_QUOTE);
        assert_eq!(buf[1], 60);
        assert_eq!(u16::from_le_bytes(buf[2..4].try_into().unwrap()), 0);
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 7);
        assert_eq!(u16::from_le_bytes(buf[8..10].try_into().unwrap()), 1);
        assert_eq!(buf[10], 0x03);
        assert_eq!(buf[11], 0);
        assert_eq!(
            u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            1_700_000_000_000_000_000
        );
        assert_eq!(i64::from_le_bytes(buf[20..28].try_into().unwrap()), 1_062_170);
        assert_eq!(u64::from_le_bytes(buf[28..36].try_into().unwrap()), 100);
        assert_eq!(i64::from_le_bytes(buf[36..44].try_into().unwrap()), 1_062_330);
        assert_eq!(u64::from_le_bytes(buf[44..52].try_into().unwrap()), 267);
        assert_eq!(u16::from_le_bytes(buf[52..54].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(buf[54..56].try_into().unwrap()), 3);
        assert_eq!(&buf[56..60], &[0; 4]);
    }

    #[test]
    fn quote_snapshot_flag() {
        let data = QuoteData {
            instrument_id: 0,
            source_id: 1,
            update_flags: 0x03,
            source_timestamp_ns: 0,
            bid_price: 0,
            bid_qty: 0,
            ask_price: 0,
            ask_qty: 0,
            bid_source_count: 0,
            ask_source_count: 0,
        };
        let mut buf = [0u8; QUOTE_SIZE];
        encode_quote(&mut buf, &data, FLAG_SNAPSHOT);
        assert_eq!(u16::from_le_bytes(buf[2..4].try_into().unwrap()), 1);
    }

    #[test]
    fn trade_layout_matches_spec() {
        let data = TradeData {
            instrument_id: 12,
            source_id: 1,
            aggressor_side: AGGRESSOR_SELL,
            trade_flags: 0,
            source_timestamp_ns: 1_700_000_001_000_000_000,
            trade_price: 1_062_960,
            trade_qty: 17,
            trade_id: 293_353_986_402_527,
            cumulative_volume: 0,
        };
        let mut buf = [0u8; TRADE_SIZE];
        encode_trade(&mut buf, &data, 0);

        assert_eq!(buf[0], MSG_TYPE_TRADE);
        assert_eq!(buf[1], 52);
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 12);
        assert_eq!(u16::from_le_bytes(buf[8..10].try_into().unwrap()), 1);
        assert_eq!(buf[10], AGGRESSOR_SELL);
        assert_eq!(buf[11], 0);
        assert_eq!(
            u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            1_700_000_001_000_000_000
        );
        assert_eq!(i64::from_le_bytes(buf[20..28].try_into().unwrap()), 1_062_960);
        assert_eq!(u64::from_le_bytes(buf[28..36].try_into().unwrap()), 17);
        assert_eq!(
            u64::from_le_bytes(buf[36..44].try_into().unwrap()),
            293_353_986_402_527
        );
        assert_eq!(u64::from_le_bytes(buf[44..52].try_into().unwrap()), 0);
    }

    #[test]
    fn heartbeat_layout_matches_spec() {
        let mut buf = [0u8; HEARTBEAT_SIZE];
        encode_heartbeat(&mut buf, 5, 999_000_000);

        assert_eq!(buf[0], MSG_TYPE_HEARTBEAT);
        assert_eq!(buf[1], 16);
        assert_eq!(u16::from_le_bytes(buf[2..4].try_into().unwrap()), 0);
        assert_eq!(buf[4], 5);
        assert_eq!(&buf[5..8], &[0; 3]);
        assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 999_000_000);
    }

    #[test]
    fn channel_reset_layout_matches_spec() {
        let mut buf = [0u8; CHANNEL_RESET_SIZE];
        encode_channel_reset(&mut buf, 123_456_789);

        assert_eq!(buf[0], MSG_TYPE_CHANNEL_RESET);
        assert_eq!(buf[1], 12);
        assert_eq!(u64::from_le_bytes(buf[4..12].try_into().unwrap()), 123_456_789);
    }

    #[test]
    fn end_of_session_layout_matches_spec() {
        let mut buf = [0u8; END_OF_SESSION_SIZE];
        encode_end_of_session(&mut buf, 987_654_321);

        assert_eq!(buf[0], MSG_TYPE_END_OF_SESSION);
        assert_eq!(buf[1], 12);
        assert_eq!(u64::from_le_bytes(buf[4..12].try_into().unwrap()), 987_654_321);
    }
}
