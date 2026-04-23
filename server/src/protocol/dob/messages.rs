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
