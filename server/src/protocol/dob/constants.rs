//! DZ Depth-of-Book wire format constants.
//!
//! Wire spec: https://github.com/malbeclabs/edge-feed-spec/blob/main/depth-of-book/spec.md

/// Frame magic bytes: "DD" = 0x4444. On the wire (little-endian): [0x44, 0x44].
/// Distinct from TOB's 0x445A and the midpoint feed's 0x4D44.
pub const MAGIC: u16 = 0x4444;
pub const MAGIC_BYTES: [u8; 2] = [0x44, 0x44];

/// Schema version for v0.1.0 of the DoB protocol.
pub const SCHEMA_VERSION: u8 = 1;

/// Frame header size in bytes.
pub const FRAME_HEADER_SIZE: usize = 24;

/// Application message header size in bytes.
pub const APP_MSG_HEADER_SIZE: usize = 4;

// Shared message type IDs (byte-identical layouts with TOB):
pub const MSG_TYPE_HEARTBEAT: u8 = 0x01;
pub const MSG_TYPE_INSTRUMENT_DEF: u8 = 0x02;
pub const MSG_TYPE_TRADE: u8 = 0x04;
pub const MSG_TYPE_END_OF_SESSION: u8 = 0x06;
pub const MSG_TYPE_MANIFEST_SUMMARY: u8 = 0x07;

// DoB-specific message type IDs:
pub const MSG_TYPE_ORDER_ADD: u8 = 0x10;
pub const MSG_TYPE_ORDER_CANCEL: u8 = 0x11;
pub const MSG_TYPE_ORDER_EXECUTE: u8 = 0x12;
pub const MSG_TYPE_BATCH_BOUNDARY: u8 = 0x13;
pub const MSG_TYPE_INSTRUMENT_RESET: u8 = 0x14;
pub const MSG_TYPE_SNAPSHOT_BEGIN: u8 = 0x20;
pub const MSG_TYPE_SNAPSHOT_ORDER: u8 = 0x21;
pub const MSG_TYPE_SNAPSHOT_END: u8 = 0x22;

// Total message sizes (header + body):
pub const HEARTBEAT_SIZE: usize = 16;
pub const INSTRUMENT_DEF_SIZE: usize = 80;
pub const TRADE_SIZE: usize = 52;
pub const END_OF_SESSION_SIZE: usize = 12;
pub const MANIFEST_SUMMARY_SIZE: usize = 24;
pub const ORDER_ADD_SIZE: usize = 52;
pub const ORDER_CANCEL_SIZE: usize = 32;
pub const ORDER_EXECUTE_SIZE: usize = 56;
pub const BATCH_BOUNDARY_SIZE: usize = 16;
pub const INSTRUMENT_RESET_SIZE: usize = 28;
pub const SNAPSHOT_BEGIN_SIZE: usize = 36;
pub const SNAPSHOT_ORDER_SIZE: usize = 44;
pub const SNAPSHOT_END_SIZE: usize = 20;

/// Max frame size per spec §Transport Framing (leaves room for GRE).
pub const DEFAULT_MTU: u16 = 1232;

// Side values (OrderAdd):
pub const SIDE_BID: u8 = 0;
pub const SIDE_ASK: u8 = 1;

// OrderAdd flag bits:
pub const ORDER_FLAG_POST_ONLY: u8 = 0x01;
pub const ORDER_FLAG_REDUCE_ONLY: u8 = 0x02;
pub const ORDER_FLAG_HIDDEN: u8 = 0x04;
pub const ORDER_FLAG_STOP: u8 = 0x08;
pub const ORDER_FLAG_TWAP_CHILD: u8 = 0x10;

// Cancel reason values (OrderCancel):
pub const CANCEL_REASON_UNKNOWN: u8 = 0;
pub const CANCEL_REASON_USER_CANCEL: u8 = 1;
pub const CANCEL_REASON_VENUE_EXPIRE: u8 = 2;
pub const CANCEL_REASON_SELF_TRADE: u8 = 3;
pub const CANCEL_REASON_MARGIN: u8 = 4;
pub const CANCEL_REASON_RISK_LIMIT: u8 = 5;
pub const CANCEL_REASON_SIBLING_FILLED: u8 = 6;
pub const CANCEL_REASON_OTHER: u8 = 255;

// Aggressor side (shared with TOB Trade semantics):
pub const AGGRESSOR_UNKNOWN: u8 = 0;
pub const AGGRESSOR_BUY: u8 = 1;
pub const AGGRESSOR_SELL: u8 = 2;

// App message header flag bits:
pub const FLAG_SNAPSHOT: u16 = 0x0001;

// Reset Count initial value:
pub const RESET_COUNT_INITIAL: u8 = 0;

// InstrumentReset reason codes:
pub const RESET_REASON_UNSPECIFIED: u8 = 0;
pub const RESET_REASON_PUBLISHER_INCONSISTENCY: u8 = 1;
pub const RESET_REASON_VENUE_RESYNC: u8 = 2;
pub const RESET_REASON_UPSTREAM_GAP: u8 = 3;
pub const RESET_REASON_OTHER: u8 = 255;
