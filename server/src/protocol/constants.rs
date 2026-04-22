/// Frame magic bytes: "DZ" = 0x445A. On the wire (little-endian): [0x5A, 0x44].
pub const MAGIC: u16 = 0x445A;
pub const MAGIC_BYTES: [u8; 2] = [0x5A, 0x44];

/// Schema version for v0.1.0 of the protocol.
pub const SCHEMA_VERSION: u8 = 1;

/// Frame header size in bytes.
pub const FRAME_HEADER_SIZE: usize = 24;

/// Application message header size in bytes.
pub const APP_MSG_HEADER_SIZE: usize = 4;

// Message type IDs
pub const MSG_TYPE_HEARTBEAT: u8 = 0x01;
pub const MSG_TYPE_INSTRUMENT_DEF: u8 = 0x02;
pub const MSG_TYPE_QUOTE: u8 = 0x03;
pub const MSG_TYPE_TRADE: u8 = 0x04;
pub const MSG_TYPE_CHANNEL_RESET: u8 = 0x05;
pub const MSG_TYPE_END_OF_SESSION: u8 = 0x06;
pub const MSG_TYPE_MANIFEST_SUMMARY: u8 = 0x07;

// Total message sizes (header + body)
pub const HEARTBEAT_SIZE: usize = 16;
pub const INSTRUMENT_DEF_SIZE: usize = 80;
pub const QUOTE_SIZE: usize = 60;
pub const TRADE_SIZE: usize = 52;
pub const CHANNEL_RESET_SIZE: usize = 12;
pub const END_OF_SESSION_SIZE: usize = 12;
pub const MANIFEST_SUMMARY_SIZE: usize = 24;

// Asset class values (InstrumentDefinition.asset_class)
pub const ASSET_CLASS_UNKNOWN: u8 = 0;
pub const ASSET_CLASS_CRYPTO_SPOT: u8 = 1;

// Market model values (InstrumentDefinition.market_model)
pub const MARKET_MODEL_UNKNOWN: u8 = 0;
pub const MARKET_MODEL_CLOB: u8 = 1;

// Settle type values (InstrumentDefinition.settle_type)
pub const SETTLE_TYPE_NA: u8 = 0;

// Price bound values (InstrumentDefinition.price_bound)
pub const PRICE_BOUND_UNBOUNDED: u8 = 0;

/// Default max frame size for GRE tunnel: 1500 - 24 (GRE+outer IP) - 20 (inner IP) - 8 (UDP).
pub const DEFAULT_MTU: u16 = 1448;

// Aggressor side values
pub const AGGRESSOR_UNKNOWN: u8 = 0;
pub const AGGRESSOR_BUY: u8 = 1;
pub const AGGRESSOR_SELL: u8 = 2;

// Quote update flag bits
pub const UPDATE_FLAG_BID_UPDATED: u8 = 0x01;
pub const UPDATE_FLAG_ASK_UPDATED: u8 = 0x02;
pub const UPDATE_FLAG_BID_GONE: u8 = 0x04;
pub const UPDATE_FLAG_ASK_GONE: u8 = 0x08;

// App message header flag bits
pub const FLAG_SNAPSHOT: u16 = 0x0001;
