# Binary Multicast Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the JSON multicast publisher with the DoubleZero Edge Top-of-Book binary protocol (v0.1.0), marketdata only.

**Architecture:** Refactor the existing multicast publisher on `ss/multicast-support` in-place. Add a `protocol/` module for binary frame encoding (pure, no I/O) and an `instruments/` module for bootstrapping numeric instrument IDs from the Hyperliquid meta API. The publisher event loop structure stays; the serialization layer changes from JSON to binary frames.

**Tech Stack:** Rust, tokio, reqwest (for HL API), serde_json (for HL API responses only — not for wire format)

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `server/src/protocol/constants.rs` | Magic bytes, type IDs, schema version, message sizes |
| Create | `server/src/protocol/frame.rs` | FrameBuilder: accumulate messages, finalize frame header |
| Create | `server/src/protocol/messages.rs` | Encode Quote, Trade, Heartbeat, ChannelReset, EndOfSession |
| Create | `server/src/protocol/mod.rs` | Re-exports |
| Create | `server/src/instruments/mod.rs` | InstrumentInfo, InstrumentRegistry, price/qty conversion |
| Create | `server/src/instruments/hyperliquid.rs` | Bootstrap from HL meta/spotMeta REST API |
| Modify | `server/src/multicast/config.rs` | Remove Channel enum, l2_levels, parse_channels. Add mtu, source_id, heartbeat_interval |
| Modify | `server/src/multicast/publisher.rs` | Replace JSON serialization with FrameBuilder + protocol module |
| Modify | `server/src/multicast/mod.rs` | Update re-exports |
| Modify | `server/src/lib.rs` | Add protocol and instruments modules, update re-exports |
| Modify | `server/Cargo.toml` | Remove uuid dependency |
| Modify | `binaries/src/bin/websocket_server.rs` | Update CLI args, remove old args, pass new config |
| Modify | `binaries/src/bin/example_multicast_subscriber.rs` | Decode binary frames instead of printing JSON |

---

### Task 1: Create protocol constants

**Files:**
- Create: `server/src/protocol/constants.rs`
- Create: `server/src/protocol/mod.rs`
- Modify: `server/src/lib.rs`

- [ ] **Step 1: Create constants module**

```rust
// server/src/protocol/constants.rs

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
pub const QUOTE_SIZE: usize = 60;
pub const TRADE_SIZE: usize = 52;
pub const CHANNEL_RESET_SIZE: usize = 12;
pub const END_OF_SESSION_SIZE: usize = 12;

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
```

- [ ] **Step 2: Create protocol mod.rs**

```rust
// server/src/protocol/mod.rs
pub mod constants;
```

- [ ] **Step 3: Add protocol module to lib.rs**

In `server/src/lib.rs`, add `pub mod protocol;` after the existing module declarations.

Current lib.rs has:
```rust
mod listeners;
pub mod multicast;
mod order_book;
mod prelude;
mod servers;
mod types;
```

Add:
```rust
pub mod protocol;
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p server`
Expected: Compiles with no errors.

- [ ] **Step 5: Commit**

```bash
git add server/src/protocol/constants.rs server/src/protocol/mod.rs server/src/lib.rs
git commit -m "protocol: add constants module with message types, sizes, and flags"
```

---

### Task 2: Implement frame header encoding

**Files:**
- Create: `server/src/protocol/frame.rs`
- Modify: `server/src/protocol/mod.rs`

- [ ] **Step 1: Write failing tests for frame header encoding**

```rust
// server/src/protocol/frame.rs

use crate::protocol::constants::*;

/// Errors returned by FrameBuilder when a message cannot be added.
#[derive(Debug, PartialEq)]
pub enum FrameError {
    /// Adding this message would exceed the configured MTU.
    ExceedsMtu { msg_size: usize, remaining: usize },
    /// Frame already has 255 messages (max for u8 msg_count).
    MaxMessages,
}

/// Accumulates binary application messages into a single frame buffer.
///
/// Usage:
/// 1. Create with `new(channel_id, sequence_number, send_timestamp_ns, mtu)`
/// 2. Call `remaining()` to check capacity, then write message bytes via `message_buffer()`
/// 3. Call `commit_message()` after writing to advance the cursor
/// 4. Call `finalize()` to write the frame header and get the complete frame bytes
pub struct FrameBuilder {
    buf: Vec<u8>,
    channel_id: u8,
    sequence_number: u64,
    send_timestamp_ns: u64,
    mtu: usize,
    msg_count: u8,
}

impl FrameBuilder {
    /// Creates a new frame builder. Reserves space for the frame header.
    pub fn new(channel_id: u8, sequence_number: u64, send_timestamp_ns: u64, mtu: u16) -> Self {
        let mtu = mtu as usize;
        let mut buf = Vec::with_capacity(mtu);
        // Reserve frame header space — will be written in finalize()
        buf.resize(FRAME_HEADER_SIZE, 0);
        Self {
            buf,
            channel_id,
            sequence_number,
            send_timestamp_ns,
            mtu,
            msg_count: 0,
        }
    }

    /// Returns the number of bytes available for more messages.
    pub fn remaining(&self) -> usize {
        self.mtu.saturating_sub(self.buf.len())
    }

    /// Returns true if no messages have been added yet.
    pub fn is_empty(&self) -> bool {
        self.msg_count == 0
    }

    /// Returns a mutable slice of exactly `size` bytes at the end of the buffer
    /// for the caller to write a message into. Returns `Err` if the message
    /// won't fit or if 255 messages have already been added.
    ///
    /// After writing, the caller MUST call `commit_message()`.
    pub fn message_buffer(&mut self, size: usize) -> Result<&mut [u8], FrameError> {
        if self.msg_count == 255 {
            return Err(FrameError::MaxMessages);
        }
        if size > self.remaining() {
            return Err(FrameError::ExceedsMtu {
                msg_size: size,
                remaining: self.remaining(),
            });
        }
        let start = self.buf.len();
        self.buf.resize(start + size, 0);
        Ok(&mut self.buf[start..start + size])
    }

    /// Commits the most recently allocated message, incrementing the message count.
    pub fn commit_message(&mut self) {
        self.msg_count += 1;
    }

    /// Writes the frame header into the reserved space and returns the complete frame.
    pub fn finalize(&mut self) -> &[u8] {
        let frame_length = self.buf.len() as u16;

        // Magic (0x445A LE → bytes [0x5A, 0x44])
        self.buf[0..2].copy_from_slice(&MAGIC_BYTES);
        // Schema Version
        self.buf[2] = SCHEMA_VERSION;
        // Channel ID
        self.buf[3] = self.channel_id;
        // Sequence Number
        self.buf[4..12].copy_from_slice(&self.sequence_number.to_le_bytes());
        // Send Timestamp
        self.buf[12..20].copy_from_slice(&self.send_timestamp_ns.to_le_bytes());
        // Message Count
        self.buf[20] = self.msg_count;
        // Reserved
        self.buf[21] = 0;
        // Frame Length
        self.buf[22..24].copy_from_slice(&frame_length.to_le_bytes());

        &self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_header_layout() {
        let mut fb = FrameBuilder::new(3, 42, 1_700_000_000_000_000_000, DEFAULT_MTU);
        // Add a dummy 8-byte message
        let buf = fb.message_buffer(8).unwrap();
        buf.copy_from_slice(&[0xAA; 8]);
        fb.commit_message();
        let frame = fb.finalize();

        // Total: 24 header + 8 message = 32
        assert_eq!(frame.len(), 32);
        // Magic
        assert_eq!(&frame[0..2], &[0x5A, 0x44]);
        // Schema version
        assert_eq!(frame[2], 1);
        // Channel ID
        assert_eq!(frame[3], 3);
        // Sequence number (42 LE)
        assert_eq!(u64::from_le_bytes(frame[4..12].try_into().unwrap()), 42);
        // Send timestamp
        assert_eq!(
            u64::from_le_bytes(frame[12..20].try_into().unwrap()),
            1_700_000_000_000_000_000
        );
        // Message count
        assert_eq!(frame[20], 1);
        // Reserved
        assert_eq!(frame[21], 0);
        // Frame length
        assert_eq!(u16::from_le_bytes(frame[22..24].try_into().unwrap()), 32);
    }

    #[test]
    fn remaining_tracks_capacity() {
        let mut fb = FrameBuilder::new(0, 0, 0, 100);
        // After header: 100 - 24 = 76 remaining
        assert_eq!(fb.remaining(), 76);
        let buf = fb.message_buffer(60).unwrap();
        buf.copy_from_slice(&[0; 60]);
        fb.commit_message();
        assert_eq!(fb.remaining(), 16);
    }

    #[test]
    fn exceeds_mtu_returns_error() {
        let mut fb = FrameBuilder::new(0, 0, 0, 30);
        // 30 - 24 header = 6 bytes remaining
        let result = fb.message_buffer(10);
        assert_eq!(
            result.unwrap_err(),
            FrameError::ExceedsMtu {
                msg_size: 10,
                remaining: 6,
            }
        );
    }

    #[test]
    fn multiple_messages_in_frame() {
        let mut fb = FrameBuilder::new(0, 0, 0, DEFAULT_MTU);
        for _ in 0..3 {
            let buf = fb.message_buffer(4).unwrap();
            buf.copy_from_slice(&[0xBB; 4]);
            fb.commit_message();
        }
        let frame = fb.finalize();
        assert_eq!(frame.len(), 24 + 3 * 4);
        assert_eq!(frame[20], 3); // msg_count
    }

    #[test]
    fn is_empty_tracks_messages() {
        let mut fb = FrameBuilder::new(0, 0, 0, DEFAULT_MTU);
        assert!(fb.is_empty());
        let buf = fb.message_buffer(4).unwrap();
        buf.copy_from_slice(&[0; 4]);
        fb.commit_message();
        assert!(!fb.is_empty());
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p server protocol::frame`
Expected: All 5 tests pass.

- [ ] **Step 3: Add frame module to protocol/mod.rs**

```rust
// server/src/protocol/mod.rs
pub mod constants;
pub mod frame;
```

- [ ] **Step 4: Run tests again after module wiring**

Run: `cargo test -p server protocol::frame`
Expected: All 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add server/src/protocol/frame.rs server/src/protocol/mod.rs
git commit -m "protocol: add FrameBuilder for accumulating messages into binary frames"
```

---

### Task 3: Implement message encoding

**Files:**
- Create: `server/src/protocol/messages.rs`
- Modify: `server/src/protocol/mod.rs`

- [ ] **Step 1: Write message encoding functions with tests**

```rust
// server/src/protocol/messages.rs

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
            bid_price: 1_062_170, // "106217.0" with exponent -1
            bid_qty: 100,
            ask_price: 1_062_330,
            ask_qty: 267,
            bid_source_count: 1,
            ask_source_count: 3,
        };
        let mut buf = [0u8; QUOTE_SIZE];
        encode_quote(&mut buf, &data, 0);

        // App header
        assert_eq!(buf[0], MSG_TYPE_QUOTE);
        assert_eq!(buf[1], 60);
        assert_eq!(u16::from_le_bytes(buf[2..4].try_into().unwrap()), 0); // flags

        // Instrument ID at offset 4
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), 7);
        // Source ID at offset 8
        assert_eq!(u16::from_le_bytes(buf[8..10].try_into().unwrap()), 1);
        // Update flags at offset 10
        assert_eq!(buf[10], 0x03);
        // Reserved at offset 11
        assert_eq!(buf[11], 0);
        // Source timestamp at offset 12
        assert_eq!(
            u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            1_700_000_000_000_000_000
        );
        // Bid price at offset 20
        assert_eq!(i64::from_le_bytes(buf[20..28].try_into().unwrap()), 1_062_170);
        // Bid qty at offset 28
        assert_eq!(u64::from_le_bytes(buf[28..36].try_into().unwrap()), 100);
        // Ask price at offset 36
        assert_eq!(i64::from_le_bytes(buf[36..44].try_into().unwrap()), 1_062_330);
        // Ask qty at offset 44
        assert_eq!(u64::from_le_bytes(buf[44..52].try_into().unwrap()), 267);
        // Bid source count at offset 52
        assert_eq!(u16::from_le_bytes(buf[52..54].try_into().unwrap()), 1);
        // Ask source count at offset 54
        assert_eq!(u16::from_le_bytes(buf[54..56].try_into().unwrap()), 3);
        // Reserved at offset 56
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
        assert_eq!(buf[4], 5); // channel_id
        assert_eq!(&buf[5..8], &[0; 3]); // reserved
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
```

- [ ] **Step 2: Add messages module to protocol/mod.rs**

```rust
// server/src/protocol/mod.rs
pub mod constants;
pub mod frame;
pub mod messages;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p server protocol::messages`
Expected: All 6 tests pass.

- [ ] **Step 4: Commit**

```bash
git add server/src/protocol/messages.rs server/src/protocol/mod.rs
git commit -m "protocol: add message encoding for Quote, Trade, Heartbeat, ChannelReset, EndOfSession"
```

---

### Task 4: Implement price/quantity string-to-fixed-point conversion

**Files:**
- Create: `server/src/instruments/mod.rs`
- Modify: `server/src/lib.rs`

- [ ] **Step 1: Write InstrumentInfo, conversion functions, and tests**

```rust
// server/src/instruments/mod.rs

pub mod hyperliquid;

use std::collections::HashMap;

/// Metadata for a single instrument, used to encode binary messages.
#[derive(Debug, Clone)]
pub struct InstrumentInfo {
    pub instrument_id: u32,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    /// Null-padded ASCII symbol for future InstrumentDefinition use.
    pub symbol: [u8; 16],
}

/// Maps coin names (e.g., "BTC") to their InstrumentInfo.
#[derive(Debug, Clone)]
pub struct InstrumentRegistry {
    instruments: HashMap<String, InstrumentInfo>,
}

impl InstrumentRegistry {
    pub fn new(instruments: HashMap<String, InstrumentInfo>) -> Self {
        Self { instruments }
    }

    pub fn get(&self, coin: &str) -> Option<&InstrumentInfo> {
        self.instruments.get(coin)
    }

    pub fn len(&self) -> usize {
        self.instruments.len()
    }
}

/// Converts a decimal string price to a fixed-point i64 using the given exponent.
///
/// Example: `"106217.0"` with exponent `-1` → `1062170_i64`
///
/// The exponent is negative: it represents how many decimal places the wire value has.
/// We multiply the float by `10^(-exponent)` to get the integer.
pub fn price_to_fixed(price_str: &str, exponent: i8) -> Option<i64> {
    let value: f64 = price_str.parse().ok()?;
    let multiplier = 10f64.powi(-exponent as i32);
    Some((value * multiplier).round() as i64)
}

/// Converts a decimal string quantity to a fixed-point u64 using the given exponent.
///
/// Example: `"0.00017"` with exponent `-5` → `17_u64`
pub fn qty_to_fixed(qty_str: &str, exponent: i8) -> Option<u64> {
    let value: f64 = qty_str.parse().ok()?;
    let multiplier = 10f64.powi(-exponent as i32);
    let result = (value * multiplier).round() as i64;
    if result < 0 {
        return None;
    }
    Some(result as u64)
}

/// Creates a null-padded 16-byte ASCII symbol from a string.
/// Truncates if longer than 16 bytes.
pub fn make_symbol(name: &str) -> [u8; 16] {
    let mut sym = [0u8; 16];
    let bytes = name.as_bytes();
    let len = bytes.len().min(16);
    sym[..len].copy_from_slice(&bytes[..len]);
    sym
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_to_fixed_basic() {
        // "106217.0" with exponent -1 → multiply by 10 → 1062170
        assert_eq!(price_to_fixed("106217.0", -1), Some(1_062_170));
    }

    #[test]
    fn price_to_fixed_two_decimals() {
        // "106217.50" with exponent -2 → multiply by 100 → 10621750
        assert_eq!(price_to_fixed("106217.50", -2), Some(10_621_750));
    }

    #[test]
    fn price_to_fixed_whole_number() {
        // "100" with exponent 0 → multiply by 1 → 100
        assert_eq!(price_to_fixed("100", 0), Some(100));
    }

    #[test]
    fn qty_to_fixed_basic() {
        // "0.00017" with exponent -5 → multiply by 100000 → 17
        assert_eq!(qty_to_fixed("0.00017", -5), Some(17));
    }

    #[test]
    fn qty_to_fixed_whole() {
        // "1.5" with exponent -1 → multiply by 10 → 15
        assert_eq!(qty_to_fixed("1.5", -1), Some(15));
    }

    #[test]
    fn qty_to_fixed_zero() {
        assert_eq!(qty_to_fixed("0", -5), Some(0));
    }

    #[test]
    fn price_to_fixed_invalid_string() {
        assert_eq!(price_to_fixed("not_a_number", -1), None);
    }

    #[test]
    fn make_symbol_short() {
        let sym = make_symbol("BTC");
        assert_eq!(&sym[..3], b"BTC");
        assert_eq!(&sym[3..], &[0; 13]);
    }

    #[test]
    fn make_symbol_exact_16() {
        let sym = make_symbol("1234567890123456");
        assert_eq!(&sym, b"1234567890123456");
    }

    #[test]
    fn make_symbol_truncates_long() {
        let sym = make_symbol("12345678901234567890");
        assert_eq!(&sym, b"1234567890123456");
    }

    #[test]
    fn registry_lookup() {
        let mut map = HashMap::new();
        map.insert(
            "BTC".to_string(),
            InstrumentInfo {
                instrument_id: 0,
                price_exponent: -1,
                qty_exponent: -5,
                symbol: make_symbol("BTC"),
            },
        );
        let reg = InstrumentRegistry::new(map);
        assert!(reg.get("BTC").is_some());
        assert_eq!(reg.get("BTC").unwrap().instrument_id, 0);
        assert!(reg.get("ETH").is_none());
        assert_eq!(reg.len(), 1);
    }
}
```

- [ ] **Step 2: Add instruments module to lib.rs**

In `server/src/lib.rs`, add:
```rust
pub mod instruments;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p server instruments`
Expected: All 11 tests pass.

- [ ] **Step 4: Commit**

```bash
git add server/src/instruments/mod.rs server/src/lib.rs
git commit -m "instruments: add InstrumentRegistry, price/qty conversion, and symbol encoding"
```

---

### Task 5: Implement Hyperliquid meta API bootstrap

**Files:**
- Create: `server/src/instruments/hyperliquid.rs`

- [ ] **Step 1: Write the HL API bootstrap with tests**

```rust
// server/src/instruments/hyperliquid.rs

use log::{info, warn};
use std::collections::HashMap;

use super::{InstrumentInfo, make_symbol};

/// Fetches perpetual and spot metadata from the Hyperliquid REST API and builds
/// an InstrumentInfo map keyed by coin name.
///
/// Perp instruments get instrument_id = array index.
/// Spot instruments get instrument_id = 10000 + array index.
pub async fn bootstrap_registry(
    api_url: &str,
) -> Result<HashMap<String, InstrumentInfo>, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let mut instruments = HashMap::new();

    // Fetch perp meta
    let perp_resp = client
        .post(format!("{api_url}/info"))
        .json(&serde_json::json!({"type": "meta"}))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    if let Some(universe) = perp_resp.get("universe").and_then(|v| v.as_array()) {
        for (idx, asset) in universe.iter().enumerate() {
            if let Some(info) = parse_perp_asset(asset, idx as u32) {
                info!(
                    "instruments: perp {} -> id={}, price_exp={}, qty_exp={}",
                    info.0, info.1.instrument_id, info.1.price_exponent, info.1.qty_exponent
                );
                instruments.insert(info.0, info.1);
            }
        }
    }

    // Fetch spot meta
    let spot_resp = client
        .post(format!("{api_url}/info"))
        .json(&serde_json::json!({"type": "spotMeta"}))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    if let Some(universe) = spot_resp.get("universe").and_then(|v| v.as_array()) {
        for (idx, asset) in universe.iter().enumerate() {
            if let Some(info) = parse_spot_asset(asset, idx as u32) {
                info!(
                    "instruments: spot {} -> id={}, price_exp={}, qty_exp={}",
                    info.0, info.1.instrument_id, info.1.price_exponent, info.1.qty_exponent
                );
                instruments.insert(info.0, info.1);
            }
        }
    }

    info!("instruments: loaded {} instruments ({} perps, {} spot)",
        instruments.len(),
        instruments.values().filter(|i| i.instrument_id < 10000).count(),
        instruments.values().filter(|i| i.instrument_id >= 10000).count(),
    );

    Ok(instruments)
}

fn parse_perp_asset(asset: &serde_json::Value, index: u32) -> Option<(String, InstrumentInfo)> {
    let name = asset.get("name")?.as_str()?;
    let sz_decimals = asset.get("szDecimals")?.as_u64()? as i8;
    let qty_exponent = -sz_decimals;

    // Derive price exponent from the number of significant price decimals.
    // HL does not expose this directly in meta, so we use a heuristic:
    // most perps use whole-number or 1-decimal pricing for large-cap,
    // and more decimals for small-cap. We'll parse from szDecimals and
    // common patterns. A more robust approach would parse the tick size
    // from the exchange's trading rules.
    //
    // For now, derive from the maxDecimals or default to a reasonable value.
    let price_exponent = derive_price_exponent(asset);

    Some((
        name.to_string(),
        InstrumentInfo {
            instrument_id: index,
            price_exponent,
            qty_exponent,
            symbol: make_symbol(name),
        },
    ))
}

fn parse_spot_asset(asset: &serde_json::Value, index: u32) -> Option<(String, InstrumentInfo)> {
    let name = asset.get("name")?.as_str()?;
    let sz_decimals = asset.get("szDecimals")?.as_u64()? as i8;
    let qty_exponent = -sz_decimals;
    let price_exponent = derive_price_exponent(asset);

    // Spot coins in the order book use @ prefix (e.g., "@107" for PURR/USDC).
    // The meta response uses the display name. We store under the name the
    // order book uses. The HL node data uses token indices like @107.
    // We'll store both the display name and any token reference.
    let tokens = asset.get("tokens");
    let token_index = tokens
        .and_then(|t| t.as_array())
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_u64());

    // Use the coin name from the order book: for spot it's typically the display name
    // but the node uses index-based names. Store under display name; the publisher
    // will need to handle the mapping.
    Some((
        name.to_string(),
        InstrumentInfo {
            instrument_id: 10_000 + index,
            price_exponent,
            qty_exponent,
            symbol: make_symbol(name),
        },
    ))
}

/// Derives the price exponent from asset metadata.
///
/// Looks for a "maxDecimals" or similar field; falls back to a
/// heuristic based on szDecimals. The goal is to determine how many
/// decimal places prices have for this instrument.
fn derive_price_exponent(asset: &serde_json::Value) -> i8 {
    // Try explicit field first (some HL responses include this)
    if let Some(max_decimals) = asset.get("maxDecimals").and_then(|v| v.as_u64()) {
        return -(max_decimals as i8);
    }

    // Heuristic: szDecimals often correlates inversely with price magnitude.
    // Assets with szDecimals=0 (like BTC) typically have 1 price decimal.
    // Assets with szDecimals=2 might have 4-6 price decimals.
    // Default to -8 (8 decimal places) as a safe fallback — this accommodates
    // all HL assets without loss of precision at the cost of larger integer values.
    // This matches the internal MULTIPLIER (10^8) used in the order book.
    -8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_perp_asset_basic() {
        let asset = serde_json::json!({
            "name": "BTC",
            "szDecimals": 5
        });
        let (name, info) = parse_perp_asset(&asset, 0).unwrap();
        assert_eq!(name, "BTC");
        assert_eq!(info.instrument_id, 0);
        assert_eq!(info.qty_exponent, -5);
        assert_eq!(&info.symbol[..3], b"BTC");
    }

    #[test]
    fn parse_perp_asset_preserves_index() {
        let asset = serde_json::json!({
            "name": "ETH",
            "szDecimals": 4
        });
        let (name, info) = parse_perp_asset(&asset, 3).unwrap();
        assert_eq!(name, "ETH");
        assert_eq!(info.instrument_id, 3);
        assert_eq!(info.qty_exponent, -4);
    }

    #[test]
    fn parse_spot_asset_offset_id() {
        let asset = serde_json::json!({
            "name": "PURR/USDC",
            "szDecimals": 2,
            "tokens": [107, 0]
        });
        let (name, info) = parse_spot_asset(&asset, 5).unwrap();
        assert_eq!(name, "PURR/USDC");
        assert_eq!(info.instrument_id, 10_005);
        assert_eq!(info.qty_exponent, -2);
    }

    #[test]
    fn parse_perp_asset_missing_name_returns_none() {
        let asset = serde_json::json!({"szDecimals": 5});
        assert!(parse_perp_asset(&asset, 0).is_none());
    }

    #[test]
    fn parse_perp_asset_missing_sz_decimals_returns_none() {
        let asset = serde_json::json!({"name": "BTC"});
        assert!(parse_perp_asset(&asset, 0).is_none());
    }

    #[test]
    fn derive_price_exponent_default_fallback() {
        let asset = serde_json::json!({"name": "BTC", "szDecimals": 5});
        assert_eq!(derive_price_exponent(&asset), -8);
    }

    #[test]
    fn derive_price_exponent_explicit_max_decimals() {
        let asset = serde_json::json!({"name": "BTC", "szDecimals": 5, "maxDecimals": 1});
        assert_eq!(derive_price_exponent(&asset), -1);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p server instruments::hyperliquid`
Expected: All 7 tests pass.

- [ ] **Step 3: Commit**

```bash
git add server/src/instruments/hyperliquid.rs
git commit -m "instruments: add Hyperliquid meta API bootstrap for instrument registry"
```

---

### Task 6: Update multicast config

**Files:**
- Modify: `server/src/multicast/config.rs`

- [ ] **Step 1: Replace config.rs contents**

Remove the `Channel` enum, `parse_channels`, and `l2_levels`. Add `mtu`, `source_id`, and `heartbeat_interval`. The new config:

```rust
// server/src/multicast/config.rs

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

/// Configuration for UDP multicast market data distribution.
#[derive(Debug, Clone)]
pub struct MulticastConfig {
    /// Multicast group address to join.
    pub group_addr: Ipv4Addr,
    /// UDP port for marketdata multicast traffic.
    pub port: u16,
    /// Local address to bind the socket to.
    pub bind_addr: Ipv4Addr,
    /// How often to send full BBO snapshots.
    pub snapshot_interval: Duration,
    /// Max frame size in bytes (default 1448 for GRE tunnels).
    pub mtu: u16,
    /// Source ID for Quote/Trade messages.
    pub source_id: u16,
    /// How long to wait with no data before sending a Heartbeat.
    pub heartbeat_interval: Duration,
    /// Hyperliquid REST API URL for instrument metadata.
    pub hl_api_url: String,
}

impl MulticastConfig {
    /// Returns the destination socket address (group address + port).
    #[must_use]
    pub fn dest(&self) -> SocketAddr {
        SocketAddr::from((self.group_addr, self.port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dest_returns_correct_socket_addr() {
        let config = MulticastConfig {
            group_addr: Ipv4Addr::new(239, 0, 0, 1),
            port: 5000,
            bind_addr: Ipv4Addr::UNSPECIFIED,
            snapshot_interval: Duration::from_secs(5),
            mtu: 1448,
            source_id: 1,
            heartbeat_interval: Duration::from_secs(5),
            hl_api_url: "https://api.hyperliquid.xyz".to_string(),
        };
        let addr = config.dest();
        assert_eq!(addr, SocketAddr::from((Ipv4Addr::new(239, 0, 0, 1), 5000)));
    }
}
```

- [ ] **Step 2: Update multicast/mod.rs**

The existing `mod.rs` is fine — it already exports `config` and `publisher`. No changes needed.

- [ ] **Step 3: Verify it compiles (it won't yet — publisher.rs and lib.rs reference old types)**

Run: `cargo check -p server`
Expected: Compile errors in publisher.rs and lib.rs referencing `Channel`, `parse_channels`, `l2_levels`. This is expected — we'll fix those in subsequent tasks.

- [ ] **Step 4: Commit**

```bash
git add server/src/multicast/config.rs
git commit -m "multicast: simplify config, remove Channel enum, add mtu/source_id/heartbeat"
```

---

### Task 7: Update lib.rs exports

**Files:**
- Modify: `server/src/lib.rs`

- [ ] **Step 1: Update lib.rs**

Replace the current lib.rs with updated exports. Remove `Channel`, `parse_channels` re-exports. Add `instruments` and `protocol` modules.

```rust
// server/src/lib.rs
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod listeners;
pub mod multicast;
mod order_book;
mod prelude;
pub mod protocol;
pub mod instruments;
mod servers;
mod types;

pub use multicast::config::MulticastConfig;
pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;

pub const HL_NODE: &str = "hl-node";
```

- [ ] **Step 2: Verify it compiles (publisher.rs still references old types)**

Run: `cargo check -p server 2>&1 | head -20`
Expected: Errors only in `publisher.rs` (old JSON types) — not in lib.rs, config.rs, protocol/, or instruments/.

- [ ] **Step 3: Commit**

```bash
git add server/src/lib.rs
git commit -m "lib: update module exports for binary protocol and instruments"
```

---

### Task 8: Rewrite publisher for binary protocol

**Files:**
- Modify: `server/src/multicast/publisher.rs`
- Modify: `server/Cargo.toml`

This is the largest task. We replace all JSON serialization with binary frame construction using the protocol module.

- [ ] **Step 1: Remove uuid dependency from server/Cargo.toml**

Remove the `uuid` line from `[dependencies]` in `server/Cargo.toml`:
```
uuid = { version = "1", features = ["v4"] }
```

- [ ] **Step 2: Rewrite publisher.rs**

```rust
// server/src/multicast/publisher.rs

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use log::{info, warn};
use tokio::net::UdpSocket;

use crate::instruments::{InstrumentRegistry, price_to_fixed, qty_to_fixed};
use crate::multicast::config::MulticastConfig;
use crate::protocol::constants::*;
use crate::protocol::frame::{FrameBuilder, FrameError};
use crate::protocol::messages::*;
use crate::types::L2Book;

pub(crate) struct MulticastPublisher {
    socket: UdpSocket,
    config: MulticastConfig,
    registry: InstrumentRegistry,
    seq: AtomicU64,
}

impl MulticastPublisher {
    pub(crate) fn new(
        socket: UdpSocket,
        config: MulticastConfig,
        registry: InstrumentRegistry,
    ) -> Self {
        Self {
            socket,
            config,
            registry,
            seq: AtomicU64::new(0),
        }
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    fn now_ns() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }

    /// Sends a single frame over the UDP socket.
    async fn send_frame(&self, frame: &[u8]) {
        if let Err(err) = self.socket.send_to(frame, self.config.dest()).await {
            warn!("multicast: failed to send frame: {err}");
        }
    }

    /// Sends a ChannelReset message as a standalone frame.
    async fn send_channel_reset(&self) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(CHANNEL_RESET_SIZE).expect("ChannelReset fits in empty frame");
        encode_channel_reset(buf, Self::now_ns());
        fb.commit_message();
        self.send_frame(fb.finalize()).await;
    }

    /// Sends a Heartbeat message as a standalone frame.
    async fn send_heartbeat(&self) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(HEARTBEAT_SIZE).expect("Heartbeat fits in empty frame");
        encode_heartbeat(buf, 0, Self::now_ns());
        fb.commit_message();
        self.send_frame(fb.finalize()).await;
    }

    /// Sends an EndOfSession message as a standalone frame.
    async fn send_end_of_session(&self) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
        let buf = fb.message_buffer(END_OF_SESSION_SIZE).expect("EndOfSession fits in empty frame");
        encode_end_of_session(buf, Self::now_ns());
        fb.commit_message();
        self.send_frame(fb.finalize()).await;
    }

    /// Encodes L2 snapshots as Quote messages, batching into frames.
    async fn publish_quotes(
        &self,
        snapshot_map: &HashMap<crate::order_book::Coin, HashMap<crate::listeners::order_book::L2SnapshotParams, crate::order_book::Snapshot<crate::types::inner::InnerLevel>>>,
        time: u64,
        is_snapshot: bool,
    ) {
        let flags = if is_snapshot { FLAG_SNAPSHOT } else { 0 };
        let source_timestamp_ns = time * 1_000_000; // HL time is ms

        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);

        let default_params = crate::listeners::order_book::L2SnapshotParams::new(None, None);

        for (coin, params_map) in snapshot_map {
            let coin_name = coin.value();
            let Some(inst) = self.registry.get(&coin_name) else {
                continue;
            };

            let Some(snapshot) = params_map.get(&default_params) else {
                continue;
            };

            // Extract BBO
            let levels = snapshot.truncate(1).export_inner_snapshot();
            let bids = &levels[0];
            let asks = &levels[1];

            let (bid_price, bid_qty, bid_n) = if let Some(level) = bids.first() {
                let px = price_to_fixed(&level.px(), inst.price_exponent).unwrap_or(0);
                let qty = qty_to_fixed(&level.sz(), inst.qty_exponent).unwrap_or(0);
                (px, qty, level.n() as u16)
            } else {
                (0, 0, 0)
            };

            let (ask_price, ask_qty, ask_n) = if let Some(level) = asks.first() {
                let px = price_to_fixed(&level.px(), inst.price_exponent).unwrap_or(0);
                let qty = qty_to_fixed(&level.sz(), inst.qty_exponent).unwrap_or(0);
                (px, qty, level.n() as u16)
            } else {
                (0, 0, 0)
            };

            let mut update_flags = 0u8;
            if bids.is_empty() {
                update_flags |= UPDATE_FLAG_BID_GONE;
            } else {
                update_flags |= UPDATE_FLAG_BID_UPDATED;
            }
            if asks.is_empty() {
                update_flags |= UPDATE_FLAG_ASK_GONE;
            } else {
                update_flags |= UPDATE_FLAG_ASK_UPDATED;
            }

            let quote = QuoteData {
                instrument_id: inst.instrument_id,
                source_id: self.config.source_id,
                update_flags,
                source_timestamp_ns,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                bid_source_count: bid_n,
                ask_source_count: ask_n,
            };

            match fb.message_buffer(QUOTE_SIZE) {
                Ok(buf) => {
                    encode_quote(buf, &quote, flags);
                    fb.commit_message();
                }
                Err(FrameError::ExceedsMtu { .. }) => {
                    // Flush current frame and start a new one
                    if !fb.is_empty() {
                        self.send_frame(fb.finalize()).await;
                    }
                    fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
                    let buf = fb.message_buffer(QUOTE_SIZE).expect("Quote fits in empty frame");
                    encode_quote(buf, &quote, flags);
                    fb.commit_message();
                }
                Err(FrameError::MaxMessages) => {
                    self.send_frame(fb.finalize()).await;
                    fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
                    let buf = fb.message_buffer(QUOTE_SIZE).expect("Quote fits in empty frame");
                    encode_quote(buf, &quote, flags);
                    fb.commit_message();
                }
            }
        }

        if !fb.is_empty() {
            self.send_frame(fb.finalize()).await;
        }
    }

    /// Encodes fills as Trade messages, batching into frames.
    async fn publish_trades(
        &self,
        trades_by_coin: &HashMap<String, Vec<crate::types::Trade>>,
    ) {
        let mut fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);

        for trades in trades_by_coin.values() {
            for trade in trades {
                let Some(inst) = self.registry.get(&trade.coin) else {
                    continue;
                };

                let aggressor_side = match trade.side {
                    crate::order_book::types::Side::Ask => AGGRESSOR_SELL,
                    crate::order_book::types::Side::Bid => AGGRESSOR_BUY,
                };

                let trade_data = TradeData {
                    instrument_id: inst.instrument_id,
                    source_id: self.config.source_id,
                    aggressor_side,
                    trade_flags: 0,
                    source_timestamp_ns: trade.time * 1_000_000,
                    trade_price: price_to_fixed(&trade.px, inst.price_exponent).unwrap_or(0),
                    trade_qty: qty_to_fixed(&trade.sz, inst.qty_exponent).unwrap_or(0),
                    trade_id: trade.tid,
                    cumulative_volume: 0,
                };

                match fb.message_buffer(TRADE_SIZE) {
                    Ok(buf) => {
                        encode_trade(buf, &trade_data, 0);
                        fb.commit_message();
                    }
                    Err(FrameError::ExceedsMtu { .. } | FrameError::MaxMessages) => {
                        if !fb.is_empty() {
                            self.send_frame(fb.finalize()).await;
                        }
                        fb = FrameBuilder::new(0, self.next_seq(), Self::now_ns(), self.config.mtu);
                        let buf = fb.message_buffer(TRADE_SIZE).expect("Trade fits in empty frame");
                        encode_trade(buf, &trade_data, 0);
                        fb.commit_message();
                    }
                }
            }
        }

        if !fb.is_empty() {
            self.send_frame(fb.finalize()).await;
        }
    }

    /// Main loop: subscribes to the broadcast channel and publishes binary frames.
    pub(crate) async fn run(
        &self,
        mut rx: tokio::sync::broadcast::Receiver<std::sync::Arc<crate::listeners::order_book::InternalMessage>>,
    ) {
        use crate::listeners::order_book::InternalMessage;

        info!(
            "multicast publisher started: group={} port={} mtu={} source_id={}",
            self.config.group_addr, self.config.port, self.config.mtu, self.config.source_id,
        );

        // Send ChannelReset on startup
        self.send_channel_reset().await;

        let mut snapshot_interval = tokio::time::interval(self.config.snapshot_interval);
        snapshot_interval.tick().await; // consume immediate first tick

        let mut heartbeat_interval = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat_interval.tick().await;

        // Cache the most recent L2 snapshots for periodic resends
        let mut cached_snapshots: HashMap<
            crate::order_book::Coin,
            HashMap<crate::listeners::order_book::L2SnapshotParams, crate::order_book::Snapshot<crate::types::inner::InnerLevel>>,
        > = HashMap::new();
        let mut cached_time: u64 = 0;
        let mut had_activity = false;

        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(msg) => match msg.as_ref() {
                            InternalMessage::Snapshot { l2_snapshots, time } => {
                                let snapshot_map = l2_snapshots.as_ref();
                                cached_snapshots = snapshot_map.clone();
                                cached_time = *time;
                                self.publish_quotes(snapshot_map, *time, false).await;
                                had_activity = true;
                                heartbeat_interval.reset();
                            }
                            InternalMessage::Fills { batch } => {
                                let trades_by_coin = crate::servers::websocket_server::coin_to_trades(batch);
                                self.publish_trades(&trades_by_coin).await;
                                had_activity = true;
                                heartbeat_interval.reset();
                            }
                            InternalMessage::L4BookUpdates { .. } => {
                                // Ignored for multicast
                            }
                        },
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("multicast: broadcast receiver lagged by {n} messages");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            warn!("multicast: broadcast channel closed, sending EndOfSession");
                            self.send_end_of_session().await;
                            return;
                        }
                    }
                }
                _ = snapshot_interval.tick() => {
                    if !cached_snapshots.is_empty() {
                        self.publish_quotes(&cached_snapshots, cached_time, true).await;
                        had_activity = true;
                        heartbeat_interval.reset();
                    }
                }
                _ = heartbeat_interval.tick() => {
                    if !had_activity {
                        self.send_heartbeat().await;
                    }
                    had_activity = false;
                }
            }
        }
    }
}
```

- [ ] **Step 3: Check that the Level type has the accessors we need**

The `publish_quotes` method calls `level.px()`, `level.sz()`, and `level.n()` on the inner level type. Check that these accessor methods exist on the `Level` type returned by `export_inner_snapshot()`. The `Level` struct in `types/mod.rs` has fields `px: String`, `sz: String`, `n: usize` — but they're not `pub`. If needed, add accessor methods.

Check `server/src/types/mod.rs` — the `Level` struct fields are private. We need to add public accessors:

```rust
impl Level {
    pub(crate) fn px(&self) -> &str {
        &self.px
    }

    pub(crate) fn sz(&self) -> &str {
        &self.sz
    }

    pub(crate) fn n(&self) -> usize {
        self.n
    }
}
```

Add these methods to the existing `impl Level` block in `server/src/types/mod.rs`, after the existing `new` method.

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p server`
Expected: Compiles with no errors (or only warnings).

- [ ] **Step 5: Commit**

```bash
git add server/src/multicast/publisher.rs server/Cargo.toml server/src/types/mod.rs
git commit -m "multicast: rewrite publisher for binary protocol encoding"
```

---

### Task 9: Update CLI and server startup

**Files:**
- Modify: `binaries/src/bin/websocket_server.rs`
- Modify: `server/src/servers/websocket_server.rs`

- [ ] **Step 1: Update websocket_server.rs binary (CLI args)**

```rust
// binaries/src/bin/websocket_server.rs
#![allow(unused_crate_dependencies)]
use std::net::Ipv4Addr;
use std::time::Duration;

use clap::Parser;
use server::{MulticastConfig, Result, run_websocket_server};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long)]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long)]
    port: u16,

    /// Compression level for WebSocket connections (0-9, default 1).
    #[arg(long)]
    websocket_compression_level: Option<u32>,

    /// Multicast group address (e.g., 239.0.0.1). Enables multicast when set.
    #[arg(long)]
    multicast_group: Option<Ipv4Addr>,

    /// UDP port for multicast traffic.
    #[arg(long, default_value_t = 5000)]
    multicast_port: u16,

    /// Local address to bind the multicast UDP socket.
    #[arg(long)]
    multicast_bind_addr: Option<Ipv4Addr>,

    /// Interval in seconds between full BBO snapshot resends.
    #[arg(long, default_value_t = 5)]
    multicast_snapshot_interval: u64,

    /// Max frame size in bytes (default 1448 for GRE tunnels).
    #[arg(long, default_value_t = 1448)]
    multicast_mtu: u16,

    /// Hyperliquid REST API URL for instrument metadata.
    #[arg(long, default_value = "https://api.hyperliquid.xyz")]
    hl_api_url: String,

    /// Source ID for Quote/Trade messages.
    #[arg(long, default_value_t = 1)]
    source_id: u16,

    /// Seconds of silence before sending a Heartbeat.
    #[arg(long, default_value_t = 5)]
    heartbeat_interval: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let compression_level = args.websocket_compression_level.unwrap_or(1);

    let multicast_config = if let Some(group_addr) = args.multicast_group {
        let Some(bind_addr) = args.multicast_bind_addr else {
            eprintln!("error: --multicast-bind-addr is required when --multicast-group is set");
            std::process::exit(1);
        };
        Some(MulticastConfig {
            group_addr,
            port: args.multicast_port,
            bind_addr,
            snapshot_interval: Duration::from_secs(args.multicast_snapshot_interval),
            mtu: args.multicast_mtu,
            source_id: args.source_id,
            heartbeat_interval: Duration::from_secs(args.heartbeat_interval),
            hl_api_url: args.hl_api_url,
        })
    } else {
        None
    };

    run_websocket_server(&full_address, true, compression_level, multicast_config).await?;

    Ok(())
}
```

- [ ] **Step 2: Update server startup to bootstrap instrument registry**

In `server/src/servers/websocket_server.rs`, update the multicast spawn block. Replace the existing `if let Some(mcast_config) = multicast_config { ... }` block with:

```rust
    if let Some(mcast_config) = multicast_config {
        let mcast_rx = internal_message_tx.subscribe();
        tokio::spawn(async move {
            // Bootstrap instrument registry from HL API
            let instruments = match crate::instruments::hyperliquid::bootstrap_registry(&mcast_config.hl_api_url).await {
                Ok(instruments) => instruments,
                Err(err) => {
                    log::error!("failed to bootstrap instrument registry: {err}");
                    std::process::exit(3);
                }
            };
            let registry = crate::instruments::InstrumentRegistry::new(instruments);
            info!("instrument registry loaded: {} instruments", registry.len());

            let bind_addr = std::net::SocketAddr::new(std::net::IpAddr::V4(mcast_config.bind_addr), 0);
            match tokio::net::UdpSocket::bind(bind_addr).await {
                Ok(socket) => {
                    if let Err(err) = socket.set_multicast_ttl_v4(64) {
                        log::error!("failed to set multicast TTL: {err}");
                        std::process::exit(3);
                    }
                    let publisher = MulticastPublisher::new(socket, mcast_config, registry);
                    publisher.run(mcast_rx).await;
                }
                Err(err) => {
                    log::error!("failed to bind multicast UDP socket to {bind_addr}: {err}");
                    std::process::exit(3);
                }
            }
        });
    }
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compiles with no errors.

- [ ] **Step 4: Commit**

```bash
git add binaries/src/bin/websocket_server.rs server/src/servers/websocket_server.rs
git commit -m "cli: update args for binary protocol, bootstrap instrument registry at startup"
```

---

### Task 10: Update example multicast subscriber

**Files:**
- Modify: `binaries/src/bin/example_multicast_subscriber.rs`

- [ ] **Step 1: Rewrite subscriber to decode binary frames**

```rust
// binaries/src/bin/example_multicast_subscriber.rs
#![allow(unused_crate_dependencies)]
use std::net::{Ipv4Addr, SocketAddrV4};

use clap::Parser;
use server::protocol::constants::*;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

#[derive(Debug, Parser)]
#[command(author, version, about = "Join a multicast group and decode binary DZ frames")]
struct Args {
    /// Multicast group IP to join (e.g., 239.0.0.1)
    #[arg(long)]
    group: Ipv4Addr,

    /// UDP port to listen on
    #[arg(long, default_value_t = 5000)]
    port: u16,

    /// Local interface address to bind for multicast membership
    #[arg(long, default_value_t = Ipv4Addr::UNSPECIFIED)]
    interface: Ipv4Addr,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, args.port).into())?;
    socket.join_multicast_v4(&args.group, &args.interface)?;
    socket.set_nonblocking(true)?;

    let std_socket: std::net::UdpSocket = socket.into();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(recv_loop(std_socket))?;

    Ok(())
}

async fn recv_loop(std_socket: std::net::UdpSocket) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::from_std(std_socket)?;

    println!("listening for DZ binary frames on port {} ...", socket.local_addr()?.port());

    let mut buf = [0u8; 2048];

    loop {
        let (len, addr) = socket.recv_from(&mut buf).await?;
        let frame = &buf[..len];

        if len < FRAME_HEADER_SIZE {
            println!("[from {addr}] short frame ({len} bytes), skipping");
            continue;
        }

        // Parse frame header
        let magic = u16::from_le_bytes([frame[0], frame[1]]);
        if magic != MAGIC {
            println!("[from {addr}] bad magic: 0x{magic:04X}, skipping");
            continue;
        }

        let schema_ver = frame[2];
        let channel_id = frame[3];
        let seq = u64::from_le_bytes(frame[4..12].try_into().unwrap());
        let send_ts = u64::from_le_bytes(frame[12..20].try_into().unwrap());
        let msg_count = frame[20];
        let frame_len = u16::from_le_bytes(frame[22..24].try_into().unwrap());

        println!(
            "[from {addr}] frame: ver={schema_ver} ch={channel_id} seq={seq} ts={send_ts} msgs={msg_count} len={frame_len}"
        );

        // Parse each message
        let mut offset = FRAME_HEADER_SIZE;
        for i in 0..msg_count {
            if offset + APP_MSG_HEADER_SIZE > len {
                println!("  msg[{i}]: truncated header");
                break;
            }
            let msg_type = frame[offset];
            let msg_len = frame[offset + 1] as usize;
            let flags = u16::from_le_bytes(frame[offset + 2..offset + 4].try_into().unwrap());

            if offset + msg_len > len {
                println!("  msg[{i}]: truncated body (need {msg_len}, have {})", len - offset);
                break;
            }

            let body = &frame[offset..offset + msg_len];
            match msg_type {
                MSG_TYPE_QUOTE => {
                    let inst_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
                    let src_id = u16::from_le_bytes(body[8..10].try_into().unwrap());
                    let upd_flags = body[10];
                    let src_ts = u64::from_le_bytes(body[12..20].try_into().unwrap());
                    let bid_px = i64::from_le_bytes(body[20..28].try_into().unwrap());
                    let bid_qty = u64::from_le_bytes(body[28..36].try_into().unwrap());
                    let ask_px = i64::from_le_bytes(body[36..44].try_into().unwrap());
                    let ask_qty = u64::from_le_bytes(body[44..52].try_into().unwrap());
                    let bid_n = u16::from_le_bytes(body[52..54].try_into().unwrap());
                    let ask_n = u16::from_le_bytes(body[54..56].try_into().unwrap());
                    println!(
                        "  Quote: inst={inst_id} src={src_id} flags=0x{upd_flags:02X} snap={} \
                         bid={bid_px}({bid_n}) ask={ask_px}({ask_n}) bid_qty={bid_qty} ask_qty={ask_qty} ts={src_ts}",
                        flags & FLAG_SNAPSHOT != 0
                    );
                }
                MSG_TYPE_TRADE => {
                    let inst_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
                    let src_id = u16::from_le_bytes(body[8..10].try_into().unwrap());
                    let side = body[10];
                    let src_ts = u64::from_le_bytes(body[12..20].try_into().unwrap());
                    let px = i64::from_le_bytes(body[20..28].try_into().unwrap());
                    let qty = u64::from_le_bytes(body[28..36].try_into().unwrap());
                    let tid = u64::from_le_bytes(body[36..44].try_into().unwrap());
                    let side_str = match side {
                        AGGRESSOR_BUY => "BUY",
                        AGGRESSOR_SELL => "SELL",
                        _ => "UNK",
                    };
                    println!(
                        "  Trade: inst={inst_id} src={src_id} side={side_str} px={px} qty={qty} tid={tid} ts={src_ts}"
                    );
                }
                MSG_TYPE_HEARTBEAT => {
                    let ts = u64::from_le_bytes(body[8..16].try_into().unwrap());
                    println!("  Heartbeat: ts={ts}");
                }
                MSG_TYPE_CHANNEL_RESET => {
                    let ts = u64::from_le_bytes(body[4..12].try_into().unwrap());
                    println!("  ChannelReset: ts={ts}");
                }
                MSG_TYPE_END_OF_SESSION => {
                    let ts = u64::from_le_bytes(body[4..12].try_into().unwrap());
                    println!("  EndOfSession: ts={ts}");
                }
                _ => {
                    println!("  msg[{i}]: unknown type=0x{msg_type:02X} len={msg_len}");
                }
            }

            offset += msg_len;
        }
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check --bin example_multicast_subscriber`
Expected: Compiles with no errors.

- [ ] **Step 3: Commit**

```bash
git add binaries/src/bin/example_multicast_subscriber.rs
git commit -m "subscriber: decode binary DZ frames instead of JSON"
```

---

### Task 11: Integration test — end-to-end loopback

**Files:**
- Modify: `server/src/multicast/publisher.rs` (add test module)

- [ ] **Step 1: Add a test constructor and loopback integration test**

Add to `server/src/multicast/publisher.rs` at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::time::Duration;

    fn test_config() -> MulticastConfig {
        MulticastConfig {
            group_addr: Ipv4Addr::LOCALHOST,
            port: 0,
            bind_addr: Ipv4Addr::LOCALHOST,
            snapshot_interval: Duration::from_secs(60),
            mtu: DEFAULT_MTU,
            source_id: 1,
            heartbeat_interval: Duration::from_secs(60),
            hl_api_url: String::new(),
        }
    }

    fn test_registry() -> InstrumentRegistry {
        let mut map = HashMap::new();
        map.insert(
            "BTC".to_string(),
            crate::instruments::InstrumentInfo {
                instrument_id: 0,
                price_exponent: -8,
                qty_exponent: -8,
                symbol: crate::instruments::make_symbol("BTC"),
            },
        );
        InstrumentRegistry::new(map)
    }

    #[tokio::test]
    async fn sends_channel_reset_on_startup_and_sequences() {
        let send_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();

        let mut config = test_config();
        config.port = recv_addr.port();

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());

        // Send a ChannelReset
        publisher.send_channel_reset().await;

        let mut buf = [0u8; 2048];
        let n = tokio::time::timeout(Duration::from_secs(2), recv_socket.recv(&mut buf))
            .await
            .expect("timed out")
            .expect("recv failed");

        let frame = &buf[..n];
        // Verify frame header
        assert_eq!(&frame[0..2], &MAGIC_BYTES);
        assert_eq!(frame[2], SCHEMA_VERSION);
        assert_eq!(frame[20], 1); // msg_count = 1
        // Verify message is ChannelReset
        assert_eq!(frame[FRAME_HEADER_SIZE], MSG_TYPE_CHANNEL_RESET);
        assert_eq!(frame[FRAME_HEADER_SIZE + 1], CHANNEL_RESET_SIZE as u8);
    }

    #[tokio::test]
    async fn heartbeat_sends_valid_frame() {
        let send_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();

        let mut config = test_config();
        config.port = recv_addr.port();

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());
        publisher.send_heartbeat().await;

        let mut buf = [0u8; 2048];
        let n = tokio::time::timeout(Duration::from_secs(2), recv_socket.recv(&mut buf))
            .await
            .expect("timed out")
            .expect("recv failed");

        let frame = &buf[..n];
        assert_eq!(&frame[0..2], &MAGIC_BYTES);
        assert_eq!(frame[FRAME_HEADER_SIZE], MSG_TYPE_HEARTBEAT);
        assert_eq!(frame[FRAME_HEADER_SIZE + 1], HEARTBEAT_SIZE as u8);
    }

    #[tokio::test]
    async fn sequence_numbers_increment() {
        let send_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();

        let mut config = test_config();
        config.port = recv_addr.port();

        let publisher = MulticastPublisher::new(send_socket, config, test_registry());

        publisher.send_heartbeat().await;
        publisher.send_heartbeat().await;
        publisher.send_heartbeat().await;

        let mut buf = [0u8; 2048];
        let mut seqs = Vec::new();
        for _ in 0..3 {
            let n = tokio::time::timeout(Duration::from_secs(2), recv_socket.recv(&mut buf))
                .await
                .expect("timed out")
                .expect("recv failed");
            let seq = u64::from_le_bytes(buf[4..12].try_into().unwrap());
            seqs.push(seq);
        }
        assert_eq!(seqs, vec![0, 1, 2]);
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p server multicast::publisher::tests`
Expected: All 3 integration tests pass.

- [ ] **Step 3: Commit**

```bash
git add server/src/multicast/publisher.rs
git commit -m "multicast: add integration tests for binary publisher"
```

---

### Task 12: Final compile check and full test run

- [ ] **Step 1: Run full cargo check**

Run: `cargo check`
Expected: No errors.

- [ ] **Step 2: Run all tests**

Run: `cargo test`
Expected: All tests pass (protocol, instruments, publisher integration).

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -- -D warnings`
Expected: No warnings.

- [ ] **Step 4: Run cargo fmt**

Run: `cargo fmt --check`
Expected: No formatting issues.

- [ ] **Step 5: Fix any issues found in steps 1-4, then commit fixes**

If any issues are found, fix them and commit:
```bash
git add -A
git commit -m "fix: address clippy warnings and formatting"
```

---

### Task 13: Remove binaries/Cargo.toml uuid if present

**Files:**
- Check: `binaries/Cargo.toml`

- [ ] **Step 1: Check if binaries/Cargo.toml has uuid dependency**

The uuid dep was in `server/Cargo.toml` (removed in Task 8). Verify `binaries/Cargo.toml` doesn't reference it either. If clean, skip this task.

- [ ] **Step 2: Run cargo check to confirm clean build**

Run: `cargo check`
Expected: No errors about missing uuid.
