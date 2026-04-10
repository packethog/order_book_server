# Binary Multicast Protocol: Design Spec

Replace the JSON-based multicast publisher with the DoubleZero Edge Top-of-Book binary protocol (v0.1.0), hot-path only.

## Context

The order book server reads Hyperliquid order book updates from a local non-validating node and distributes them via WebSocket and UDP multicast. The `ss/multicast-support` branch has a working JSON multicast publisher. This work replaces the JSON wire format with the DoubleZero Edge binary protocol defined in `spec/dz_topofbook.ksy` and the [Top-of-Book Feed v0.1.0 spec](https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/v0.1.0.md).

Reference data (InstrumentDefinition, ManifestSummary, second port) is deferred to a follow-up.

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Branch base | `ss/multicast-support` | Reuse publisher event loop, UDP socket, broadcast channel, CLI wiring |
| Approach | Refactor publisher in-place | Same event loop structure, swap JSON for binary encoding |
| Channel sharding | Single channel (channel_id=0) | ~200 instruments at ~2 Hz fits easily in one stream |
| Port model | Single hot-path port | Reference-data port deferred; nothing to send on it yet |
| Frame batching | Multiple messages per frame | HL delivers batch updates per block; pack Quotes/Trades up to MTU |
| Max frame size | 1448 bytes default | 1500 MTU - 24 GRE - 20 IP - 8 UDP; configurable via CLI |
| Source ID | Hardcoded to 1 | Spec allows fixed value for single-source publishers; CLI-overridable |
| Instrument IDs | From HL meta API | perps = array index, spot = 10000 + index |
| BBO source | Existing L2 snapshots | HL ticks at block boundaries (~2 Hz) regardless of feed type |

## Module Structure

```
server/src/
  protocol/
    mod.rs          -- public API: FrameBuilder, message constructors
    frame.rs        -- frame header encoding (24 bytes)
    messages.rs     -- app message header + Quote/Trade/Heartbeat/EndOfSession/ChannelReset
    constants.rs    -- magic bytes, message type IDs, schema version, sizes
  instruments/
    mod.rs          -- InstrumentRegistry: coin name -> InstrumentInfo
    hyperliquid.rs  -- bootstrap from HL meta/spotMeta API, periodic refresh
  multicast/
    config.rs       -- existing, simplified
    publisher.rs    -- existing event loop, refactored
```

### protocol/

Pure encoding module. No I/O, no async. Takes structured data, writes bytes into a buffer.

**FrameBuilder pattern:**

```rust
let mut frame = FrameBuilder::new(channel_id, seq, send_ts);
frame.add_quote(&quote_data)?;   // Err if would exceed MTU
frame.add_quote(&quote_data2)?;
let bytes: &[u8] = frame.finalize();
// send bytes over UDP
```

- `add_*` methods check remaining capacity against MTU before writing
- Returns `Err` when message would overflow, signaling the caller to flush and start a new frame
- `finalize()` writes the frame header (now that msg_count and frame_length are known) and returns the complete buffer

**Frame Header (24 bytes):**

| Offset | Field | Type |
|--------|-------|------|
| 0 | Magic | u16 = 0x445A |
| 2 | Schema Version | u8 = 1 |
| 3 | Channel ID | u8 |
| 4 | Sequence Number | u64 |
| 12 | Send Timestamp | u64 (ns) |
| 20 | Message Count | u8 |
| 21 | Reserved | u8 = 0 |
| 22 | Frame Length | u16 |

**Application Message Header (4 bytes):**

| Offset | Field | Type |
|--------|-------|------|
| 0 | Message Type | u8 |
| 1 | Message Length | u8 |
| 2 | Flags | u16 |

**Hot-path messages and sizes:**

| Message | Type ID | Total Size |
|---------|---------|------------|
| Quote | 0x03 | 60 bytes |
| Trade | 0x04 | 52 bytes |
| Heartbeat | 0x01 | 16 bytes |
| ChannelReset | 0x05 | 12 bytes |
| EndOfSession | 0x06 | 12 bytes |

**Price/quantity conversion:**

Helper function converts string prices/sizes to fixed-point integers using the instrument's exponent:
- `"106217.0"` with price_exponent=-1 becomes `1062170_i64`
- `"0.00017"` with qty_exponent=-5 becomes `17_u64`

### instruments/

**InstrumentInfo:**

```rust
struct InstrumentInfo {
    instrument_id: u32,
    price_exponent: i8,
    qty_exponent: i8,
    symbol: [u8; 16],    // null-padded ASCII, for future InstrumentDefinition use
}
```

**Bootstrap:** On startup, POST to HL REST API (`/info` with `{"type":"meta"}` and `{"type":"spotMeta"}`). Parse responses, build `HashMap<String, InstrumentInfo>` keyed by coin name.

**Exponent derivation:**
- `qty_exponent`: negative of `szDecimals` (e.g., szDecimals=5 -> qty_exponent=-5)
- `price_exponent`: derived from the number of decimal places in the asset's minimum tick size. For example, if the tick size string is `"0.1"`, price_exponent=-1; if `"0.01"`, price_exponent=-2. The meta response includes tick size rules per asset that determine this.

**Refresh:** Periodic re-poll (every 60s) to detect listings/delistings. On diff, update the map. Unknown coins at lookup time are logged and skipped.

### multicast/publisher.rs Changes

The event loop structure stays. What changes is the handling inside each arm:

**On `InternalMessage::Snapshot`:**
1. For each coin in L2 snapshots, look up InstrumentInfo (skip if unknown)
2. Extract BBO: top level from each side (bids[0], asks[0])
3. Convert string prices/sizes to fixed-point via instrument exponents
4. Build Quote messages: set update_flags (bid/ask updated, or gone if side is null)
5. Pack into frames via FrameBuilder, flush at MTU boundary
6. Set app message header flags: snapshot=1 on periodic resends, 0 on normal updates

**On `InternalMessage::Fills`:**
1. For each trade, look up InstrumentInfo by coin (skip if unknown)
2. Map aggressor side: HL "A" (Ask) -> 2 (Sell), HL "B" (Bid) -> 1 (Buy)
3. Convert price/size to fixed-point
4. Set trade_id from HL's `tid`, source_timestamp from `time * 1_000_000` (ms to ns)
5. cumulative_volume = 0 (HL doesn't provide session cumulative volume)
6. Pack into frames, flush at MTU

**On snapshot timer tick (no data):**
- Send Heartbeat frame for channel liveness

**Startup:** Send ChannelReset frame before entering event loop.

**Shutdown:** Send EndOfSession frame on graceful shutdown (tokio signal handler).

**Removed:** MulticastEnvelope, MulticastTrade, all serde_json::to_vec calls, UUID session ID, l2_levels truncation logic.

### multicast/config.rs Changes

**Removed:**
- `l2_levels` config field
- `Channel` enum (L2, Trades) -- hot-path always publishes both quotes and trades

**New fields:**
- `mtu: u16` -- max frame size, default 1448
- `source_id: u16` -- default 1
- `heartbeat_interval: Duration` -- default 5s

**Kept:**
- `group_addr`, `port`, `bind_addr`, `snapshot_interval`

### CLI Changes

**New args:**

| Flag | Default | Type | Description |
|------|---------|------|-------------|
| `--multicast-mtu` | `1448` | u16 | Max frame size (GRE + IP + UDP overhead accounted) |
| `--hl-api-url` | `https://api.hyperliquid.xyz` | String | HL REST API for meta bootstrap |
| `--source-id` | `1` | u16 | Source ID for Quote/Trade messages |
| `--heartbeat-interval` | `5` | u64 (secs) | Heartbeat cadence when idle |

**Removed args:**
- `--multicast-channels` (always publish both quotes and trades)
- `--multicast-l2-levels` (always BBO only)

**Kept:**
- `--multicast-group`, `--multicast-port`, `--multicast-bind-addr`, `--multicast-snapshot-interval`

## Example Multicast Subscriber

Update `example_multicast_subscriber.rs` to decode binary frames using the `protocol` module (in reverse -- read frame header, iterate messages, pretty-print). Replaces the current JSON printing.

## Testing

**protocol/ unit tests:**
- Encode each message type, verify exact byte layout against spec
- Field offsets and sizes match Kaitai struct definition
- FrameBuilder: single message, multiple messages, MTU overflow
- Price/qty string-to-fixed-point conversion edge cases

**instruments/ unit tests:**
- Parse mock meta/spotMeta JSON
- Instrument ID assignment (perps=index, spot=10000+index)
- Exponent derivation from szDecimals
- Unknown coin returns None

**publisher.rs integration tests:**
- Loopback UDP: send InternalMessage through broadcast channel, receive datagram, decode frame header and verify content
- Sequence number increments across frames
- Heartbeat after idle period
- ChannelReset on startup

**Manual validation:**
- Updated example_multicast_subscriber for live decoding

## Wire Format Reference

The canonical wire format definition is `spec/dz_topofbook.ksy` (Kaitai Struct). The protocol module's encoding must match this definition exactly.
