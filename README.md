# Local WebSocket Server

## Disclaimer

This was a standalone project, not written by the Hyperliquid Labs core team. It is made available "as is", without warranty of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, or noninfringement. Use at your own risk. It is intended for educational or illustrative purposes only and may be incomplete, insecure, or incompatible with future systems. No commitment is made to maintain, update, or fix any issues in this repository.

## Functionality

This server provides the `l2book` and `trades` endpoints from [Hyperliquid’s official API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), with roughly the same API.

- The `l2book` subscription now includes an optional field:
  `n_levels`, which can be up to `100` and defaults to `20`.
- This server also introduces a new endpoint: `l4book`.

The `l4book` subscription first sends a snapshot of the entire book and then forwards order diffs by block. The subscription format is:

```json
{
  "method": "subscribe",
  "subscription": {
    "type": "l4Book",
    "coin": "<coin_symbol>"
  }
}
```

## Setup

1. Run a non-validating node (from [`hyperliquid-dex/node`](https://github.com/hyperliquid-dex/node)). Block ingest requires batching by block and recording fills, order statuses, and raw book diffs. Streaming ingest requires `--stream-with-block-info --disable-output-file-buffering` and the same three outputs.

2. Then run this local server:

```bash
cargo run --release --bin dz_hl_publisher -- --address 0.0.0.0 --port 8000
```

By default the server reads `$HOME/hl/data/node_*_by_block`. To opt into streaming disk ingest, use:

```bash
cargo run --release --bin dz_hl_publisher -- \
  --address 0.0.0.0 --port 8000 \
  --ingest-mode stream \
  --hl-data-root /data/hl-data
```

`--hl-data-root` is the directory containing the `node_*` output directories; it defaults to `$HOME/hl/data`. Block mode reads `node_fills_by_block`, `node_order_statuses_by_block`, and `node_raw_book_diffs_by_block`. Streaming mode reads `node_fills_streaming`, `node_order_statuses_streaming`, and `node_raw_book_diffs_streaming`. Snapshot validation output still defaults to `$HOME/out.json`.

If this local server does not detect the node writing down any new events, it will automatically exit after some amount of time (currently set to 5 seconds).
In addition, the local server periodically fetches order book snapshots from the node, and compares to its own internal state. If a difference is detected, it will exit.

If you want logging, prepend the command with `RUST_LOG=info`.

The WebSocket server comes with compression built-in. The compression ratio can be tuned using the `--websocket-compression-level` flag.

## Multicast

The server can optionally publish market data as UDP multicast datagrams alongside the WebSocket feed. This is useful for distributing L2 book updates and trades over a multicast-capable network such as [DoubleZero](https://docs.doublezero.io/).

### Enabling multicast

Pass `--multicast-group` to enable multicast publishing:

```bash
cargo run --release --bin dz_hl_publisher -- \
  --address 0.0.0.0 --port 8000 \
  --multicast-group 239.0.0.1 \
  --multicast-bind-addr 0.0.0.0
```

### CLI arguments

| Flag | Default | Description |
|------|---------|-------------|
| `--multicast-group` | *(none — multicast disabled)* | Multicast group address (e.g. `239.0.0.1`). Enables multicast when set. |
| `--multicast-port` | `5000` | UDP port for multicast traffic. |
| `--multicast-bind-addr` | *(required when group is set)* | Local address to bind the multicast UDP socket. |
| `--multicast-channels` | `l2,trades` | Comma-separated list of channels to publish. Valid values: `l2`, `trades`. |
| `--multicast-l2-levels` | `5` | Number of price levels to include in L2 snapshots. |
| `--multicast-snapshot-interval` | `5` | Interval in seconds between periodic full book snapshots. |

### Wire format

Every UDP datagram is a JSON object with this envelope:

```json
{
  "session": "<uuid-v4>",
  "seq": 0,
  "channel": "l2Book",
  "data": { ... }
}
```

- **`session`** — a random UUID generated on server startup. A new session ID indicates the server restarted and sequence numbers have reset.
- **`seq`** — monotonically increasing sequence number (starting at 0). Gaps indicate missed datagrams.
- **`channel`** — one of `l2Book`, `l2Snapshot`, or `trades`.
- **`data`** — the channel-specific payload.

Channels:

- **`l2Book`** — incremental L2 book update, sent whenever the book changes.
- **`l2Snapshot`** — periodic full L2 book snapshot (same format as `l2Book`), sent on the `--multicast-snapshot-interval` timer. Subscribers can use these to recover from gaps without reconnecting.
- **`trades`** — an array of trades for a single coin. The `users` field is omitted from multicast trades.

Datagrams that exceed 1400 bytes are dropped to stay within typical MTU limits.

### Example subscriber

An example subscriber binary is included for testing:

```bash
cargo run --release --bin example_multicast_subscriber -- \
  --group 239.0.0.1 --port 5000
```

This joins the multicast group and prints received datagrams to stdout.

## DZ-DoB

Binary depth-of-book multicast (frame magic `0x4444`). Three streams off the same `--dob-group`:

- **mktdata** (`--dob-mktdata-port`, default `6000`) — incremental L4 events.
- **refdata** (`--dob-refdata-port`, default `6001`) — `InstrumentDefinition` retransmissions.
- **snapshot** (`--dob-snapshot-port`, default `6002`) — round-robin per-instrument snapshots.

### DoB Phase 2

Phase 2 adds the snapshot stream and the `InstrumentReset` recovery flow:

- The snapshot stream emits a continuous round-robin per-instrument snapshot anchored to the mktdata sequence number. Configure cycle duration via `--dob-snapshot-round-duration` (seconds, default `30`) and frame size via `--dob-snapshot-mtu` (default `1232`).
- When the publisher's per-coin validation check detects divergence from the venue, it emits an `InstrumentReset` (msg type `0x14`) on mktdata and immediately schedules a priority snapshot for that instrument on the snapshot port.
- `Per-Instrument Seq` is preserved across `InstrumentReset` per the wire spec — it only resets on a `Reset Count` change.

## Wireshark dissectors

Lua dissectors for DZ-TOB and DZ-DoB live under `spec/`. To install:

```bash
mkdir -p ~/.local/lib/wireshark/plugins
cp spec/dz_topofbook.lua spec/dz_depthofbook.lua ~/.local/lib/wireshark/plugins/
```

Then open a capture. The DZ-TOB dissector triggers on frame magic `0x445A`; the DZ-DoB dissector triggers on `0x4444`. Both support preference-based port registration (Edit → Preferences → Protocols → DZ-TOB / DZ-DoB) and ad-hoc loading:

```bash
tshark -X lua_script:spec/dz_depthofbook.lua -f "udp port 6000" -i lo
```

## Test fixtures

`server/tests/fixtures/hl_block_mode/` contains a reduced real Hyperliquid
by-block replay fixture used by the multicast e2e tests. The tests replay the
fixture through block ingest and a generated streaming-ingest equivalent,
capture TOB/DoB UDP output on loopback sockets, normalize runtime-only fields,
and compare each stream to golden packet files. They also assert final L4/L2
book parity between block and streaming replay.

The reduced unfiltered source extracts are versioned beside the fixture, so the
BTC-only block fixture and derived `_streaming` fixture can be regenerated
without fetching from the validator again. The same fixture directory also
includes a host-fetch script for refreshing the source archive and goldens from
an SSH-accessible HL node. See
`server/tests/fixtures/hl_block_mode/regenerate.md` for source paths and
regeneration commands.

`server/tests/fixtures/hl_dual_validator/` contains a compact BTC-only
dual-validator fixture with compressed source archives from matching by-block
and streaming validators, normalized multicast goldens, and semantic parity
goldens for block-vs-stream correctness checks.

## CI

GitHub Actions runs `cargo clippy --workspace --all-targets` and
`cargo test --workspace` on pushes to `main` and on pull requests.

## Caveats

- This server does **not** show untriggered trigger orders.
- It currently **does not** support spot order books.
- Block ingest remains the default rollout mode. Streaming ingest is opt-in while parity coverage hardens.
