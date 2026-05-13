# Local WebSocket Server

## Disclaimer

This was a standalone project, not written by the Hyperliquid Labs core team. It is made available "as is", without warranty of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, or noninfringement. Use at your own risk. It is intended for educational or illustrative purposes only and may be incomplete, insecure, or incompatible with future systems. No commitment is made to maintain, update, or fix any issues in this repository.

## Functionality

This server provides the `l2book` and `trades` endpoints from [Hyperliquid’s official API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), with roughly the same API.

- The `l2book` subscription now includes an optional field:
  `n_levels`, which can be up to `100` and defaults to `20`.
- This server also introduces a new endpoint: `l4book`.

For a codebase-level mental model, including the block-mode and streaming-mode
publishing hot paths, see [`ARCHITECTURE.md`](ARCHITECTURE.md).

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

Streaming mode can optionally tail fills on a separate listener:

```bash
cargo run --release --bin dz_hl_publisher -- \
  --address 0.0.0.0 --port 8000 \
  --ingest-mode stream \
  --hl-data-root /data/hl-data \
  --separate-fill-ingest
```

`--separate-fill-ingest` is streaming-only and defaults off. It keeps status/raw-diff processing on the normal book listener, but sends `node_fills_streaming` rows directly to the TOB trade publisher. This is useful when trade latency should not wait behind high-volume book reconstruction. Block mode does not support the flag. This flag only affects TOB trades; TOB quotes still come from book snapshots produced by the normal status/raw-diff listener.

If this local server does not detect the node writing down any new events, it will automatically exit after some amount of time (currently set to 5 seconds).
In addition, the local server periodically fetches order book snapshots from the node and compares them to its own internal state. When a per-coin divergence is detected, the publisher repairs only the affected coin. If DoB is enabled, that repair emits an `InstrumentReset` and schedules a priority snapshot for the affected instrument.

If you want logging, prepend the command with `RUST_LOG=info`.

The WebSocket server comes with compression built-in. The compression ratio can be tuned using the `--websocket-compression-level` flag.

## Order Book Reconstruction

Block and streaming ingest both treat Hyperliquid raw-book `New` diffs as the
authoritative resting-book mutation. The matching order-status row supplies
identity and metadata such as user, coin, side, order type, reduce-only, client
order id, and non-conflicting time-in-force values. The raw diff supplies the
actual resting price and resting size. This matters for triggered orders, where
the status row can contain the trigger or submitted limit price while the raw
diff contains the price that actually rests in the validator snapshot.

If an order-status row reports `Ioc` but the raw-book `New` diff shows a
residual resting quantity, the publisher normalizes that reconstructed resting
order to `Gtc`. That matches validator snapshots: a raw-book `New` represents
book state that survived matching, so downstream L4/DOB state should model it as
a resting order rather than a still-active immediate-or-cancel instruction.

## Metrics

The publisher exposes Prometheus metrics on a separate HTTP listener by default:

```bash
curl http://127.0.0.1:9090/metrics
```

Metrics are enabled unless `--disable-metrics` is set. Use `--metrics-address` and `--metrics-port` to move the listener, for example:

```bash
cargo run --release --bin dz_hl_publisher -- \
  --address 0.0.0.0 --port 8000 \
  --metrics-address 0.0.0.0 \
  --metrics-port 9090
```

The metric families focus on multicast hot paths: listener latency phases, per-source ingest delay/backlog, TOB queue/source-lag/send timing, TOB packet and suppression counters, DOB queue/encode/send timing, and DOB channel drops. Labels intentionally stay low-cardinality and do not include coins, order IDs, instruments, block heights, or websocket subscriptions.

### Metric reference

All duration metrics are exported in seconds. Log summaries may render the same values in milliseconds or microseconds.

| Metric | Labels | Meaning |
|--------|--------|---------|
| `orderbook_listener_latency_seconds` | `phase=gossip\|hl_to_wake\|process\|e2e` | Listener-side latency measured from parsed HL timestamps and local processing. `gossip` is `local_time - block_time`, `hl_to_wake` is validator row `local_time` to publisher wake-up, `process` is local drain/apply time, and `e2e` is block time through local processing completion. |
| `orderbook_ingest_source_gossip_seconds` | `source=statuses\|diffs\|fills` | Per-source `local_time - block_time` from the HL JSONL wrapper. If this is high while file mtime lag and backlog are low, the validator is already writing late rows. |
| `orderbook_ingest_file_tail_lag_seconds` | `source=statuses\|diffs\|fills` | Current wall-clock lag of the newest processed row: `now - local_time`. This answers “how stale is the latest row we have consumed?” |
| `orderbook_ingest_file_mtime_lag_seconds` | `source=statuses\|diffs\|fills` | Time between the file modification timestamp and publisher processing. High values point at file notification, scheduling, or reader contention after the validator has written the file. |
| `orderbook_ingest_row_file_visibility_lag_seconds` | `source=statuses\|diffs\|fills` | File modification timestamp minus the parsed row `local_time`. High values mean the row was timestamped before it became visible to the publisher through the streaming file. |
| `orderbook_ingest_backlog_bytes` | `source=statuses\|diffs\|fills` | Unread bytes remaining in the tailed file after a drain pass. Sustained non-zero backlog means the publisher is not keeping up with disk output. |
| `orderbook_stream_out_of_order_rows_total` | `source=statuses\|diffs` | Streaming rows whose block number is below that source's current watermark. Meaningful non-spot rows switch finalization into grace fallback. |
| `orderbook_stream_late_finalized_rows_total` | `source=statuses\|diffs`, `action=ignored\|fatal` | Rows received after their block was finalized. Late statuses are metadata-only once no pending raw diff can consume them and are ignored; meaningful late diffs are fatal because they would mutate a block whose DoB boundary has already closed. |
| `orderbook_stream_finalization_mode` | `mode=watermark\|grace_fallback` | Gauge set to `1` for the active streaming finalization mode and `0` for the inactive mode. |
| `orderbook_stream_finalization_lag_seconds` | `mode=watermark\|grace_fallback` | Time from last row received for a streaming block to block finalization. Watermark mode should avoid the fixed grace delay in healthy monotonic periods. |
| `orderbook_stream_reorder_delay_seconds` | `source=statuses\|diffs` | Difference between the latest source `local_time` and an out-of-order row's `local_time`, when computable. |
| `orderbook_tob_snapshot_source_block_lag_seconds` | `source=statuses\|diffs\|fills` | For TOB snapshots, source-row block time minus emitted snapshot block time. High values mean the book snapshot itself is behind the newest ingested source rows, often due to streaming finalization waiting for watermarks or grace fallback. |
| `orderbook_tob_snapshot_validator_write_lag_seconds` | `source=statuses\|diffs\|fills` | For rows that trigger TOB snapshot enqueue, `local_time - block_time`. High values mean the validator wrote that source row late. |
| `orderbook_tob_snapshot_listener_to_publisher_seconds` | `source=statuses\|diffs\|fills` | For TOB snapshots, source-row `local_time` to TOB publisher receive time. This includes local listener/apply/finalization work plus internal enqueue/receive time. |
| `orderbook_tob_fill_enqueue_lag_seconds` | `path=main_listener\|separate_listener` | For TOB fills, source lag at listener enqueue. This is measured before the internal TOB broadcast send, so it exposes local delay that happened before enqueue. |
| `orderbook_tob_fill_listener_to_publisher_seconds` | `path=main_listener\|separate_listener` | For TOB fills, source-row `local_time` to TOB publisher receive time. Compare this with `orderbook_tob_queue_delay_seconds{message_type="fill"}` to split listener-side delay from internal queue delay. |
| `orderbook_tob_fill_pair_total` | `outcome=paired\|orphan_dropped\|one_sided_token_alias\|malformed\|duplicate_replaced` | TOB fill pairing outcomes. Streaming fills can deliver the two sides of one trade in separate rows; `paired` is the count that should track trade emission eligibility. `one_sided_token_alias` counts expected one-sided `#...` token-alias fills and is not treated as a missing normal trade pair. Non-zero malformed/orphan/duplicate counts need inspection. |
| `orderbook_tob_fill_pair_pending` | *(none)* | Current number of one-sided fills waiting for the opposite side by trade id. This should stay bounded; growth means the publisher is seeing unmatched fill sides. |
| `orderbook_tob_queue_delay_seconds` | `message_type=snapshot\|fill` | Time TOB messages spend between listener enqueue and TOB publisher receive. High values indicate internal publisher contention before packet encoding/sending. |
| `orderbook_tob_source_lag_seconds` | `message_type=snapshot\|fill`, `decision=published\|suppressed` | Source age at the TOB freshness decision. `suppressed` means the message was older than the TOB freshness threshold and quote/trade marketdata was intentionally not published. |
| `orderbook_tob_socket_send_seconds` | `channel=marketdata\|refdata` | Time spent in UDP send calls for TOB packets. High values suggest socket/kernel/network backpressure. |
| `orderbook_tob_packets_total` | `message_type=quote\|trade\|heartbeat\|channel_reset\|instrument_definition\|manifest_summary` | Count of TOB multicast packets sent by packet/message class. |
| `orderbook_tob_suppressed_total` | `message_type=snapshot\|fill` | Count of TOB source messages suppressed by stale-source freshness checks. Heartbeats and refdata can still be emitted while marketdata is suppressed. |
| `orderbook_tob_receiver_lag_total` | `kind=event\|message` | Internal broadcast receiver lag seen by the TOB publisher. `event` counts lag occurrences; `message` counts dropped internal broadcast messages reported by Tokio. |
| `orderbook_dob_queue_delay_seconds` | `event_type=order_add\|order_cancel\|order_execute\|batch_boundary\|instrument_reset\|heartbeat` | Time DOB events spend between apply/tap enqueue and DOB emitter receive. High values indicate contention before DOB encoding. |
| `orderbook_dob_encode_seconds` | `event_type=...` | Time spent packing DOB events into multicast frames. |
| `orderbook_dob_socket_send_seconds` | `stream=mktdata\|refdata\|snapshot` | Time spent in UDP send calls for DOB packets. |
| `orderbook_dob_channel_drops_total` | `reason=full\|closed`, `event_type=...` | Count of DOB events dropped before reaching the emitter because the bounded channel was full or closed. Any sustained increase is a correctness/health issue. |

For latency triage, start with the ingest split. High `orderbook_ingest_source_gossip_seconds` with low `orderbook_ingest_file_mtime_lag_seconds` and near-zero `orderbook_ingest_backlog_bytes` points to validator or upstream HL delay. High `orderbook_ingest_row_file_visibility_lag_seconds` means the row's `local_time` was assigned materially before the append became visible in the file. Low source gossip with high file mtime lag or growing backlog points to publisher-side wake/drain contention. In streaming mode, `orderbook_stream_finalization_mode{mode="watermark"}` should be active during healthy monotonic output; `grace_fallback` means the listener is in startup synchronization or a conservative fallback after out-of-order rows. If ingest looks fresh but `orderbook_tob_queue_delay_seconds` or `orderbook_dob_queue_delay_seconds` is high, the bottleneck is inside multicast publishing. For TOB fills specifically, compare `orderbook_tob_fill_listener_to_publisher_seconds` to `orderbook_tob_queue_delay_seconds{message_type="fill"}`: high listener-to-publisher with low queue delay means delay before enqueue; high queue delay means receiver/publisher contention after enqueue.

TOB trades are emitted only after both sides of a Hyperliquid fill pair are
available. Block-mode rows often contain both sides in one batch, while live
streaming rows can split the ask and bid sides across separate
`node_fills_streaming` rows. The publisher pairs fills by trade id and verifies
opposite sides plus matching coin, hash, price, size, and trade time before
publishing a trade. Pending one-sided fills expire after a short bound. Stable
one-sided `#...` token-alias fills are classified separately as
`one_sided_token_alias` because live validator output can contain them without
an opposite-side fill row. Pairing health is exposed through
`orderbook_tob_fill_pair_total` and `orderbook_tob_fill_pair_pending`.

### Streaming Startup Synchronization

Streaming status and raw-diff files are tailed independently. On process startup the publisher can begin in the middle of a block, with one source already ahead of the other. To avoid closing a DoB block before matching status rows have become visible, streaming finalization starts in grace-fallback mode until both status and diff watermarks have crossed the startup sync height. After that point, healthy monotonic output uses watermark finalization and avoids the fixed grace delay.

### Validator Output Latency

For streaming ingest, run `hl-node` with `--stream-with-block-info --disable-output-file-buffering`. Hyperliquid documents `--disable-output-file-buffering` as the low-latency mode for output files: it flushes each line immediately, reducing latency at the cost of additional disk IO. Without it, the validator can assign a row `local_time` well before the row becomes visible in the streaming file, which can cause TOB trades or snapshots to be suppressed by the freshness guard even when the publisher is keeping up.

A canary on `aws-tyo-hl-mainnet` measured `node_fills_streaming` visibility before and after enabling this flag:

| Validator mode | `local_time` to file mtime avg | p99 | max |
|----------------|-------------------------------:|----:|----:|
| default output buffering | `190.6ms` | `890.5ms` | `2897.5ms` |
| `--disable-output-file-buffering` | `0.5ms` | `1.3ms` | `3.3ms` |

In that same live window, inotify and polling both observed new fill rows within a few milliseconds of the file mtime, while publisher `hl_to_wake` stayed at `0-1ms`. That means fill suppression was caused by validator output buffering, not inotify delay or publisher queueing. A later canary with `--separate-fill-ingest` showed the local fill handoff dropping to roughly `1ms`, confirming that split fill ingest removes local trade delay when status/diff processing is busy. Snapshot suppression can still occur during upstream validator or Hyperliquid source jitter, or when status/raw-diff processing is near the freshness threshold; diagnose that separately with `orderbook_ingest_source_gossip_seconds`, `orderbook_tob_snapshot_validator_write_lag_seconds`, `orderbook_tob_snapshot_listener_to_publisher_seconds`, and `orderbook_tob_queue_delay_seconds`.

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
