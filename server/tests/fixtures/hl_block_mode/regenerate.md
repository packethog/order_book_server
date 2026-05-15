# Hyperliquid Block-Mode Multicast Fixture

This fixture is reduced from real by-block output on `ubuntu@tyo-hl-node`.
It is intentionally BTC-only so refdata packet ordering is deterministic and
the snapshot stays small enough for CI.

Source paths:

- Snapshot: `/home/ubuntu/out.json`
- Fills: `/data/hl-data/node_fills_by_block/hourly/20260430/20`
- Order statuses: `/data/hl-data/node_order_statuses_by_block/hourly/20260430/20`
- Raw book diffs: `/data/hl-data/node_raw_book_diffs_by_block/hourly/20260430/20`

The committed fixture covers the block range recorded in `manifest.json`.
The source extracts used to derive the BTC-only replay fixture are also
versioned in `source/hl_block_mode_unfiltered.tar.gz`; its SHA-256 is recorded
in `manifest.json`. The `_streaming` fixture files are generated from the
filtered by-block files so streaming ingest has a deterministic parity oracle
without requiring a live streaming capture.

## Regenerating From A Host

To refresh the versioned source archive from an SSH-accessible validator host,
rebuild the BTC-only replay fixture, regenerate goldens, and verify the golden
comparison:

```bash
server/tests/fixtures/hl_block_mode/regenerate_from_host.py --host <user@hl-node>
```

The host is intentionally required so refreshes do not accidentally connect to
a baked-in machine. With a host provided, the script reads `/home/ubuntu/out.json`,
infers `start_block = snapshot_height + 1`, captures the next 40 blocks,
discovers recent hourly by-block files under `/data/hl-data`, and derives a
BTC-only fixture.

To reproduce the exact committed fixture, pass explicit overrides:

```bash
server/tests/fixtures/hl_block_mode/regenerate_from_host.py \
  --host ubuntu@tyo-hl-node \
  --snapshot /home/ubuntu/out.json \
  --data-base /data/hl-data \
  --date 20260430 \
  --hour 20 \
  --start-block 978995377 \
  --end-block 978995416
```

The script fetches the snapshot with `ssh cat`, fetches the tail of each hourly
by-block file with `ssh tail`, filters those source files to the selected block
range, writes `source/hl_block_mode_unfiltered.tar.gz`, regenerates the derived
block and streaming fixtures, then runs the multicast e2e test once with
`HL_BLOCK_MODE_REGENERATE=1` and once again to verify stable goldens.

Useful options:

- `--block-count 40` controls the default post-snapshot block range.
- `--scan-files 6` controls how many recent hourly files are searched per
  by-block source when `--date/--hour` are not provided.
- `--tail-lines 5000` controls how much of each remote hourly file is fetched
  before local block-range filtering.
- `--date YYYYMMDD --hour HH` pins the remote hourly file instead of
  discovering recent files dynamically.
- `--start-block N --end-block M` pins the block range instead of deriving it
  from the snapshot height.
- `--coin BTC` can be repeated to derive a multi-coin replay fixture.
- `--skip-goldens` refreshes only the source archive and derived JSON inputs.
- `--skip-verify` regenerates goldens but skips the second verification run.

## Regenerating Replay Inputs

To rebuild the committed BTC-only `out.json` and by-block JSONL files from the
versioned source archive:

```bash
server/tests/fixtures/hl_block_mode/generate_from_source.py
```

The generator extracts the reduced unfiltered snapshot/block files, filters
them to the fixture coin set, derives `_streaming` files from the filtered
by-block lines, and rewrites `manifest.json` with source, block, and streaming
counts. Raw book diffs, order statuses, and fills are split to one event per
line. This intentionally models live streaming output, where the two sides of a
trade can arrive in separate `node_fills_streaming` rows. Block-mode rows can
still contain the full fill pair in one batch.

## Regenerating Goldens

After regenerating replay inputs or changing multicast encoding/replay logic,
regenerate normalized block and streaming golden packet files with:

```bash
HL_BLOCK_MODE_REGENERATE=1 cargo test -p server listeners::order_book::block_mode_multicast_e2e -- --nocapture
```

Then verify the generated files are stable:

```bash
cargo test -p server listeners::order_book::block_mode_multicast_e2e -- --nocapture
```

The test captures actual UDP datagrams on loopback sockets for block replay and
streaming replay. It normalizes runtime-only timestamps and TOB frame sequence
numbers before comparing bytes. TOB sequence numbers are normalized because the
TOB publisher uses one shared counter across marketdata and refdata, so timer
cadence can change per-stream sequence values without changing payload
behavior. The same suite also compares final L4 and L2 books between block and
streaming replay and covers streaming accumulator regressions such as
diff-before-status waiting, status-before-diff application, unresolved new
orders, late finalized-block data, and partial trailing JSONL.

## Dual Validator Block/Streaming Fixture

`../hl_dual_validator/` contains the committed dual-validator fixture for
validating live streaming ingest against by-block ingest. It was captured from
two validator hosts started from the same snapshot window:

- streaming: `--stream-with-block-info --disable-output-file-buffering`
- by-block: `--batch-by-block`

The fixture preserves enough source material to explain every derived replay
file. Reduced source archives are versioned, not only the filtered fixture JSONL
files:

- `source/streaming_btc_985148182_985148232.tar.gz`
- `source/by_block_btc_985148182_985148232.tar.gz`
- `source/snapshot_985148181.json.gz`
- `manifest.json`

The manifest records both hosts, snapshot path and height, source paths,
block range, fixture coin set, capture command, file sizes, SHA-256 hashes, and
the expected semantic deltas caused by minimization.

The committed CI fixture uses a short BTC-only replay window but preserves
whole-block metadata needed by the assertions:

1. Keep all selected-coin raw diffs, fills, and order statuses needed by new
   diffs.
2. Keep empty rows for every block in the range so block-mode height advancement
   remains explicit.
3. Record the unfiltered per-block raw-diff counts before filtering. This is
   required because block mode emits DoB `BatchBoundary` only for blocks with at
   least two emittable diffs, while streaming emits an open/close boundary for
   any block with emittable streaming activity. A coin-minimized fixture can
   therefore create extra streaming boundaries for blocks that had one selected
   coin diff after filtering but many total diffs in the real block.
4. Generate normalized golden packet files for each mode, plus a parity report
   that compares final L4/L2 state and DoB order-event payloads across modes.

The dual-validator e2e assertion treats state-changing output as the primary
correctness oracle:

- final listener height matches the fixture end block;
- final L4 snapshots match exactly;
- final L2 snapshots match exactly;
- DoB `OrderAdd`, `OrderCancel`, and `OrderExecute` payload streams match
  exactly after normalizing runtime send timestamps;
- DoB refdata and TOB refdata payloads match after existing normalization;
- DoB `BatchBoundary` differences are asserted against the manifest's expected
  minimization delta instead of being treated as book-state divergence.

TOB marketdata is intentionally not a byte-for-byte block-vs-stream oracle:
streaming mode publishes at mutation cadence, while block mode publishes at
block cadence. Use final L2 parity and stream-specific TOB goldens to validate
TOB correctness.
