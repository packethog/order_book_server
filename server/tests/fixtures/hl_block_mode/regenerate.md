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
in `manifest.json`.

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
fixture, then runs the block-mode multicast e2e test once with
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
them to the fixture coin set, and rewrites `manifest.json` with both source and
post-filter counts.

## Regenerating Goldens

After regenerating replay inputs or changing multicast encoding/replay logic,
regenerate normalized golden packet files with:

```bash
HL_BLOCK_MODE_REGENERATE=1 cargo test -p server listeners::order_book::block_mode_multicast_e2e -- --nocapture
```

Then verify the generated files are stable:

```bash
cargo test -p server listeners::order_book::block_mode_multicast_e2e -- --nocapture
```

The test captures actual UDP datagrams on loopback sockets. It normalizes
runtime-only timestamps and TOB frame sequence numbers before comparing bytes.
TOB sequence numbers are normalized because the TOB publisher uses one shared
counter across marketdata and refdata, so timer cadence can change per-stream
sequence values without changing payload behavior.
