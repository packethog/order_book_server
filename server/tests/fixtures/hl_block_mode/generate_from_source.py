#!/usr/bin/env python3
"""Regenerate the BTC-only block-mode fixture from versioned source extracts."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_ARCHIVE = ROOT / "source" / "hl_block_mode_unfiltered.tar.gz"

EVENT_FILES = {
    "diffs": "hl/data/node_raw_book_diffs_by_block/hourly/20260430/20",
    "fills": "hl/data/node_fills_by_block/hourly/20260430/20",
    "statuses": "hl/data/node_order_statuses_by_block/hourly/20260430/20",
}

STREAMING_EVENT_FILES = {
    "diffs": "hl/data/node_raw_book_diffs_streaming/hourly/20260430/20",
    "fills": "hl/data/node_fills_streaming/hourly/20260430/20",
    "statuses": "hl/data/node_order_statuses_streaming/hourly/20260430/20",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--coin", action="append", dest="coins", default=["BTC"])
    return parser.parse_args()


def safe_extract(archive: Path, destination: Path) -> Path:
    with tarfile.open(archive, "r:gz") as tar:
        destination = destination.resolve()
        for member in tar.getmembers():
            target = (destination / member.name).resolve()
            if not target.is_relative_to(destination):
                raise ValueError(f"unsafe tar member path: {member.name}")
        tar.extractall(destination, filter="data")
    return destination / "hl_block_mode"


def event_coin(event: dict) -> str | None:
    if isinstance(event, list) and len(event) > 1 and isinstance(event[1], dict):
        return event[1].get("coin")
    if not isinstance(event, dict):
        return None
    coin = event.get("coin")
    if coin is not None:
        return coin
    order = event.get("order")
    if isinstance(order, dict):
        return order.get("coin")
    return None


def count_events(path: Path) -> dict:
    lines = 0
    events = 0
    first_block = None
    last_block = None
    with path.open() as handle:
        for line in handle:
            lines += 1
            data = json.loads(line)
            events += len(data.get("events", []))
            block = data["block_number"]
            first_block = block if first_block is None else min(first_block, block)
            last_block = block if last_block is None else max(last_block, block)
    out = {"bytes": path.stat().st_size, "events": events, "lines": lines}
    if first_block is not None:
        out["first_block"] = first_block
        out["last_block"] = last_block
    return out


def write_compact_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")))
            handle.write("\n")


def filter_events(source_root: Path, coins: set[str]) -> dict:
    counts = {}
    for name, relative in EVENT_FILES.items():
        source_path = source_root / relative
        output_path = ROOT / relative
        rows = []
        with source_path.open() as handle:
            for line in handle:
                data = json.loads(line)
                data["events"] = [event for event in data.get("events", []) if event_coin(event) in coins]
                rows.append(data)
        write_compact_jsonl(output_path, rows)
        counts[name] = count_events(output_path)
        counts[name].pop("first_block", None)
        counts[name].pop("last_block", None)
    return counts


def split_events_for_streaming(name: str, events: list) -> list[list]:
    return [[event] for event in events]


def derive_streaming_events() -> dict:
    counts = {}
    for name, block_relative in EVENT_FILES.items():
        streaming_relative = STREAMING_EVENT_FILES[name]
        rows = []
        with (ROOT / block_relative).open() as handle:
            for line in handle:
                data = json.loads(line)
                events = data.get("events", [])
                if not events:
                    row = copy.deepcopy(data)
                    row["events"] = []
                    rows.append(row)
                    continue
                for group in split_events_for_streaming(name, events):
                    row = copy.deepcopy(data)
                    row["events"] = group
                    rows.append(row)
        output_path = ROOT / streaming_relative
        write_compact_jsonl(output_path, rows)
        counts[name] = count_events(output_path)
    return counts


def filter_snapshot(source_root: Path, coins: set[str]) -> int:
    snapshot = json.loads((source_root / "out.json").read_text())
    snapshot[1] = [entry for entry in snapshot[1] if entry[0] in coins]
    (ROOT / "out.json").write_text(json.dumps(snapshot, separators=(",", ":")))
    return len(snapshot[1])


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(
    source_root: Path,
    archive: Path,
    coins: list[str],
    counts_after: dict,
    streaming_counts: dict,
    snapshot_coin_count: int,
) -> None:
    manifest = copy.deepcopy(json.loads((source_root / "manifest.json").read_text()))
    manifest["counts_after_coin_filter"] = counts_after
    manifest["streaming_fixture"] = {
        "generation": "derived from filtered by-block fixture",
        "files": STREAMING_EVENT_FILES,
        "counts": streaming_counts,
        "fills_grouping": "one event per line; live streaming can split the two sides of a trade across rows",
        "diffs_grouping": "one event per line",
        "statuses_grouping": "one event per line",
    }
    manifest["fixture_coin_filter"] = coins
    manifest["snapshot_coin_count_after_coin_filter"] = snapshot_coin_count
    manifest["source_archive"] = {
        "path": str(archive.relative_to(ROOT)),
        "sha256": sha256(archive),
    }
    manifest["notes"] = (
        f"{manifest['notes']} Further filtered locally to {', '.join(coins)} "
        "for CI-sized deterministic replay and stable refdata ordering."
    )
    (ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def main() -> None:
    args = parse_args()
    coins = list(dict.fromkeys(args.coins))
    archive = args.archive.resolve()
    with tempfile.TemporaryDirectory() as tmp:
        source_root = safe_extract(archive, Path(tmp))
        counts_after = filter_events(source_root, set(coins))
        streaming_counts = derive_streaming_events()
        snapshot_coin_count = filter_snapshot(source_root, set(coins))
        write_manifest(source_root, archive, coins, counts_after, streaming_counts, snapshot_coin_count)


if __name__ == "__main__":
    main()
