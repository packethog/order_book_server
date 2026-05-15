#!/usr/bin/env python3
"""Capture a reduced dual-validator block/streaming fixture from HL nodes."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import shlex
import subprocess
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "source"

BY_BLOCK_DIRS = {
    "diffs": "node_raw_book_diffs_by_block",
    "fills": "node_fills_by_block",
    "statuses": "node_order_statuses_by_block",
}
STREAMING_DIRS = {
    "diffs": "node_raw_book_diffs_streaming",
    "fills": "node_fills_streaming",
    "statuses": "node_order_statuses_streaming",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream-host", required=True)
    parser.add_argument("--block-host", required=True)
    parser.add_argument("--snapshot", default="/home/ubuntu/out.json")
    parser.add_argument("--data-base", default="/data/hl-data")
    parser.add_argument("--coin", default="BTC")
    parser.add_argument("--block-count", type=int, default=51)
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--end-block", type=int)
    parser.add_argument("--date", help="Optional YYYYMMDD hourly directory override")
    parser.add_argument("--hour", help="Optional hour file override, for example 17")
    parser.add_argument("--scan-files", type=int, default=6)
    return parser.parse_args()


def ssh_text(host: str, remote_cmd: str) -> str:
    print(f"+ ssh {host} {remote_cmd!r}", flush=True)
    return subprocess.check_output(["ssh", host, remote_cmd], text=True)


def ssh_bytes(host: str, remote_cmd: str) -> bytes:
    print(f"+ ssh {host} {remote_cmd!r}", flush=True)
    return subprocess.check_output(["ssh", host, remote_cmd])


def compact(obj: object) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def event_coin(kind: str, event: object) -> str | None:
    if kind == "fills":
        if isinstance(event, list) and len(event) >= 2 and isinstance(event[1], dict):
            return event[1].get("coin")
        if isinstance(event, dict):
            return event.get("coin")
        return None
    if not isinstance(event, dict):
        return None
    if kind == "statuses":
        order = event.get("order")
        return order.get("coin") if isinstance(order, dict) else None
    return event.get("coin")


def hourly_files(args: argparse.Namespace, host: str, source_dir: str) -> list[str]:
    base = args.data_base.rstrip("/")
    if args.date is not None or args.hour is not None:
        if args.date is None or args.hour is None:
            raise ValueError("--date and --hour must be passed together")
        return [f"{base}/{source_dir}/hourly/{args.date}/{args.hour}"]

    root = f"{base}/{source_dir}/hourly"
    cmd = f"find {shlex.quote(root)} -type f | sort | tail -n {args.scan_files}"
    files = [line for line in ssh_text(host, cmd).splitlines() if line]
    if not files:
        raise ValueError(f"no files found under {root} on {host}")
    return files


def infer_range(args: argparse.Namespace, snapshot: list) -> tuple[int, int, int]:
    snapshot_height = int(snapshot[0])
    start = args.start_block if args.start_block is not None else snapshot_height + 1
    end = args.end_block if args.end_block is not None else start + args.block_count - 1
    if end < start:
        raise ValueError("--end-block must be >= --start-block")
    return snapshot_height, start, end


def remote_filtered_rows(
    host: str,
    remote_path: str,
    kind: str,
    coin: str,
    start: int,
    end: int,
    keep_empty_rows: bool,
) -> tuple[list[dict], dict[int, dict[str, int]]]:
    script = r"""
import json, re, sys
path, kind, coin, start, end, keep_empty = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), sys.argv[6] == "1"
block_re = re.compile(r'"block_number"\s*:\s*(\d+)')
def event_coin(kind, event):
    if kind == "fills":
        if isinstance(event, list) and len(event) >= 2 and isinstance(event[1], dict):
            return event[1].get("coin")
        if isinstance(event, dict):
            return event.get("coin")
        return None
    if not isinstance(event, dict):
        return None
    if kind == "statuses":
        order = event.get("order")
        return order.get("coin") if isinstance(order, dict) else None
    return event.get("coin")
rows = []
unfiltered = {}
with open(path, encoding="utf-8") as handle:
    for line in handle:
        match = block_re.search(line)
        if not match:
            continue
        block = int(match.group(1))
        if block < start:
            continue
        if block > end:
            break
        row = json.loads(line)
        events = row.get("events", [])
        if kind == "diffs":
            unfiltered[block] = {
                "total": len(events),
                "non_spot": sum(1 for event in events if not str(event.get("coin", "")).startswith("@")),
                "coin": sum(1 for event in events if event.get("coin") == coin),
            }
        filtered = [event for event in events if event_coin(kind, event) == coin]
        if filtered or keep_empty:
            row["events"] = filtered
            rows.append(row)
print(json.dumps({"rows": rows, "unfiltered": unfiltered}, separators=(",", ":")))
"""
    cmd = "python3 - " + " ".join(
        shlex.quote(part)
        for part in [remote_path, kind, coin, str(start), str(end), "1" if keep_empty_rows else "0"]
    )
    payload = ssh_text(host, cmd + " <<'PY'\n" + script + "\nPY")
    data = json.loads(payload)
    return data["rows"], {int(k): v for k, v in data["unfiltered"].items()}


def require_blocks(name: str, rows: list[dict], start: int, end: int) -> None:
    got = {row["block_number"] for row in rows}
    missing = [block for block in range(start, end + 1) if block not in got]
    if missing:
        raise ValueError(f"{name} missing blocks: {missing[:8]}")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(compact(row))
            handle.write("\n")


def count_file(path: Path) -> dict:
    rows = 0
    events = 0
    first = None
    last = None
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            rows += 1
            row = json.loads(line)
            events += len(row.get("events", []))
            block = row["block_number"]
            first = block if first is None else min(first, block)
            last = block if last is None else max(last, block)
    out = {"bytes": path.stat().st_size, "events": events, "rows": rows}
    if first is not None:
        out["first_block"] = first
        out["last_block"] = last
    return out


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def add_file_to_tar(tar: tarfile.TarFile, path: Path, arcname: Path) -> None:
    data = path.read_bytes()
    info = tarfile.TarInfo(str(arcname))
    info.size = len(data)
    info.mode = 0o644
    info.mtime = 0
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    tar.addfile(info, io.BytesIO(data))


def make_tar_gz(root: Path, archive: Path) -> None:
    archive.parent.mkdir(parents=True, exist_ok=True)
    with archive.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            with tarfile.open(fileobj=gz, mode="w") as tar:
                for path in sorted(root.rglob("*")):
                    if path.is_file():
                        add_file_to_tar(tar, path, path.relative_to(root))


def write_gzip(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            gz.write(data)


def main() -> None:
    args = parse_args()
    snapshot_bytes = ssh_bytes(args.stream_host, f"cat {shlex.quote(args.snapshot)}")
    snapshot = json.loads(snapshot_bytes)
    snapshot_height, start, end = infer_range(args, snapshot)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_root = Path(tmp)
        block_root = tmp_root / "by_block"
        stream_root = tmp_root / "streaming"
        source_paths = {"by_block": {}, "streaming": {}}
        counts = {"by_block": {}, "streaming": {}}
        unfiltered_diff_counts: dict[int, dict[str, int]] = {}

        for kind, source_dir in BY_BLOCK_DIRS.items():
            remote_path = hourly_files(args, args.block_host, source_dir)[-1]
            source_paths["by_block"][kind] = remote_path
            rows, unfiltered = remote_filtered_rows(args.block_host, remote_path, kind, args.coin, start, end, True)
            require_blocks(f"by_block {kind}", rows, start, end)
            unfiltered_diff_counts.update(unfiltered)
            rel = Path("hl/data") / source_dir / "hourly" / hourly_suffix(remote_path)
            write_jsonl(block_root / rel, rows)
            counts["by_block"][kind] = count_file(block_root / rel)

        for kind, source_dir in STREAMING_DIRS.items():
            remote_path = hourly_files(args, args.stream_host, source_dir)[-1]
            source_paths["streaming"][kind] = remote_path
            rows, _ = remote_filtered_rows(args.stream_host, remote_path, kind, args.coin, start, end, False)
            rel = Path("hl/data") / source_dir / "hourly" / hourly_suffix(remote_path)
            write_jsonl(stream_root / rel, rows)
            counts["streaming"][kind] = count_file(stream_root / rel)

        by_block_archive = SOURCE / f"by_block_btc_{start}_{end}.tar.gz"
        streaming_archive = SOURCE / f"streaming_btc_{start}_{end}.tar.gz"
        snapshot_archive = SOURCE / f"snapshot_{snapshot_height}.json.gz"
        make_tar_gz(block_root, by_block_archive)
        make_tar_gz(stream_root, streaming_archive)
        write_gzip(snapshot_archive, snapshot_bytes)

    single_blocks = [
        block
        for block in range(start, end + 1)
        if unfiltered_diff_counts.get(block, {}).get("coin", 0) == 1
    ]
    manifest = {
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "streaming_source_host": args.stream_host,
        "by_block_source_host": args.block_host,
        "source_snapshot": args.snapshot,
        "source_data_base": args.data_base,
        "source_paths": source_paths,
        "snapshot_height": snapshot_height,
        "fixture_block_start": start,
        "fixture_block_end": end,
        "coin_filter": [args.coin],
        "counts": counts,
        "unfiltered_by_block_raw_diff_counts": {str(k): v for k, v in sorted(unfiltered_diff_counts.items())},
        "expected_boundary_delta": {"count": len(single_blocks) * 2, "single_coin_diff_blocks": single_blocks},
        "source_archives": {
            "by_block": {"path": str(by_block_archive.relative_to(ROOT)), "sha256": sha256(by_block_archive)},
            "streaming": {"path": str(streaming_archive.relative_to(ROOT)), "sha256": sha256(streaming_archive)},
            "snapshot": {"path": str(snapshot_archive.relative_to(ROOT)), "sha256": sha256(snapshot_archive)},
        },
    }
    (ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def hourly_suffix(remote_path: str) -> Path:
    match = re.search(r"/hourly/(\d{8})/([^/]+)$", remote_path)
    if not match:
        raise ValueError(f"cannot infer hourly date/hour from {remote_path}")
    return Path(match.group(1)) / match.group(2)


if __name__ == "__main__":
    main()
