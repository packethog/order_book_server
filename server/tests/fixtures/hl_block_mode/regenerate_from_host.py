#!/usr/bin/env python3
"""Fetch Hyperliquid block-mode source extracts from a host and refresh fixtures."""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import shlex
import subprocess
import sys
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from generate_from_source import EVENT_FILES, count_events, event_coin

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parents[3]
DEFAULT_ARCHIVE = ROOT / "source" / "hl_block_mode_unfiltered.tar.gz"

REMOTE_SOURCE_DIRS = {
    "diffs": "node_raw_book_diffs_by_block",
    "fills": "node_fills_by_block",
    "statuses": "node_order_statuses_by_block",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--snapshot", default="/home/ubuntu/out.json")
    parser.add_argument("--data-base", default="/data/hl-data")
    parser.add_argument("--date", help="Optional YYYYMMDD hourly directory override")
    parser.add_argument("--hour", help="Optional hour file override, for example 20")
    parser.add_argument("--start-block", type=int, help="Defaults to snapshot_height + 1")
    parser.add_argument("--end-block", type=int, help="Defaults to start_block + block_count - 1")
    parser.add_argument("--block-count", type=int, default=40)
    parser.add_argument("--scan-files", type=int, default=6, help="Recent hourly files to inspect per source")
    parser.add_argument("--tail-lines", type=int, default=5_000)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--coin", action="append", dest="coins", default=["BTC"])
    parser.add_argument("--skip-goldens", action="store_true")
    parser.add_argument("--skip-verify", action="store_true")
    return parser.parse_args()


def run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    print("+", " ".join(shlex.quote(part) for part in cmd), flush=True)
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def capture_remote(host: str, remote_cmd: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    print(f"+ ssh {host} {remote_cmd!r} > {output}", flush=True)
    with output.open("wb") as handle:
        subprocess.run(["ssh", host, remote_cmd], stdout=handle, check=True)


def capture_remote_text(host: str, remote_cmd: str) -> str:
    print(f"+ ssh {host} {remote_cmd!r}", flush=True)
    return subprocess.check_output(["ssh", host, remote_cmd], text=True)


def jsonl_rows_in_range(path: Path, start_block: int, end_block: int) -> list[dict]:
    rows = []
    with path.open() as handle:
        for line in handle:
            data = json.loads(line)
            block = data.get("block_number")
            if start_block <= block <= end_block:
                rows.append(data)
    return rows


def require_complete_block_range(name: str, rows: list[dict], start_block: int, end_block: int) -> None:
    wanted = set(range(start_block, end_block + 1))
    got = {row["block_number"] for row in rows}
    missing = sorted(wanted - got)
    if missing:
        sample = ", ".join(str(block) for block in missing[:8])
        raise ValueError(
            f"{name} source files were missing {len(missing)} block(s) in "
            f"{start_block}..{end_block}; first missing: {sample}. "
            "Increase --tail-lines/--scan-files or pass explicit --date/--hour/--start-block."
        )


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")))
            handle.write("\n")


def source_counts(root: Path) -> dict:
    return {name: count_events(root / relative) for name, relative in EVENT_FILES.items()}


def touched_coin_count(root: Path) -> int:
    coins = set()
    for relative in EVENT_FILES.values():
        with (root / relative).open() as handle:
            for line in handle:
                data = json.loads(line)
                for event in data.get("events", []):
                    coin = event_coin(event)
                    if coin is not None:
                        coins.add(coin)
    return len(coins)


def snapshot_data(snapshot_path: Path) -> list:
    return json.loads(snapshot_path.read_text())


def infer_block_range(args: argparse.Namespace, snapshot_path: Path) -> tuple[int, int, int]:
    height = int(snapshot_data(snapshot_path)[0])
    start_block = args.start_block if args.start_block is not None else height + 1
    end_block = args.end_block if args.end_block is not None else start_block + args.block_count - 1
    if end_block < start_block:
        raise ValueError("--end-block must be greater than or equal to --start-block")
    return height, start_block, end_block


def remote_hourly_files(args: argparse.Namespace, source_dir: str) -> tuple[list[str], list[str] | str]:
    data_base = args.data_base.rstrip("/")
    if args.date is not None or args.hour is not None:
        if args.date is None or args.hour is None:
            raise ValueError("--date and --hour must be passed together")
        relative = f"{source_dir}/hourly/{args.date}/{args.hour}"
        return [f"{data_base}/{relative}"], relative

    hourly_root = f"{data_base}/{source_dir}/hourly"
    find_cmd = f"find {shlex.quote(hourly_root)} -type f | sort | tail -n {args.scan_files}"
    files = [line for line in capture_remote_text(args.host, find_cmd).splitlines() if line]
    if not files:
        raise ValueError(f"no hourly files found under {hourly_root} on {args.host}")

    prefix = f"{data_base}/"
    manifest_files = [path[len(prefix) :] if path.startswith(prefix) else path for path in files]
    return files, manifest_files


def build_source_tree(args: argparse.Namespace, workdir: Path) -> Path:
    source_root = workdir / "hl_block_mode"
    source_root.mkdir(parents=True)

    capture_remote(args.host, f"cat {shlex.quote(args.snapshot)}", source_root / "out.json")
    snapshot = snapshot_data(source_root / "out.json")
    snapshot_height, start_block, end_block = infer_block_range(args, source_root / "out.json")
    print(f"using block range {start_block}..{end_block} from snapshot height {snapshot_height}", flush=True)

    source_files = {}
    for name, source_dir in REMOTE_SOURCE_DIRS.items():
        remote_paths, manifest_paths = remote_hourly_files(args, source_dir)
        source_files[name] = manifest_paths
        rows = []
        for index, remote_path in enumerate(remote_paths):
            staged = workdir / f"{name}.{index}.tail.jsonl"
            capture_remote(args.host, f"tail -n {args.tail_lines} -- {shlex.quote(remote_path)}", staged)
            rows.extend(jsonl_rows_in_range(staged, start_block, end_block))
        rows.sort(key=lambda row: row["block_number"])
        require_complete_block_range(name, rows, start_block, end_block)
        write_jsonl(source_root / EVENT_FILES[name], rows)

    manifest = {
        "counts": source_counts(source_root),
        "fixture_block_end": end_block,
        "fixture_block_start": start_block,
        "fixture_files": EVENT_FILES,
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "notes": (
            "Reduced from real Hyperliquid by-block output. Source files were sampled with "
            f"tail -n {args.tail_lines} across the latest {args.scan_files} hourly file(s) "
            "to avoid scanning large hourly files."
        ),
        "snapshot_coin_count": len(snapshot[1]),
        "snapshot_height": snapshot_height,
        "source_data_base": args.data_base,
        "source_files": source_files,
        "source_host": args.host,
        "source_snapshot": args.snapshot,
        "touched_coin_count": touched_coin_count(source_root),
    }
    (source_root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return source_root


def add_file_to_tar(tar: tarfile.TarFile, path: Path, arcname: Path) -> None:
    data = path.read_bytes()
    info = tarfile.TarInfo(str(arcname))
    info.size = len(data)
    info.mode = 0o644
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    tar.addfile(info, io.BytesIO(data))


def create_source_archive(source_root: Path, archive: Path) -> None:
    archive.parent.mkdir(parents=True, exist_ok=True)
    with archive.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            with tarfile.open(fileobj=gz, mode="w") as tar:
                for path in sorted(source_root.rglob("*")):
                    if path.is_file():
                        add_file_to_tar(tar, path, Path("hl_block_mode") / path.relative_to(source_root))


def regenerate_from_archive(args: argparse.Namespace) -> None:
    cmd = [sys.executable, str(ROOT / "generate_from_source.py"), "--archive", str(args.archive)]
    for coin in dict.fromkeys(args.coins):
        cmd.extend(["--coin", coin])
    run(cmd, cwd=REPO_ROOT)


def regenerate_goldens(args: argparse.Namespace) -> None:
    test_filter = "listeners::order_book::block_mode_multicast_e2e"
    env = os.environ.copy()
    env["HL_BLOCK_MODE_REGENERATE"] = "1"
    run(["cargo", "test", "-p", "server", test_filter, "--", "--nocapture"], cwd=REPO_ROOT, env=env)
    if not args.skip_verify:
        run(["cargo", "test", "-p", "server", test_filter, "--", "--nocapture"], cwd=REPO_ROOT)


def main() -> None:
    args = parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        source_root = build_source_tree(args, Path(tmp))
        create_source_archive(source_root, args.archive)
    regenerate_from_archive(args)
    if not args.skip_goldens:
        regenerate_goldens(args)


if __name__ == "__main__":
    main()
