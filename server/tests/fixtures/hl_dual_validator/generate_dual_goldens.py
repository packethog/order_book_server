#!/usr/bin/env python3
"""Regenerate dual-validator multicast goldens from committed source archives."""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parents[3]


def run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print("+", " ".join(shlex.quote(part) for part in cmd), flush=True)
    subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=True)


def main() -> None:
    test_filter = "dual_validator_fixture_matches_block_and_stream_goldens"
    env = os.environ.copy()
    env["HL_DUAL_VALIDATOR_REGENERATE"] = "1"
    run(["cargo", "test", "-p", "server", test_filter, "--", "--nocapture"], env=env)
    run(["cargo", "test", "-p", "server", test_filter, "--", "--nocapture"])

    if env.get("EDGE_MULTICAST_REF_DIR"):
        docker_runner = ROOT / "run_tob_parser_e2e_in_docker.sh"
        run([str(docker_runner)], env=env)
        stable_env = os.environ.copy()
        run([str(docker_runner)], env=stable_env)
    else:
        print(f"skipping parser goldens: set EDGE_MULTICAST_REF_DIR to an edge-multicast-ref checkout", flush=True)


if __name__ == "__main__":
    main()
