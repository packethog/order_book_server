#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

if [[ -z "${EDGE_MULTICAST_REF_DIR:-}" ]]; then
  echo "EDGE_MULTICAST_REF_DIR must point at a pinned edge-multicast-ref checkout" >&2
  exit 2
fi

EDGE_DIR="$(cd "${EDGE_MULTICAST_REF_DIR}" && pwd)"
IMAGE="${HL_TOB_PARSER_E2E_IMAGE:-order-book-tob-parser-e2e:local}"
TEST_FILTER="dual_validator_tob_streaming_feed_is_consumed_by_edge_parser"

docker build \
  -f "${SCRIPT_DIR}/Dockerfile.tob-parser-e2e" \
  -t "${IMAGE}" \
  "${SCRIPT_DIR}"

docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e EDGE_MULTICAST_REF_DIR=/edge-multicast-ref \
  -e HL_DUAL_VALIDATOR_REGENERATE="${HL_DUAL_VALIDATOR_REGENERATE:-}" \
  -v "${REPO_ROOT}:/workspace" \
  -v "${EDGE_DIR}:/edge-multicast-ref:ro" \
  -w /workspace \
  "${IMAGE}" \
  cargo test -p server "${TEST_FILTER}" -- --ignored --nocapture
