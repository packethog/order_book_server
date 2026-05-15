# Hyperliquid Dual Validator Fixture

This fixture captures matching BTC data from two Hyperliquid validators:

- streaming source: `ubuntu@tyo-hl-node`
- by-block source: `ubuntu@aws-tyo-hl-mainnet`

It covers snapshot height `985148181` and block range
`985148182..985148232`. Source inputs are committed as deterministic compressed
archives under `source/`; normalized multicast goldens and semantic parity
goldens live under `golden/`.

Regenerate source archives from validators:

```bash
server/tests/fixtures/hl_dual_validator/capture_dual_validator_fixture.py \
  --stream-host ubuntu@tyo-hl-node \
  --block-host ubuntu@aws-tyo-hl-mainnet \
  --start-block 985148182 \
  --end-block 985148232 \
  --date 20260505 \
  --hour 17
```

Regenerate goldens from the committed source archives:

```bash
server/tests/fixtures/hl_dual_validator/generate_dual_goldens.py
```

Regenerate the downstream top-of-book parser goldens too:

```bash
git clone https://github.com/malbeclabs/edge-multicast-ref.git /tmp/edge-multicast-ref
git -C /tmp/edge-multicast-ref checkout fa98eb78255f9d8492902db4b04c23aa04b074b8
EDGE_MULTICAST_REF_DIR=/tmp/edge-multicast-ref \
  server/tests/fixtures/hl_dual_validator/generate_dual_goldens.py
```

Run only the downstream parser e2e in Docker:

```bash
EDGE_MULTICAST_REF_DIR=/tmp/edge-multicast-ref \
  server/tests/fixtures/hl_dual_validator/run_tob_parser_e2e_in_docker.sh
```

The CI test extracts the compressed source archives to a temp directory, replays
both ingest modes through the production listener/publisher path, and compares:

- final L4 and L2 book state;
- DoB `OrderAdd`, `OrderCancel`, and `OrderExecute` payload bytes;
- expected DoB `BatchBoundary` minimization delta;
- DoB and TOB refdata payloads;
- streaming end-of-block TOB quotes against block-mode quotes with the same HL
  source timestamp.

The ignored downstream parser test is run through Docker so local multicast is
validated in a Linux network environment. It sends the committed TOB packet
goldens to `edge-multicast-ref`'s `go/topofbook-parser`, captures JSONL, and
compares parsed refdata plus end-of-block BTC quotes. It is intentionally
TOB-only; DoB parser validation belongs in a separate fixture once the parser
contract is pinned.
