//! Integration test: synthetic DoB byte-stream golden capture.
//!
//! Drives DobEvent → DobEmitter → UDP loopback, collects datagrams, masks the
//! non-deterministic frame send_timestamp_ns (bytes 12–19 in each datagram),
//! and compares byte-for-byte against a committed expected.bin.
//!
//! The sender is dropped without sending Shutdown so the emitter exits cleanly
//! without emitting EndOfSession.  The only non-deterministic bytes are the
//! per-frame send_timestamp_ns field, which the test zeros before comparison.
//!
//! # Regenerating expected.bin
//!
//! See `tests/fixtures/dob_golden/regenerate.md`.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::indexing_slicing)]
#![allow(unused_crate_dependencies)]

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

use server::multicast::dob::{DobEmitter, DobEvent, DobMktdataConfig, channel, run_dob_emitter};
use server::protocol::dob::messages::{BatchBoundary, OrderAdd, OrderCancel, OrderExecute};
use tokio::net::UdpSocket;

// 24 (frame header) + 16 (BB) + 52 (OA) + 32 (OC) + 56 (OE) + 16 (BB) = 196.
// Exactly fills the frame with the 5 test events; the 6th event triggers flush.
const TEST_MTU: u16 = 196;

// Frame header layout: bytes [12, 20) are the non-deterministic send_timestamp_ns.
const TS_OFFSET: usize = 12;
const TS_END: usize = 20;

#[tokio::test]
async fn golden_dob_capture_matches() {
    let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/dob_golden");

    let mut datagrams = collect_test_datagrams().await;
    for dg in &mut datagrams {
        if dg.len() >= TS_END {
            dg[TS_OFFSET..TS_END].fill(0);
        }
    }
    let actual: Vec<u8> = datagrams.into_iter().flatten().collect();

    let expected_path = fixture_dir.join("expected.bin");
    if !expected_path.exists() {
        std::fs::create_dir_all(&fixture_dir).unwrap();
        std::fs::write(&expected_path, &actual).unwrap();
        // First-run seed: inspect expected.bin with xxd and commit it.
        return;
    }

    let expected = std::fs::read(&expected_path).expect("failed to read expected.bin");
    if actual != expected {
        let actual_path = fixture_dir.join("actual.bin");
        std::fs::write(&actual_path, &actual).unwrap();
        panic!(
            "golden mismatch: expected {} bytes, got {}. \
             Diff: xxd tests/fixtures/dob_golden/{{expected,actual}}.bin",
            expected.len(),
            actual.len(),
        );
    }
}

async fn collect_test_datagrams() -> Vec<Vec<u8>> {
    // Bind a collector on a random loopback port; the emitter sends to it.
    let collector = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let collector_port = match collector.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a.port(),
        _ => unreachable!(),
    };

    let emitter = DobEmitter::bind(DobMktdataConfig {
        group_addr: Ipv4Addr::LOCALHOST,
        port: collector_port,
        bind_addr: Ipv4Addr::LOCALHOST,
        channel_id: 3,
        mtu: TEST_MTU,
        heartbeat_interval: Duration::from_secs(3600),
    })
    .await
    .unwrap();

    let (tx, rx) = channel(64);
    let handle = tokio::spawn(run_dob_emitter(emitter, rx));

    // 5 events that exactly fill the TEST_MTU frame.
    tx.send(DobEvent::BatchBoundary(BatchBoundary { channel_id: 3, phase: 0, batch_id: 1_000 })).await.unwrap();
    tx.send(DobEvent::OrderAdd(OrderAdd {
        instrument_id: 0,
        source_id: 1,
        side: 0,
        order_flags: 0,
        per_instrument_seq: 1,
        order_id: 9_999_999_001,
        enter_timestamp_ns: 1_700_000_000_000_000_000,
        price: 50_000_000_000_000,
        quantity: 1_000_000_000,
    }))
    .await
    .unwrap();
    tx.send(DobEvent::OrderCancel(OrderCancel {
        instrument_id: 0,
        source_id: 1,
        reason: 0,
        per_instrument_seq: 2,
        order_id: 9_999_999_001,
        timestamp_ns: 1_700_000_000_000_000_100,
    }))
    .await
    .unwrap();
    tx.send(DobEvent::OrderExecute(OrderExecute {
        instrument_id: 1,
        source_id: 1,
        aggressor_side: 2,
        exec_flags: 0,
        per_instrument_seq: 1,
        order_id: 9_999_999_002,
        trade_id: 42,
        timestamp_ns: 1_700_000_000_000_000_200,
        exec_price: 49_900_000_000_000,
        exec_quantity: 500_000_000,
    }))
    .await
    .unwrap();
    tx.send(DobEvent::BatchBoundary(BatchBoundary { channel_id: 3, phase: 1, batch_id: 1_000 })).await.unwrap();

    // This event does not fit in the now-full frame, so append() flushes the
    // 5-event frame above, then starts a new incomplete frame for this event.
    tx.send(DobEvent::OrderAdd(OrderAdd {
        instrument_id: 0,
        source_id: 1,
        side: 1,
        order_flags: 0,
        per_instrument_seq: 3,
        order_id: 9_999_999_003,
        enter_timestamp_ns: 1_700_000_000_000_001_000,
        price: 50_100_000_000_000,
        quantity: 200_000_000,
    }))
    .await
    .unwrap();

    // Dropping tx causes the emitter to exit cleanly without EndOfSession.
    // The incomplete frame holding the 6th event is discarded on exit.
    drop(tx);
    handle.await.unwrap().unwrap();

    let mut out: Vec<Vec<u8>> = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match tokio::time::timeout(Duration::from_millis(100), collector.recv(&mut buf)).await {
            Ok(Ok(n)) => out.push(buf[..n].to_vec()),
            _ => break,
        }
    }
    out
}
