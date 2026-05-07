#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::indexing_slicing)]

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    net::{Ipv4Addr, SocketAddrV4},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy::primitives::Address;
use tokio::{
    io::AsyncReadExt,
    net::UdpSocket,
    process::Command as TokioCommand,
    sync::{Mutex, broadcast::channel as broadcast_channel, mpsc},
    time::{sleep, timeout},
};

use super::{EventSource, IngestMode, InternalMessage, OrderBookListener, utils::EventBatch};
use crate::{
    instruments::{InstrumentInfo, RegistryState, UniverseEntry, make_symbol, new_shared_registry},
    listeners::{
        directory::DirectoryListener,
        order_book::dob_tap::{DobApplyTap, SharedSeqCounter},
    },
    multicast::{
        config::MulticastConfig,
        dob::{
            DobEmitter, DobEvent, DobMktdataConfig, DobRefdataConfig, channel as dob_channel, run_dob_emitter,
            run_dob_refdata_task,
        },
        publisher::MulticastPublisher,
    },
    order_book::{Coin, PerInstrumentSeqCounter, multi_book::load_snapshots_from_json},
    protocol::{constants as tob_const, dob::constants as dob_const},
    types::{
        L4Order, OrderDiff,
        inner::InnerL4Order,
        node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};

const FIXTURE_ROOT: &str = "tests/fixtures/hl_block_mode";
const DUAL_FIXTURE_ROOT: &str = "tests/fixtures/hl_dual_validator";
const DUAL_REGENERATE_ENV: &str = "HL_DUAL_VALIDATOR_REGENERATE";
const EDGE_MULTICAST_REF_ENV: &str = "EDGE_MULTICAST_REF_DIR";
const EDGE_MULTICAST_REF_MAIN_SHA: &str = "fa98eb78255f9d8492902db4b04c23aa04b074b8";
const BTC_INSTRUMENT_ID: u32 = 0;
const SOURCE_ID: u16 = 1;
const DOB_CHANNEL_ID: u8 = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_block_mode_fixture_publishes_expected_multicast_packets() {
    let root = fixture_root();
    let snapshot_path = root.join("out.json");
    let (snapshot_height, snapshot) = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&snapshot_path)
        .await
        .expect("fixture snapshot loads");

    let last_block = fixture_last_block(&root);
    assert!(last_block > snapshot_height, "fixture must start after snapshot");

    let registry = new_shared_registry(test_registry_state());
    let (internal_tx, _) = broadcast_channel::<Arc<InternalMessage>>(512);

    let mut listener = OrderBookListener::new_with_ingest_mode(Some(internal_tx.clone()), true, IngestMode::Block);
    listener.init_from_snapshot(snapshot, snapshot_height);

    let (dob_tx, dob_rx) = dob_channel(8192);
    let seq_counter: SharedSeqCounter = Arc::new(StdMutex::new(PerInstrumentSeqCounter::new()));
    listener.set_dob_tap(DobApplyTap::new(
        dob_tx.clone(),
        SOURCE_ID,
        DOB_CHANNEL_ID,
        seq_counter,
        Box::new(|coin: &Coin| if coin.value() == "BTC" { Some((BTC_INSTRUMENT_ID, -8)) } else { None }),
    ));
    let listener = Arc::new(Mutex::new(listener));

    let tob_market = UdpCollector::bind().await;
    let tob_refdata = UdpCollector::bind().await;
    let dob_mktdata = UdpCollector::bind().await;
    let dob_refdata = UdpCollector::bind().await;

    let tob_handle =
        spawn_tob_publisher(internal_tx.subscribe(), registry.clone(), tob_market.port(), tob_refdata.port()).await;
    let dob_handle = spawn_dob_mktdata(dob_rx, dob_mktdata.port()).await;
    let dob_refdata_handle = tokio::spawn(run_dob_refdata_task(
        DobRefdataConfig {
            group_addr: Ipv4Addr::LOCALHOST,
            port: dob_refdata.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: DOB_CHANNEL_ID,
            mtu: dob_const::DEFAULT_MTU,
            definition_cycle: Duration::from_millis(20),
            manifest_cadence: Duration::from_millis(20),
        },
        crate::instruments::InstrumentRegistry::from_arc(registry.clone()),
    ));

    replay_fixture_blocks(&root, &listener).await;
    assert_eq!(
        listener.lock().await.order_book_state.as_ref().unwrap().height(),
        last_block,
        "listener height after fixture replay",
    );

    dob_tx.send(DobEvent::Shutdown).await.unwrap();
    timeout(Duration::from_secs(2), dob_handle)
        .await
        .expect("DoB emitter shuts down")
        .expect("DoB emitter join")
        .expect("DoB emitter result");

    sleep(Duration::from_millis(700)).await;
    tob_handle.abort();
    dob_refdata_handle.abort();

    let tob_quote_packets = tob_market.finish();
    let tob_reference_packets = tob_refdata.finish();
    let dob_delta_packets = dob_mktdata.finish();
    let dob_reference_packets = dob_refdata.finish();

    assert_contiguous_frame_seqs(&dob_delta_packets, "DoB mktdata");
    assert_contiguous_per_instrument_seqs(&dob_delta_packets);

    let streams = [
        ("tob_marketdata.bin", normalize_tob(tob_quote_packets)),
        (
            "tob_refdata.bin",
            first_packets_by_msg_type(
                normalize_tob(tob_reference_packets),
                &[
                    tob_const::MSG_TYPE_CHANNEL_RESET,
                    tob_const::MSG_TYPE_INSTRUMENT_DEF,
                    tob_const::MSG_TYPE_MANIFEST_SUMMARY,
                ],
            ),
        ),
        ("dob_mktdata.bin", normalize_dob(dob_delta_packets)),
        (
            "dob_refdata.bin",
            first_packets_by_msg_type(
                normalize_dob_refdata(dob_reference_packets),
                &[dob_const::MSG_TYPE_INSTRUMENT_DEF, dob_const::MSG_TYPE_MANIFEST_SUMMARY],
            ),
        ),
    ];

    for (name, packets) in &streams {
        assert!(!packets.is_empty(), "{name} captured at least one packet");
        assert_golden(name, packets);
    }

    assert_has_msg_type(&streams[0].1, tob_const::MSG_TYPE_QUOTE, "TOB quote");
    assert_has_msg_type(&streams[0].1, tob_const::MSG_TYPE_TRADE, "TOB trade");
    assert_has_msg_type(&streams[1].1, tob_const::MSG_TYPE_CHANNEL_RESET, "TOB refdata reset");
    assert_has_msg_type(&streams[1].1, tob_const::MSG_TYPE_INSTRUMENT_DEF, "TOB instrument definition");
    assert_has_msg_type(&streams[1].1, tob_const::MSG_TYPE_MANIFEST_SUMMARY, "TOB manifest summary");

    assert_has_msg_type(&streams[2].1, dob_const::MSG_TYPE_ORDER_ADD, "DoB order add");
    assert_has_msg_type(&streams[2].1, dob_const::MSG_TYPE_ORDER_CANCEL, "DoB order cancel");
    assert_has_msg_type(&streams[2].1, dob_const::MSG_TYPE_ORDER_EXECUTE, "DoB order execute");
    assert_has_msg_type(&streams[2].1, dob_const::MSG_TYPE_BATCH_BOUNDARY, "DoB batch boundary");
    assert_no_msg_type(&streams[2].1, dob_const::MSG_TYPE_INSTRUMENT_RESET, "DoB instrument reset");
    assert_has_msg_type(&streams[3].1, dob_const::MSG_TYPE_INSTRUMENT_DEF, "DoB instrument definition");
    assert_has_msg_type(&streams[3].1, dob_const::MSG_TYPE_MANIFEST_SUMMARY, "DoB manifest summary");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_fixture_publishes_expected_multicast_packets() {
    let root = fixture_root();
    let snapshot_path = root.join("out.json");
    let (snapshot_height, snapshot) = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&snapshot_path)
        .await
        .expect("fixture snapshot loads");

    let last_block = fixture_last_block(&root);
    assert!(last_block > snapshot_height, "fixture must start after snapshot");

    let registry = new_shared_registry(test_registry_state());
    let (internal_tx, _) = broadcast_channel::<Arc<InternalMessage>>(512);

    let mut listener = OrderBookListener::new_with_ingest_mode(Some(internal_tx.clone()), true, IngestMode::Stream);
    listener.init_from_snapshot(snapshot, snapshot_height);

    let (dob_tx, dob_rx) = dob_channel(8192);
    let seq_counter: SharedSeqCounter = Arc::new(StdMutex::new(PerInstrumentSeqCounter::new()));
    listener.set_dob_tap(DobApplyTap::new(
        dob_tx.clone(),
        SOURCE_ID,
        DOB_CHANNEL_ID,
        seq_counter,
        Box::new(|coin: &Coin| if coin.value() == "BTC" { Some((BTC_INSTRUMENT_ID, -8)) } else { None }),
    ));
    let listener = Arc::new(Mutex::new(listener));

    let tob_market = UdpCollector::bind().await;
    let tob_refdata = UdpCollector::bind().await;
    let dob_mktdata = UdpCollector::bind().await;
    let dob_refdata = UdpCollector::bind().await;

    let tob_handle =
        spawn_tob_publisher(internal_tx.subscribe(), registry.clone(), tob_market.port(), tob_refdata.port()).await;
    let dob_handle = spawn_dob_mktdata(dob_rx, dob_mktdata.port()).await;
    let dob_refdata_handle = tokio::spawn(run_dob_refdata_task(
        DobRefdataConfig {
            group_addr: Ipv4Addr::LOCALHOST,
            port: dob_refdata.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: DOB_CHANNEL_ID,
            mtu: dob_const::DEFAULT_MTU,
            definition_cycle: Duration::from_millis(20),
            manifest_cadence: Duration::from_millis(20),
        },
        crate::instruments::InstrumentRegistry::from_arc(registry.clone()),
    ));

    replay_fixture_streams(&root, &listener).await;
    assert_eq!(
        listener.lock().await.order_book_state.as_ref().unwrap().height(),
        last_block,
        "listener height after streaming fixture replay",
    );

    dob_tx.send(DobEvent::Shutdown).await.unwrap();
    timeout(Duration::from_secs(2), dob_handle)
        .await
        .expect("DoB emitter shuts down")
        .expect("DoB emitter join")
        .expect("DoB emitter result");

    sleep(Duration::from_millis(700)).await;
    tob_handle.abort();
    dob_refdata_handle.abort();

    let tob_quote_packets = tob_market.finish();
    let tob_reference_packets = tob_refdata.finish();
    let dob_delta_packets = dob_mktdata.finish();
    let dob_reference_packets = dob_refdata.finish();

    assert_contiguous_frame_seqs(&dob_delta_packets, "DoB streaming mktdata");
    assert_contiguous_per_instrument_seqs(&dob_delta_packets);

    let streams = [
        ("stream_tob_marketdata.bin", normalize_tob(tob_quote_packets)),
        (
            "stream_tob_refdata.bin",
            first_packets_by_msg_type(
                normalize_tob(tob_reference_packets),
                &[
                    tob_const::MSG_TYPE_CHANNEL_RESET,
                    tob_const::MSG_TYPE_INSTRUMENT_DEF,
                    tob_const::MSG_TYPE_MANIFEST_SUMMARY,
                ],
            ),
        ),
        ("stream_dob_mktdata.bin", normalize_dob(dob_delta_packets)),
        (
            "stream_dob_refdata.bin",
            first_packets_by_msg_type(
                normalize_dob_refdata(dob_reference_packets),
                &[dob_const::MSG_TYPE_INSTRUMENT_DEF, dob_const::MSG_TYPE_MANIFEST_SUMMARY],
            ),
        ),
    ];

    for (name, packets) in &streams {
        assert!(!packets.is_empty(), "{name} captured at least one packet");
        assert_golden(name, packets);
    }

    assert_has_msg_type(&streams[0].1, tob_const::MSG_TYPE_QUOTE, "streaming TOB quote");
    assert_has_msg_type(&streams[0].1, tob_const::MSG_TYPE_TRADE, "streaming TOB trade");
    assert_has_msg_type(&streams[2].1, dob_const::MSG_TYPE_ORDER_ADD, "streaming DoB order add");
    assert_has_msg_type(&streams[2].1, dob_const::MSG_TYPE_ORDER_CANCEL, "streaming DoB order cancel");
    assert_has_msg_type(&streams[2].1, dob_const::MSG_TYPE_ORDER_EXECUTE, "streaming DoB order execute");
    assert_has_msg_type(&streams[2].1, dob_const::MSG_TYPE_BATCH_BOUNDARY, "streaming DoB batch boundary");
    assert_no_msg_type(&streams[2].1, dob_const::MSG_TYPE_INSTRUMENT_RESET, "streaming DoB instrument reset");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires HL_STREAM_CAPTURE_ROOT pointing at a real hl-node _streaming capture"]
async fn live_stream_capture_replays_expected_multicast_packets() {
    let root = PathBuf::from(
        std::env::var("HL_STREAM_CAPTURE_ROOT").expect("HL_STREAM_CAPTURE_ROOT points at a live capture root"),
    );
    let snapshot_path = root.join("out.json");
    let (snapshot_height, snapshot) = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&snapshot_path)
        .await
        .expect("live snapshot loads");

    let last_block = fixture_last_stream_block(&root);
    assert!(last_block > snapshot_height, "live stream capture must start after snapshot");

    let replay_coin = live_capture_coin(&root);
    let expected = expected_stream_messages(&root, &replay_coin);
    assert!(expected.order_events > 0, "live stream capture includes {replay_coin} order events");

    let registry = new_shared_registry(test_registry_state_for(&replay_coin));
    let (internal_tx, _) = broadcast_channel::<Arc<InternalMessage>>(512);

    let mut listener = OrderBookListener::new_with_ingest_mode(Some(internal_tx.clone()), true, IngestMode::Stream);
    listener.init_from_snapshot(snapshot, snapshot_height);

    let (dob_tx, dob_rx) = dob_channel(8192);
    let seq_counter: SharedSeqCounter = Arc::new(StdMutex::new(PerInstrumentSeqCounter::new()));
    listener.set_dob_tap(DobApplyTap::new(
        dob_tx.clone(),
        SOURCE_ID,
        DOB_CHANNEL_ID,
        seq_counter,
        Box::new(move |coin: &Coin| if coin.value() == replay_coin { Some((BTC_INSTRUMENT_ID, -8)) } else { None }),
    ));
    let listener = Arc::new(Mutex::new(listener));

    let tob_market = UdpCollector::bind().await;
    let tob_refdata = UdpCollector::bind().await;
    let dob_mktdata = UdpCollector::bind().await;
    let dob_refdata = UdpCollector::bind().await;

    let tob_handle =
        spawn_tob_publisher(internal_tx.subscribe(), registry.clone(), tob_market.port(), tob_refdata.port()).await;
    let dob_handle = spawn_dob_mktdata(dob_rx, dob_mktdata.port()).await;
    let dob_refdata_handle = tokio::spawn(run_dob_refdata_task(
        DobRefdataConfig {
            group_addr: Ipv4Addr::LOCALHOST,
            port: dob_refdata.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: DOB_CHANNEL_ID,
            mtu: dob_const::DEFAULT_MTU,
            definition_cycle: Duration::from_millis(20),
            manifest_cadence: Duration::from_millis(20),
        },
        crate::instruments::InstrumentRegistry::from_arc(registry.clone()),
    ));

    replay_fixture_streams(&root, &listener).await;
    assert_eq!(
        listener.lock().await.order_book_state.as_ref().unwrap().height(),
        last_block,
        "listener height after live streaming replay",
    );

    dob_tx.send(DobEvent::Shutdown).await.unwrap();
    timeout(Duration::from_secs(2), dob_handle)
        .await
        .expect("DoB emitter shuts down")
        .expect("DoB emitter join")
        .expect("DoB emitter result");

    sleep(Duration::from_millis(700)).await;
    tob_handle.abort();
    dob_refdata_handle.abort();

    let tob_quote_packets = normalize_tob(tob_market.finish());
    let tob_reference_packets = first_packets_by_msg_type(
        normalize_tob(tob_refdata.finish()),
        &[tob_const::MSG_TYPE_CHANNEL_RESET, tob_const::MSG_TYPE_INSTRUMENT_DEF, tob_const::MSG_TYPE_MANIFEST_SUMMARY],
    );
    let dob_delta_packets = normalize_dob(dob_mktdata.finish());
    let dob_reference_packets = first_packets_by_msg_type(
        normalize_dob_refdata(dob_refdata.finish()),
        &[dob_const::MSG_TYPE_INSTRUMENT_DEF, dob_const::MSG_TYPE_MANIFEST_SUMMARY],
    );

    assert!(!tob_quote_packets.is_empty(), "live TOB marketdata captured packets");
    assert!(!tob_reference_packets.is_empty(), "live TOB refdata captured packets");
    assert!(!dob_delta_packets.is_empty(), "live DoB marketdata captured packets");
    assert!(!dob_reference_packets.is_empty(), "live DoB refdata captured packets");
    assert_contiguous_frame_seqs(&dob_delta_packets, "live DoB streaming mktdata");
    assert_contiguous_per_instrument_seqs(&dob_delta_packets);

    assert_has_msg_type(&tob_quote_packets, tob_const::MSG_TYPE_QUOTE, "live streaming TOB quote");
    assert_has_msg_type(&tob_reference_packets, tob_const::MSG_TYPE_CHANNEL_RESET, "live streaming TOB refdata reset");
    assert_has_msg_type(&tob_reference_packets, tob_const::MSG_TYPE_INSTRUMENT_DEF, "live streaming TOB refdata");
    assert_has_msg_type(&tob_reference_packets, tob_const::MSG_TYPE_MANIFEST_SUMMARY, "live streaming TOB refdata");
    assert_has_msg_type(&dob_delta_packets, dob_const::MSG_TYPE_BATCH_BOUNDARY, "live streaming DoB batch boundary");
    assert_no_msg_type(&dob_delta_packets, dob_const::MSG_TYPE_INSTRUMENT_RESET, "live streaming DoB instrument reset");
    assert_has_msg_type(&dob_reference_packets, dob_const::MSG_TYPE_INSTRUMENT_DEF, "live streaming DoB refdata");
    assert_has_msg_type(&dob_reference_packets, dob_const::MSG_TYPE_MANIFEST_SUMMARY, "live streaming DoB refdata");

    if expected.adds > 0 {
        assert_has_msg_type(&dob_delta_packets, dob_const::MSG_TYPE_ORDER_ADD, "live streaming DoB order add");
    }
    if expected.cancels > 0 {
        assert_has_msg_type(&dob_delta_packets, dob_const::MSG_TYPE_ORDER_CANCEL, "live streaming DoB order cancel");
    }
    if expected.executes > 0 {
        assert_has_msg_type(&dob_delta_packets, dob_const::MSG_TYPE_ORDER_EXECUTE, "live streaming DoB order execute");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dual_validator_fixture_matches_block_and_stream_goldens() {
    let fixture = extract_dual_validator_fixture();
    let root = fixture.path();
    let manifest = dual_validator_manifest();
    let replay_coin = dual_fixture_coin(&manifest);

    let block = capture_fixture_replay(root, IngestMode::Block, &replay_coin).await;
    let stream = capture_fixture_replay(root, IngestMode::Stream, &replay_coin).await;

    let expected_end = manifest["fixture_block_end"].as_u64().expect("dual manifest has fixture_block_end");
    assert_eq!(block.height, expected_end, "block replay final height");
    assert_eq!(stream.height, expected_end, "stream replay final height");
    assert_eq!(block.l4_snapshot, stream.l4_snapshot, "dual final L4 snapshot parity");
    assert_eq!(block.l2_snapshot, stream.l2_snapshot, "dual final L2 snapshot parity");

    let block_dob_orders = dob_order_messages(&block.dob_mktdata);
    let stream_dob_orders = dob_order_messages(&stream.dob_mktdata);
    assert_message_stream_eq("dual DoB order-event payload parity", &block_dob_orders, &stream_dob_orders);

    let expected_boundary_delta = manifest["expected_boundary_delta"]["count"]
        .as_u64()
        .expect("dual manifest has expected boundary delta") as usize;
    assert_eq!(
        boundary_count(&stream.dob_mktdata).saturating_sub(boundary_count(&block.dob_mktdata)),
        expected_boundary_delta,
        "dual streaming boundary delta matches minimization manifest",
    );
    assert_eq!(
        expected_minimized_boundary_delta(root),
        expected_boundary_delta,
        "dual fixture data produces manifest boundary delta",
    );

    assert_packet_stream_eq("dual DoB refdata payload parity", &block.dob_refdata, &stream.dob_refdata);
    assert_packet_stream_eq("dual TOB refdata payload parity", &block.tob_refdata, &stream.tob_refdata);

    let block_end_quote_map = end_of_block_quote_map(&block.tob_marketdata);
    let stream_end_quote_map = end_of_block_quote_map(&stream.tob_marketdata);
    let common_end_quotes = assert_stream_end_quotes_match_block(&block_end_quote_map, &stream_end_quote_map);

    assert_dual_packet_golden("block_dob_mktdata.bin", &block.dob_mktdata);
    assert_dual_packet_golden("stream_dob_mktdata.bin", &stream.dob_mktdata);
    assert_dual_packet_golden("block_dob_refdata.bin", &block.dob_refdata);
    assert_dual_packet_golden("stream_dob_refdata.bin", &stream.dob_refdata);
    assert_dual_packet_golden("block_tob_marketdata.bin", &block.tob_marketdata);
    assert_dual_packet_golden("stream_tob_marketdata.bin", &stream.tob_marketdata);
    assert_dual_packet_golden("block_tob_refdata.bin", &block.tob_refdata);
    assert_dual_packet_golden("stream_tob_refdata.bin", &stream.tob_refdata);
    assert_dual_message_golden("dob_order_events.bin", &block_dob_orders);
    assert_dual_message_golden("tob_end_of_block_quotes.bin", &common_end_quotes);
    assert_dual_json_golden(
        "parity_report.json",
        &serde_json::json!({
            "block_height": block.height,
            "stream_height": stream.height,
            "dob": {
                "block_boundary_count": boundary_count(&block.dob_mktdata),
                "stream_boundary_count": boundary_count(&stream.dob_mktdata),
                "boundary_delta": expected_boundary_delta,
                "order_event_count": block_dob_orders.len(),
                "order_event_type_counts": msg_type_counts(&block_dob_orders.iter().filter_map(|msg| msg.first().copied()).collect::<Vec<_>>()),
            },
            "tob": {
                "block_marketdata_packets": block.tob_marketdata.len(),
                "stream_marketdata_packets": stream.tob_marketdata.len(),
                "block_end_quote_count": block_end_quote_map.len(),
                "stream_end_quote_count": stream_end_quote_map.len(),
                "matched_end_quote_count": common_end_quotes.len(),
            },
            "fixture": {
                "coin": replay_coin,
                "first_block": manifest["fixture_block_start"].clone(),
                "last_block": manifest["fixture_block_end"].clone(),
                "snapshot_height": manifest["snapshot_height"].clone(),
            },
        }),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires EDGE_MULTICAST_REF_DIR pointing at the pinned edge-multicast-ref checkout"]
async fn dual_validator_tob_streaming_feed_is_consumed_by_edge_parser() {
    let edge_repo = edge_multicast_ref_dir();
    assert_edge_multicast_ref_pinned(&edge_repo);
    let parser_binary = build_edge_tob_parser(&edge_repo);

    let block = run_edge_tob_parser_replay(
        &parser_binary,
        "block",
        &read_dual_packet_golden("block_tob_refdata.bin"),
        &read_dual_packet_golden("block_tob_marketdata.bin"),
    )
    .await;
    let stream = run_edge_tob_parser_replay(
        &parser_binary,
        "stream",
        &read_dual_packet_golden("stream_tob_refdata.bin"),
        &read_dual_packet_golden("stream_tob_marketdata.bin"),
    )
    .await;

    assert!(!block.records.is_empty(), "block parser produced TOB records");
    assert!(!stream.records.is_empty(), "stream parser produced TOB records");
    assert!(!parser_records_by_type(&stream.records, "quote").is_empty(), "stream parser produced TOB quotes");

    assert_eq!(
        parser_records_by_types(&block.records, &["channel_reset", "instrument_definition", "manifest_summary"]),
        parser_records_by_types(&stream.records, &["channel_reset", "instrument_definition", "manifest_summary"]),
        "parsed TOB refdata matches between block and stream",
    );

    let block_quote_map = parser_quote_map(&block.records);
    let stream_quote_map = parser_quote_map(&stream.records);
    let common_end_quotes = assert_parser_stream_end_quotes_match_block(&block_quote_map, &stream_quote_map);

    assert_dual_text_golden("parser_block_tob.jsonl", &parser_jsonl(&block.records));
    assert_dual_text_golden("parser_stream_tob.jsonl", &parser_jsonl(&stream.records));
    assert_dual_text_golden("parser_tob_end_of_block_quotes.jsonl", &parser_jsonl(&common_end_quotes));
    assert_dual_json_golden(
        "parser_tob_parity_report.json",
        &serde_json::json!({
            "edge_multicast_ref": {
                "repo": "https://github.com/malbeclabs/edge-multicast-ref.git",
                "commit": EDGE_MULTICAST_REF_MAIN_SHA,
            },
            "block": {
                "record_count": block.records.len(),
                "quote_count": parser_records_by_type(&block.records, "quote").len(),
                "refdata_count": parser_records_by_types(
                    &block.records,
                    &["channel_reset", "instrument_definition", "manifest_summary"],
                ).len(),
            },
            "stream": {
                "record_count": stream.records.len(),
                "quote_count": parser_records_by_type(&stream.records, "quote").len(),
                "refdata_count": parser_records_by_types(
                    &stream.records,
                    &["channel_reset", "instrument_definition", "manifest_summary"],
                ).len(),
            },
            "parity": {
                "block_end_quote_count": block_quote_map.len(),
                "stream_end_quote_count": stream_quote_map.len(),
                "matched_end_quote_count": common_end_quotes.len(),
            },
        }),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires HL_DUAL_CAPTURE_ROOT with matching by-block and streaming validator captures"]
async fn dual_validator_capture_matches_block_and_stream_payloads() {
    let root = PathBuf::from(
        std::env::var("HL_DUAL_CAPTURE_ROOT").expect("HL_DUAL_CAPTURE_ROOT points at a dual-validator capture root"),
    );
    let replay_coin = live_capture_coin(&root);

    let block = capture_fixture_replay(&root, IngestMode::Block, &replay_coin).await;
    let stream = capture_fixture_replay(&root, IngestMode::Stream, &replay_coin).await;

    assert_eq!(block.height, stream.height, "final listener height parity");
    assert_eq!(block.l4_snapshot, stream.l4_snapshot, "final L4 snapshot parity");
    assert_eq!(block.l2_snapshot, stream.l2_snapshot, "final L2 snapshot parity");
    assert_message_stream_eq(
        "normalized DoB order-event payload parity",
        &dob_order_messages(&block.dob_mktdata),
        &dob_order_messages(&stream.dob_mktdata),
    );
    assert_eq!(
        boundary_count(&stream.dob_mktdata).saturating_sub(boundary_count(&block.dob_mktdata)),
        expected_minimized_boundary_delta(&root),
        "streaming emits open/close boundaries for single-emittable-diff blocks in coin-minimized captures",
    );
    assert_packet_stream_eq("normalized DoB refdata payload parity", &block.dob_refdata, &stream.dob_refdata);
    assert_packet_stream_eq("normalized TOB refdata payload parity", &block.tob_refdata, &stream.tob_refdata);
}

#[tokio::test]
async fn streaming_fixture_matches_block_fixture_final_books() {
    let root = fixture_root();
    let snapshot_path = root.join("out.json");
    let (snapshot_height, snapshot_a) = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&snapshot_path)
        .await
        .expect("fixture snapshot loads");
    let (_, snapshot_b) = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&snapshot_path)
        .await
        .expect("fixture snapshot loads twice");

    let mut block_listener = OrderBookListener::new_with_ingest_mode(None, true, IngestMode::Block);
    block_listener.init_from_snapshot(snapshot_a, snapshot_height);
    let block_listener = Arc::new(Mutex::new(block_listener));
    replay_fixture_blocks(&root, &block_listener).await;

    let mut stream_listener = OrderBookListener::new_with_ingest_mode(None, true, IngestMode::Stream);
    stream_listener.init_from_snapshot(snapshot_b, snapshot_height);
    let stream_listener = Arc::new(Mutex::new(stream_listener));
    replay_fixture_streams(&root, &stream_listener).await;

    let mut block_guard = block_listener.lock().await;
    let mut stream_guard = stream_listener.lock().await;
    let block_snapshot = block_guard.compute_snapshot().unwrap();
    let stream_snapshot = stream_guard.compute_snapshot().unwrap();
    assert_eq!(block_snapshot.height, stream_snapshot.height, "final L4 height parity");
    assert_eq!(
        format!("{:?}", block_snapshot.snapshot.as_ref()),
        format!("{:?}", stream_snapshot.snapshot.as_ref()),
        "final L4 snapshot parity",
    );

    let block_l2 = block_guard.l2_snapshots_for_test().unwrap();
    let stream_l2 = stream_guard.l2_snapshots_for_test().unwrap();
    assert_eq!(block_l2.0, stream_l2.0, "final L2 timestamp parity");
    assert_eq!(stable_l2_debug(&block_l2.1), stable_l2_debug(&stream_l2.1), "final L2 snapshot parity",);
}

#[test]
fn partial_trailing_json_line_is_deferred_without_panic() {
    let mut listener = OrderBookListener::new_with_ingest_mode(None, true, IngestMode::Block);
    let partial = "{\"local_time\":\"2026-04-30T20:00:00\",\"block_time\":\"2026-04-30T20:00:00\"";
    listener.process_data(partial.to_string(), EventSource::OrderDiffs).expect("partial line should be deferred");
    assert!(listener.order_book_state.is_none());
}

#[tokio::test]
async fn streaming_diff_before_status_waits_then_applies() {
    let (mut listener, snapshot_height) = stream_listener_from_fixture().await;
    let (status_batch, diff_batch) = first_streaming_new_diff_and_status();

    listener.receive_batch(EventBatch::BookDiffs(diff_batch.clone())).unwrap();
    assert_eq!(listener.compute_snapshot().unwrap().height, snapshot_height);

    listener.receive_batch(EventBatch::Orders(status_batch)).unwrap();
    assert_eq!(listener.compute_snapshot().unwrap().height, diff_batch.block_number());
}

#[tokio::test]
async fn streaming_status_before_diff_applies_immediately() {
    let (mut listener, _snapshot_height) = stream_listener_from_fixture().await;
    let (status_batch, diff_batch) = first_streaming_new_diff_and_status();

    listener.receive_batch(EventBatch::Orders(status_batch)).unwrap();
    listener.receive_batch(EventBatch::BookDiffs(diff_batch.clone())).unwrap();

    assert_eq!(listener.compute_snapshot().unwrap().height, diff_batch.block_number());
}

#[tokio::test]
async fn streaming_unresolved_new_order_fails_when_forced_to_finalize() {
    let (mut listener, _snapshot_height) = stream_listener_from_fixture().await;
    let (_status_batch, diff_batch) = first_streaming_new_diff_and_status();

    listener.receive_batch(EventBatch::BookDiffs(diff_batch)).unwrap();

    let err = listener.finalize_streaming_for_test().unwrap_err().to_string();
    assert!(err.contains("unresolved diffs"), "{err}");
}

#[tokio::test]
async fn init_from_snapshot_prunes_stale_pre_snapshot_streaming_blocks() {
    // The streaming file watcher races ahead of the snapshot fetch — so by the
    // time `init_from_snapshot` runs, `streaming_state.blocks` typically holds
    // events for heights at or below the snapshot's height. Replaying those
    // would either dup orders (New) or be rejected by `apply_stream_diff`'s
    // `block_number < self.height` check (which would set order_book_state
    // back to None and leave the publisher unable to emit deltas). The
    // listener must drop pre-snapshot blocks before draining.
    let root = fixture_root();
    let (snapshot_height, snapshot) =
        load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&root.join("out.json"))
            .await
            .expect("fixture snapshot loads");
    let mut listener = OrderBookListener::new_with_ingest_mode(None, true, IngestMode::Stream);

    // Inject a pair of streaming events at a height strictly below the
    // snapshot, mimicking what the file watcher accumulates pre-snapshot.
    let (status_batch, diff_batch) = first_streaming_new_diff_and_status();
    let stale_height = snapshot_height.saturating_sub(1);
    let stale_block_time = diff_batch.block_time().saturating_sub(1_000);
    let stale_status_batch = Batch::new_for_test(stale_height, stale_block_time, status_batch.events_ref().to_vec());
    let stale_diff_batch = Batch::new_for_test(stale_height, stale_block_time, diff_batch.events_ref().to_vec());
    listener.receive_batch(EventBatch::Orders(stale_status_batch)).unwrap();
    listener.receive_batch(EventBatch::BookDiffs(stale_diff_batch)).unwrap();

    // Now run init_from_snapshot. Without the prune step, this would call
    // drain_streaming_blocks which would feed apply_stream_diff a stale block
    // at height < self.height, triggering "Received finalized streaming
    // block ..." and leaving order_book_state = None.
    listener.init_from_snapshot(snapshot, snapshot_height);

    assert!(
        listener.is_ready(),
        "order_book_state must be Some after init_from_snapshot — pre-snapshot streaming events should have been pruned",
    );
    assert_eq!(
        listener.compute_snapshot().unwrap().height,
        snapshot_height,
        "book height matches snapshot height; the stale pre-snapshot stream events were not applied",
    );
}

#[tokio::test]
async fn streaming_late_data_for_finalized_block_is_dropped() {
    // hl-node writes the streaming statuses, diffs, and fills files concurrently;
    // they're delivered through one notify channel. If a later block's diffs
    // arrive and finalize an earlier block before that earlier block's statuses/
    // diffs from another file land, the listener must drop the late events with
    // a warn log rather than crash. This test simulates that race.
    let (mut listener, _snapshot_height) = stream_listener_from_fixture().await;
    let (status_batch, diff_batch) = first_streaming_new_diff_and_status();
    let next_empty_batch = Batch::new_for_test(
        diff_batch.block_number() + 1,
        diff_batch.block_time() + 1_000,
        Vec::<NodeDataOrderDiff>::new(),
    );

    listener.receive_batch(EventBatch::Orders(status_batch)).unwrap();
    listener.receive_batch(EventBatch::BookDiffs(diff_batch.clone())).unwrap();
    listener.receive_batch(EventBatch::BookDiffs(next_empty_batch)).unwrap();

    // Replay the now-finalized block's diff: must be dropped silently (Ok),
    // not propagated as a fatal error that would tear down the listener.
    listener.receive_batch(EventBatch::BookDiffs(diff_batch)).expect("late events drop, do not crash");
}

async fn stream_listener_from_fixture() -> (OrderBookListener, u64) {
    let root = fixture_root();
    let (snapshot_height, snapshot) =
        load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&root.join("out.json"))
            .await
            .expect("fixture snapshot loads");
    let mut listener = OrderBookListener::new_with_ingest_mode(None, true, IngestMode::Stream);
    listener.init_from_snapshot(snapshot, snapshot_height);
    (listener, snapshot_height)
}

struct CapturedReplay {
    height: u64,
    l4_snapshot: String,
    l2_snapshot: String,
    tob_marketdata: Vec<Vec<u8>>,
    tob_refdata: Vec<Vec<u8>>,
    dob_mktdata: Vec<Vec<u8>>,
    dob_refdata: Vec<Vec<u8>>,
}

async fn capture_fixture_replay(root: &Path, ingest_mode: IngestMode, replay_coin: &str) -> CapturedReplay {
    let snapshot_path = root.join("out.json");
    let (snapshot_height, snapshot) = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&snapshot_path)
        .await
        .expect("capture snapshot loads");

    let registry = new_shared_registry(test_registry_state_for(replay_coin));
    let (internal_tx, _) = broadcast_channel::<Arc<InternalMessage>>(8192);

    let mut listener = OrderBookListener::new_with_ingest_mode(Some(internal_tx.clone()), true, ingest_mode);
    listener.init_from_snapshot(snapshot, snapshot_height);

    let (dob_tx, dob_rx) = dob_channel(32768);
    let seq_counter: SharedSeqCounter = Arc::new(StdMutex::new(PerInstrumentSeqCounter::new()));
    let tap_coin = replay_coin.to_string();
    listener.set_dob_tap(DobApplyTap::new(
        dob_tx.clone(),
        SOURCE_ID,
        DOB_CHANNEL_ID,
        seq_counter,
        Box::new(move |coin: &Coin| if coin.value() == tap_coin { Some((BTC_INSTRUMENT_ID, -8)) } else { None }),
    ));
    let listener = Arc::new(Mutex::new(listener));

    let tob_market = UdpCollector::bind().await;
    let tob_refdata = UdpCollector::bind().await;
    let dob_mktdata = UdpCollector::bind().await;
    let dob_refdata = UdpCollector::bind().await;

    let tob_handle =
        spawn_tob_publisher(internal_tx.subscribe(), registry.clone(), tob_market.port(), tob_refdata.port()).await;
    let dob_handle = spawn_dob_mktdata(dob_rx, dob_mktdata.port()).await;
    let dob_refdata_handle = tokio::spawn(run_dob_refdata_task(
        DobRefdataConfig {
            group_addr: Ipv4Addr::LOCALHOST,
            port: dob_refdata.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: DOB_CHANNEL_ID,
            mtu: dob_const::DEFAULT_MTU,
            definition_cycle: Duration::from_millis(20),
            manifest_cadence: Duration::from_millis(20),
        },
        crate::instruments::InstrumentRegistry::from_arc(registry),
    ));

    match ingest_mode {
        IngestMode::Block => replay_fixture_blocks(root, &listener).await,
        IngestMode::Stream => replay_fixture_streams(root, &listener).await,
    }

    dob_tx.send(DobEvent::Shutdown).await.unwrap();
    timeout(Duration::from_secs(2), dob_handle)
        .await
        .expect("DoB emitter shuts down")
        .expect("DoB emitter join")
        .expect("DoB emitter result");

    sleep(Duration::from_millis(700)).await;
    tob_handle.abort();
    dob_refdata_handle.abort();

    let mut guard = listener.lock().await;
    let height = guard.order_book_state.as_ref().unwrap().height();
    let l4_snapshot = format!("{:?}", guard.compute_snapshot().unwrap().snapshot.as_ref());
    let l2_snapshot = stable_l2_debug(&guard.l2_snapshots_for_test().unwrap().1);
    drop(guard);

    CapturedReplay {
        height,
        l4_snapshot,
        l2_snapshot,
        tob_marketdata: normalize_tob(tob_market.finish()),
        tob_refdata: first_packets_by_msg_type(
            normalize_tob(tob_refdata.finish()),
            &[
                tob_const::MSG_TYPE_CHANNEL_RESET,
                tob_const::MSG_TYPE_INSTRUMENT_DEF,
                tob_const::MSG_TYPE_MANIFEST_SUMMARY,
            ],
        ),
        dob_mktdata: normalize_dob(dob_mktdata.finish()),
        dob_refdata: first_packets_by_msg_type(
            normalize_dob_refdata(dob_refdata.finish()),
            &[dob_const::MSG_TYPE_INSTRUMENT_DEF, dob_const::MSG_TYPE_MANIFEST_SUMMARY],
        ),
    }
}

fn first_streaming_new_diff_and_status() -> (Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>) {
    let root = fixture_root();
    let mut statuses = HashMap::new();
    for line in
        fs::read_to_string(root.join("hl/data/node_order_statuses_streaming/hourly/20260430/20")).unwrap().lines()
    {
        let batch: Batch<NodeDataOrderStatus> = serde_json::from_str(line).unwrap();
        let Some(status) = batch.events_ref().first() else {
            continue;
        };
        if status.is_inserted_into_book() {
            statuses.insert(status.order.oid, batch);
        }
    }

    for line in
        fs::read_to_string(root.join("hl/data/node_raw_book_diffs_streaming/hourly/20260430/20")).unwrap().lines()
    {
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_str(line).unwrap();
        let Some(diff) = batch.events_ref().first() else {
            continue;
        };
        if matches!(diff.raw_book_diff, OrderDiff::New { .. })
            && let Some(status_batch) = statuses.remove(&diff.oid().into_inner())
        {
            return (status_batch, batch);
        }
    }
    panic!("fixture contains a streaming new diff with matching open status");
}

fn stable_l2_debug(snapshots: &super::L2Snapshots) -> String {
    let mut coins = snapshots.as_ref().iter().collect::<Vec<_>>();
    coins.sort_by(|(left, _), (right, _)| left.value().cmp(&right.value()));

    let mut out = String::new();
    for (coin, params_map) in coins {
        out.push_str(&coin.value());
        out.push(':');

        let mut params = params_map.iter().collect::<Vec<_>>();
        params.sort_by_key(|(params, _)| format!("{params:?}"));
        for (params, snapshot) in params {
            out.push_str(&format!("{params:?}={snapshot:?};"));
        }
        out.push('\n');
    }
    out
}

async fn spawn_tob_publisher(
    rx: tokio::sync::broadcast::Receiver<Arc<InternalMessage>>,
    registry: crate::instruments::SharedRegistry,
    market_port: u16,
    refdata_port: u16,
) -> tokio::task::JoinHandle<()> {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let publisher = MulticastPublisher::new(
        socket,
        MulticastConfig {
            group_addr: Ipv4Addr::LOCALHOST,
            port: market_port,
            refdata_port,
            bind_addr: Ipv4Addr::LOCALHOST,
            snapshot_interval: Duration::from_secs(3600),
            mtu: tob_const::DEFAULT_MTU,
            source_id: SOURCE_ID,
            heartbeat_interval: Duration::from_secs(3600),
            hl_api_url: "fixture://hl-block-mode".to_string(),
            instruments_refresh_interval: Duration::from_secs(3600),
            definition_cycle: Duration::from_millis(20),
            manifest_cadence: Duration::from_millis(20),
        },
        registry,
    );
    tokio::spawn(async move {
        publisher.run(rx).await;
    })
}

async fn spawn_dob_mktdata(
    rx: crate::multicast::dob::DobEventReceiver,
    port: u16,
) -> tokio::task::JoinHandle<std::io::Result<()>> {
    let emitter = DobEmitter::bind(DobMktdataConfig {
        group_addr: Ipv4Addr::LOCALHOST,
        port,
        bind_addr: Ipv4Addr::LOCALHOST,
        channel_id: DOB_CHANNEL_ID,
        mtu: dob_const::DEFAULT_MTU,
        heartbeat_interval: Duration::from_secs(3600),
    })
    .await
    .unwrap();
    tokio::spawn(run_dob_emitter(emitter, rx))
}

async fn replay_fixture_blocks(root: &Path, listener: &Arc<Mutex<OrderBookListener>>) {
    let statuses = read_lines_by_block(&block_file(root, "node_order_statuses_by_block"));
    let diffs = read_lines_by_block(&block_file(root, "node_raw_book_diffs_by_block"));
    let fills = read_lines_by_block(&block_file(root, "node_fills_by_block"));

    for height in statuses.keys() {
        let mut guard = listener.lock().await;
        guard.process_data(statuses.get(height).unwrap().clone(), EventSource::OrderStatuses).unwrap();
        guard.process_data(diffs.get(height).unwrap().clone(), EventSource::OrderDiffs).unwrap();
        guard.process_data(fills.get(height).unwrap().clone(), EventSource::Fills).unwrap();
    }
}

async fn replay_fixture_streams(root: &Path, listener: &Arc<Mutex<OrderBookListener>>) {
    let statuses = read_stream_lines_by_block(&stream_file(root, "node_order_statuses_streaming"));
    let diffs = read_stream_lines_by_block(&stream_file(root, "node_raw_book_diffs_streaming"));
    let fills = read_stream_lines_by_block(&stream_file(root, "node_fills_streaming"));
    let mut heights = BTreeMap::<u64, ()>::new();
    heights.extend(statuses.keys().map(|height| (*height, ())));
    heights.extend(diffs.keys().map(|height| (*height, ())));
    heights.extend(fills.keys().map(|height| (*height, ())));

    for height in heights.keys() {
        let mut guard = listener.lock().await;
        for line in statuses.get(height).into_iter().flatten() {
            guard.process_data(line.clone(), EventSource::OrderStatuses).unwrap();
        }
        for line in diffs.get(height).into_iter().flatten() {
            guard.process_data(line.clone(), EventSource::OrderDiffs).unwrap();
        }
        for line in fills.get(height).into_iter().flatten() {
            guard.process_data(line.clone(), EventSource::Fills).unwrap();
        }
    }
    listener.lock().await.finalize_streaming_for_test().unwrap();
}

fn read_lines_by_block(path: &Path) -> BTreeMap<u64, String> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            let height = value["block_number"].as_u64().unwrap();
            (height, format!("{line}\n"))
        })
        .collect()
}

fn read_stream_lines_by_block(path: &Path) -> BTreeMap<u64, Vec<String>> {
    let mut out = BTreeMap::<u64, Vec<String>>::new();
    for line in fs::read_to_string(path).unwrap().lines() {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        let height = value["block_number"].as_u64().unwrap();
        out.entry(height).or_default().push(format!("{line}\n"));
    }
    out
}

fn fixture_last_block(root: &Path) -> u64 {
    read_lines_by_block(&block_file(root, "node_raw_book_diffs_by_block")).keys().next_back().copied().unwrap()
}

fn fixture_last_stream_block(root: &Path) -> u64 {
    read_stream_lines_by_block(&stream_file(root, "node_raw_book_diffs_streaming")).keys().next_back().copied().unwrap()
}

struct ExpectedStreamMessages {
    adds: usize,
    cancels: usize,
    executes: usize,
    order_events: usize,
}

fn live_capture_coin(root: &Path) -> String {
    let manifest: serde_json::Value = serde_json::from_str(&fs::read_to_string(root.join("manifest.json")).unwrap())
        .expect("live capture manifest parses");
    manifest["coin"].as_str().expect("live capture manifest has coin").to_string()
}

fn dual_validator_manifest() -> serde_json::Value {
    serde_json::from_str(&fs::read_to_string(dual_fixture_root().join("manifest.json")).unwrap())
        .expect("dual validator manifest parses")
}

fn dual_fixture_coin(manifest: &serde_json::Value) -> String {
    manifest["coin_filter"][0].as_str().expect("dual manifest has coin_filter").to_string()
}

struct TempReplayFixture {
    path: PathBuf,
}

impl TempReplayFixture {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempReplayFixture {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.path));
    }
}

fn extract_dual_validator_fixture() -> TempReplayFixture {
    let root = unique_temp_dir("hl_dual_validator_fixture");
    fs::create_dir_all(&root).unwrap();
    let source = dual_fixture_root().join("source");

    let snapshot = Command::new("gzip")
        .args(["-dc"])
        .arg(source.join("snapshot_985148181.json.gz"))
        .output()
        .expect("run gzip to extract dual validator snapshot");
    assert!(snapshot.status.success(), "gzip extracted dual validator snapshot");
    fs::write(root.join("out.json"), snapshot.stdout).unwrap();

    extract_tar_gz(&source.join("by_block_btc_985148182_985148232.tar.gz"), &root);
    extract_tar_gz(&source.join("streaming_btc_985148182_985148232.tar.gz"), &root);
    fs::copy(dual_fixture_root().join("manifest.json"), root.join("manifest.json")).unwrap();

    TempReplayFixture { path: root }
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}

fn extract_tar_gz(archive: &Path, destination: &Path) {
    let status = Command::new("tar")
        .arg("-xzf")
        .arg(archive)
        .arg("-C")
        .arg(destination)
        .status()
        .unwrap_or_else(|err| panic!("run tar for {}: {err}", archive.display()));
    assert!(status.success(), "tar extracted {}", archive.display());
}

fn edge_multicast_ref_dir() -> PathBuf {
    PathBuf::from(
        std::env::var(EDGE_MULTICAST_REF_ENV)
            .unwrap_or_else(|_| panic!("{EDGE_MULTICAST_REF_ENV} points at edge-multicast-ref")),
    )
}

fn assert_edge_multicast_ref_pinned(repo: &Path) {
    let output = Command::new("git")
        .args(["-C"])
        .arg(repo)
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap_or_else(|err| panic!("run git rev-parse in {}: {err}", repo.display()));
    assert!(
        output.status.success(),
        "edge-multicast-ref git rev-parse failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    let actual = String::from_utf8_lossy(&output.stdout).trim().to_string();
    assert_eq!(
        actual, EDGE_MULTICAST_REF_MAIN_SHA,
        "edge-multicast-ref checkout must be pinned to main commit {EDGE_MULTICAST_REF_MAIN_SHA}",
    );
}

fn build_edge_tob_parser(repo: &Path) -> PathBuf {
    let out_dir = unique_temp_dir("edge_tob_parser_bin");
    fs::create_dir_all(&out_dir).unwrap();
    let binary = out_dir.join("dz-topofbook-parser");
    let output = Command::new("go")
        .args(["build", "-o"])
        .arg(&binary)
        .arg(".")
        .current_dir(repo.join("go/topofbook-parser"))
        .output()
        .unwrap_or_else(|err| panic!("run go build for edge topofbook parser: {err}"));
    assert!(
        output.status.success(),
        "edge topofbook parser build failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    binary
}

struct EdgeTobParserRun {
    records: Vec<serde_json::Value>,
}

async fn run_edge_tob_parser_replay(
    binary: &Path,
    label: &str,
    refdata_packets: &[Vec<u8>],
    marketdata_packets: &[Vec<u8>],
) -> EdgeTobParserRun {
    let root = unique_temp_dir(&format!("edge_tob_parser_{label}"));
    fs::create_dir_all(&root).unwrap();
    let output_path = root.join("records.jsonl");
    let stderr_path = root.join("stderr.log");
    let group = unique_multicast_group();
    let (marketdata_port, refdata_port) = unused_udp_ports();

    let mut child = TokioCommand::new(binary)
        .arg("--group")
        .arg(group.to_string())
        .arg("--marketdata-port")
        .arg(marketdata_port.to_string())
        .arg("--refdata-port")
        .arg(refdata_port.to_string())
        .arg("--format")
        .arg("json")
        .arg("--output")
        .arg(&output_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|err| panic!("spawn edge TOB parser for {label}: {err}"));

    let mut stderr = child.stderr.take().expect("edge parser stderr is piped");
    let stderr_task = tokio::spawn(async move {
        let mut out = String::new();
        stderr.read_to_string(&mut out).await.expect("read edge parser stderr");
        out
    });

    let (marketdata_resets, marketdata_body) =
        partition_tob_packets_by_msg_type(marketdata_packets, tob_const::MSG_TYPE_CHANNEL_RESET);

    sleep(Duration::from_millis(1500)).await;
    send_multicast_packets(group, marketdata_port, &marketdata_resets).await;
    sleep(Duration::from_millis(250)).await;
    send_multicast_packets(group, refdata_port, refdata_packets).await;
    sleep(Duration::from_millis(500)).await;
    send_multicast_packets(group, marketdata_port, &marketdata_body).await;
    sleep(Duration::from_millis(1500)).await;

    terminate_child(&mut child).await;
    let status = timeout(Duration::from_secs(5), child.wait()).await.unwrap_or_else(|_| {
        panic!("edge TOB parser for {label} did not exit after SIGTERM");
    });
    let status = status.expect("wait for edge TOB parser");
    let stderr = stderr_task.await.expect("edge parser stderr task");
    fs::write(&stderr_path, &stderr).unwrap();
    assert!(status.success(), "edge TOB parser for {label} exited with {status}; stderr:\n{stderr}");
    let stderr_lower = stderr.to_ascii_lowercase();
    assert!(!stderr_lower.contains("parse error"), "edge TOB parser for {label} logged parse errors:\n{stderr}");
    assert!(!stderr_lower.contains("fatal"), "edge TOB parser for {label} logged fatal errors:\n{stderr}");

    let records = read_parser_jsonl(&output_path);
    drop(fs::remove_dir_all(&root));
    EdgeTobParserRun { records }
}

async fn terminate_child(child: &mut tokio::process::Child) {
    let pid = child.id().expect("child has pid");
    let status = Command::new("sh").arg("-c").arg(format!("kill -TERM {pid}")).status().expect("send SIGTERM");
    assert!(status.success(), "SIGTERM sent to edge parser pid {pid}");
}

fn partition_tob_packets_by_msg_type(packets: &[Vec<u8>], msg_type: u8) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut matching = Vec::new();
    let mut rest = Vec::new();
    for packet in packets {
        if packet_contains_msg_type(packet, msg_type) {
            matching.push(packet.clone());
        } else {
            rest.push(packet.clone());
        }
    }
    (matching, rest)
}

fn packet_contains_msg_type(packet: &[u8], msg_type: u8) -> bool {
    if packet.len() < 24 {
        return false;
    }
    let msg_count = packet[20] as usize;
    let mut offset = 24;
    for _ in 0..msg_count {
        if offset + 4 > packet.len() {
            return false;
        }
        let current_type = packet[offset];
        let msg_len = packet[offset + 1] as usize;
        if current_type == msg_type {
            return true;
        }
        offset += 4 + msg_len;
        if offset > packet.len() {
            return false;
        }
    }
    false
}

async fn send_multicast_packets(group: Ipv4Addr, port: u16, packets: &[Vec<u8>]) {
    let std_socket = std::net::UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)).unwrap();
    std_socket.set_multicast_loop_v4(true).unwrap();
    std_socket.set_multicast_ttl_v4(1).unwrap();
    std_socket.set_nonblocking(true).unwrap();
    let socket = UdpSocket::from_std(std_socket).unwrap();
    let destination = SocketAddrV4::new(group, port);
    for packet in packets {
        socket.send_to(packet, destination).await.unwrap();
        sleep(Duration::from_millis(1)).await;
    }
}

fn unique_multicast_group() -> Ipv4Addr {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
    Ipv4Addr::new(224, 0, 0, (nanos % 250) as u8 + 1)
}

fn unused_udp_ports() -> (u16, u16) {
    let first = std::net::UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    let second = std::net::UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    let first_port = first.local_addr().unwrap().port();
    let second_port = second.local_addr().unwrap().port();
    assert_ne!(first_port, second_port, "test allocated distinct UDP ports");
    (first_port, second_port)
}

fn read_parser_jsonl(path: &Path) -> Vec<serde_json::Value> {
    fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("read parser JSONL {}: {err}", path.display()))
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
            normalize_parser_record(&mut value);
            value
        })
        .collect()
}

fn normalize_parser_record(value: &mut serde_json::Value) {
    let object = value.as_object_mut().expect("parser record is an object");
    object.remove("recv_ts");
    object.remove("parser_kernel_recv_ts_ns");
    object.remove("recv_ts_kind");
    object.remove("multicast_group");
    object.remove("port");
}

fn parser_records_by_type(records: &[serde_json::Value], record_type: &str) -> Vec<serde_json::Value> {
    records.iter().filter(|record| record["type"].as_str() == Some(record_type)).cloned().collect()
}

fn parser_records_by_types(records: &[serde_json::Value], record_types: &[&str]) -> Vec<serde_json::Value> {
    let wanted = record_types.iter().copied().collect::<HashSet<_>>();
    records
        .iter()
        .filter(|record| record["type"].as_str().is_some_and(|record_type| wanted.contains(record_type)))
        .cloned()
        .collect()
}

fn parser_quote_map(records: &[serde_json::Value]) -> BTreeMap<u64, serde_json::Value> {
    let mut by_source_timestamp = BTreeMap::<u64, serde_json::Value>::new();
    for record in parser_records_by_type(records, "quote") {
        let source_timestamp = record["fields"]["source_ts_ns"].as_u64().expect("quote has source_ts_ns");
        by_source_timestamp.insert(source_timestamp, record);
    }
    by_source_timestamp
}

fn assert_parser_stream_end_quotes_match_block(
    block: &BTreeMap<u64, serde_json::Value>,
    stream: &BTreeMap<u64, serde_json::Value>,
) -> Vec<serde_json::Value> {
    let mut matched = Vec::new();
    for (timestamp, stream_quote) in stream {
        let block_quote = block.get(timestamp).unwrap_or_else(|| {
            panic!("parsed streaming end-of-block quote timestamp {timestamp} missing from block replay")
        });
        assert_eq!(block_quote, stream_quote, "parsed TOB end-of-block quote at source timestamp {timestamp}");
        matched.push(stream_quote.clone());
    }
    matched
}

fn parser_jsonl(records: &[serde_json::Value]) -> String {
    let mut out = String::new();
    for record in records {
        out.push_str(&serde_json::to_string(record).unwrap());
        out.push('\n');
    }
    out
}

fn read_dual_packet_golden(name: &str) -> Vec<Vec<u8>> {
    decode_packets(&fs::read(dual_fixture_root().join("golden").join(name)).unwrap())
}

fn expected_stream_messages(root: &Path, replay_coin: &str) -> ExpectedStreamMessages {
    let mut expected = ExpectedStreamMessages { adds: 0, cancels: 0, executes: 0, order_events: 0 };
    let contents = fs::read_to_string(stream_file(root, "node_raw_book_diffs_streaming")).unwrap();
    for line in contents.lines() {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        for event in value["events"].as_array().into_iter().flatten() {
            if event["coin"].as_str() != Some(replay_coin) {
                continue;
            }
            let raw = &event["raw_book_diff"];
            if raw.get("new").is_some() {
                expected.adds += 1;
                expected.order_events += 1;
            } else if raw.get("remove").is_some() {
                expected.cancels += 1;
                expected.order_events += 1;
            } else if raw.get("update").is_some() {
                expected.executes += 1;
                expected.order_events += 1;
            }
        }
    }
    expected
}

fn stream_file(root: &Path, source_dir: &str) -> PathBuf {
    event_file(root, source_dir)
}

fn block_file(root: &Path, source_dir: &str) -> PathBuf {
    event_file(root, source_dir)
}

fn event_file(root: &Path, source_dir: &str) -> PathBuf {
    let hourly = root.join("hl/data").join(source_dir).join("hourly");
    let mut files = Vec::new();
    collect_files(&hourly, &mut files);
    files.sort();
    files.into_iter().next().unwrap_or_else(|| panic!("missing streaming file under {}", hourly.display()))
}

fn collect_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|err| panic!("read {}: {err}", dir.display())) {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
}

fn test_registry_state() -> RegistryState {
    test_registry_state_for("BTC")
}

fn test_registry_state_for(coin: &str) -> RegistryState {
    let info = InstrumentInfo {
        instrument_id: BTC_INSTRUMENT_ID,
        price_exponent: -1,
        qty_exponent: -8,
        symbol: make_symbol(coin),
    };
    RegistryState::new(vec![UniverseEntry {
        instrument_id: BTC_INSTRUMENT_ID,
        coin: coin.to_string(),
        is_delisted: false,
        info,
    }])
}

struct UdpCollector {
    port: u16,
    task: tokio::task::JoinHandle<()>,
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl UdpCollector {
    async fn bind() -> Self {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = match socket.local_addr().unwrap() {
            std::net::SocketAddr::V4(addr) => addr.port(),
            _ => unreachable!(),
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            while let Ok((n, _peer)) = socket.recv_from(&mut buf).await {
                if tx.send(buf[..n].to_vec()).is_err() {
                    break;
                }
            }
        });
        Self { port, task, rx }
    }

    const fn port(&self) -> u16 {
        self.port
    }

    fn finish(mut self) -> Vec<Vec<u8>> {
        self.task.abort();
        let mut packets = Vec::new();
        while let Ok(packet) = self.rx.try_recv() {
            packets.push(packet);
        }
        packets
    }
}

fn normalize_tob(mut packets: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    for packet in &mut packets {
        normalize_frame(packet, false);
    }
    packets
}

fn normalize_dob(mut packets: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    for packet in &mut packets {
        normalize_frame(packet, true);
    }
    packets
}

fn normalize_dob_refdata(mut packets: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    for packet in &mut packets {
        normalize_frame(packet, true);
        if packet.len() >= 12 {
            // Definition and manifest tick ordering is scheduler-dependent,
            // so the first definition and first manifest can receive seqs in
            // either order without changing the refdata payload contract.
            packet[4..12].fill(0);
        }
    }
    packets
}

fn normalize_frame(frame: &mut [u8], dob: bool) {
    if frame.len() < 24 {
        return;
    }
    if !dob {
        // TOB uses one publisher-wide seq across marketdata and refdata, so
        // timer scheduling can change the seq values observed by a single
        // collector without changing any stream payload.
        frame[4..12].fill(0);
    }
    frame[12..20].fill(0);
    let msg_count = frame[20] as usize;
    let mut offset = 24;
    for _ in 0..msg_count {
        if offset + 2 > frame.len() {
            break;
        }
        let msg_type = frame[offset];
        let msg_len = frame[offset + 1] as usize;
        if offset + msg_len > frame.len() || msg_len < 4 {
            break;
        }
        let msg = &mut frame[offset..offset + msg_len];
        match msg_type {
            tob_const::MSG_TYPE_HEARTBEAT => zero_range(msg, 8, 16),
            tob_const::MSG_TYPE_CHANNEL_RESET | tob_const::MSG_TYPE_END_OF_SESSION => {
                zero_range(msg, 4, 12);
            }
            tob_const::MSG_TYPE_MANIFEST_SUMMARY => zero_range(msg, 16, 24),
            _ => {}
        }
        if dob && msg_type == dob_const::MSG_TYPE_SNAPSHOT_BEGIN {
            zero_range(msg, 28, 36);
        }
        offset += msg_len;
    }
}

fn first_packets_by_msg_type(packets: Vec<Vec<u8>>, wanted: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for &msg_type in wanted {
        if let Some(packet) = packets.iter().find(|packet| first_msg_type(packet) == Some(msg_type)) {
            out.push(packet.clone());
        }
    }
    out
}

fn first_msg_type(frame: &[u8]) -> Option<u8> {
    if frame.len() >= 25 && frame[20] > 0 { Some(frame[24]) } else { None }
}

fn zero_range(buf: &mut [u8], start: usize, end: usize) {
    if buf.len() >= end {
        buf[start..end].fill(0);
    }
}

fn assert_golden(name: &str, packets: &[Vec<u8>]) {
    let bytes = encode_packets(packets);
    let path = fixture_root().join("golden").join(name);
    if std::env::var_os("HL_BLOCK_MODE_REGENERATE").is_some() || !path.exists() {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, &bytes).unwrap();
        return;
    }
    let expected = fs::read(&path).unwrap();
    if bytes != expected {
        let actual = path.with_extension("actual");
        fs::write(&actual, bytes).unwrap();
        panic!("golden mismatch for {} (wrote {})", path.display(), actual.display(),);
    }
}

fn assert_packet_stream_eq(label: &str, left: &[Vec<u8>], right: &[Vec<u8>]) {
    if left == right {
        return;
    }

    let first_mismatch = left.iter().zip(right).position(|(left, right)| left != right);
    let left_types = frame_msg_types(left);
    let right_types = frame_msg_types(right);
    let first_type_mismatch = left_types.iter().zip(&right_types).position(|(left, right)| left != right);
    panic!(
        "{label}: packet streams differ; left_packets={}, right_packets={}, first_packet_mismatch={first_mismatch:?}, left_msg_count={}, right_msg_count={}, first_msg_type_mismatch={first_type_mismatch:?}, left_type_counts={:?}, right_type_counts={:?}",
        left.len(),
        right.len(),
        left_types.len(),
        right_types.len(),
        msg_type_counts(&left_types),
        msg_type_counts(&right_types),
    );
}

fn msg_type_counts(types: &[u8]) -> BTreeMap<u8, usize> {
    let mut counts = BTreeMap::new();
    for msg_type in types {
        *counts.entry(*msg_type).or_default() += 1;
    }
    counts
}

fn assert_dual_packet_golden(name: &str, packets: &[Vec<u8>]) {
    assert_dual_bytes_golden(name, &encode_packets(packets));
}

fn assert_dual_message_golden(name: &str, messages: &[Vec<u8>]) {
    assert_dual_bytes_golden(name, &encode_packets(messages));
}

fn assert_dual_json_golden(name: &str, value: &serde_json::Value) {
    let bytes = serde_json::to_string_pretty(value).unwrap() + "\n";
    assert_dual_bytes_golden(name, bytes.as_bytes());
}

fn assert_dual_text_golden(name: &str, value: &str) {
    assert_dual_bytes_golden(name, value.as_bytes());
}

fn assert_dual_bytes_golden(name: &str, bytes: &[u8]) {
    let path = dual_fixture_root().join("golden").join(name);
    if std::env::var_os(DUAL_REGENERATE_ENV).is_some() || !path.exists() {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        return;
    }
    let expected = fs::read(&path).unwrap();
    if bytes != expected {
        let actual = path.with_extension("actual");
        fs::write(&actual, bytes).unwrap();
        panic!("dual validator golden mismatch for {} (wrote {})", path.display(), actual.display(),);
    }
}

fn assert_message_stream_eq(label: &str, left: &[Vec<u8>], right: &[Vec<u8>]) {
    if left == right {
        return;
    }

    let first_mismatch = left.iter().zip(right).position(|(left, right)| left != right);
    let left_types = left.iter().filter_map(|msg| msg.first().copied()).collect::<Vec<_>>();
    let right_types = right.iter().filter_map(|msg| msg.first().copied()).collect::<Vec<_>>();
    panic!(
        "{label}: message streams differ; left_messages={}, right_messages={}, first_mismatch={first_mismatch:?}, left_type_counts={:?}, right_type_counts={:?}",
        left.len(),
        right.len(),
        msg_type_counts(&left_types),
        msg_type_counts(&right_types),
    );
}

fn dob_order_messages(packets: &[Vec<u8>]) -> Vec<Vec<u8>> {
    packets
        .iter()
        .flat_map(|packet| frame_messages(packet))
        .filter(|msg| {
            matches!(
                msg.first().copied(),
                Some(
                    dob_const::MSG_TYPE_ORDER_ADD
                        | dob_const::MSG_TYPE_ORDER_CANCEL
                        | dob_const::MSG_TYPE_ORDER_EXECUTE
                )
            )
        })
        .map(<[u8]>::to_vec)
        .collect()
}

fn end_of_block_quote_map(packets: &[Vec<u8>]) -> BTreeMap<u64, Vec<u8>> {
    let mut by_source_timestamp = BTreeMap::<u64, Vec<u8>>::new();
    for msg in packets
        .iter()
        .flat_map(|packet| frame_messages(packet))
        .filter(|msg| msg.first().copied() == Some(tob_const::MSG_TYPE_QUOTE))
    {
        by_source_timestamp.insert(quote_source_timestamp_ns(msg), msg.to_vec());
    }
    by_source_timestamp
}

fn assert_stream_end_quotes_match_block(
    block: &BTreeMap<u64, Vec<u8>>,
    stream: &BTreeMap<u64, Vec<u8>>,
) -> Vec<Vec<u8>> {
    let mut matched = Vec::new();
    for (timestamp, stream_quote) in stream {
        let block_quote = block
            .get(timestamp)
            .unwrap_or_else(|| panic!("streaming end-of-block quote timestamp {timestamp} missing from block replay"));
        assert_eq!(block_quote, stream_quote, "TOB end-of-block quote at source timestamp {timestamp}");
        matched.push(stream_quote.clone());
    }
    matched
}

fn quote_source_timestamp_ns(msg: &[u8]) -> u64 {
    assert!(msg.len() >= 20, "quote message includes source timestamp");
    u64::from_le_bytes(msg[12..20].try_into().unwrap())
}

fn boundary_count(packets: &[Vec<u8>]) -> usize {
    frame_msg_types(packets).into_iter().filter(|msg_type| *msg_type == dob_const::MSG_TYPE_BATCH_BOUNDARY).count()
}

fn expected_minimized_boundary_delta(root: &Path) -> usize {
    read_lines_by_block(&block_file(root, "node_raw_book_diffs_by_block"))
        .values()
        .filter(|line| {
            let batch: Batch<NodeDataOrderDiff> = serde_json::from_str(line).unwrap();
            batch.events_ref().iter().filter(|diff| !diff.coin().is_spot()).count() == 1
        })
        .count()
        * 2
}

fn encode_packets(packets: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for packet in packets {
        let len = u32::try_from(packet.len()).unwrap();
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(packet);
    }
    out
}

fn decode_packets(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        assert!(offset + 4 <= bytes.len(), "packet golden has length prefix");
        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        assert!(offset + len <= bytes.len(), "packet golden has full packet body");
        packets.push(bytes[offset..offset + len].to_vec());
        offset += len;
    }
    packets
}

fn assert_has_msg_type(packets: &[Vec<u8>], msg_type: u8, label: &str) {
    assert!(frame_msg_types(packets).contains(&msg_type), "{label} message type {msg_type:#x} present",);
}

fn assert_no_msg_type(packets: &[Vec<u8>], msg_type: u8, label: &str) {
    assert!(!frame_msg_types(packets).contains(&msg_type), "{label} message type {msg_type:#x} absent",);
}

fn assert_contiguous_frame_seqs(packets: &[Vec<u8>], label: &str) {
    let seqs: Vec<u64> = packets.iter().filter_map(|packet| frame_seq(packet)).collect();
    assert!(!seqs.is_empty(), "{label} has frame seqs");
    for pair in seqs.windows(2) {
        assert_eq!(pair[1], pair[0] + 1, "{label} frame seqs are contiguous");
    }
}

fn assert_contiguous_per_instrument_seqs(packets: &[Vec<u8>]) {
    let mut last_by_instrument = BTreeMap::<u32, u32>::new();
    for frame in packets {
        for msg in frame_messages(frame) {
            if !matches!(
                msg.first().copied(),
                Some(
                    dob_const::MSG_TYPE_ORDER_ADD
                        | dob_const::MSG_TYPE_ORDER_CANCEL
                        | dob_const::MSG_TYPE_ORDER_EXECUTE
                )
            ) {
                continue;
            }
            assert!(msg.len() >= 16, "DoB order event has per-instrument seq");
            let instrument_id = u32::from_le_bytes(msg[4..8].try_into().unwrap());
            let seq = u32::from_le_bytes(msg[12..16].try_into().unwrap());
            if let Some(last) = last_by_instrument.insert(instrument_id, seq) {
                assert_eq!(seq, last + 1, "DoB per-instrument seqs are contiguous for instrument {instrument_id}",);
            }
        }
    }
    assert!(!last_by_instrument.is_empty(), "DoB order events carried per-instrument seqs");
}

fn frame_seq(frame: &[u8]) -> Option<u64> {
    (frame.len() >= 12).then(|| u64::from_le_bytes(frame[4..12].try_into().unwrap()))
}

fn frame_msg_types(packets: &[Vec<u8>]) -> Vec<u8> {
    let mut types = Vec::new();
    for frame in packets {
        types.extend(frame_messages(frame).iter().filter_map(|msg| msg.first().copied()));
    }
    types
}

fn frame_messages(frame: &[u8]) -> Vec<&[u8]> {
    let mut messages = Vec::new();
    if frame.len() < 24 {
        return messages;
    }
    let msg_count = frame[20] as usize;
    let mut offset = 24;
    for _ in 0..msg_count {
        if offset + 2 > frame.len() {
            break;
        }
        let msg_len = frame[offset + 1] as usize;
        if msg_len == 0 || offset + msg_len > frame.len() {
            break;
        }
        messages.push(&frame[offset..offset + msg_len]);
        offset += msg_len;
    }
    messages
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT)
}

fn dual_fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(DUAL_FIXTURE_ROOT)
}
