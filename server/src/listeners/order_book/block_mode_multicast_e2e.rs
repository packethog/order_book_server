#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::indexing_slicing)]

use super::{EventSource, InternalMessage, OrderBookListener};
use crate::{
    instruments::{InstrumentInfo, RegistryState, UniverseEntry, make_symbol, new_shared_registry},
    listeners::directory::DirectoryListener,
    listeners::order_book::dob_tap::{DobApplyTap, SharedSeqCounter},
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
    types::{L4Order, inner::InnerL4Order},
};
use alloy::primitives::Address;
use std::{
    collections::BTreeMap,
    fs,
    net::{Ipv4Addr, SocketAddrV4},
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{Mutex, broadcast::channel as broadcast_channel, mpsc},
    time::{sleep, timeout},
};

const FIXTURE_ROOT: &str = "tests/fixtures/hl_block_mode";
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

    let mut listener = OrderBookListener::new(Some(internal_tx.clone()), true);
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

#[test]
fn partial_trailing_json_line_is_deferred_without_panic() {
    let mut listener = OrderBookListener::new(None, true);
    let partial = "{\"local_time\":\"2026-04-30T20:00:00\",\"block_time\":\"2026-04-30T20:00:00\"";
    listener.process_data(partial.to_string(), EventSource::OrderDiffs).expect("partial line should be deferred");
    assert!(listener.order_book_state.is_none());
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
    let statuses = read_lines_by_block(&root.join("hl/data/node_order_statuses_by_block/hourly/20260430/20"));
    let diffs = read_lines_by_block(&root.join("hl/data/node_raw_book_diffs_by_block/hourly/20260430/20"));
    let fills = read_lines_by_block(&root.join("hl/data/node_fills_by_block/hourly/20260430/20"));

    for height in statuses.keys() {
        let mut guard = listener.lock().await;
        guard.process_data(statuses.get(height).unwrap().clone(), EventSource::OrderStatuses).unwrap();
        guard.process_data(diffs.get(height).unwrap().clone(), EventSource::OrderDiffs).unwrap();
        guard.process_data(fills.get(height).unwrap().clone(), EventSource::Fills).unwrap();
    }
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

fn fixture_last_block(root: &Path) -> u64 {
    read_lines_by_block(&root.join("hl/data/node_raw_book_diffs_by_block/hourly/20260430/20"))
        .keys()
        .next_back()
        .copied()
        .unwrap()
}

fn test_registry_state() -> RegistryState {
    let info = InstrumentInfo {
        instrument_id: BTC_INSTRUMENT_ID,
        price_exponent: -1,
        qty_exponent: -8,
        symbol: make_symbol("BTC"),
    };
    RegistryState::new(vec![UniverseEntry {
        instrument_id: BTC_INSTRUMENT_ID,
        coin: "BTC".to_string(),
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

fn encode_packets(packets: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for packet in packets {
        let len = u32::try_from(packet.len()).unwrap();
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(packet);
    }
    out
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
