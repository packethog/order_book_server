use std::{
    collections::{HashMap, HashSet},
    env::home_dir,
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock, atomic::AtomicU64},
};

use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use tokio::{
    net::TcpListener,
    select,
    sync::{
        Mutex,
        broadcast::{Sender, channel},
    },
};
use yawc::{FrameView, OpCode, WebSocket};

use crate::{
    instruments::{RegistryState, SharedRegistry},
    listeners::order_book::{
        DobReplayTaps, InternalMessage, L2SnapshotParams, L2Snapshots, OrderBookListener, TimedSnapshots,
        dob_tap::DobApplyTap, hl_listen, hl_listen_fills_only,
    },
    multicast::{
        config::{DobConfig, MulticastConfig},
        dob::{
            DobEmitter, DobEvent, DobMktdataConfig, DobRefdataConfig, DobSnapshotConfig, DobSnapshotEmitter,
            channel as dob_channel, run_dob_emitter, run_dob_refdata_task, run_dob_snapshot_task,
            snapshot_request_channel,
        },
        publisher::MulticastPublisher,
    },
    order_book::{Coin, Snapshot},
    prelude::*,
    types::{
        L2Book, L4Book, L4BookUpdates, L4Order, Trade,
        inner::InnerLevel,
        node_data::{Batch, IngestMode, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ClientMessage, DEFAULT_LEVELS, ServerResponse, Subscription, SubscriptionManager},
    },
};

/// Starts the WebSocket server and, optionally, TOB and/or DoB UDP multicast publishers.
///
/// When `multicast_config` is `Some`, a background task is spawned that publishes
/// top-of-book market data as UDP multicast datagrams.  When `dob_config` is `Some`,
/// separate tasks are spawned for the DoB mktdata, refdata, and heartbeat channels.
/// Both channels share a single instrument registry, bootstrapped from the HL API URL
/// in `multicast_config` when available.
pub async fn run_websocket_server(
    address: &str,
    ignore_spot: bool,
    compression_level: u32,
    multicast_config: Option<MulticastConfig>,
    dob_config: Option<DobConfig>,
    ingest_mode: IngestMode,
    hl_data_root: Option<PathBuf>,
    separate_fill_ingest: bool,
) -> Result<()> {
    if separate_fill_ingest && ingest_mode != IngestMode::Stream {
        return Err("--separate-fill-ingest requires streaming ingest mode".into());
    }

    let (market_message_tx, _) = channel::<Arc<InternalMessage>>(100);
    let (l4_message_tx, _) = channel::<Arc<InternalMessage>>(4096);

    // Central task: listen to messages and forward them for distribution.
    // Keep snapshots under $HOME/out.json for default compatibility while
    // allowing event files to live under /data/hl-data on production nodes.
    let home_dir = home_dir().ok_or("Could not find home directory")?;
    let hl_data_root = hl_data_root.unwrap_or_else(|| home_dir.join("hl/data"));
    let listener = {
        let market_message_tx = market_message_tx.clone();
        let mut listener = OrderBookListener::new_with_ingest_mode(Some(market_message_tx), ignore_spot, ingest_mode);
        listener.set_l4_message_tx(l4_message_tx.clone());
        listener
    };
    let listener = Arc::new(Mutex::new(listener));
    {
        let listener = listener.clone();
        let listen_hl_data_root = hl_data_root.clone();
        tokio::spawn(async move {
            if let Err(err) =
                hl_listen(listener, home_dir, listen_hl_data_root, ingest_mode, !separate_fill_ingest).await
            {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }
    if separate_fill_ingest {
        let market_message_tx = market_message_tx.clone();
        let fill_hl_data_root = hl_data_root.clone();
        tokio::spawn(async move {
            if let Err(err) = hl_listen_fills_only(market_message_tx, fill_hl_data_root).await {
                error!("Fill listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    // Bootstrap a shared instrument registry when any multicast channel is enabled.
    // If only DoB is enabled (no TOB), the registry starts empty (no HL API URL available).
    let registry: Option<SharedRegistry> = if let Some(ref cfg) = multicast_config {
        let universe = match crate::instruments::hyperliquid::fetch_universe(&cfg.hl_api_url).await {
            Ok(u) => u,
            Err(err) => {
                log::error!("failed to bootstrap instrument registry: {err}");
                std::process::exit(3);
            }
        };
        let state = crate::instruments::new_shared_registry(RegistryState::new(universe));
        {
            let guard = state.load();
            info!(
                "instrument registry loaded: {} active ({} in universe), manifest_seq={}",
                guard.active.len(),
                guard.universe.len(),
                guard.manifest_seq
            );
        }
        {
            let refresh_state = Arc::clone(&state);
            let api_url = cfg.hl_api_url.clone();
            let interval = cfg.instruments_refresh_interval;
            tokio::spawn(async move {
                crate::instruments::hyperliquid::refresh_task(api_url, refresh_state, interval).await;
            });
        }
        Some(state)
    } else if dob_config.is_some() {
        Some(crate::instruments::new_shared_registry(RegistryState::empty()))
    } else {
        None
    };

    if let Some(mcast_config) = multicast_config {
        let registry = registry.clone().expect("registry was created above");
        let mcast_rx = market_message_tx.subscribe();
        tokio::spawn(async move {
            let bind_addr = std::net::SocketAddr::new(std::net::IpAddr::V4(mcast_config.bind_addr), 0);
            match tokio::net::UdpSocket::bind(bind_addr).await {
                Ok(socket) => {
                    if let Err(err) = socket.set_multicast_ttl_v4(64) {
                        log::error!("failed to set multicast TTL: {err}");
                        std::process::exit(3);
                    }
                    let publisher = MulticastPublisher::new(socket, mcast_config, registry);
                    publisher.run(mcast_rx).await;
                }
                Err(err) => {
                    log::error!("failed to bind multicast UDP socket to {bind_addr}: {err}");
                    std::process::exit(3);
                }
            }
        });
    }

    if let Some(cfg) = dob_config {
        let registry = registry.clone().expect("registry was created above");

        let (dob_tx, dob_rx) = dob_channel(cfg.channel_bound);

        // Single shared mktdata-port frame seq, threaded through:
        //   1. the mktdata emitter (writer; fetch_add for each frame),
        //   2. the snapshot scheduler (reader; load to stamp anchor_seq), and
        //   3. the recovery path inside the listener (writer; fetch_add when
        //      reserving an InstrumentReset.new_anchor_seq).
        // Sharing this atomic across all three is what guarantees subscribers
        // can reconcile snapshots vs. deltas via anchor_seq without collisions.
        let mktdata_seq = Arc::new(AtomicU64::new(0));

        // Snapshot request channel: DobReplayTaps pushes Priority requests on
        // recovery; the snapshot scheduler drains them ahead of round-robin.
        let (snapshot_req_tx, snapshot_req_rx) = snapshot_request_channel(cfg.channel_bound);

        // Coin resolver for the apply tap: backed by `ArcSwap`, so `.load()`
        // never blocks and never fails. Safe to call from the sync apply_updates
        // path without risk of stalling a tokio worker. Returns
        // `(instrument_id, qty_exponent)` so the tap can scale internal `Sz`
        // (10^8 fixed) to the venue's wire representation per instrument.
        let coin_resolver: crate::listeners::order_book::dob_tap::CoinResolver = {
            let reg = Arc::clone(&registry);
            Box::new(move |coin: &Coin| {
                reg.load().active.get(&coin.value()).map(|info| (info.instrument_id, info.qty_exponent))
            })
        };
        // Shared per-instrument seq counter: bumped by the apply tap, read by
        // the snapshot emitter when populating SnapshotBegin.last_instrument_seq.
        let seq_counter = Arc::new(StdMutex::new(crate::order_book::PerInstrumentSeqCounter::new()));
        let tap = DobApplyTap::new(dob_tx.clone(), cfg.source_id, cfg.channel_id, seq_counter.clone(), coin_resolver);
        listener.lock().await.set_dob_tap(tap);

        // Synchronous instrument-id cache for the recovery path. The
        // DobReplayTaps closure must be sync (called from inside the
        // synchronous apply_recovery), so we maintain a HashMap<Coin, u32>
        // snapshot of the registry's `active` map under a std::sync::RwLock.
        // TODO: Refresh this cache on each registry-refresh tick. Hook point:
        // `instruments/hyperliquid.rs::refresh_task` — at the end of its success
        // path (after `*guard = new_state` around line 225), rebuild the cache
        // from the new universe and store via this Arc<RwLock>. Pass the Arc
        // into refresh_task as a new parameter, or wrap both in a small struct
        // that holds the registry + cache atomically.
        //
        // Until this is wired, the cache reflects the active set as of process
        // startup. New listings (e.g., builder-DEX coins added mid-run) won't
        // resolve here; recovery for those coins will skip InstrumentReset and
        // log an error per the resolver's contract in
        // listeners/order_book/mod.rs's emit_dob_instrument_reset.
        let instrument_id_cache: Arc<StdRwLock<HashMap<Coin, u32>>> = {
            let registry_state = registry.load();
            let mut map = HashMap::new();
            for (coin_str, info) in &registry_state.active {
                map.insert(Coin::new(coin_str), info.instrument_id);
            }
            Arc::new(StdRwLock::new(map))
        };
        let instrument_id_for: Box<dyn Fn(&Coin) -> Option<u32> + Send + Sync> = {
            let cache = instrument_id_cache.clone();
            Box::new(move |coin: &Coin| -> Option<u32> { cache.read().ok()?.get(coin).copied() })
        };

        let dob_replay_taps = DobReplayTaps {
            mktdata_tx: dob_tx.clone(),
            snapshot_request_tx: snapshot_req_tx,
            mktdata_seq: mktdata_seq.clone(),
            instrument_id_for,
        };
        listener.lock().await.attach_dob_replay_taps(dob_replay_taps);

        // Bind the mktdata and snapshot sockets up front, BEFORE spawning any
        // emitter task. If snapshot bind fails we exit before any mktdata
        // frames hit the wire — externally-visible subscribers must never
        // observe deltas without a snapshot stream backing them.
        // (Refdata's task constructs its socket internally inside
        // `run_dob_refdata_task`, so it is spawned alongside the others
        // below; a refdata bind failure would surface from that task only.)
        let mktdata_cfg = DobMktdataConfig {
            group_addr: cfg.group_addr,
            port: cfg.mktdata_port,
            bind_addr: cfg.bind_addr,
            channel_id: cfg.channel_id,
            mtu: cfg.mtu,
            heartbeat_interval: cfg.heartbeat_interval,
        };
        let mktdata_emitter = match DobEmitter::bind_with_seq(mktdata_cfg, mktdata_seq.clone()).await {
            Ok(e) => e,
            Err(err) => {
                log::error!("failed to bind DoB mktdata socket: {err}");
                std::process::exit(3);
            }
        };
        let snapshot_emitter = match DobSnapshotEmitter::bind(DobSnapshotConfig {
            group_addr: cfg.group_addr,
            port: cfg.snapshot_port,
            bind_addr: cfg.bind_addr,
            channel_id: cfg.channel_id,
            mtu: cfg.snapshot_mtu,
            round_duration: cfg.snapshot_round_duration,
        })
        .await
        {
            Ok(e) => e,
            Err(err) => {
                log::error!("failed to bind DoB snapshot socket: {err}");
                std::process::exit(3);
            }
        };

        // Both sockets are bound; now spawn the emitter tasks.
        tokio::spawn(run_dob_emitter(mktdata_emitter, dob_rx));
        tokio::spawn(run_dob_refdata_task(
            DobRefdataConfig {
                group_addr: cfg.group_addr,
                port: cfg.refdata_port,
                bind_addr: cfg.bind_addr,
                channel_id: cfg.channel_id,
                mtu: cfg.mtu,
                definition_cycle: cfg.definition_cycle,
                manifest_cadence: cfg.manifest_cadence,
            },
            crate::instruments::InstrumentRegistry::from_arc(registry.clone()),
        ));
        tokio::spawn(run_dob_snapshot_task(
            snapshot_emitter,
            listener.clone(),
            seq_counter.clone(),
            registry.clone(),
            mktdata_seq.clone(),
            snapshot_req_rx,
        ));

        // Drive the DoB heartbeat via HeartbeatTick events on the MPSC channel.
        // The emitter task converts each tick into a Heartbeat frame on the wire.
        let hb_tx = dob_tx;
        let hb_int = cfg.heartbeat_interval;
        tokio::spawn(async move {
            let mut t = tokio::time::interval(hb_int);
            t.tick().await; // skip the first immediate tick
            loop {
                t.tick().await;
                if hb_tx.send(DobEvent::HeartbeatTick.with_enqueue_timestamp()).await.is_err() {
                    break;
                }
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));
    let app = Router::new().route(
        "/ws",
        get({
            let market_message_tx = market_message_tx.clone();
            let l4_message_tx = l4_message_tx.clone();
            async move |ws_upgrade| {
                ws_handler(
                    ws_upgrade,
                    market_message_tx.clone(),
                    l4_message_tx.clone(),
                    listener.clone(),
                    ignore_spot,
                    websocket_opts,
                )
            }
        }),
    );

    let listener = TcpListener::bind(address).await?;
    info!("WebSocket server running at ws://{address}");

    if let Err(err) = axum::serve(listener, app.into_make_service()).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    market_message_tx: Sender<Arc<InternalMessage>>,
    l4_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
    websocket_opts: yawc::Options,
) -> impl IntoResponse {
    let (resp, fut) = incoming.upgrade(websocket_opts).unwrap();
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, market_message_tx, l4_message_tx, listener, ignore_spot).await
    });

    resp
}

async fn handle_socket(
    mut socket: WebSocket,
    market_message_tx: Sender<Arc<InternalMessage>>,
    l4_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
) {
    let mut market_message_rx = market_message_tx.subscribe();
    let mut l4_message_rx = l4_message_tx.subscribe();
    let is_ready = listener.lock().await.is_ready();
    let mut manager = SubscriptionManager::default();
    let mut universe = listener.lock().await.universe().into_iter().map(|c| c.value()).collect();
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        send_socket_message(&mut socket, msg).await;
        return;
    }
    loop {
        select! {
            recv_result = market_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        match msg.as_ref() {
                            InternalMessage::Snapshot { l2_snapshots, time, .. } => {
                                universe = new_universe(l2_snapshots, ignore_spot);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time).await;
                                }
                            },
                            InternalMessage::Fills { batch, .. } => {
                                let mut trades = coin_to_trades(batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_trades(&mut socket, sub, &mut trades).await;
                                }
                            },
                            InternalMessage::L4BookUpdates{ .. } => {}
                        }

                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("websocket market receiver lagged by {n} messages");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        error!("WebSocket market receiver closed");
                        return;
                    }
                }
            }

            recv_result = l4_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        if let InternalMessage::L4BookUpdates{ diff_batch, status_batch } = msg.as_ref() {
                            let mut book_updates = coin_to_book_updates(diff_batch, status_batch);
                            for sub in manager.subscriptions() {
                                send_ws_data_from_book_updates(&mut socket, sub, &mut book_updates).await;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("websocket l4 receiver lagged by {n} messages");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        error!("WebSocket l4 receiver closed");
                        return;
                    }
                }
            }

            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    return;
                                }
                            };

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                receive_client_message(&mut socket, &mut manager, value, &universe, listener.clone()).await;
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return;
                }
            }
        }
    }
}

async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
) {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
    };
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        let msg = ServerResponse::Error(format!("Invalid subscription: {sub}"));
        send_socket_message(socket, msg).await;
        return;
    }
    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => ("", manager.subscribe(subscription)),
        ClientMessage::Unsubscribe { .. } => ("un", manager.unsubscribe(subscription)),
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                    send_socket_message(socket, msg).await;
                    return;
                }
            }
        } else {
            None
        };
        let msg = ServerResponse::SubscriptionResponse(client_message);
        send_socket_message(socket, msg).await;
        if let Some(snapshot_msg) = snapshot_msg {
            send_socket_message(socket, snapshot_msg).await;
        }
    } else {
        let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub}"));
        send_socket_message(socket, msg).await;
    }
}

async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) {
    let msg = serde_json::to_string(&msg);
    match msg {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
            }
        }
        Err(err) => {
            error!("Server response serialization error: {err}");
        }
    }
}

// derive it from l2_snapshots because thats convenient
fn new_universe(l2_snapshots: &L2Snapshots, ignore_spot: bool) -> HashSet<String> {
    l2_snapshots
        .as_ref()
        .iter()
        .filter_map(|(c, _)| if !c.is_spot() || !ignore_spot { Some(c.clone().value()) } else { None })
        .collect()
}

async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>,
    time: u64,
) {
    if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
        let snapshot = snapshot.get(&Coin::new(coin));
        if let Some(snapshot) =
            snapshot.and_then(|snapshot| snapshot.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
        {
            let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
            let snapshot = snapshot.truncate(n_levels);
            let snapshot = snapshot.export_inner_snapshot();
            let l2_book = L2Book::from_l2_snapshot(coin.clone(), snapshot, time);
            let msg = ServerResponse::L2Book(l2_book);
            send_socket_message(socket, msg).await;
        } else {
            error!("Coin {coin} not found");
        }
    }
}

pub(crate) fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
    let mut fills = batch.clone().events();
    let mut trades = HashMap::new();
    while fills.len() >= 2 {
        let f2 = fills.pop();
        let f1 = fills.pop();
        if let Some(f1) = f1 {
            if let Some(f2) = f2 {
                let mut fills = HashMap::new();
                fills.insert(f1.1.side, f1);
                fills.insert(f2.1.side, f2);
                // from_fills returns None if the pair has two same-side fills
                // (one overwrites the other in the HashMap by Side key) or if
                // coin/tid don't match across the pair. Log and skip the pair —
                // upstream the pair-up by adjacent batch order is best-effort.
                if let Some(trade) = Trade::from_fills(fills) {
                    let coin = trade.coin.clone();
                    trades.entry(coin).or_insert_with(Vec::new).push(trade);
                } else {
                    log::warn!("coin_to_trades: skipping malformed fill pair (duplicate side or coin/tid mismatch)");
                }
            }
        }
    }
    for list in trades.values_mut() {
        list.reverse();
    }
    trades
}

fn coin_to_book_updates(
    diff_batch: &Batch<NodeDataOrderDiff>,
    status_batch: &Batch<NodeDataOrderStatus>,
) -> HashMap<String, L4BookUpdates> {
    let diffs = diff_batch.clone().events();
    let statuses = status_batch.clone().events();
    let time = diff_batch.block_time();
    let height = diff_batch.block_number();
    let mut updates = HashMap::new();
    for diff in diffs {
        let coin = diff.coin().value();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff);
    }
    for status in statuses {
        let coin = status.order.coin.clone();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status);
    }
    updates
}

async fn send_ws_data_from_book_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_updates: &mut HashMap<String, L4BookUpdates>,
) {
    if let Subscription::L4Book { coin } = subscription {
        if let Some(updates) = book_updates.remove(coin) {
            let msg = ServerResponse::L4Book(L4Book::Updates(updates));
            send_socket_message(socket, msg).await;
        }
    }
}

async fn send_ws_data_from_trades(
    socket: &mut WebSocket,
    subscription: &Subscription,
    trades: &mut HashMap<String, Vec<Trade>>,
) {
    if let Subscription::Trades { coin } = subscription {
        if let Some(trades) = trades.remove(coin) {
            let msg = ServerResponse::Trades(trades);
            send_socket_message(socket, msg).await;
        }
    }
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
    ) -> Result<Option<ServerResponse>> {
        if let Self::L4Book { coin } = self {
            let snapshot = listener.lock().await.compute_snapshot();
            if let Some(TimedSnapshots { time, height, snapshot }) = snapshot {
                let snapshot =
                    snapshot.value().into_iter().filter(|(c, _)| *c == Coin::new(coin)).collect::<Vec<_>>().pop();
                if let Some((coin, snapshot)) = snapshot {
                    let snapshot =
                        snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                    return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot {
                        coin: coin.value(),
                        time,
                        height,
                        levels: snapshot,
                    })));
                }
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn separate_fill_ingest_rejects_block_mode_before_startup() {
        let err = run_websocket_server("127.0.0.1:0", true, 1, None, None, IngestMode::Block, None, true)
            .await
            .err()
            .map(|err| err.to_string());
        assert_eq!(err.as_deref(), Some("--separate-fill-ingest requires streaming ingest mode"));
    }
}
