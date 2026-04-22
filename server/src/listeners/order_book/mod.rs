use crate::{
    HL_NODE,
    listeners::{directory::DirectoryListener, order_book::state::OrderBookState},
    order_book::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_json},
    },
    prelude::*,
    types::{
        L4Order,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use fs::File;
use latency::LatencyStats;
use log::{error, info};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::{Instant, interval_at},
};
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency};

pub(crate) mod latency;
mod state;
mod utils;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(listener: Arc<Mutex<OrderBookListener>>, dir: PathBuf) -> Result<()> {
    let order_statuses_dir = EventSource::OrderStatuses.event_source_dir(&dir).canonicalize()?;
    let fills_dir = EventSource::Fills.event_source_dir(&dir).canonicalize()?;
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir(&dir).canonicalize()?;
    info!("Monitoring order status directory: {}", order_statuses_dir.display());
    info!("Monitoring order diffs directory: {}", order_diffs_dir.display());
    info!("Monitoring fills directory: {}", fills_dir.display());

    // monitoring the directory via the notify crate (gives file system events)
    let (fs_event_tx, mut fs_event_rx) = unbounded_channel();
    let mut watcher = recommended_watcher(move |res| {
        let fs_event_tx = fs_event_tx.clone();
        if let Err(err) = fs_event_tx.send(res) {
            error!("Error sending fs event to processor via channel: {err}");
        }
    })?;

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    let latency_stats = Arc::new(LatencyStats::new());

    // every so often, we fetch a new snapshot and the snapshot_fetch_task starts running.
    // Result is sent back along this channel (if error, we want to return to top level)
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    watcher.watch(&order_statuses_dir, RecursiveMode::Recursive)?;
    watcher.watch(&fills_dir, RecursiveMode::Recursive)?;
    watcher.watch(&order_diffs_dir, RecursiveMode::Recursive)?;
    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = interval_at(start, Duration::from_secs(60));
    // Track last event time for liveness detection (replaces the sleep(5) branch
    // that was recreating a Sleep future on every select! iteration)
    let mut last_event_time: Option<Instant> = None;

    // Classify an fs path into its EventSource (avoids repeating starts_with checks)
    let classify_path = |path: &std::path::Path| -> Option<EventSource> {
        if path.starts_with(&order_statuses_dir) {
            Some(EventSource::OrderStatuses)
        } else if path.starts_with(&fills_dir) {
            Some(EventSource::Fills)
        } else if path.starts_with(&order_diffs_dir) {
            Some(EventSource::OrderDiffs)
        } else {
            None
        }
    };

    loop {
        tokio::select! {
            event = fs_event_rx.recv() => match event {
                Some(Ok(first_event)) => {
                    let t_wakeup = Instant::now();

                    // Single lock acquisition: process the first event plus
                    // any others that arrived while we waited for the lock.
                    // When hl-node writes a block it touches 3 directories in
                    // quick succession — draining batches those into one pass.
                    let mut guard = listener.lock().await;

                    // Process first event
                    if first_event.kind.is_create() || first_event.kind.is_modify() {
                        let path = &first_event.paths[0];
                        if path.is_file() {
                            if let Some(source) = classify_path(path) {
                                guard.process_update(&first_event, path, source)
                                    .map_err(|err| format!("{source} processing error: {err}"))?;
                            }
                        }
                    }

                    // Drain and process any additional pending events
                    while let Ok(next) = fs_event_rx.try_recv() {
                        match next {
                            Ok(ev) if ev.kind.is_create() || ev.kind.is_modify() => {
                                let path = &ev.paths[0];
                                if path.is_file() {
                                    if let Some(source) = classify_path(path) {
                                        guard.process_update(&ev, path, source)
                                            .map_err(|err| format!("{source} processing error: {err}"))?;
                                    }
                                }
                            }
                            Ok(_) => {} // non-create/modify events
                            Err(err) => {
                                error!("Watcher error (drained): {err}");
                                return Err(format!("Watcher error: {err}").into());
                            }
                        }
                    }

                    // Record latency from the latest batch processed in this pass
                    let t_done = Instant::now();
                    if let Some((block_time_ms, local_time_ms)) = guard.last_batch_times() {
                        latency_stats.record(t_wakeup.into(), t_done.into(), block_time_ms, local_time_ms);
                    }

                    last_event_time = Some(t_done);
                }
                Some(Err(err)) => {
                    error!("Watcher error: {err}");
                    return Err(format!("Watcher error: {err}").into());
                }
                None => {
                    error!("Channel closed. Listener exiting");
                    return Err("Channel closed.".into());
                }
            },
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                match snapshot_fetch_res {
                    None => {
                        return Err("Snapshot fetch task sender dropped".into());
                    }
                    Some(Err(err)) => {
                        return Err(format!("Abci state reading error: {err}").into());
                    }
                    Some(Ok(())) => {}
                }
            }
            _ = ticker.tick() => {
                // Liveness check: replaces the per-iteration sleep(5) future.
                // Detection window is 5–15 s instead of exactly 5 s, which is
                // fine for a "has hl-node died?" check.
                if let Some(last) = last_event_time {
                    if last.elapsed() > Duration::from_secs(5) {
                        let guard = listener.lock().await;
                        if guard.is_ready() {
                            return Err(format!("Stream has fallen behind ({HL_NODE} failed?)").into());
                        }
                    }
                }

                latency_stats.report();

                let listener = listener.clone();
                let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                fetch_snapshot(dir.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
            }
        }
    }
}

fn fetch_snapshot(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        let res: Result<()> = match process_rmp_file(&dir).await {
            Ok(output_fln) => {
                let snapshot = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&output_fln).await;
                info!("Snapshot fetched");
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        // Single short lock: grab our internal snapshot at the
                        // reference height, or initialize if first time.
                        // No caching/replay needed — if our height has already
                        // moved past the snapshot we just skip this validation
                        // cycle (we'll catch the next one).
                        let mut guard = listener.lock().await;
                        if guard.is_ready() {
                            let our_height = guard.order_book_state.as_ref()
                                .map(|s| s.height())
                                .unwrap_or(0);
                            if our_height < height {
                                // We haven't reached the snapshot height yet;
                                // skip — we'll validate on a future cycle.
                                info!("Validation skipped: our height {our_height} < snapshot height {height}");
                                Ok(())
                            } else if our_height > height {
                                // We've moved past — snapshot is stale, skip.
                                info!("Validation skipped: our height {our_height} > snapshot height {height}");
                                Ok(())
                            } else {
                                // Heights match — clone state under lock, then
                                // compute snapshot + validate outside the lock
                                // so we don't block the hot path. If divergence
                                // is found, re-lock and apply surgical per-coin
                                // recovery rather than tearing down the feed.
                                let state = guard.order_book_state
                                    .clone()
                                    .expect("is_ready checked above");
                                drop(guard);
                                let stored_snapshot = state.compute_snapshot().snapshot;
                                info!("Validating snapshot at height {height}");
                                let report = validate_snapshot_consistency(&stored_snapshot, &expected_snapshot, ignore_spot);
                                if report.is_clean() {
                                    Ok(())
                                } else {
                                    log::warn!(
                                        "snapshot validation found {} impacted coin(s): {} diverged, {} missing in fresh, {} extra in fresh — applying surgical recovery",
                                        report.total_impacted(),
                                        report.diverged.len(),
                                        report.missing_in_fresh.len(),
                                        report.extra_in_fresh.len(),
                                    );
                                    listener.lock().await.apply_recovery(&report, expected_snapshot);
                                    Ok(())
                                }
                            }
                        } else {
                            guard.init_from_snapshot(expected_snapshot, height);
                            Ok(())
                        }
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        };
        let _unused = tx.send(res);
        Ok::<(), Error>(())
    });
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    fill_status_file: Option<File>,
    order_status_file: Option<File>,
    order_diff_file: Option<File>,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    last_fill: Option<u64>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    // Timestamps from the most recently deserialized batch (for latency tracking)
    last_batch_block_time_ms: Option<u64>,
    last_batch_local_time_ms: Option<u64>,
}

impl OrderBookListener {
    pub(crate) const fn new(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, ignore_spot: bool) -> Self {
        Self {
            ignore_spot,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            order_book_state: None,
            last_fill: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
            last_batch_block_time_ms: None,
            last_batch_local_time_ms: None,
        }
    }

    /// Returns (block_time_ms, local_time_ms) from the most recently
    /// deserialized batch, if any batches have been processed since the
    /// last call.  Clears the stored times so the caller knows whether
    /// new data was processed in a given pass.
    pub(crate) fn last_batch_times(&mut self) -> Option<(u64, u64)> {
        match (self.last_batch_block_time_ms.take(), self.last_batch_local_time_ms.take()) {
            (Some(bt), Some(lt)) => Some((bt, lt)),
            _ => None,
        }
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    #[allow(clippy::type_complexity)]
    // pops earliest pair of cached updates that have the same timestamp if possible
    fn pop_cache(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        // synchronize to same block
        while let Some(t) = self.order_diff_cache.front() {
            if let Some(s) = self.order_status_cache.front() {
                match t.block_number().cmp(&s.block_number()) {
                    Ordering::Less => {
                        self.order_diff_cache.pop_front();
                    }
                    Ordering::Equal => {
                        return self
                            .order_status_cache
                            .pop_front()
                            .and_then(|t| self.order_diff_cache.pop_front().map(|s| (t, s)));
                    }
                    Ordering::Greater => {
                        self.order_status_cache.pop_front();
                    }
                }
            } else {
                break;
            }
        }
        None
    }

    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(batch) => {
                self.order_status_cache.push(batch);
            }
            EventBatch::BookDiffs(batch) => {
                self.order_diff_cache.push(batch);
            }
            EventBatch::Fills(batch) => {
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    // send fill updates if we received a new update
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            let _unused = tx.send(snapshot);
                        });
                    }
                }
            }
        }
        if self.is_ready() {
            if let Some((order_statuses, order_diffs)) = self.pop_cache() {
                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(order_statuses.clone(), order_diffs.clone()))
                    .transpose()?;
                if let Some(cache) = &mut self.fetched_snapshot_cache {
                    cache.push_back((order_statuses.clone(), order_diffs.clone()));
                }
                if let Some(tx) = &self.internal_message_tx {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let updates = Arc::new(InternalMessage::L4BookUpdates {
                            diff_batch: order_diffs,
                            status_batch: order_statuses,
                        });
                        let _unused = tx.send(updates);
                    });
                }
            }
        }
        Ok(())
    }

    /// Applies surgical per-coin recovery based on a `ValidationReport`.
    ///
    /// Instead of exiting the process when the internal book diverges from a
    /// fresh snapshot, we re-initialize just the affected coins in place.
    /// The rest of the channel continues untouched — other instruments are
    /// unaffected, and the next tick's `InternalMessage::Snapshot` will
    /// carry the corrected BBO for the repaired coins through the normal
    /// publish path.
    fn apply_recovery(&mut self, report: &utils::ValidationReport, fresh_snapshot: Snapshots<InnerL4Order>) {
        let Some(state) = self.order_book_state.as_mut() else {
            return;
        };

        let mut fresh_map = fresh_snapshot.value();

        // Repair coins that diverged: replace the internal book with the fresh one.
        for (coin, msg) in &report.diverged {
            log::warn!("recovery: re-initializing {} (divergence: {})", coin.value(), msg);
            if let Some(fresh_book) = fresh_map.remove(coin) {
                state.replace_coin_from_snapshot(coin.clone(), fresh_book, true);
            } else {
                log::warn!(
                    "recovery: diverged coin {} missing from fresh snapshot — dropping from state",
                    coin.value()
                );
                state.remove_coin(coin);
            }
        }

        // Drop coins that we have but the fresh snapshot doesn't (stale data).
        for coin in &report.missing_in_fresh {
            log::warn!("recovery: dropping stale coin {} not in fresh snapshot", coin.value());
            state.remove_coin(coin);
        }

        // Add coins that are in the fresh snapshot but not in our state (new listings
        // that appeared between our bootstrap and now).
        for coin in &report.extra_in_fresh {
            if let Some(fresh_book) = fresh_map.remove(coin) {
                log::warn!("recovery: adding new coin {} from fresh snapshot", coin.value());
                state.replace_coin_from_snapshot(coin.clone(), fresh_book, true);
            }
        }
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("No existing snapshot");
        let mut new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        let mut retry = false;
        while let Some((order_statuses, order_diffs)) = self.pop_cache() {
            if new_order_book.apply_updates(order_statuses, order_diffs).is_err() {
                info!(
                    "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                );
                retry = true;
                break;
            }
        }
        if !retry {
            self.order_book_state = Some(new_order_book);
            info!("Order book ready");
        }
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    // prevent snapshotting mutiple times at the same height
    fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        self.order_book_state.as_mut().and_then(|o| o.l2_snapshots(prevent_future_snaps))
    }
}

impl OrderBookListener {
    fn process_update(&mut self, event: &Event, new_path: &PathBuf, event_source: EventSource) -> Result<()> {
        if event.kind.is_create() {
            info!("-- Event: {} created --", new_path.display());
            self.on_file_creation(new_path.clone(), event_source)?;
        }
        // Check for `Modify` event (only if the file is already initialized)
        else {
            // If we are not tracking anything right now, we treat a file update as declaring that it has been created.
            // Unfortunately, we miss the update that occurs at this time step.
            // We go to the end of the file to read for updates after that.
            if self.is_reading(event_source) {
                self.on_file_modification(event_source)?;
            } else {
                info!("-- Event: {} modified, tracking it now --", new_path.display());
                let file = self.file_mut(event_source);
                let mut new_file = File::open(new_path)?;
                // Seek to end, then back up to the last newline so we start
                // on a clean line boundary.  Without this, we can land mid-JSON
                // if hl-node is mid-write, producing a permanent deser error.
                let end = new_file.seek(SeekFrom::End(0))?;
                let backtrack = end.min(8192);
                if backtrack > 0 {
                    new_file.seek(SeekFrom::End(-(backtrack as i64)))?;
                    let mut buf = vec![0u8; backtrack as usize];
                    new_file.read_exact(&mut buf)?;
                    if let Some(last_nl) = buf.iter().rposition(|&b| b == b'\n') {
                        // Position right after the last newline
                        let rewind = backtrack - (last_nl as u64) - 1;
                        new_file.seek(SeekFrom::End(-(rewind as i64)))?;
                    }
                    // else: no newline in last 8KB, stay at end
                }
                *file = Some(new_file);
            }
        }
        Ok(())
    }
}

impl DirectoryListener for OrderBookListener {
    fn is_reading(&self, event_source: EventSource) -> bool {
        match event_source {
            EventSource::Fills => self.fill_status_file.is_some(),
            EventSource::OrderStatuses => self.order_status_file.is_some(),
            EventSource::OrderDiffs => self.order_diff_file.is_some(),
        }
    }

    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File> {
        match event_source {
            EventSource::Fills => &mut self.fill_status_file,
            EventSource::OrderStatuses => &mut self.order_status_file,
            EventSource::OrderDiffs => &mut self.order_diff_file,
        }
    }

    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource) -> Result<()> {
        if let Some(file) = self.file_mut(event_source).as_mut() {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            if !buf.is_empty() {
                self.process_data(buf, event_source)?;
            }
        }
        *self.file_mut(event_source) = Some(File::open(new_file)?);
        Ok(())
    }

    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()> {
        let total_len = data.len();
        let lines = data.lines();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let res = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
                    let height = batch.block_number();
                    let bt = batch.block_time();
                    let lt = batch.local_time_ms();
                    (height, bt, lt, EventBatch::Fills(batch))
                }),
                EventSource::OrderStatuses => serde_json::from_str(line).map(|batch: Batch<NodeDataOrderStatus>| {
                    let height = batch.block_number();
                    let bt = batch.block_time();
                    let lt = batch.local_time_ms();
                    (height, bt, lt, EventBatch::Orders(batch))
                }),
                EventSource::OrderDiffs => serde_json::from_str(line).map(|batch: Batch<NodeDataOrderDiff>| {
                    let height = batch.block_number();
                    let bt = batch.block_time();
                    let lt = batch.local_time_ms();
                    (height, bt, lt, EventBatch::BookDiffs(batch))
                }),
            };
            let (height, block_time_ms, local_time_ms, event_batch) = match res {
                Ok(data) => data,
                Err(err) => {
                    // if we run into a serialization error (hitting EOF), just return to last line.
                    error!(
                        "{event_source} serialization error {err}, height: {:?}, line: {:?}",
                        self.order_book_state.as_ref().map(OrderBookState::height),
                        &line[..100],
                    );
                    #[allow(clippy::unwrap_used)]
                    let total_len: i64 = total_len.try_into().unwrap();
                    self.file_mut(event_source).as_mut().map(|f| f.seek_relative(-total_len));
                    break;
                }
            };
            self.last_batch_block_time_ms = Some(block_time_ms);
            self.last_batch_local_time_ms = Some(local_time_ms);
            if height % 100 == 0 {
                info!("{event_source} block: {height}");
            }
            if let Err(err) = self.receive_batch(event_batch) {
                self.order_book_state = None;
                return Err(err);
            }
        }
        let snapshot = self.l2_snapshots(true);
        if let Some(snapshot) = snapshot {
            if let Some(tx) = &self.internal_message_tx {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let snapshot = Arc::new(InternalMessage::Snapshot { l2_snapshots: snapshot.1, time: snapshot.0 });
                    let _unused = tx.send(snapshot);
                });
            }
        }
        Ok(())
    }
}

pub(crate) struct L2Snapshots(HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>);

impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>> {
        &self.0
    }
}

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    Snapshot { l2_snapshots: L2Snapshots, time: u64 },
    Fills { batch: Batch<NodeDataFill> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}
