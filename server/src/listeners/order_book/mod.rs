use crate::{
    HL_NODE,
    listeners::{directory::DirectoryListener, order_book::state::OrderBookState},
    multicast::dob::{
        DobEvent, DobEventSender, DobSnapshotRequest, DobSnapshotRequestSender, SharedMktdataSeq,
    },
    order_book::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_json},
    },
    prelude::*,
    protocol::dob::{
        constants::RESET_REASON_PUBLISHER_INCONSISTENCY, messages::InstrumentReset,
    },
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
    time::{Duration, SystemTime, UNIX_EPOCH},
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

pub(crate) mod dob_tap;
pub(crate) mod latency;
#[cfg(test)]
mod parity_tests;
mod state;
mod utils;

/// Wall-clock nanoseconds since the Unix epoch. Mirrors the helper in
/// `multicast::dob` (kept module-local there) so `apply_recovery` can stamp
/// `InstrumentReset.timestamp_ns` without crossing a privacy boundary.
#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

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

/// Channels and resolution callbacks the DoB recovery path uses to emit
/// `InstrumentReset` on mktdata and schedule a priority snapshot. Constructed
/// at startup once the DoB pipeline is running and attached to the listener
/// via `attach_dob_replay_taps`. Recovery falls back to a no-op emission
/// (book mutation still happens) when no taps are attached.
pub(crate) struct DobReplayTaps {
    pub(crate) mktdata_tx: DobEventSender,
    pub(crate) snapshot_request_tx: DobSnapshotRequestSender,
    pub(crate) mktdata_seq: SharedMktdataSeq,
    /// Resolves a public `instrument_id` for a given internal `Coin`. Called
    /// from the recovery path (which runs synchronously inside `apply_recovery`),
    /// so this resolver MUST be a synchronous lookup against a cached snapshot
    /// — Task 9 wires this against a HashMap snapshot of the instrument
    /// registry's `active` map (refreshed on each registry-refresh tick), not
    /// directly against the async `InstrumentRegistry::get` API.
    pub(crate) instrument_id_for: Box<dyn Fn(&Coin) -> Option<u32> + Send + Sync>,
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
    // Held until the first snapshot is ready, then attached to the state.
    pending_dob_tap: Option<dob_tap::DobApplyTap>,
    // Channels into the DoB pipeline, used by `apply_recovery` to emit
    // `InstrumentReset` and schedule priority snapshots. `None` until
    // `attach_dob_replay_taps` is called; recovery still runs without it.
    dob_replay_taps: Option<DobReplayTaps>,
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
            pending_dob_tap: None,
            dob_replay_taps: None,
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

    /// Returns a deterministic clone of the resting orders for `coin`, or
    /// `None` if the book state has not yet initialized or the coin is
    /// absent. Called by the DoB snapshot emitter task once per snapshot
    /// slot — keeps the lock held only for the duration of the clone.
    pub(crate) fn clone_coin_orders(&self, coin: &Coin) -> Option<Vec<InnerL4Order>> {
        self.order_book_state.as_ref().and_then(|s| s.clone_coin_orders(coin))
    }

    /// Attaches a `DobApplyTap` to the book state. If the snapshot has not yet
    /// arrived, stores the tap as pending; it will be attached when the first
    /// snapshot is ready.
    pub(crate) fn set_dob_tap(&mut self, tap: dob_tap::DobApplyTap) {
        if let Some(state) = self.order_book_state.as_mut() {
            state.attach_dob_tap(tap);
        } else {
            self.pending_dob_tap = Some(tap);
        }
    }

    /// Attaches the channels and instrument resolver that `apply_recovery`
    /// uses to emit `InstrumentReset` and schedule priority snapshots.
    pub(crate) fn attach_dob_replay_taps(&mut self, taps: DobReplayTaps) {
        self.dob_replay_taps = Some(taps);
    }

    /// Emits `InstrumentReset` on mktdata for `coin` and schedules a priority
    /// snapshot. Per spec invariant, `Per-Instrument Seq` is NOT reset on
    /// `InstrumentReset` — it only resets on `Reset Count` change.
    ///
    /// Both sends are best-effort: if either receiver is gone (shutdown), we
    /// log and let the caller proceed with the book mutation. The mutation
    /// must still happen even if the DoB pipeline isn't draining.
    ///
    /// Takes `&Option<DobReplayTaps>` (rather than `&self`) so the apply
    /// path can call this while holding a disjoint mutable borrow on
    /// `self.order_book_state`.
    fn emit_dob_instrument_reset(taps: Option<&DobReplayTaps>, coin: &Coin) {
        use std::sync::atomic::Ordering;

        let Some(taps) = taps else { return };
        let Some(instrument_id) = (taps.instrument_id_for)(coin) else {
            log::error!(
                "dob taps: coin '{}' has no instrument_id at recovery time — \
                 book mutation will proceed but no InstrumentReset will be sent. \
                 Stream consumers for this instrument may be inconsistent until \
                 next refresh.",
                coin.value()
            );
            return;
        };

        // Reserve the next mktdata seq as the anchor. fetch_add returns the
        // previous value, so the +1 is what the next-emitted mktdata frame
        // will carry.
        // Relaxed: monotonicity comes from fetch_add itself; no other memory
        // is published through this atomic. The consumer at `multicast::dob`'s
        // snapshot scheduler reads via `load(Relaxed)` for the same reason —
        // see SharedMktdataSeq's doc comment in multicast/dob.rs.
        let prev = taps.mktdata_seq.fetch_add(1, Ordering::Relaxed);
        let new_anchor_seq = prev + 1;

        let msg = InstrumentReset {
            instrument_id,
            reason: RESET_REASON_PUBLISHER_INCONSISTENCY,
            new_anchor_seq,
            timestamp_ns: now_ns(),
        };

        if let Err(err) = taps.mktdata_tx.try_send(DobEvent::InstrumentReset(msg)) {
            log::warn!(
                "dob taps: dropped InstrumentReset for {}: {err}",
                coin.value()
            );
        }
        if let Err(err) = taps.snapshot_request_tx.try_send(DobSnapshotRequest::Priority {
            instrument_id,
            anchor_seq: new_anchor_seq,
        }) {
            log::warn!(
                "dob taps: dropped priority snapshot request for {}: {err}",
                coin.value()
            );
        }
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
        // Disjoint borrows: `state` is the &mut on the book, `taps` is the
        // &shared on the DoB channels. Splitting at the field level lets the
        // emit helper borrow `taps` while `state` mutation continues.
        let taps = self.dob_replay_taps.as_ref();
        let Some(state) = self.order_book_state.as_mut() else {
            return;
        };

        let mut fresh_map = fresh_snapshot.value();

        // Repair coins that diverged: replace the internal book with the fresh one.
        // Emit InstrumentReset BEFORE the mutation so subscribers discard
        // delta state for the affected instrument before any new deltas
        // derived from the replaced book begin flowing.
        for (coin, msg) in &report.diverged {
            log::warn!("recovery: re-initializing {} (divergence: {})", coin.value(), msg);
            Self::emit_dob_instrument_reset(taps, coin);
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
            Self::emit_dob_instrument_reset(taps, coin);
            state.remove_coin(coin);
        }

        // Add coins that are in the fresh snapshot but not in our state (new listings
        // that appeared between our bootstrap and now).
        for coin in &report.extra_in_fresh {
            if let Some(fresh_book) = fresh_map.remove(coin) {
                log::warn!("recovery: adding new coin {} from fresh snapshot", coin.value());
                Self::emit_dob_instrument_reset(taps, coin);
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
            if let Some(tap) = self.pending_dob_tap.take() {
                new_order_book.attach_dob_tap(tap);
            }
            self.order_book_state = Some(new_order_book);
            info!("Order book ready");
        }
    }

    /// Test-only constructor: builds a listener pre-seeded from `snapshot`,
    /// bypassing the file-watcher and validation paths. Mirrors the call
    /// `init_from_snapshot` makes during normal startup.
    #[cfg(test)]
    pub(crate) fn for_test_with_snapshot(snapshot: Snapshots<InnerL4Order>, height: u64) -> Self {
        let mut listener = Self::new(None, false);
        listener.init_from_snapshot(snapshot, height);
        listener
    }

    /// Test-only entry point: feeds a synthetic `(order_statuses, order_diffs)`
    /// pair through the same `apply_updates` path the file-watcher uses. The
    /// DoB tap (if attached) fires on every successful apply, and the
    /// internal book mutates exactly as in production. Bypasses the
    /// `BatchQueue` cache and `pop_cache` reordering — callers must supply
    /// matched batches directly.
    #[cfg(test)]
    pub(crate) fn apply_test_batch(
        &mut self,
        order_statuses: Batch<NodeDataOrderStatus>,
        order_diffs: Batch<NodeDataOrderDiff>,
    ) -> Result<()> {
        self.order_book_state
            .as_mut()
            .ok_or("apply_test_batch: order book state not ready")?
            .apply_updates(order_statuses, order_diffs)
    }

    /// Test-only accessor for the listener's `l2_snapshots` so parity tests
    /// can derive what the TOB publisher would emit without spinning up the
    /// full publisher loop.
    #[cfg(test)]
    pub(crate) fn l2_snapshots_for_test(&mut self) -> Option<(u64, L2Snapshots)> {
        self.l2_snapshots(false)
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

#[cfg(test)]
mod instrument_reset_recovery_tests {
    //! End-to-end `InstrumentReset` recovery flow test.
    //!
    //! After a divergence is reported by validation, `apply_recovery` MUST:
    //!   1. Emit `InstrumentReset` on the `mktdata` port with
    //!      `reason == RESET_REASON_PUBLISHER_INCONSISTENCY` and a
    //!      `new_anchor_seq` strictly greater than the pre-mismatch
    //!      `mktdata_seq` (it reserves the next mktdata frame seq).
    //!   2. Schedule a priority snapshot on the `snapshot` port with
    //!      `Anchor Seq == new_anchor_seq` (matching the reset's reservation).
    //!   3. NOT reset the per-instrument delta seq counter — subsequent
    //!      deltas continue from the pre-mismatch counter value (the wire
    //!      spec invariant: `Per-Instrument Seq` resets only on
    //!      `Reset Count` change, not on `InstrumentReset`).
    //!
    //! This test wires the real recovery path (no mocks beyond the UDP
    //! collectors and a synchronous instrument resolver) and asserts all
    //! three observable contract clauses on the wire / shared counter.
    use super::*;
    use crate::listeners::order_book::dob_tap::SharedSeqCounter;
    use crate::multicast::dob::{
        DobMktdataConfig, DobSnapshotConfig, DobSnapshotEmitter, DobEmitter, channel,
        run_dob_emitter, run_dob_snapshot_task, snapshot_request_channel,
    };
    use crate::order_book::{Coin, PerInstrumentSeqCounter};
    use crate::protocol::dob::constants::{
        DEFAULT_MTU, FRAME_HEADER_SIZE, INSTRUMENT_RESET_SIZE, MSG_TYPE_HEARTBEAT,
        MSG_TYPE_INSTRUMENT_RESET, MSG_TYPE_SNAPSHOT_BEGIN, RESET_REASON_PUBLISHER_INCONSISTENCY,
        SNAPSHOT_BEGIN_SIZE,
    };
    use crate::test_fixtures::{build_one_coin_snapshot, build_registry_with_one_instrument};
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
    use tokio::net::UdpSocket;

    async fn bind_collector() -> (UdpSocket, SocketAddrV4) {
        let sock = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = match sock.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        (sock, addr)
    }

    /// Receives one or more datagrams on `sock`, scanning past frames whose
    /// first message type is in `skip_types`, and returns the first frame
    /// whose first message type is `target_type`. Bounded by `timeout`.
    /// Used to skip past any heartbeat or other "in-band noise" the emitter
    /// might happen to send before our event of interest.
    async fn recv_first_with_msg_type(
        sock: &UdpSocket,
        target_type: u8,
        skip_types: &[u8],
        timeout: Duration,
    ) -> Vec<u8> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut buf = [0u8; 2048];
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for msg_type 0x{target_type:02X}",
            );
            let (n, _) = tokio::time::timeout(remaining, sock.recv_from(&mut buf))
                .await
                .expect("recv timed out")
                .expect("recv error");
            assert!(
                n >= FRAME_HEADER_SIZE + 1,
                "frame too short to contain a message type byte",
            );
            assert_eq!(&buf[0..2], &[0x44, 0x44], "DoB magic on wire");
            let msg_type = buf[FRAME_HEADER_SIZE];
            if msg_type == target_type {
                return buf[..n].to_vec();
            }
            if !skip_types.contains(&msg_type) {
                panic!(
                    "unexpected msg_type 0x{msg_type:02X} on collector while waiting \
                     for 0x{target_type:02X}",
                );
            }
        }
    }

    #[tokio::test]
    async fn instrument_reset_emits_on_mktdata_then_priority_snapshot_then_resumes_with_continuing_seq()
    {
        // 1. Two collector sockets standing in for the mktdata and snapshot
        //    multicast subscribers.
        let (mktdata_collector, mktdata_addr) = bind_collector().await;
        let (snapshot_collector, snapshot_addr) = bind_collector().await;

        // 2. Pre-populate shared state. The mktdata seq baseline is 100;
        //    apply_recovery's fetch_add(1, ...) returns 100 (the prior value)
        //    so `new_anchor_seq` will be 101. The per-instrument seq counter
        //    is bumped 5 times so its `last(0) == 5` — the post-recovery
        //    `next(0)` MUST return 6 (NOT 1).
        let mktdata_seq: SharedMktdataSeq = Arc::new(AtomicU64::new(100));
        let seq_counter: SharedSeqCounter = Arc::new(StdMutex::new(PerInstrumentSeqCounter::new()));
        {
            let mut g = seq_counter.lock().unwrap();
            for _ in 0..5 {
                g.next(0);
            }
            assert_eq!(g.last(0), 5, "pre-recovery per-instrument seq baseline");
        }

        // 3. Build a listener pre-seeded with one coin at a known initial
        //    snapshot. apply_recovery consumes a "fresh" Snapshots that we
        //    construct with a different oid offset so validation would have
        //    found the coin diverged.
        let coin_str = "BTC";
        let coin = Coin::new(coin_str);
        let initial = build_one_coin_snapshot(coin_str, 3, /* oid_offset = */ 0);
        let listener = OrderBookListener::for_test_with_snapshot(initial, /* height = */ 1);
        let listener = Arc::new(tokio::sync::Mutex::new(listener));

        // 4. Mktdata pipeline: bounded mpsc -> run_dob_emitter -> mktdata UDP
        //    socket. The emitter shares the same SharedMktdataSeq atomic as
        //    apply_recovery, so seq bumps from both sides stay coherent.
        let (mkt_tx, mkt_rx) = channel(64);
        let mkt_emitter = DobEmitter::bind_with_seq(
            DobMktdataConfig {
                group_addr: *mktdata_addr.ip(),
                port: mktdata_addr.port(),
                bind_addr: Ipv4Addr::LOCALHOST,
                channel_id: 0,
                mtu: DEFAULT_MTU,
                // Short interval drives the emitter loop's periodic flush
                // so a single in-flight InstrumentReset gets out on the wire
                // promptly. The interval ONLY flushes — it does not emit a
                // Heartbeat message itself (that requires an explicit
                // DobEvent::HeartbeatTick), so the collector never sees a
                // heartbeat byte regardless of the cadence.
                heartbeat_interval: Duration::from_millis(50),
            },
            mktdata_seq.clone(),
        )
        .await
        .unwrap();
        let mkt_handle = tokio::spawn(run_dob_emitter(mkt_emitter, mkt_rx));

        // 5. Snapshot pipeline: priority-request channel -> run_dob_snapshot_task.
        //    Long round_duration so the round-robin path stays parked on its
        //    sleep — only the priority-path snapshot can fire within the
        //    test's recv timeout.
        let (snap_tx, snap_rx) = snapshot_request_channel(8);
        let snap_emitter = DobSnapshotEmitter::bind(DobSnapshotConfig {
            group_addr: *snapshot_addr.ip(),
            port: snapshot_addr.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: 1,
            mtu: DEFAULT_MTU,
            round_duration: Duration::from_secs(60),
        })
        .await
        .unwrap();
        let registry = build_registry_with_one_instrument(coin_str, 0);
        let snap_handle = tokio::spawn(run_dob_snapshot_task(
            snap_emitter,
            listener.clone(),
            seq_counter.clone(),
            registry.clone(),
            mktdata_seq.clone(),
            snap_rx,
        ));

        // 6. Attach replay taps and trigger the recovery. The instrument
        //    resolver is a sync closure (matches Task 9's contract). The
        //    fresh snapshot we hand to apply_recovery has the same coin
        //    with different orders — the report's `diverged` entry is what
        //    drives the InstrumentReset emission.
        {
            let mut guard = listener.lock().await;
            guard.attach_dob_replay_taps(DobReplayTaps {
                mktdata_tx: mkt_tx.clone(),
                snapshot_request_tx: snap_tx.clone(),
                mktdata_seq: mktdata_seq.clone(),
                instrument_id_for: Box::new(|c: &Coin| {
                    if c.value() == "BTC" { Some(0) } else { None }
                }),
            });

            let mut report = utils::ValidationReport::default();
            report.diverged.push((coin.clone(), "test-injected divergence".into()));
            let fresh = build_one_coin_snapshot(coin_str, 3, /* oid_offset = */ 5_000);

            guard.apply_recovery(&report, fresh);
        }

        // 7. Mktdata: expect the InstrumentReset.
        let frame = recv_first_with_msg_type(
            &mktdata_collector,
            MSG_TYPE_INSTRUMENT_RESET,
            // Tolerate a Heartbeat slipping in (msg_type 0x01). None expected
            // given the 60s interval, but we don't want flakes if the timer
            // happened to fire at startup.
            &[MSG_TYPE_HEARTBEAT],
            Duration::from_secs(2),
        )
        .await;
        assert!(
            frame.len() >= FRAME_HEADER_SIZE + INSTRUMENT_RESET_SIZE,
            "mktdata frame too short for InstrumentReset (got {} bytes)",
            frame.len(),
        );
        let body = &frame[FRAME_HEADER_SIZE..];
        // InstrumentReset wire layout (28 bytes):
        //   msg_type(1) + msg_size(1) + flags(2) = app header (4)
        //   instrument_id(4)                       offset 4..8
        //   reason(1)                              offset 8
        //   reserved(3)                            offset 9..12
        //   new_anchor_seq(8)                      offset 12..20
        //   timestamp_ns(8)                        offset 20..28
        let instrument_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
        let reason = body[8];
        let new_anchor_seq = u64::from_le_bytes(body[12..20].try_into().unwrap());
        assert_eq!(instrument_id, 0, "instrument_id from coin resolver");
        assert_eq!(
            reason, RESET_REASON_PUBLISHER_INCONSISTENCY,
            "reason byte must be 1 (publisher-inconsistency)",
        );
        assert_eq!(
            new_anchor_seq, 101,
            "new_anchor_seq must be the pre-mismatch mktdata_seq + 1 (100 + 1)",
        );

        // 8. Snapshot port: expect a SnapshotBegin with anchor_seq == 101.
        let frame = recv_first_with_msg_type(
            &snapshot_collector,
            MSG_TYPE_SNAPSHOT_BEGIN,
            &[],
            Duration::from_secs(2),
        )
        .await;
        assert!(
            frame.len() >= FRAME_HEADER_SIZE + SNAPSHOT_BEGIN_SIZE,
            "snapshot frame too short for SnapshotBegin (got {} bytes)",
            frame.len(),
        );
        let body = &frame[FRAME_HEADER_SIZE..];
        let snap_instrument_id = u32::from_le_bytes(body[4..8].try_into().unwrap());
        let snap_anchor_seq = u64::from_le_bytes(body[8..16].try_into().unwrap());
        assert_eq!(snap_instrument_id, 0, "SnapshotBegin instrument_id");
        assert_eq!(
            snap_anchor_seq, 101,
            "SnapshotBegin anchor_seq must match the InstrumentReset new_anchor_seq",
        );

        // 9. Wire-spec invariant: per-instrument seq is NOT reset on
        //    InstrumentReset. The next delta must carry seq = 6 (not 1).
        let next = seq_counter.lock().unwrap().next(0);
        assert_eq!(
            next, 6,
            "post-recovery per-instrument seq must continue from pre-mismatch baseline (5 + 1)",
        );

        // Clean shutdown: drop senders so the emitter loop exits, then abort
        // the snapshot task (which sleeps in its round-robin path).
        drop(mkt_tx);
        let _ = tokio::time::timeout(Duration::from_secs(1), mkt_handle).await;
        snap_handle.abort();
    }
}
