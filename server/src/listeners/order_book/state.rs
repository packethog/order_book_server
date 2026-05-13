use std::collections::{HashMap, HashSet, VecDeque};

use crate::{
    listeners::order_book::{L2Snapshots, TimedSnapshots, dob_tap::DobApplyTap, utils::compute_l2_snapshots},
    order_book::{
        Coin, InnerOrder, Oid,
        multi_book::{OrderBooks, Snapshots},
    },
    prelude::*,
    types::{
        inner::{InnerL4Order, InnerOrderDiff},
        node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};

pub(super) struct OrderBookState {
    order_book: OrderBooks<InnerL4Order>,
    height: u64,
    time: u64,
    snapped: bool,
    ignore_spot: bool,
    /// Present when the DoB emitter is wired in. The tap is NOT propagated to
    /// the cloned copy used for snapshot validation (validation reads only; no
    /// events should be emitted from it).
    dob_tap: Option<DobApplyTap>,
}

fn resting_order_from_raw_new(
    order: NodeDataOrderStatus,
    diff: &NodeDataOrderDiff,
    resting_sz: crate::order_book::Sz,
) -> Result<InnerL4Order> {
    let time = order.time.and_utc().timestamp_millis();
    let mut inner_order: InnerL4Order = order.try_into()?;
    inner_order.limit_px = diff.px()?;
    inner_order.modify_sz(resting_sz);
    #[allow(clippy::unwrap_used)]
    inner_order.convert_trigger(time.try_into().unwrap());
    if inner_order.tif.as_deref() == Some("Ioc") {
        inner_order.tif = Some("Gtc".to_string());
    }
    Ok(inner_order)
}

impl Clone for OrderBookState {
    fn clone(&self) -> Self {
        Self {
            order_book: self.order_book.clone(),
            height: self.height,
            time: self.time,
            snapped: self.snapped,
            ignore_spot: self.ignore_spot,
            // The tap is intentionally not cloned: the clone is used only for
            // snapshot validation and must not emit DoB events.
            dob_tap: None,
        }
    }
}

impl OrderBookState {
    pub(super) fn from_snapshot(
        snapshot: Snapshots<InnerL4Order>,
        height: u64,
        time: u64,
        ignore_triggers: bool,
        ignore_spot: bool,
    ) -> Self {
        Self {
            ignore_spot,
            time,
            height,
            order_book: OrderBooks::from_snapshots(snapshot, ignore_triggers),
            snapped: false,
            dob_tap: None,
        }
    }

    /// Attaches a `DobApplyTap` that will receive events on every successful apply.
    pub(super) fn attach_dob_tap(&mut self, tap: DobApplyTap) {
        self.dob_tap = Some(tap);
    }

    pub(super) const fn height(&self) -> u64 {
        self.height
    }

    // forcibly take snapshot - (time, height, snapshot)
    pub(super) fn compute_snapshot(&self) -> TimedSnapshots {
        TimedSnapshots { time: self.time, height: self.height, snapshot: self.order_book.to_snapshots_par() }
    }

    // (time, snapshot)
    pub(super) fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        if self.snapped {
            None
        } else {
            self.snapped = prevent_future_snaps || self.snapped;
            Some((self.time, compute_l2_snapshots(&self.order_book)))
        }
    }

    #[cfg(test)]
    pub(super) fn compute_l2_snapshots_for_test(&self) -> (u64, L2Snapshots) {
        (self.time, compute_l2_snapshots(&self.order_book))
    }

    pub(super) fn compute_universe(&self) -> HashSet<Coin> {
        self.order_book.as_ref().keys().cloned().collect()
    }

    /// Returns a deterministic clone of the resting orders for `coin`, or
    /// `None` if the coin is not present. Used by the DoB snapshot emitter
    /// to take a brief, lock-held copy that it can then drain into wire
    /// messages without keeping the listener mutex.
    pub(super) fn clone_coin_orders(&self, coin: &Coin) -> Option<Vec<InnerL4Order>> {
        self.order_book.clone_coin_orders(coin)
    }

    /// Replaces a single coin's book with a fresh one from the given snapshot.
    /// Used to recover from per-coin drift without resetting all other coins.
    /// Also clears the `snapped` flag so the next tick will emit a fresh L2 snapshot
    /// carrying the corrected state downstream.
    pub(super) fn replace_coin_from_snapshot(
        &mut self,
        coin: Coin,
        snapshot: crate::order_book::Snapshot<InnerL4Order>,
        ignore_triggers: bool,
    ) {
        self.order_book.replace_coin_from_snapshot(coin, snapshot, ignore_triggers);
        self.snapped = false;
    }

    /// Removes a coin's book entirely. Used when a coin is in our state but missing
    /// from the fresh snapshot. Also clears the `snapped` flag.
    pub(super) fn remove_coin(&mut self, coin: &Coin) {
        self.order_book.remove_coin(coin);
        self.snapped = false;
    }

    pub(super) fn apply_updates(
        &mut self,
        order_statuses: Batch<NodeDataOrderStatus>,
        order_diffs: Batch<NodeDataOrderDiff>,
    ) -> Result<()> {
        let height = order_statuses.block_number();
        // block_time() returns milliseconds; multiply by 1_000_000 for nanoseconds
        let time = order_statuses.block_time();
        let time_ns = time * 1_000_000;
        assert_eq!(order_statuses.block_number(), order_diffs.block_number());
        if height > self.height + 1 {
            return Err(format!("Expecting block {}, got block {}", self.height + 1, height).into());
        } else if height <= self.height {
            // This is not an error in case we started caching long before a snapshot is fetched
            return Ok(());
        }
        let mut diffs = order_diffs.events().into_iter().collect::<VecDeque<_>>();
        let mut order_map = order_statuses
            .events()
            .into_iter()
            .filter_map(|order_status| {
                if order_status.is_opening_status_for_raw_diff() {
                    Some((Oid::new(order_status.order.oid), order_status))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();

        // Count how many diffs will be emitted as DoB events (same filter as the loop).
        // If ≥ 2, wrap the block with BatchBoundary open/close.
        let emittable_count = if self.dob_tap.is_some() {
            diffs.iter().filter(|d| !(d.coin().is_spot() && self.ignore_spot)).count()
        } else {
            0
        };
        if emittable_count >= 2 {
            if let Some(tap) = self.dob_tap.as_mut() {
                tap.emit_batch_boundary(0 /* open */, height, time_ns);
            }
        }

        while let Some(diff) = diffs.pop_front() {
            let oid = diff.oid();
            let coin = diff.coin();
            if coin.is_spot() && self.ignore_spot {
                continue;
            }
            let inner_diff = diff.diff().try_into()?;
            match inner_diff {
                InnerOrderDiff::New { sz } => {
                    if let Some(order) = order_map.remove(&oid) {
                        let inner_order = resting_order_from_raw_new(order, &diff, sz)?;
                        let order_for_tap = inner_order.clone();
                        self.order_book.add_resting_order_from_diff(inner_order);
                        if !self.order_book.contains_order(&oid, &coin) {
                            log::warn!(
                                "apply_updates: New order did not rest after raw-diff insert, later updates will be missing; \
                                 height={height} oid={oid:?} coin={coin:?} order={order_for_tap:?}"
                            );
                        }
                        if let Some(tap) = self.dob_tap.as_mut() {
                            tap.emit_order_add(&coin, &order_for_tap, time_ns);
                        }
                    } else {
                        // Soft-tolerance: New diff without matching opening status.
                        // We can't add the order without the user/time/cloid info from
                        // the status, so skip and let snapshot validation reconcile.
                        // Same shape as the Update/Remove missing-order branches below.
                        log::warn!("apply_updates: New diff without matching opening status, skipping {diff:?}");
                    }
                }
                InnerOrderDiff::Update { new_sz, .. } => {
                    match self.order_book.modify_sz(oid.clone(), coin.clone(), new_sz) {
                        Some((old_sz, px)) => {
                            if let Some(tap) = self.dob_tap.as_mut() {
                                // exec_quantity = reduction in resting size
                                let exec_quantity =
                                    crate::order_book::Sz::new(old_sz.value().saturating_sub(new_sz.value()));
                                tap.emit_order_execute(&coin, oid, px, exec_quantity, time_ns);
                            }
                        }
                        None => {
                            // Soft-tolerance: the order isn't on our book, but the venue
                            // has an Update for it. Snapshot validation runs every 60s
                            // and applies surgical recovery on divergence; missing this
                            // event won't permanently corrupt state. Crashing here would
                            // turn what's likely a transient ordering race into a hard
                            // failure cycle.
                            log::warn!("apply_updates: Update for missing order at height {height}, skipping {diff:?}");
                        }
                    }
                }
                InnerOrderDiff::Remove => {
                    if self.order_book.cancel_order(oid.clone(), coin.clone()) {
                        if let Some(tap) = self.dob_tap.as_mut() {
                            tap.emit_order_cancel(&coin, oid, time_ns);
                        }
                    } else {
                        // Soft-tolerance — see Update branch above.
                        log::warn!("apply_updates: Remove for missing order at height {height}, skipping {diff:?}");
                    }
                }
            }
        }

        if emittable_count >= 2 {
            if let Some(tap) = self.dob_tap.as_mut() {
                tap.emit_batch_boundary(1 /* close */, height, time_ns);
            }
        }

        self.height += 1;
        self.time = time;
        self.snapped = false;
        Ok(())
    }

    pub(super) fn emit_batch_boundary(&mut self, phase: u8, block_number: u64, block_time_ms: u64) {
        if let Some(tap) = self.dob_tap.as_mut() {
            tap.emit_batch_boundary(phase, block_number, block_time_ms * 1_000_000);
        }
    }

    pub(super) fn apply_stream_diff(
        &mut self,
        block_number: u64,
        block_time_ms: u64,
        diff: NodeDataOrderDiff,
        order_status: Option<NodeDataOrderStatus>,
    ) -> Result<bool> {
        if block_number < self.height {
            return Err(
                format!("Received finalized streaming block {}, current height {}", block_number, self.height).into()
            );
        }

        let time_ns = block_time_ms * 1_000_000;
        let oid = diff.oid();
        let coin = diff.coin();
        if coin.is_spot() && self.ignore_spot {
            self.height = self.height.max(block_number);
            self.time = block_time_ms;
            return Ok(false);
        }

        let inner_diff = diff.diff().try_into()?;
        match inner_diff {
            InnerOrderDiff::New { sz } => {
                let Some(order) = order_status else {
                    // Soft-tolerance: New diff without matching opening status.
                    // Snapshot validation will reconcile. Advance height anyway so
                    // we don't replay this diff and so subsequent diffs apply.
                    log::warn!("apply_stream_diff: New diff without matching opening status, skipping {diff:?}");
                    self.height = self.height.max(block_number);
                    self.time = block_time_ms;
                    self.snapped = false;
                    return Ok(false);
                };
                let inner_order = resting_order_from_raw_new(order, &diff, sz)?;
                let order_for_tap = inner_order.clone();
                self.order_book.add_resting_order_from_diff(inner_order);
                if !self.order_book.contains_order(&oid, &coin) {
                    log::warn!(
                        "apply_stream_diff: New order did not rest after raw-diff insert, later updates will be missing; \
                         block_number={block_number} oid={oid:?} coin={coin:?} order={order_for_tap:?}"
                    );
                }
                if let Some(tap) = self.dob_tap.as_mut() {
                    tap.emit_order_add(&coin, &order_for_tap, time_ns);
                }
            }
            InnerOrderDiff::Update { new_sz, .. } => match self.order_book.modify_sz(oid.clone(), coin.clone(), new_sz)
            {
                Some((old_sz, px)) => {
                    if let Some(tap) = self.dob_tap.as_mut() {
                        let exec_quantity = crate::order_book::Sz::new(old_sz.value().saturating_sub(new_sz.value()));
                        tap.emit_order_execute(&coin, oid, px, exec_quantity, time_ns);
                    }
                }
                None => {
                    // Soft-tolerance: see apply_updates' matching branch.
                    log::warn!(
                        "apply_stream_diff: Update for missing order at block {block_number}, skipping {diff:?}"
                    );
                }
            },
            InnerOrderDiff::Remove => {
                if self.order_book.cancel_order(oid.clone(), coin.clone()) {
                    if let Some(tap) = self.dob_tap.as_mut() {
                        tap.emit_order_cancel(&coin, oid, time_ns);
                    }
                } else {
                    // Soft-tolerance — see apply_updates.
                    log::warn!(
                        "apply_stream_diff: Remove for missing order at block {block_number}, skipping {diff:?}"
                    );
                }
            }
        }

        self.height = self.height.max(block_number);
        self.time = block_time_ms;
        self.snapped = false;
        Ok(true)
    }
}
