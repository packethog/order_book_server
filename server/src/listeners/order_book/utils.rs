use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::{
        Snapshot,
        multi_book::{OrderBooks, Snapshots},
        types::InnerOrder,
    },
    prelude::*,
    types::{
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reqwest::Client;
use serde_json::json;
use std::collections::VecDeque;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

pub(super) async fn process_rmp_file(dir: &Path) -> Result<PathBuf> {
    let output_path = dir.join("out.json");
    let payload = json!({
        "type": "fileSnapshot",
        "request": {
            "type": "l4Snapshots",
            "includeUsers": true,
            "includeTriggerOrders": false
        },
        "outPath": output_path,
        "includeHeightInOutput": true
    });

    let client = Client::new();
    client
        .post("http://localhost:3001/info")
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;
    Ok(output_path)
}

/// Validates that our internally-reconstructed order book (`internal`) matches
/// the fresh snapshot pulled from the node (`fresh`).
///
/// The two are walked level-by-level and order-by-order. On mismatch, a rich
/// diagnostic error is returned with enough context to characterize the
/// divergence (length mismatch vs. order content mismatch, position within the
/// level, and a window of surrounding orders on both sides).
pub(super) fn validate_snapshot_consistency<O: Clone + PartialEq + Debug>(
    internal: &Snapshots<O>,
    fresh: Snapshots<O>,
    ignore_spot: bool,
) -> Result<()> {
    let mut fresh_map: HashMap<_, _> =
        fresh.value().into_iter().filter(|(c, _)| !c.is_spot() || !ignore_spot).collect();

    for (coin, book) in internal.as_ref() {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let internal_book = book.as_ref();
        if let Some(fresh_book) = fresh_map.remove(coin) {
            let internal_sides = internal_book.as_ref();
            let fresh_sides = fresh_book.as_ref();

            for (side_idx, (internal_orders, fresh_orders)) in internal_sides.iter().zip(fresh_sides.iter()).enumerate()
            {
                let side_name = if side_idx == 0 { "bid" } else { "ask" };

                // Length divergence is usually the clearest symptom of a
                // missed or phantom event. Report it before walking orders.
                if internal_orders.len() != fresh_orders.len() {
                    return Err(
                        format_length_divergence(&coin.value(), side_name, internal_orders, fresh_orders).into()
                    );
                }

                // Lengths agree — find the first position where content diverges.
                for (pos, (internal_order, fresh_order)) in internal_orders.iter().zip(fresh_orders.iter()).enumerate()
                {
                    if *internal_order != *fresh_order {
                        return Err(format_order_divergence(
                            &coin.value(),
                            side_name,
                            pos,
                            internal_orders,
                            fresh_orders,
                        )
                        .into());
                    }
                }
            }
        } else if !internal_book[0].is_empty() || !internal_book[1].is_empty() {
            return Err(format!("Missing {} book in fresh snapshot", coin.value()).into());
        }
    }
    if !fresh_map.is_empty() {
        let extra: Vec<String> = fresh_map.keys().map(|c| c.value()).collect();
        return Err(format!("Extra orderbooks in fresh snapshot: {extra:?}").into());
    }
    Ok(())
}

/// Builds a multi-line diagnostic for a length mismatch on a single side.
fn format_length_divergence<O: Debug>(coin: &str, side: &str, internal_orders: &[O], fresh_orders: &[O]) -> String {
    let internal_len = internal_orders.len();
    let fresh_len = fresh_orders.len();
    let common = internal_len.min(fresh_len);

    // Find where they actually start to differ (they may match for a while).
    let first_diff = internal_orders
        .iter()
        .zip(fresh_orders.iter())
        .position(|(a, b)| format!("{a:?}") != format!("{b:?}"))
        .unwrap_or(common);

    format!(
        "level length mismatch on coin={coin} side={side}:\n  \
         internal has {internal_len} orders, fresh snapshot has {fresh_len} orders \
         (diff: {signed:+})\n  \
         first content divergence at position {first_diff} of {common} shared positions\n  \
         internal[{lo}..{hi}]: {internal_slice:#?}\n  \
         fresh[{lo}..{hi}]:    {fresh_slice:#?}\n  \
         likely cause: missed or duplicated event for this coin/side",
        signed = internal_len as isize - fresh_len as isize,
        lo = first_diff.saturating_sub(2),
        hi = (first_diff + 3).min(common.max(internal_len.max(fresh_len))),
        internal_slice = internal_orders.iter().skip(first_diff.saturating_sub(2)).take(5).collect::<Vec<_>>(),
        fresh_slice = fresh_orders.iter().skip(first_diff.saturating_sub(2)).take(5).collect::<Vec<_>>(),
    )
}

/// Builds a multi-line diagnostic for a same-length book where an order at
/// some position differs between internal and fresh state.
fn format_order_divergence<O: Debug>(
    coin: &str,
    side: &str,
    pos: usize,
    internal_orders: &[O],
    fresh_orders: &[O],
) -> String {
    let lo = pos.saturating_sub(2);
    let hi = (pos + 3).min(internal_orders.len());
    format!(
        "order content mismatch on coin={coin} side={side} position={pos}:\n  \
         level size: {level_size} orders (lengths match on both sides)\n  \
         internal[{pos}]: {internal_order:#?}\n  \
         fresh[{pos}]:    {fresh_order:#?}\n  \
         internal[{lo}..{hi}]: {internal_window:#?}\n  \
         fresh[{lo}..{hi}]:    {fresh_window:#?}\n  \
         likely cause: bad order reconstruction (trigger order conversion, update event misapplied, etc.)",
        level_size = internal_orders.len(),
        internal_order = &internal_orders[pos],
        fresh_order = &fresh_orders[pos],
        internal_window = internal_orders[lo..hi].iter().collect::<Vec<_>>(),
        fresh_window = fresh_orders[lo..hi].iter().collect::<Vec<_>>(),
    )
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

pub(super) fn compute_l2_snapshots<O: InnerOrder + Send + Sync>(order_books: &OrderBooks<O>) -> L2Snapshots {
    L2Snapshots(
        order_books
            .as_ref()
            .par_iter()
            .map(|(coin, order_book)| {
                let mut entries = Vec::new();
                let snapshot = order_book.to_l2_snapshot(None, None, None);
                entries.push((L2SnapshotParams { n_sig_figs: None, mantissa: None }, snapshot));
                let mut add_new_snapshot = |n_sig_figs: Option<u32>, mantissa: Option<u64>, idx: usize| {
                    if let Some((_, last_snapshot)) = &entries.get(entries.len() - idx) {
                        let snapshot = last_snapshot.to_l2_snapshot(None, n_sig_figs, mantissa);
                        entries.push((L2SnapshotParams { n_sig_figs, mantissa }, snapshot));
                    }
                };
                for n_sig_figs in (2..=5).rev() {
                    if n_sig_figs == 5 {
                        for mantissa in [None, Some(2), Some(5)] {
                            if mantissa == Some(5) {
                                // Some(2) is NOT a superset of this info!
                                add_new_snapshot(Some(n_sig_figs), mantissa, 2);
                            } else {
                                add_new_snapshot(Some(n_sig_figs), mantissa, 1);
                            }
                        }
                    } else {
                        add_new_snapshot(Some(n_sig_figs), None, 1);
                    }
                }
                (coin.clone(), entries.into_iter().collect::<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>())
            })
            .collect(),
    )
}

pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

pub(super) struct BatchQueue<T> {
    deque: VecDeque<Batch<T>>,
    last_ts: Option<u64>,
}

impl<T> BatchQueue<T> {
    pub(super) const fn new() -> Self {
        Self { deque: VecDeque::new(), last_ts: None }
    }

    pub(super) fn push(&mut self, block: Batch<T>) -> bool {
        if let Some(last_ts) = self.last_ts {
            if last_ts >= block.block_number() {
                return false;
            }
        }
        self.last_ts = Some(block.block_number());
        self.deque.push_back(block);
        true
    }

    pub(super) fn pop_front(&mut self) -> Option<Batch<T>> {
        self.deque.pop_front()
    }

    pub(super) fn front(&self) -> Option<&Batch<T>> {
        self.deque.front()
    }
}
