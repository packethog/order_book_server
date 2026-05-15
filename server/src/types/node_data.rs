use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use alloy::primitives::Address;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::{
    order_book::{Coin, Oid, Px},
    types::{Fill, L4Order, OrderDiff},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderDiff {
    user: Address,
    oid: u64,
    px: String,
    coin: String,
    pub(crate) raw_book_diff: OrderDiff,
}

impl NodeDataOrderDiff {
    pub(crate) fn diff(&self) -> OrderDiff {
        self.raw_book_diff.clone()
    }
    pub(crate) const fn oid(&self) -> Oid {
        Oid::new(self.oid)
    }

    pub(crate) fn coin(&self) -> Coin {
        Coin::new(&self.coin)
    }

    pub(crate) fn px(&self) -> crate::prelude::Result<Px> {
        Px::parse_from_str(&self.px)
    }

    /// Test-only constructor.  Lets parity tests build a synthetic diff
    /// without round-tripping through serde.
    #[cfg(test)]
    #[allow(clippy::missing_const_for_fn)]
    pub(crate) fn new_for_test(user: Address, oid: u64, px: String, coin: String, raw_book_diff: OrderDiff) -> Self {
        Self { user, oid, px, coin, raw_book_diff }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataFill(pub Address, pub Fill);

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderStatus {
    pub time: NaiveDateTime,
    pub user: Address,
    pub status: String,
    pub order: L4Order,
}

impl NodeDataOrderStatus {
    pub(crate) fn is_opening_status_for_raw_diff(&self) -> bool {
        (self.status == "open" && !self.order.is_trigger) || (self.order.is_trigger && self.status == "triggered")
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display)]
#[strum(serialize_all = "snake_case")]
pub enum IngestMode {
    Block,
    Stream,
}

impl FromStr for IngestMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "block" => Ok(Self::Block),
            "stream" => Ok(Self::Stream),
            other => Err(format!("invalid ingest mode {other:?}; expected block or stream")),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display)]
pub(crate) enum EventSource {
    Fills,
    OrderStatuses,
    OrderDiffs,
}

impl EventSource {
    #[cfg(test)]
    #[must_use]
    pub(crate) fn event_source_dir(self, dir: &Path) -> PathBuf {
        match self {
            Self::Fills => dir.join("hl/data/node_fills_by_block"),
            Self::OrderStatuses => dir.join("hl/data/node_order_statuses_by_block"),
            Self::OrderDiffs => dir.join("hl/data/node_raw_book_diffs_by_block"),
        }
    }

    #[must_use]
    pub(crate) fn event_source_dir_for(self, hl_data_root: &Path, ingest_mode: IngestMode) -> PathBuf {
        match self {
            Self::Fills => match ingest_mode {
                IngestMode::Block => hl_data_root.join("node_fills_by_block"),
                IngestMode::Stream => hl_data_root.join("node_fills_streaming"),
            },
            Self::OrderStatuses => match ingest_mode {
                IngestMode::Block => hl_data_root.join("node_order_statuses_by_block"),
                IngestMode::Stream => hl_data_root.join("node_order_statuses_streaming"),
            },
            Self::OrderDiffs => match ingest_mode {
                IngestMode::Block => hl_data_root.join("node_raw_book_diffs_by_block"),
                IngestMode::Stream => hl_data_root.join("node_raw_book_diffs_streaming"),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Batch<E> {
    local_time: NaiveDateTime,
    block_time: NaiveDateTime,
    block_number: u64,
    events: Vec<E>,
}

impl<E> Batch<E> {
    #[allow(clippy::unwrap_used)]
    pub(crate) fn block_time(&self) -> u64 {
        self.block_time.and_utc().timestamp_millis().try_into().unwrap()
    }

    pub(crate) const fn block_number(&self) -> u64 {
        self.block_number
    }

    #[allow(clippy::unwrap_used)]
    pub(crate) fn local_time_ms(&self) -> u64 {
        self.local_time.and_utc().timestamp_millis().try_into().unwrap()
    }

    pub(crate) fn events(self) -> Vec<E> {
        self.events
    }

    pub(crate) fn events_ref(&self) -> &[E] {
        &self.events
    }

    pub(crate) fn with_events<T>(&self, events: Vec<T>) -> Batch<T> {
        Batch { local_time: self.local_time, block_time: self.block_time, block_number: self.block_number, events }
    }

    /// Test-only constructor.  Lets parity / synthetic-driver tests build a
    /// `Batch<E>` directly without round-tripping through serde.  The
    /// `block_time_ms` is interpreted as UTC milliseconds since epoch and
    /// stored as the `block_time` `NaiveDateTime`; `local_time` mirrors it.
    #[cfg(test)]
    #[allow(
        clippy::unwrap_used,
        clippy::cast_possible_wrap,
        clippy::cast_possible_truncation,
        clippy::missing_const_for_fn
    )]
    pub(crate) fn new_for_test(block_number: u64, block_time_ms: u64, events: Vec<E>) -> Self {
        let secs = (block_time_ms / 1_000) as i64;
        let nsecs = ((block_time_ms % 1_000) * 1_000_000) as u32;
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs).unwrap().naive_utc();
        Self { local_time: dt, block_time: dt, block_number, events }
    }
}
