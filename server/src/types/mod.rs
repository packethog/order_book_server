use std::collections::HashMap;

use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use crate::{
    order_book::types::Side,
    types::node_data::{NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
};

pub(crate) mod inner;
pub(crate) mod node_data;
pub(crate) mod subscription;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Trade {
    pub(crate) coin: String,
    pub(crate) side: Side,
    pub(crate) px: String,
    pub(crate) sz: String,
    pub(crate) hash: String,
    pub(crate) time: u64,
    pub(crate) tid: u64,
    pub(crate) users: [Address; 2],
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Level {
    px: String,
    sz: String,
    n: usize,
}

impl Level {
    pub(crate) const fn new(px: String, sz: String, n: usize) -> Self {
        Self { px, sz, n }
    }

    pub(crate) fn px(&self) -> &str {
        &self.px
    }

    pub(crate) fn sz(&self) -> &str {
        &self.sz
    }

    pub(crate) fn n(&self) -> usize {
        self.n
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct L2Book {
    coin: String,
    time: u64,
    levels: [Vec<Level>; 2],
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum L4Book {
    Snapshot { coin: String, time: u64, height: u64, levels: [Vec<L4Order>; 2] },
    Updates(L4BookUpdates),
}

impl L2Book {
    pub(crate) const fn from_l2_snapshot(coin: String, snapshot: [Vec<Level>; 2], time: u64) -> Self {
        Self { coin, time, levels: snapshot }
    }
}

#[cfg(test)]
mod trade_from_fills_tests {
    use super::*;
    use crate::order_book::types::Side;
    use crate::types::node_data::NodeDataFill;
    use alloy::primitives::Address;

    fn fill(side: Side, coin: &str, tid: u64) -> NodeDataFill {
        NodeDataFill(
            Address::new([0; 20]),
            Fill {
                coin: coin.to_string(),
                px: "100.0".to_string(),
                sz: "1.0".to_string(),
                side,
                time: 0,
                start_position: "0".to_string(),
                dir: "Open Long".to_string(),
                closed_pnl: "0".to_string(),
                hash: "0x0".to_string(),
                oid: 1,
                crossed: false,
                fee: "0".to_string(),
                tid,
                fee_token: "USDC".to_string(),
                liquidation: None,
            },
        )
    }

    #[test]
    fn from_fills_matched_pair_succeeds() {
        let mut fills = HashMap::new();
        fills.insert(Side::Ask, fill(Side::Ask, "BTC", 42));
        fills.insert(Side::Bid, fill(Side::Bid, "BTC", 42));
        assert!(Trade::from_fills(fills).is_some());
    }

    #[test]
    fn from_fills_missing_one_side_returns_none() {
        // Only an Ask present — used to panic in Trade::from_fills via
        // unwrap() on the missing Bid (issue malbeclabs/hyperliquid#4).
        let mut fills = HashMap::new();
        fills.insert(Side::Ask, fill(Side::Ask, "BTC", 42));
        assert!(Trade::from_fills(fills).is_none());

        let mut fills = HashMap::new();
        fills.insert(Side::Bid, fill(Side::Bid, "BTC", 42));
        assert!(Trade::from_fills(fills).is_none());
    }

    #[test]
    fn from_fills_coin_mismatch_returns_none_does_not_panic() {
        let mut fills = HashMap::new();
        fills.insert(Side::Ask, fill(Side::Ask, "BTC", 42));
        fills.insert(Side::Bid, fill(Side::Bid, "ETH", 42));
        assert!(Trade::from_fills(fills).is_none());
    }

    #[test]
    fn from_fills_tid_mismatch_returns_none_does_not_panic() {
        let mut fills = HashMap::new();
        fills.insert(Side::Ask, fill(Side::Ask, "BTC", 42));
        fills.insert(Side::Bid, fill(Side::Bid, "BTC", 99));
        assert!(Trade::from_fills(fills).is_none());
    }
}

impl Trade {
    /// Reconstruct a Trade from a HashMap of `Side` → `NodeDataFill`.
    /// Returns `None` if `fills` doesn't contain both an Ask and a Bid entry —
    /// e.g. when the upstream pair-up step in `coin_to_trades` happened to
    /// pop two same-side fills consecutively, so the second insert overwrote
    /// the first by key. Callers should log+skip in that case rather than
    /// crash; tearing down the publisher's tokio worker on a malformed pair
    /// silently kills the TOB multicast emit path until process restart
    /// (issue malbeclabs/hyperliquid#4).
    pub(crate) fn from_fills(mut fills: HashMap<Side, NodeDataFill>) -> Option<Self> {
        let NodeDataFill(seller, ask_fill) = fills.remove(&Side::Ask)?;
        let NodeDataFill(buyer, bid_fill) = fills.remove(&Side::Bid)?;
        let ask_is_taker = ask_fill.crossed;
        let side = if ask_is_taker { Side::Ask } else { Side::Bid };
        let coin = ask_fill.coin.clone();
        if coin != bid_fill.coin {
            log::warn!("Trade::from_fills: ask/bid coin mismatch ({} vs {}), skipping", coin, bid_fill.coin);
            return None;
        }
        let tid = ask_fill.tid;
        if tid != bid_fill.tid {
            log::warn!("Trade::from_fills: ask/bid tid mismatch ({} vs {}), skipping", tid, bid_fill.tid);
            return None;
        }
        let px = ask_fill.px;
        let sz = ask_fill.sz;
        let hash = ask_fill.hash;
        let time = ask_fill.time;
        let users = [buyer, seller];
        Some(Self { coin, side, px, sz, hash, time, tid, users })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct L4BookUpdates {
    pub time: u64,
    pub height: u64,
    pub order_statuses: Vec<NodeDataOrderStatus>,
    pub book_diffs: Vec<NodeDataOrderDiff>,
}

impl L4BookUpdates {
    pub(crate) const fn new(time: u64, height: u64) -> Self {
        Self { time, height, order_statuses: Vec::new(), book_diffs: Vec::new() }
    }
}

// RawL4Order is the version of a L4Order we want to serialize and deserialize directly
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct L4Order {
    // when serializing, this field is found outside of this struct
    // when deserializing, we move it into this struct
    pub user: Option<Address>,
    pub coin: String,
    pub side: Side,
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    pub trigger_condition: String,
    pub is_trigger: bool,
    pub trigger_px: String,
    pub is_position_tpsl: bool,
    pub reduce_only: bool,
    pub order_type: String,
    pub tif: Option<String>,
    pub cloid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum OrderDiff {
    #[serde(rename_all = "camelCase")]
    New {
        sz: String,
    },
    #[serde(rename_all = "camelCase")]
    Update {
        orig_sz: String,
        new_sz: String,
    },
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Fill {
    pub coin: String,
    pub px: String,
    pub sz: String,
    pub side: Side,
    pub time: u64,
    pub start_position: String,
    pub dir: String,
    pub closed_pnl: String,
    pub hash: String,
    pub oid: u64,
    pub crossed: bool,
    pub fee: String,
    pub tid: u64,
    pub fee_token: String,
    pub liquidation: Option<Liquidation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Liquidation {
    liquidated_user: String,
    mark_px: String,
    method: String,
}
