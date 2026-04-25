use crate::{
    order_book::{Coin, InnerOrder, Oid, OrderBook, Px, Snapshot, Sz},
    prelude::*,
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};
use tokio::fs::read_to_string;

pub(crate) struct Snapshots<O>(HashMap<Coin, Snapshot<O>>);

impl<O> Snapshots<O> {
    pub(crate) const fn new(value: HashMap<Coin, Snapshot<O>>) -> Self {
        Self(value)
    }

    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, Snapshot<O>> {
        &self.0
    }

    pub(crate) fn value(self) -> HashMap<Coin, Snapshot<O>> {
        self.0
    }
}

#[derive(Clone)]
pub(crate) struct OrderBooks<O> {
    order_books: BTreeMap<Coin, OrderBook<O>>,
}

impl<O: InnerOrder> OrderBooks<O> {
    pub(crate) const fn as_ref(&self) -> &BTreeMap<Coin, OrderBook<O>> {
        &self.order_books
    }
    #[must_use]
    pub(crate) fn from_snapshots(snapshot: Snapshots<O>, ignore_triggers: bool) -> Self {
        Self {
            order_books: snapshot
                .value()
                .into_iter()
                .map(|(coin, book)| (coin, OrderBook::from_snapshot(book, ignore_triggers)))
                .collect(),
        }
    }

    pub(crate) fn add_order(&mut self, order: O) {
        let coin = &order.coin();
        self.order_books.entry(coin.clone()).or_insert_with(OrderBook::new).add_order(order);
    }

    /// Replaces the book for a single coin with a fresh one built from the given snapshot.
    /// Used for surgical per-coin recovery when drift is detected on validation.
    pub(crate) fn replace_coin_from_snapshot(&mut self, coin: Coin, snapshot: Snapshot<O>, ignore_triggers: bool) {
        self.order_books.insert(coin, OrderBook::from_snapshot(snapshot, ignore_triggers));
    }

    /// Removes a coin's book entirely. Used when a coin exists in our state
    /// but is missing from the fresh snapshot (likely stale data).
    pub(crate) fn remove_coin(&mut self, coin: &Coin) -> Option<OrderBook<O>> {
        self.order_books.remove(coin)
    }

    pub(crate) fn cancel_order(&mut self, oid: Oid, coin: Coin) -> bool {
        self.order_books.get_mut(&coin).is_some_and(|book| book.cancel_order(oid))
    }

    /// Changes size to reflect how much gets matched during the block.
    /// Returns `Some((old_sz, px))` on success, `None` if the order is not found.
    pub(crate) fn modify_sz(&mut self, oid: Oid, coin: Coin, sz: Sz) -> Option<(Sz, Px)> {
        self.order_books.get_mut(&coin).and_then(|book| book.modify_sz(oid, sz))
    }

    /// Returns a `Vec<O>` clone of `coin`'s resting orders in deterministic
    /// iteration order, or `None` if the coin is absent.
    ///
    /// Iteration order matches `OrderBook::iter_orders`: bids first
    /// (highest price descending), then asks (lowest price ascending), with
    /// FIFO arrival order within each price level.
    ///
    /// Used by the DoB snapshot stream emitter: it calls this once per
    /// per-instrument snapshot slot to obtain a consistent point-in-time view
    /// of a coin's orders without holding the book lock for the duration of
    /// emission. Iteration order MUST be deterministic so subscriber replay
    /// tracking is reproducible.
    pub(crate) fn clone_coin_orders(&self, coin: &Coin) -> Option<Vec<O>>
    where
        O: Clone,
    {
        self.order_books.get(coin).map(|book| book.iter_orders().cloned().collect())
    }
}

impl<O: Send + Sync + InnerOrder> OrderBooks<O> {
    #[must_use]
    pub(crate) fn to_snapshots_par(&self) -> Snapshots<O> {
        let snapshots = self.order_books.par_iter().map(|(c, book)| (c.clone(), book.to_snapshot())).collect();
        Snapshots(snapshots)
    }
}

pub(crate) fn load_snapshots_from_str<O, R>(str: &str) -> Result<(u64, Snapshots<O>)>
where
    O: TryFrom<R, Error = Error>,
    R: Serialize + for<'a> Deserialize<'a>,
{
    #[allow(clippy::type_complexity)]
    let (height, snapshot): (u64, Vec<(String, [Vec<R>; 2])>) = serde_json::from_str(str)?;
    Ok((
        height,
        Snapshots::new(
            snapshot
                .into_iter()
                .map(|(coin, [bids, asks])| {
                    let bids: Vec<O> = bids.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
                    let asks: Vec<O> = asks.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
                    Ok((Coin::new(&coin), Snapshot([bids, asks])))
                })
                .collect::<Result<HashMap<Coin, Snapshot<O>>>>()?,
        ),
    ))
}

pub(crate) async fn load_snapshots_from_json<O, R>(path: &Path) -> Result<(u64, Snapshots<O>)>
where
    O: TryFrom<R, Error = Error>,
    R: Serialize + for<'a> Deserialize<'a>,
{
    let file_contents = read_to_string(path).await?;
    load_snapshots_from_str(&file_contents)
}

#[cfg(test)]
mod tests {
    use crate::{
        order_book::{
            InnerOrder, OrderBook, Px, Side, Snapshot, Sz,
            levels::build_l2_level,
            multi_book::{Coin, OrderBooks, Snapshots, load_snapshots_from_json, load_snapshots_from_str},
        },
        prelude::*,
        types::{
            L4Order, Level,
            inner::{InnerL4Order, InnerLevel},
        },
    };
    use alloy::primitives::Address;
    use itertools::Itertools;
    use std::{collections::HashMap, fs::create_dir_all, path::PathBuf};

    #[must_use]
    fn snapshot_to_l2_snapshot<O: InnerOrder>(
        snapshot: &Snapshot<O>,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Snapshot<InnerLevel> {
        let [bids, asks] = &snapshot.0;
        let bids = orders_to_l2_levels(bids, Side::Bid, n_levels, n_sig_figs, mantissa);
        let asks = orders_to_l2_levels(asks, Side::Ask, n_levels, n_sig_figs, mantissa);
        Snapshot([bids, asks])
    }

    #[must_use]
    fn orders_to_l2_levels<O: InnerOrder>(
        orders: &[O],
        side: Side,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Vec<InnerLevel> {
        let mut levels = Vec::new();
        if n_levels == Some(0) {
            return levels;
        }
        let mut cur_level: Option<InnerLevel> = None;

        for order in orders {
            if build_l2_level(
                &mut cur_level,
                &mut levels,
                n_levels,
                n_sig_figs,
                mantissa,
                side,
                InnerLevel { px: order.limit_px(), sz: order.sz(), n: 1 },
            ) {
                break;
            }
        }
        levels.extend(cur_level.take());
        levels
    }

    #[derive(Default)]
    struct OrderManager {
        next_oid: u64,
    }

    fn simple_inner_order(oid: u64, side: Side, sz: String, px: String) -> Result<InnerL4Order> {
        let px = Px::parse_from_str(&px)?;
        let sz = Sz::parse_from_str(&sz)?;
        Ok(InnerL4Order {
            user: Address::new([0; 20]),
            coin: Coin::new(""),
            side,
            limit_px: px,
            sz,
            oid,
            timestamp: 0,
            trigger_condition: String::new(),
            is_trigger: false,
            trigger_px: String::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: String::new(),
            tif: None,
            cloid: None,
        })
    }

    impl OrderManager {
        fn order(&mut self, sz: &str, limit_px: &str, side: Side) -> Result<InnerL4Order> {
            let order = simple_inner_order(self.next_oid, side, sz.to_string(), limit_px.to_string())?;
            self.next_oid += 1;
            Ok(order)
        }

        fn batch_order(&mut self, sz: &str, limit_px: &str, side: Side, mult: u64) -> Result<Vec<InnerL4Order>> {
            (0..mult).map(|_| self.order(sz, limit_px, side)).try_collect()
        }
    }

    fn setup_book(book: &mut OrderBook<InnerL4Order>) -> Snapshots<InnerL4Order> {
        let mut o = OrderManager::default();
        let buy_orders1 = o.batch_order("100", "34.01", Side::Bid, 4).unwrap();
        let buy_orders2 = o.batch_order("200", "34.5", Side::Bid, 2).unwrap();
        let buy_orders3 = o.batch_order("300", "34.6", Side::Bid, 1).unwrap();
        let sell_orders1 = o.batch_order("100", "35", Side::Ask, 4).unwrap();
        let sell_orders2 = o.batch_order("200", "35.1", Side::Ask, 2).unwrap();
        let sell_orders3 = o.batch_order("300", "35.5", Side::Ask, 1).unwrap();
        for orders in [buy_orders1, buy_orders2, buy_orders3, sell_orders1, sell_orders2, sell_orders3] {
            for o in orders {
                book.add_order(o);
            }
        }
        Snapshots(vec![(Coin::new(""), book.to_snapshot()); 2].into_iter().collect())
    }

    const SNAPSHOT_JSON: &str = r#"[100, 
    [
        [
            "@1",
            [
                [
                    [
                        "0x0000000000000000000000000000000000000000",
                        {
                            "coin": "@1",
                            "side": "B",
                            "limitPx": "30.444",
                            "sz": "100.0",
                            "oid": 105338503859,
                            "timestamp": 1750660644034,
                            "triggerCondition": "N/A",
                            "isTrigger": false,
                            "triggerPx": "0.0",
                            "children": [],
                            "isPositionTpsl": false,
                            "reduceOnly": false,
                            "orderType": "Limit",
                            "origSz": "100.0",
                            "tif": "Alo",
                            "cloid": null
                        }
                    ],
                    [
                        "0x0000000000000000000000000000000000000000",
                        {
                            "coin": "@1",
                            "side": "B",
                            "limitPx": "30.385",
                            "sz": "5.45",
                            "oid": 105337808436,
                            "timestamp": 1750660453608,
                            "triggerCondition": "N/A",
                            "isTrigger": false,
                            "triggerPx": "0.0",
                            "children": [],
                            "isPositionTpsl": false,
                            "reduceOnly": false,
                            "orderType": "Limit",
                            "origSz": "5.45",
                            "tif": "Gtc",
                            "cloid": null
                        }
                    ]
                ],
                []
            ]
        ]
    ]
]"#;

    #[tokio::test]
    async fn test_deserialization_from_json() -> Result<()> {
        create_dir_all("tmp/deserialization_test")?;
        fs::write("tmp/deserialization_test/out.json", SNAPSHOT_JSON)?;
        load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&PathBuf::from(
            "tmp/deserialization_test/out.json",
        ))
        .await?;
        Ok(())
    }

    #[test]
    fn test_deserialization() -> Result<()> {
        load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(SNAPSHOT_JSON)?;
        Ok(())
    }

    #[test]
    fn test_l4_snapshot_to_l2_snapshot() {
        let mut book = OrderBook::new();
        let coin = Coin::new("");
        let snapshot = setup_book(&mut book);
        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), Some(2), Some(1));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![Level::new("34".to_string(), "1100".to_string(), 7)],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("36".to_string(), "700".to_string(), 3),
            ],
        ];
        assert_eq!(ans, raw_levels);

        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), Some(3), Some(5));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![
                Level::new("34.5".to_string(), "700".to_string(), 3),
                Level::new("34".to_string(), "400".to_string(), 4),
            ],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("35.5".to_string(), "700".to_string(), 3),
            ],
        ];
        assert_eq!(ans, raw_levels);
        let snapshot_from_book = book.to_l2_snapshot(Some(2), Some(3), Some(5));
        let raw_levels_from_book = snapshot_from_book.export_inner_snapshot();
        let snapshot_from_book = book.to_l2_snapshot(None, None, None);
        let snapshot_from_snapshot = snapshot_from_book.to_l2_snapshot(Some(2), Some(3), Some(5));
        let raw_levels_from_snapshot = snapshot_from_snapshot.export_inner_snapshot();
        assert_eq!(raw_levels_from_book, ans);
        assert_eq!(raw_levels_from_snapshot, ans);

        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), None, Some(5));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![
                Level::new("34.6".to_string(), "300".to_string(), 1),
                Level::new("34.5".to_string(), "400".to_string(), 2),
            ],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("35.1".to_string(), "400".to_string(), 2),
            ],
        ];
        assert_eq!(ans, raw_levels);
    }

    #[test]
    fn clone_coin_orders_returns_resting_orders_for_known_coin() {
        let mut book = OrderBook::new();
        let coin = Coin::new("");
        let snapshot = setup_book(&mut book);
        let books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(snapshot, false);

        let orders = books.clone_coin_orders(&coin).expect("coin present");
        assert!(!orders.is_empty(), "expected at least one resting order");

        // Calling again returns the same content — no mutation on read.
        let orders2 = books.clone_coin_orders(&coin).expect("coin still present");
        assert_eq!(orders.len(), orders2.len());

        // Deterministic order: the two clones should match element-wise on stable fields.
        for (a, b) in orders.iter().zip(orders2.iter()) {
            assert_eq!(a.oid(), b.oid());
            assert_eq!(a.limit_px(), b.limit_px());
            assert_eq!(a.sz(), b.sz());
            assert_eq!(a.side(), b.side());
        }

        // Iteration order: bid side first then ask side, bids descending in price,
        // asks ascending in price, FIFO within each level.
        let bid_count = orders.iter().take_while(|o| o.side() == Side::Bid).count();
        assert!(bid_count > 0, "expected at least one bid in the fixture");
        assert!(bid_count < orders.len(), "expected at least one ask in the fixture");

        let bids = &orders[..bid_count];
        let asks = &orders[bid_count..];
        // All bids appear before any ask — verify the partitioning is clean.
        assert!(bids.iter().all(|o| o.side() == Side::Bid));
        assert!(asks.iter().all(|o| o.side() == Side::Ask));

        // Bids are non-increasing in price, asks are non-decreasing.
        for w in bids.windows(2) {
            assert!(w[0].limit_px() >= w[1].limit_px(), "bids should be price-descending");
        }
        for w in asks.windows(2) {
            assert!(w[0].limit_px() <= w[1].limit_px(), "asks should be price-ascending");
        }

        // Within a single price level, oids should be in arrival order (monotonically
        // increasing under the OrderManager fixture which assigns oids sequentially).
        for window_side in [bids, asks] {
            let mut prev: Option<(Px, u64)> = None;
            for order in window_side {
                let px = order.limit_px();
                let oid = order.oid().into_inner();
                if let Some((prev_px, prev_oid)) = prev {
                    if prev_px == px {
                        assert!(oid > prev_oid, "FIFO order broken within price level");
                    }
                }
                prev = Some((px, oid));
            }
        }
    }

    #[test]
    fn clone_coin_orders_returns_none_for_unknown_coin() {
        let books: OrderBooks<InnerL4Order> =
            OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), false);
        assert!(books.clone_coin_orders(&Coin::new("NOPE")).is_none());
    }
}
