#![cfg(test)]
//! Shared test fixtures for unit tests in the `server` crate.
//!
//! These helpers are used by multiple unit-test modules (e.g.
//! `multicast::dob::snapshot_anchor_tests` and
//! `listeners::order_book::instrument_reset_recovery_tests`) to build
//! deterministic single-coin order-book snapshots and matching registry
//! state.  They live in a single module to avoid copy-paste drift across
//! tests.

use std::collections::HashMap;
use std::sync::Arc;

use alloy::primitives::Address;
use tokio::sync::RwLock;

use crate::instruments::{InstrumentInfo, RegistryState, UniverseEntry, make_symbol};
use crate::order_book::multi_book::Snapshots;
use crate::order_book::{Coin, OrderBook, Px, Side, Snapshot, Sz};
use crate::types::inner::InnerL4Order;

/// Build an `InnerL4Order` with deterministic, test-friendly defaults for
/// the fields we don't care about. Callers control `coin`, `oid`, `side`,
/// `px`, and `sz`.
pub(crate) fn make_order(coin: &Coin, oid: u64, side: Side, px: u64, sz: u64) -> InnerL4Order {
    InnerL4Order {
        user: Address::new([0; 20]),
        coin: coin.clone(),
        side,
        limit_px: Px::new(px),
        sz: Sz::new(sz),
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
    }
}

/// Build a one-coin `Snapshots<InnerL4Order>` with `num_orders` resting
/// orders on each side, using distinct, non-crossing prices.
///
/// `oid_offset` lets callers construct two snapshots that diverge by oid
/// (so the order-book validator's content check sees them as different),
/// while keeping price levels identical.  Pass `0` if you don't need the
/// distinction.
pub(crate) fn build_one_coin_snapshot(
    coin_str: &str,
    num_orders: usize,
    oid_offset: u64,
) -> Snapshots<InnerL4Order> {
    let coin = Coin::new(coin_str);
    let mut book: OrderBook<InnerL4Order> = OrderBook::new();
    for i in 0..num_orders {
        // Distinct, non-crossing bid/ask prices.
        book.add_order(make_order(
            &coin,
            oid_offset + 1000 + i as u64,
            Side::Bid,
            100 - i as u64,
            10,
        ));
        book.add_order(make_order(
            &coin,
            oid_offset + 2000 + i as u64,
            Side::Ask,
            200 + i as u64,
            10,
        ));
    }
    let mut map: HashMap<Coin, Snapshot<InnerL4Order>> = HashMap::new();
    map.insert(coin, book.to_snapshot());
    Snapshots::new(map)
}

/// Build an `Arc<RwLock<RegistryState>>` with a single instrument entry
/// suitable for tests that need a registry to look up one coin.
pub(crate) fn build_registry_with_one_instrument(
    coin_str: &str,
    instrument_id: u32,
) -> Arc<RwLock<RegistryState>> {
    Arc::new(RwLock::new(RegistryState::new(vec![UniverseEntry {
        instrument_id,
        coin: coin_str.to_string(),
        is_delisted: false,
        info: InstrumentInfo {
            instrument_id,
            price_exponent: -1,
            qty_exponent: -5,
            symbol: make_symbol(coin_str),
        },
    }])))
}
