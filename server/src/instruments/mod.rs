pub mod hyperliquid;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

/// Metadata for a single instrument, used to encode binary messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentInfo {
    pub instrument_id: u32,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    /// Null-padded ASCII symbol for future `InstrumentDefinition` use.
    pub symbol: [u8; 16],
}

/// A single entry in the HL universe, including delisted instruments.
///
/// Kept in `RegistryState::universe` for integrity checks across refreshes.
/// The publisher does not look at these directly — it uses `active`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseEntry {
    pub instrument_id: u32,
    pub coin: String,
    pub is_delisted: bool,
    pub info: InstrumentInfo,
}

/// The full shared state of the instrument registry.
///
/// `active` is the lookup map used by the publisher (delisted instruments excluded).
/// `universe` is the full list (including delisted) used for cross-refresh integrity checks.
/// `manifest_seq` is the current version of the active set, bumped on any change.
#[derive(Debug, Clone)]
pub struct RegistryState {
    pub active: HashMap<String, InstrumentInfo>,
    pub universe: Vec<UniverseEntry>,
    pub manifest_seq: u16,
}

impl RegistryState {
    #[must_use]
    pub fn new(universe: Vec<UniverseEntry>) -> Self {
        let active = universe.iter().filter(|e| !e.is_delisted).map(|e| (e.coin.clone(), e.info.clone())).collect();
        Self { active, universe, manifest_seq: 1 }
    }

    #[must_use]
    pub fn empty() -> Self {
        Self { active: HashMap::new(), universe: Vec::new(), manifest_seq: 1 }
    }
}

/// Handle to the shared instrument registry.
///
/// Reads are lock-free from the caller's perspective (a short read lock internally).
/// Writes (refresh) are coordinated via the publisher's refresh task.
#[derive(Debug, Clone)]
pub struct InstrumentRegistry {
    state: Arc<RwLock<RegistryState>>,
}

impl InstrumentRegistry {
    #[must_use]
    pub fn new(state: RegistryState) -> Self {
        Self { state: Arc::new(RwLock::new(state)) }
    }

    #[must_use]
    pub fn from_arc(state: Arc<RwLock<RegistryState>>) -> Self {
        Self { state }
    }

    #[must_use]
    pub fn shared(&self) -> Arc<RwLock<RegistryState>> {
        Arc::clone(&self.state)
    }

    /// Look up an instrument by coin name. Returns an owned copy so the
    /// caller doesn't need to hold the read lock.
    pub async fn get(&self, coin: &str) -> Option<InstrumentInfo> {
        self.state.read().await.active.get(coin).cloned()
    }

    pub async fn len(&self) -> usize {
        self.state.read().await.active.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.state.read().await.active.is_empty()
    }

    pub async fn manifest_seq(&self) -> u16 {
        self.state.read().await.manifest_seq
    }
}

/// Converts a decimal string price to a fixed-point `i64` using the given exponent.
///
/// Example: `"106217.0"` with exponent `-1` -> `1_062_170_i64`
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn price_to_fixed(price_str: &str, exponent: i8) -> Option<i64> {
    let value: f64 = price_str.parse().ok()?;
    let multiplier = 10f64.powi(i32::from(-exponent));
    Some((value * multiplier).round() as i64)
}

/// Converts a decimal string quantity to a fixed-point `u64` using the given exponent.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn qty_to_fixed(qty_str: &str, exponent: i8) -> Option<u64> {
    let value: f64 = qty_str.parse().ok()?;
    let multiplier = 10f64.powi(i32::from(-exponent));
    let result = (value * multiplier).round() as i64;
    if result < 0 {
        return None;
    }
    Some(result as u64)
}

/// Creates a null-padded 16-byte ASCII symbol from a string.
/// Truncates if longer than 16 bytes.
#[must_use]
pub fn make_symbol(name: &str) -> [u8; 16] {
    let mut sym = [0u8; 16];
    let bytes = name.as_bytes();
    let len = bytes.len().min(16);
    sym[..len].copy_from_slice(&bytes[..len]);
    sym
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_entry(id: u32, coin: &str, delisted: bool) -> UniverseEntry {
        UniverseEntry {
            instrument_id: id,
            coin: coin.to_string(),
            is_delisted: delisted,
            info: InstrumentInfo { instrument_id: id, price_exponent: -1, qty_exponent: -5, symbol: make_symbol(coin) },
        }
    }

    #[test]
    fn price_to_fixed_basic() {
        assert_eq!(price_to_fixed("106217.0", -1), Some(1_062_170));
    }

    #[test]
    fn price_to_fixed_two_decimals() {
        assert_eq!(price_to_fixed("106217.50", -2), Some(10_621_750));
    }

    #[test]
    fn price_to_fixed_whole_number() {
        assert_eq!(price_to_fixed("100", 0), Some(100));
    }

    #[test]
    fn qty_to_fixed_basic() {
        assert_eq!(qty_to_fixed("0.00017", -5), Some(17));
    }

    #[test]
    fn qty_to_fixed_whole() {
        assert_eq!(qty_to_fixed("1.5", -1), Some(15));
    }

    #[test]
    fn qty_to_fixed_zero() {
        assert_eq!(qty_to_fixed("0", -5), Some(0));
    }

    #[test]
    fn price_to_fixed_invalid_string() {
        assert_eq!(price_to_fixed("not_a_number", -1), None);
    }

    #[test]
    fn make_symbol_short() {
        let sym = make_symbol("BTC");
        assert_eq!(&sym[..3], b"BTC");
        assert_eq!(&sym[3..], &[0; 13]);
    }

    #[test]
    fn make_symbol_exact_16() {
        let sym = make_symbol("1234567890123456");
        assert_eq!(&sym, b"1234567890123456");
    }

    #[test]
    fn make_symbol_truncates_long() {
        let sym = make_symbol("12345678901234567890");
        assert_eq!(&sym, b"1234567890123456");
    }

    #[test]
    fn registry_state_excludes_delisted_from_active() {
        let universe = vec![test_entry(0, "BTC", false), test_entry(1, "DEADCOIN", true), test_entry(2, "ETH", false)];
        let state = RegistryState::new(universe);
        assert_eq!(state.active.len(), 2);
        assert_eq!(state.universe.len(), 3);
        assert!(state.active.contains_key("BTC"));
        assert!(!state.active.contains_key("DEADCOIN"));
        assert!(state.active.contains_key("ETH"));
        assert_eq!(state.manifest_seq, 1);
    }

    #[tokio::test]
    async fn registry_handle_lookup() {
        let universe = vec![test_entry(0, "BTC", false), test_entry(1, "ETH", false)];
        let reg = InstrumentRegistry::new(RegistryState::new(universe));
        assert!(reg.get("BTC").await.is_some());
        assert_eq!(reg.get("BTC").await.unwrap().instrument_id, 0);
        assert!(reg.get("MISSING").await.is_none());
        assert_eq!(reg.len().await, 2);
        assert_eq!(reg.manifest_seq().await, 1);
    }
}
