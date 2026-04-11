pub mod hyperliquid;

use std::collections::HashMap;

/// Metadata for a single instrument, used to encode binary messages.
#[derive(Debug, Clone)]
pub struct InstrumentInfo {
    pub instrument_id: u32,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    /// Null-padded ASCII symbol for future InstrumentDefinition use.
    pub symbol: [u8; 16],
}

/// Maps coin names (e.g., "BTC") to their InstrumentInfo.
#[derive(Debug, Clone)]
pub struct InstrumentRegistry {
    instruments: HashMap<String, InstrumentInfo>,
}

impl InstrumentRegistry {
    pub fn new(instruments: HashMap<String, InstrumentInfo>) -> Self {
        Self { instruments }
    }

    pub fn get(&self, coin: &str) -> Option<&InstrumentInfo> {
        self.instruments.get(coin)
    }

    pub fn len(&self) -> usize {
        self.instruments.len()
    }
}

/// Converts a decimal string price to a fixed-point i64 using the given exponent.
///
/// Example: `"106217.0"` with exponent `-1` -> `1062170_i64`
///
/// The exponent is negative: it represents how many decimal places the wire value has.
/// We multiply the float by `10^(-exponent)` to get the integer.
pub fn price_to_fixed(price_str: &str, exponent: i8) -> Option<i64> {
    let value: f64 = price_str.parse().ok()?;
    let multiplier = 10f64.powi(-exponent as i32);
    Some((value * multiplier).round() as i64)
}

/// Converts a decimal string quantity to a fixed-point u64 using the given exponent.
///
/// Example: `"0.00017"` with exponent `-5` -> `17_u64`
pub fn qty_to_fixed(qty_str: &str, exponent: i8) -> Option<u64> {
    let value: f64 = qty_str.parse().ok()?;
    let multiplier = 10f64.powi(-exponent as i32);
    let result = (value * multiplier).round() as i64;
    if result < 0 {
        return None;
    }
    Some(result as u64)
}

/// Creates a null-padded 16-byte ASCII symbol from a string.
/// Truncates if longer than 16 bytes.
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
    fn registry_lookup() {
        let mut map = HashMap::new();
        map.insert(
            "BTC".to_string(),
            InstrumentInfo {
                instrument_id: 0,
                price_exponent: -1,
                qty_exponent: -5,
                symbol: make_symbol("BTC"),
            },
        );
        let reg = InstrumentRegistry::new(map);
        assert!(reg.get("BTC").is_some());
        assert_eq!(reg.get("BTC").unwrap().instrument_id, 0);
        assert!(reg.get("ETH").is_none());
        assert_eq!(reg.len(), 1);
    }
}
