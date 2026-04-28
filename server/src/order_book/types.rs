use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::ops::Add;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum Side {
    #[serde(rename = "A")]
    Ask,
    #[serde(rename = "B")]
    Bid,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Oid(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Px(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Sz(u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Coin(String);

impl Sz {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
    pub(super) const fn is_positive(self) -> bool {
        self.0 > 0
    }
    pub(super) const fn is_zero(self) -> bool {
        self.0 == 0
    }
    pub(crate) const fn value(self) -> u64 {
        self.0
    }
    pub(crate) const fn decrement_sz(&mut self, dec: u64) {
        self.0 = self.0.saturating_sub(dec);
    }
}

impl Px {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
    pub(crate) const fn value(self) -> u64 {
        self.0
    }
}

impl Oid {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the inner `u64` value.
    pub(crate) const fn into_inner(self) -> u64 {
        self.0
    }
}

pub(crate) trait InnerOrder: Clone {
    fn coin(&self) -> Coin;
    fn oid(&self) -> Oid;
    fn side(&self) -> Side;
    fn limit_px(&self) -> Px;
    fn sz(&self) -> Sz;
    fn decrement_sz(&mut self, dec: Sz);
    fn fill(&mut self, maker_order: &mut Self) -> Sz;
    fn modify_sz(&mut self, sz: Sz);
    fn convert_trigger(&mut self, ts: u64);
}

impl Coin {
    pub(crate) fn new(coin: &str) -> Self {
        Self(coin.to_string())
    }

    pub(crate) fn value(&self) -> String {
        self.0.clone()
    }

    pub(crate) fn is_spot(&self) -> bool {
        self.0.starts_with('@') || self.0 == "PURR/USDC"
    }
}

impl Add<Self> for Sz {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }
}

// Multiply all sizes and prices by 10^MAX_DECIMALS for ease of computation.
const MULTIPLIER: f64 = 100_000_000.0;

impl Debug for Px {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (self.value() as f64 / MULTIPLIER))
    }
}

impl Debug for Sz {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (self.value() as f64 / MULTIPLIER))
    }
}

impl Px {
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn parse_from_str(value: &str) -> Result<Self> {
        let value = (value.parse::<f64>()? * MULTIPLIER).round() as u64;
        Ok(Self::new(value))
    }

    #[must_use]
    pub(crate) fn to_str(self) -> String {
        let s = format!("{:.8}", (self.value() as f64) / MULTIPLIER);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
    }

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn num_digits(self) -> u32 {
        if self.value() == 0 { 1 } else { (self.value() as f64).log10().floor() as u32 + 1 }
    }
}

impl Sz {
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn parse_from_str(value: &str) -> Result<Self> {
        let value = (value.parse::<f64>()? * MULTIPLIER).round() as u64;
        Ok(Self::new(value))
    }

    #[must_use]
    pub(crate) fn to_str(self) -> String {
        let s = format!("{:.8}", (self.value() as f64) / MULTIPLIER);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
    }
}

/// Converts an internal `Sz` (which carries a quantity at the publisher's
/// fixed 10^8 scale) to the wire encoding for an instrument whose
/// `qty_exponent` is `qty_exponent`. The wire encoding is `qty * 10^-qty_exponent`,
/// and `Sz::value() == qty * 10^8`, so the divisor is `10^(8 + qty_exponent)`.
///
/// HL's `qty_exponent` range is `-8..=0` in practice (today: `-5..=0`); for any
/// value in that range the divisor is `>= 1` and the division is exact for any
/// quantity emitted by the venue (which is always an integer multiple of
/// `10^qty_exponent`).
#[must_use]
pub(crate) fn sz_to_fixed(sz: Sz, qty_exponent: i8) -> u64 {
    let div_pow = 8i32 + i32::from(qty_exponent);
    if div_pow <= 0 {
        sz.value().saturating_mul(10u64.pow((-div_pow) as u32))
    } else {
        sz.value() / 10u64.pow(div_pow as u32)
    }
}

#[cfg(test)]
mod sz_to_fixed_tests {
    use super::*;

    #[test]
    fn qty_exponent_zero_divides_by_1e8() {
        // Sz::parse_from_str("2921") -> 2921 * 10^8. qty_exponent=0 wants 2921.
        let sz = Sz::parse_from_str("2921").unwrap();
        assert_eq!(sz_to_fixed(sz, 0), 2921);
    }

    #[test]
    fn qty_exponent_negative_three() {
        // "1.234" with qty_exponent=-3 should yield 1234.
        let sz = Sz::parse_from_str("1.234").unwrap();
        assert_eq!(sz_to_fixed(sz, -3), 1234);
    }

    #[test]
    fn qty_exponent_negative_eight_is_identity() {
        // qty_exponent=-8 means the wire representation already matches Sz::value().
        let sz = Sz::parse_from_str("0.00000017").unwrap();
        assert_eq!(sz_to_fixed(sz, -8), sz.value());
    }

    #[test]
    fn zero_quantity_is_zero() {
        assert_eq!(sz_to_fixed(Sz::new(0), -3), 0);
        assert_eq!(sz_to_fixed(Sz::new(0), 0), 0);
    }
}
