//! Per-instrument delta sequence counter, scoped to the current `Reset Count` era.
//!
//! Per the DZ-DoB wire spec, `Per-Instrument Seq` is a `u32` that:
//! - Starts at 1 on the first delta for an instrument after a `Reset Count` change.
//! - Increments densely (by exactly 1) on every subsequent delta for that instrument.
//! - Is NOT reset at snapshot boundaries or on `InstrumentReset`.
//! - Resets only on `Reset Count` change.

use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct PerInstrumentSeqCounter {
    counters: HashMap<u32, u32>,
}

impl PerInstrumentSeqCounter {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the next `Per-Instrument Seq` for `instrument_id`. First call for a
    /// given instrument returns 1; each subsequent call returns the previous value + 1.
    pub fn next(&mut self, instrument_id: u32) -> u32 {
        let slot = self.counters.entry(instrument_id).or_insert(0);
        *slot = slot.checked_add(1).expect("per-instrument seq u32 overflow");
        *slot
    }

    /// Returns the most recently emitted seq for `instrument_id`, or 0 if none.
    /// Used to populate `SnapshotBegin.last_instrument_seq` (phase 2).
    #[must_use]
    pub fn last(&self, instrument_id: u32) -> u32 {
        self.counters.get(&instrument_id).copied().unwrap_or(0)
    }

    /// Resets all counters. Called on `Reset Count` change.
    pub fn reset_all(&mut self) {
        self.counters.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_next_returns_one() {
        let mut c = PerInstrumentSeqCounter::new();
        assert_eq!(c.next(42), 1);
    }

    #[test]
    fn next_increments_densely() {
        let mut c = PerInstrumentSeqCounter::new();
        assert_eq!(c.next(42), 1);
        assert_eq!(c.next(42), 2);
        assert_eq!(c.next(42), 3);
    }

    #[test]
    fn separate_instruments_are_independent() {
        let mut c = PerInstrumentSeqCounter::new();
        assert_eq!(c.next(1), 1);
        assert_eq!(c.next(2), 1);
        assert_eq!(c.next(1), 2);
        assert_eq!(c.next(2), 2);
    }

    #[test]
    fn last_returns_zero_before_any_next() {
        let c = PerInstrumentSeqCounter::new();
        assert_eq!(c.last(42), 0);
    }

    #[test]
    fn last_returns_most_recent() {
        let mut c = PerInstrumentSeqCounter::new();
        c.next(42);
        c.next(42);
        assert_eq!(c.last(42), 2);
    }

    #[test]
    fn reset_all_clears_counters() {
        let mut c = PerInstrumentSeqCounter::new();
        c.next(42);
        c.next(42);
        c.reset_all();
        assert_eq!(c.next(42), 1, "first after reset returns 1");
    }
}
