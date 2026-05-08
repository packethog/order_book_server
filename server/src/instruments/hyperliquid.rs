use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::{error, info, warn};

use super::{InstrumentInfo, RegistryState, SharedRegistry, UniverseEntry, make_symbol};

/// Fetches perpetual, spot, and builder-DEX metadata from the Hyperliquid REST API
/// and builds a full `UniverseEntry` list keyed by index (including delisted entries).
///
/// Instrument id ranges:
/// - Core perps: array index (0..~230)
/// - Spot: 10_000 + array index
/// - Builder-DEX perps: 20_000 + dex_idx * 1_000 + asset_idx
///   (dex_idx = position in `perpDexs` response; coin name is prefixed `<dex>:<name>`
///    to match what hl-node writes in the block files)
pub async fn fetch_universe(api_url: &str) -> Result<Vec<UniverseEntry>, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let mut universe = Vec::new();

    // Fetch perp meta
    let perp_resp = client
        .post(format!("{api_url}/info"))
        .json(&serde_json::json!({"type": "meta"}))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    if let Some(arr) = perp_resp.get("universe").and_then(serde_json::Value::as_array) {
        for (idx, asset) in arr.iter().enumerate() {
            if let Some(entry) = parse_perp_asset(asset, u32::try_from(idx).unwrap_or(u32::MAX)) {
                universe.push(entry);
            }
        }
    }

    // Fetch spot meta
    let spot_resp = client
        .post(format!("{api_url}/info"))
        .json(&serde_json::json!({"type": "spotMeta"}))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    if let Some(arr) = spot_resp.get("universe").and_then(serde_json::Value::as_array) {
        for (idx, asset) in arr.iter().enumerate() {
            if let Some(entry) = parse_spot_asset(asset, u32::try_from(idx).unwrap_or(u32::MAX)) {
                universe.push(entry);
            }
        }
    }

    // Fetch builder-DEX perps. Failures here are non-fatal: we log and return
    // whatever we've already collected so core perps stay on the air.
    match client.post(format!("{api_url}/info")).json(&serde_json::json!({"type": "perpDexs"})).send().await {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(value) => {
                if let Some(dexs) = value.as_array() {
                    for (dex_idx, dex) in dexs.iter().enumerate() {
                        let Some(dex_name) = dex.get("name").and_then(serde_json::Value::as_str) else {
                            continue;
                        };
                        let id_base = 20_000 + u32::try_from(dex_idx).unwrap_or(u32::MAX) * 1_000;
                        match fetch_dex_universe(&client, api_url, dex_name, id_base).await {
                            Ok(mut entries) => universe.append(&mut entries),
                            Err(err) => {
                                warn!("instruments: skipping builder dex \"{dex_name}\": {err}");
                            }
                        }
                    }
                }
            }
            Err(err) => warn!("instruments: perpDexs response parse failed, skipping builder dexes: {err}"),
        },
        Err(err) => warn!("instruments: perpDexs fetch failed, skipping builder dexes: {err}"),
    }

    let active = universe.iter().filter(|e| !e.is_delisted).count();
    let delisted = universe.len() - active;
    info!("instruments: fetched {} entries ({} active, {} delisted)", universe.len(), active, delisted);

    Ok(universe)
}

async fn fetch_dex_universe(
    client: &reqwest::Client,
    api_url: &str,
    dex_name: &str,
    id_base: u32,
) -> Result<Vec<UniverseEntry>, Box<dyn std::error::Error + Send + Sync>> {
    let resp = client
        .post(format!("{api_url}/info"))
        .json(&serde_json::json!({"type": "meta", "dex": dex_name}))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    let mut entries = Vec::new();
    if let Some(arr) = resp.get("universe").and_then(serde_json::Value::as_array) {
        for (idx, asset) in arr.iter().enumerate() {
            let id = id_base + u32::try_from(idx).unwrap_or(u32::MAX);
            if let Some(entry) = parse_builder_dex_asset(asset, dex_name, id) {
                entries.push(entry);
            }
        }
    }
    Ok(entries)
}

/// Backwards-compatible wrapper that returns only the active instrument map.
/// Used for the initial bootstrap call before the refresh task takes over.
pub async fn bootstrap_registry(api_url: &str) -> Result<Vec<UniverseEntry>, Box<dyn std::error::Error + Send + Sync>> {
    fetch_universe(api_url).await
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn parse_perp_asset(asset: &serde_json::Value, index: u32) -> Option<UniverseEntry> {
    let name = asset.get("name")?.as_str()?;
    let sz_decimals = asset.get("szDecimals")?.as_u64()?;
    let qty_exponent = -(sz_decimals as i8);
    let price_exponent = derive_price_exponent(asset);
    let is_delisted = asset.get("isDelisted").and_then(serde_json::Value::as_bool).unwrap_or(false);

    let info = InstrumentInfo { instrument_id: index, price_exponent, qty_exponent, symbol: make_symbol(name) };

    Some(UniverseEntry { instrument_id: index, coin: name.to_string(), is_delisted, info })
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn parse_spot_asset(asset: &serde_json::Value, index: u32) -> Option<UniverseEntry> {
    let name = asset.get("name")?.as_str()?;
    let sz_decimals = asset.get("szDecimals")?.as_u64()?;
    let qty_exponent = -(sz_decimals as i8);
    let price_exponent = derive_price_exponent(asset);
    let is_delisted = asset.get("isDelisted").and_then(serde_json::Value::as_bool).unwrap_or(false);

    let info =
        InstrumentInfo { instrument_id: 10_000 + index, price_exponent, qty_exponent, symbol: make_symbol(name) };

    Some(UniverseEntry { instrument_id: 10_000 + index, coin: name.to_string(), is_delisted, info })
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn parse_builder_dex_asset(asset: &serde_json::Value, dex: &str, id: u32) -> Option<UniverseEntry> {
    let name = asset.get("name")?.as_str()?;
    let sz_decimals = asset.get("szDecimals")?.as_u64()?;
    let qty_exponent = -(sz_decimals as i8);
    let price_exponent = derive_price_exponent(asset);
    let is_delisted = asset.get("isDelisted").and_then(serde_json::Value::as_bool).unwrap_or(false);

    // The HL `meta {dex}` endpoint already returns names prefixed with the dex
    // (e.g. `"xyz:XYZ100"`, `"cash:USA500"`) — that's the format hl-node also
    // writes into the streaming/by-block files. Re-prefixing here would
    // double-up (`"xyz:xyz:XYZ100"`) and the resolver would never match the
    // wire form, silently dropping every event for that dex. Use the API
    // name as-is, but defend against an API change by prefixing only if the
    // name doesn't already start with `<dex>:`.
    let prefix = format!("{dex}:");
    let coin = if name.starts_with(&prefix) { name.to_string() } else { format!("{prefix}{name}") };
    let info = InstrumentInfo { instrument_id: id, price_exponent, qty_exponent, symbol: make_symbol(&coin) };

    Some(UniverseEntry { instrument_id: id, coin, is_delisted, info })
}

/// Derives the price exponent from asset metadata.
///
/// Looks for a `maxDecimals` field first; falls back to -8 (matching the
/// internal MULTIPLIER of 10^8 used throughout the order book).
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn derive_price_exponent(asset: &serde_json::Value) -> i8 {
    if let Some(max_decimals) = asset.get("maxDecimals").and_then(serde_json::Value::as_u64) {
        return -(max_decimals as i8);
    }
    -8
}

/// Periodically refreshes the instrument registry from the HL REST API.
///
/// On each cycle:
/// 1. Fetches the full universe (including delisted entries)
/// 2. Runs integrity checks against the current state (identity + disappearance)
/// 3. If checks pass and the data changed, bumps `manifest_seq` and replaces the state
/// 4. If checks fail, logs an error and keeps the current state (fail-safe)
/// 5. If the HL API is unreachable, logs a warning and retries next cycle
pub async fn refresh_task(api_url: String, state: SharedRegistry, interval: Duration) {
    info!("instruments: refresh task started (interval={:?})", interval);
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await; // consume immediate first tick

    loop {
        ticker.tick().await;

        let new_universe = match fetch_universe(&api_url).await {
            Ok(u) => u,
            Err(err) => {
                warn!("instruments: refresh failed, keeping current state: {err}");
                continue;
            }
        };

        // Snapshot the current state lock-free for the integrity / equality checks.
        // ArcSwap reads return a Guard that doesn't block any reader or any other
        // writer; as the only writer in the system, we can read+compare freely
        // and only emit a store on a real change.
        let current = state.load();

        if let Err(violation) = check_integrity(&current.universe, &new_universe) {
            error!("instruments: integrity check failed, rejecting refresh: {violation}");
            continue;
        }

        if new_universe == current.universe {
            // No change — don't bump seq
            continue;
        }

        let old_seq = current.manifest_seq;
        let diff = summarize_diff(&current.universe, &new_universe);
        let new_seq = next_manifest_seq(old_seq);
        let new_state = RegistryState {
            active: new_universe.iter().filter(|e| !e.is_delisted).map(|e| (e.coin.clone(), e.info.clone())).collect(),
            universe: new_universe,
            manifest_seq: new_seq,
        };
        // Drop the read snapshot before the store so the Arc backing `current`
        // can be reclaimed promptly once no other reader holds it.
        drop(current);
        state.store(Arc::new(new_state));

        info!("instruments: manifest_seq {old_seq} -> {new_seq}: {diff}");
    }
}

/// Integrity check errors.
#[derive(Debug)]
pub enum IntegrityViolation {
    /// Same `instrument_id` now maps to a different coin name.
    IdentityRemapped { id: u32, old_coin: String, new_coin: String },
    /// A previously-seen `instrument_id` is missing from the new response entirely.
    /// Delisting is OK (entry still present with `is_delisted=true`); disappearance is not.
    SlotDisappeared { id: u32, coin: String },
}

impl std::fmt::Display for IntegrityViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IdentityRemapped { id, old_coin, new_coin } => {
                write!(f, "instrument_id {id} remapped: was \"{old_coin}\", now \"{new_coin}\"")
            }
            Self::SlotDisappeared { id, coin } => {
                write!(f, "instrument_id {id} (\"{coin}\") disappeared from HL meta response")
            }
        }
    }
}

/// Verifies that every previously-seen `instrument_id` is still present in the new universe
/// and still maps to the same coin name.
fn check_integrity(old: &[UniverseEntry], new: &[UniverseEntry]) -> Result<(), IntegrityViolation> {
    if old.is_empty() {
        return Ok(());
    }

    // Index the new universe by instrument_id for O(1) lookup
    let new_by_id: HashMap<u32, &UniverseEntry> = new.iter().map(|e| (e.instrument_id, e)).collect();

    for old_entry in old {
        match new_by_id.get(&old_entry.instrument_id) {
            None => {
                return Err(IntegrityViolation::SlotDisappeared {
                    id: old_entry.instrument_id,
                    coin: old_entry.coin.clone(),
                });
            }
            Some(new_entry) if new_entry.coin != old_entry.coin => {
                return Err(IntegrityViolation::IdentityRemapped {
                    id: old_entry.instrument_id,
                    old_coin: old_entry.coin.clone(),
                    new_coin: new_entry.coin.clone(),
                });
            }
            Some(_) => {}
        }
    }

    Ok(())
}

/// Computes the next manifest_seq, skipping 0 on rollover.
fn next_manifest_seq(current: u16) -> u16 {
    let next = current.wrapping_add(1);
    if next == 0 { 1 } else { next }
}

/// Human-readable summary of what changed between two universe snapshots.
fn summarize_diff(old: &[UniverseEntry], new: &[UniverseEntry]) -> String {
    let old_by_id: HashMap<u32, &UniverseEntry> = old.iter().map(|e| (e.instrument_id, e)).collect();
    let new_by_id: HashMap<u32, &UniverseEntry> = new.iter().map(|e| (e.instrument_id, e)).collect();

    let mut added = Vec::new();
    let mut delisted = Vec::new();
    let mut reactivated = Vec::new();
    let mut modified = Vec::new();

    for (id, new_entry) in &new_by_id {
        match old_by_id.get(id) {
            None => added.push(new_entry.coin.clone()),
            Some(old_entry) => {
                if old_entry.is_delisted && !new_entry.is_delisted {
                    reactivated.push(new_entry.coin.clone());
                } else if !old_entry.is_delisted && new_entry.is_delisted {
                    delisted.push(new_entry.coin.clone());
                } else if old_entry.info != new_entry.info {
                    modified.push(format!(
                        "{}(px_exp: {} -> {}, qty_exp: {} -> {})",
                        new_entry.coin,
                        old_entry.info.price_exponent,
                        new_entry.info.price_exponent,
                        old_entry.info.qty_exponent,
                        new_entry.info.qty_exponent,
                    ));
                }
            }
        }
    }

    format!(
        "added=[{}] delisted=[{}] reactivated=[{}] modified=[{}]",
        added.join(","),
        delisted.join(","),
        reactivated.join(","),
        modified.join(","),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: u32, coin: &str, delisted: bool, px_exp: i8, qty_exp: i8) -> UniverseEntry {
        UniverseEntry {
            instrument_id: id,
            coin: coin.to_string(),
            is_delisted: delisted,
            info: InstrumentInfo {
                instrument_id: id,
                price_exponent: px_exp,
                qty_exponent: qty_exp,
                symbol: make_symbol(coin),
            },
        }
    }

    #[test]
    fn parse_perp_asset_basic() {
        let asset = serde_json::json!({"name": "BTC", "szDecimals": 5});
        let e = parse_perp_asset(&asset, 0).unwrap();
        assert_eq!(e.coin, "BTC");
        assert_eq!(e.instrument_id, 0);
        assert_eq!(e.info.qty_exponent, -5);
        assert!(!e.is_delisted);
    }

    #[test]
    fn parse_perp_asset_delisted() {
        let asset = serde_json::json!({"name": "OLDCOIN", "szDecimals": 3, "isDelisted": true});
        let e = parse_perp_asset(&asset, 42).unwrap();
        assert_eq!(e.coin, "OLDCOIN");
        assert!(e.is_delisted);
    }

    #[test]
    fn parse_spot_asset_offset_id() {
        let asset = serde_json::json!({"name": "PURR/USDC", "szDecimals": 2});
        let e = parse_spot_asset(&asset, 5).unwrap();
        assert_eq!(e.coin, "PURR/USDC");
        assert_eq!(e.instrument_id, 10_005);
    }

    #[test]
    fn parse_builder_dex_asset_prefixes_coin() {
        let asset = serde_json::json!({"name": "CL", "szDecimals": 3, "maxDecimals": 2});
        let e = parse_builder_dex_asset(&asset, "xyz", 21_000).unwrap();
        assert_eq!(e.coin, "xyz:CL");
        assert_eq!(e.instrument_id, 21_000);
        assert_eq!(e.info.qty_exponent, -3);
        assert_eq!(e.info.price_exponent, -2);
        assert!(!e.is_delisted);
    }

    #[test]
    fn parse_builder_dex_asset_delisted() {
        let asset = serde_json::json!({"name": "OLD", "szDecimals": 2, "isDelisted": true});
        let e = parse_builder_dex_asset(&asset, "abcd", 27_000).unwrap();
        assert_eq!(e.coin, "abcd:OLD");
        assert!(e.is_delisted);
    }

    #[test]
    fn parse_builder_dex_asset_missing_fields() {
        assert!(parse_builder_dex_asset(&serde_json::json!({"szDecimals": 3}), "xyz", 20_000).is_none());
        assert!(parse_builder_dex_asset(&serde_json::json!({"name": "CL"}), "xyz", 20_000).is_none());
    }

    /// The actual HL `meta {dex}` endpoint returns `name` already prefixed
    /// with `<dex>:` (verified against `xyz`, `cash`, `hyna`, `km`, `flx`,
    /// `vntl`, `para` on mainnet 2026-05-08). hl-node also writes coin names
    /// with that prefix into the streaming/by-block files. So the registry
    /// `coin` key MUST equal the API name verbatim — no extra prefix layer.
    /// Without this, every event for a builder-dex coin silently fails the
    /// resolver (584 unknown-coin warns/sec on mainnet pre-fix).
    #[test]
    fn parse_builder_dex_asset_uses_api_prefixed_name_verbatim() {
        let asset = serde_json::json!({"name": "xyz:XYZ100", "szDecimals": 4, "maxDecimals": 2});
        let e = parse_builder_dex_asset(&asset, "xyz", 21_000).unwrap();
        // Critical invariant: the registry key matches what hl-node writes
        // on the wire (`xyz:XYZ100`), NOT a doubled `xyz:xyz:XYZ100`.
        assert_eq!(e.coin, "xyz:XYZ100");
    }

    /// Defensive: should the API ever return an unprefixed name (it does not
    /// today), we still produce the correctly-prefixed registry key so the
    /// resolver continues to work.
    #[test]
    fn parse_builder_dex_asset_prefixes_unprefixed_name_defensively() {
        let asset = serde_json::json!({"name": "BAREONLY", "szDecimals": 3});
        let e = parse_builder_dex_asset(&asset, "newdex", 22_000).unwrap();
        assert_eq!(e.coin, "newdex:BAREONLY");
    }

    #[test]
    fn parse_perp_asset_missing_fields() {
        assert!(parse_perp_asset(&serde_json::json!({"szDecimals": 5}), 0).is_none());
        assert!(parse_perp_asset(&serde_json::json!({"name": "BTC"}), 0).is_none());
    }

    #[test]
    fn derive_price_exponent_default() {
        assert_eq!(derive_price_exponent(&serde_json::json!({"name": "X", "szDecimals": 5})), -8);
    }

    #[test]
    fn derive_price_exponent_explicit() {
        assert_eq!(derive_price_exponent(&serde_json::json!({"name": "X", "szDecimals": 5, "maxDecimals": 1})), -1);
    }

    #[test]
    fn next_manifest_seq_basic() {
        assert_eq!(next_manifest_seq(1), 2);
        assert_eq!(next_manifest_seq(100), 101);
    }

    #[test]
    fn next_manifest_seq_skips_zero_on_rollover() {
        assert_eq!(next_manifest_seq(u16::MAX), 1);
    }

    #[test]
    fn integrity_ok_on_empty_old() {
        let new = vec![entry(0, "BTC", false, -1, -5)];
        assert!(check_integrity(&[], &new).is_ok());
    }

    #[test]
    fn integrity_ok_on_no_change() {
        let snap = vec![entry(0, "BTC", false, -1, -5), entry(1, "ETH", false, -1, -4)];
        assert!(check_integrity(&snap, &snap).is_ok());
    }

    #[test]
    fn integrity_ok_on_append() {
        let old = vec![entry(0, "BTC", false, -1, -5)];
        let new = vec![entry(0, "BTC", false, -1, -5), entry(1, "ETH", false, -1, -4)];
        assert!(check_integrity(&old, &new).is_ok());
    }

    #[test]
    fn integrity_ok_on_delisting() {
        let old = vec![entry(0, "BTC", false, -1, -5), entry(1, "OLDCOIN", false, -1, -3)];
        let new = vec![entry(0, "BTC", false, -1, -5), entry(1, "OLDCOIN", true, -1, -3)];
        assert!(check_integrity(&old, &new).is_ok());
    }

    #[test]
    fn integrity_ok_on_exponent_change() {
        // Exponent change is detected by the diff (not integrity check),
        // so integrity passes and the seq bump happens via the != comparison.
        let old = vec![entry(0, "BTC", false, -1, -5)];
        let new = vec![entry(0, "BTC", false, -2, -5)];
        assert!(check_integrity(&old, &new).is_ok());
    }

    #[test]
    fn integrity_fails_on_identity_remap() {
        let old = vec![entry(0, "BTC", false, -1, -5)];
        let new = vec![entry(0, "EVIL", false, -1, -5)];
        let result = check_integrity(&old, &new);
        assert!(matches!(result, Err(IntegrityViolation::IdentityRemapped { id: 0, .. })));
    }

    #[test]
    fn integrity_fails_on_slot_disappearance() {
        let old = vec![entry(0, "BTC", false, -1, -5), entry(1, "ETH", false, -1, -4)];
        let new = vec![entry(0, "BTC", false, -1, -5)]; // ETH vanished
        let result = check_integrity(&old, &new);
        assert!(matches!(result, Err(IntegrityViolation::SlotDisappeared { id: 1, .. })));
    }

    #[test]
    fn summarize_diff_append() {
        let old = vec![entry(0, "BTC", false, -1, -5)];
        let new = vec![entry(0, "BTC", false, -1, -5), entry(1, "ETH", false, -1, -4)];
        let s = summarize_diff(&old, &new);
        assert!(s.contains("added=[ETH]"), "got: {s}");
    }

    #[test]
    fn summarize_diff_delisting() {
        let old = vec![entry(0, "BTC", false, -1, -5)];
        let new = vec![entry(0, "BTC", true, -1, -5)];
        let s = summarize_diff(&old, &new);
        assert!(s.contains("delisted=[BTC]"), "got: {s}");
    }

    #[test]
    fn summarize_diff_modified() {
        let old = vec![entry(0, "BTC", false, -1, -5)];
        let new = vec![entry(0, "BTC", false, -2, -5)];
        let s = summarize_diff(&old, &new);
        assert!(s.contains("modified=[BTC(px_exp: -1 -> -2"), "got: {s}");
    }
}
