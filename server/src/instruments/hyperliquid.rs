use log::info;
use std::collections::HashMap;

use super::{InstrumentInfo, make_symbol};

/// Fetches perpetual and spot metadata from the Hyperliquid REST API and builds
/// an InstrumentInfo map keyed by coin name.
///
/// Perp instruments get instrument_id = array index.
/// Spot instruments get instrument_id = 10000 + array index.
pub async fn bootstrap_registry(
    api_url: &str,
) -> Result<HashMap<String, InstrumentInfo>, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let mut instruments = HashMap::new();

    // Fetch perp meta
    let perp_resp = client
        .post(format!("{api_url}/info"))
        .json(&serde_json::json!({"type": "meta"}))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    if let Some(universe) = perp_resp.get("universe").and_then(|v| v.as_array()) {
        for (idx, asset) in universe.iter().enumerate() {
            if let Some(info) = parse_perp_asset(asset, idx as u32) {
                info!(
                    "instruments: perp {} -> id={}, price_exp={}, qty_exp={}",
                    info.0, info.1.instrument_id, info.1.price_exponent, info.1.qty_exponent
                );
                instruments.insert(info.0, info.1);
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

    if let Some(universe) = spot_resp.get("universe").and_then(|v| v.as_array()) {
        for (idx, asset) in universe.iter().enumerate() {
            if let Some(info) = parse_spot_asset(asset, idx as u32) {
                info!(
                    "instruments: spot {} -> id={}, price_exp={}, qty_exp={}",
                    info.0, info.1.instrument_id, info.1.price_exponent, info.1.qty_exponent
                );
                instruments.insert(info.0, info.1);
            }
        }
    }

    info!(
        "instruments: loaded {} instruments ({} perps, {} spot)",
        instruments.len(),
        instruments.values().filter(|i| i.instrument_id < 10000).count(),
        instruments.values().filter(|i| i.instrument_id >= 10000).count(),
    );

    Ok(instruments)
}

fn parse_perp_asset(asset: &serde_json::Value, index: u32) -> Option<(String, InstrumentInfo)> {
    let name = asset.get("name")?.as_str()?;
    let sz_decimals = asset.get("szDecimals")?.as_u64()? as i8;
    let qty_exponent = -sz_decimals;
    let price_exponent = derive_price_exponent(asset);

    Some((
        name.to_string(),
        InstrumentInfo {
            instrument_id: index,
            price_exponent,
            qty_exponent,
            symbol: make_symbol(name),
        },
    ))
}

fn parse_spot_asset(asset: &serde_json::Value, index: u32) -> Option<(String, InstrumentInfo)> {
    let name = asset.get("name")?.as_str()?;
    let sz_decimals = asset.get("szDecimals")?.as_u64()? as i8;
    let qty_exponent = -sz_decimals;
    let price_exponent = derive_price_exponent(asset);

    Some((
        name.to_string(),
        InstrumentInfo {
            instrument_id: 10_000 + index,
            price_exponent,
            qty_exponent,
            symbol: make_symbol(name),
        },
    ))
}

/// Derives the price exponent from asset metadata.
///
/// Looks for a "maxDecimals" field first; falls back to -8 (matching the
/// internal MULTIPLIER of 10^8 used throughout the order book) which
/// accommodates all HL assets without loss of precision.
fn derive_price_exponent(asset: &serde_json::Value) -> i8 {
    if let Some(max_decimals) = asset.get("maxDecimals").and_then(|v| v.as_u64()) {
        return -(max_decimals as i8);
    }
    -8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_perp_asset_basic() {
        let asset = serde_json::json!({
            "name": "BTC",
            "szDecimals": 5
        });
        let (name, info) = parse_perp_asset(&asset, 0).unwrap();
        assert_eq!(name, "BTC");
        assert_eq!(info.instrument_id, 0);
        assert_eq!(info.qty_exponent, -5);
        assert_eq!(&info.symbol[..3], b"BTC");
    }

    #[test]
    fn parse_perp_asset_preserves_index() {
        let asset = serde_json::json!({
            "name": "ETH",
            "szDecimals": 4
        });
        let (name, info) = parse_perp_asset(&asset, 3).unwrap();
        assert_eq!(name, "ETH");
        assert_eq!(info.instrument_id, 3);
        assert_eq!(info.qty_exponent, -4);
    }

    #[test]
    fn parse_spot_asset_offset_id() {
        let asset = serde_json::json!({
            "name": "PURR/USDC",
            "szDecimals": 2,
            "tokens": [107, 0]
        });
        let (name, info) = parse_spot_asset(&asset, 5).unwrap();
        assert_eq!(name, "PURR/USDC");
        assert_eq!(info.instrument_id, 10_005);
        assert_eq!(info.qty_exponent, -2);
    }

    #[test]
    fn parse_perp_asset_missing_name_returns_none() {
        let asset = serde_json::json!({"szDecimals": 5});
        assert!(parse_perp_asset(&asset, 0).is_none());
    }

    #[test]
    fn parse_perp_asset_missing_sz_decimals_returns_none() {
        let asset = serde_json::json!({"name": "BTC"});
        assert!(parse_perp_asset(&asset, 0).is_none());
    }

    #[test]
    fn derive_price_exponent_default_fallback() {
        let asset = serde_json::json!({"name": "BTC", "szDecimals": 5});
        assert_eq!(derive_price_exponent(&asset), -8);
    }

    #[test]
    fn derive_price_exponent_explicit_max_decimals() {
        let asset = serde_json::json!({"name": "BTC", "szDecimals": 5, "maxDecimals": 1});
        assert_eq!(derive_price_exponent(&asset), -1);
    }
}
