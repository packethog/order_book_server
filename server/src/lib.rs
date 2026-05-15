#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod instruments;
mod listeners;
pub mod metrics;
pub mod multicast;
mod order_book;
mod prelude;
pub mod protocol;
mod servers;
#[cfg(test)]
mod test_fixtures;
#[cfg(test)]
mod test_subscriber;
mod types;

pub use multicast::config::{DobConfig, MulticastConfig};
pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;
pub use types::node_data::IngestMode;

pub const HL_NODE: &str = "hl-node";
