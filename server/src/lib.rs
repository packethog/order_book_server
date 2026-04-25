#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod instruments;
mod listeners;
pub mod multicast;
mod order_book;
mod prelude;
pub mod protocol;
mod servers;
mod types;

pub use multicast::config::{DobConfig, MulticastConfig};
pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;

pub const HL_NODE: &str = "hl-node";
