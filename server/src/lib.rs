#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod listeners;
mod order_book;
mod prelude;
mod servers;
mod types;
pub mod multicast;

pub use multicast::config::{Channel, MulticastConfig, parse_channels};
pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;

pub const HL_NODE: &str = "hl-node";
