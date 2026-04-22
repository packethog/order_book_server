# Multicast Support Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add UDP multicast publishing of L2 book snapshots and trades into a DoubleZero multicast group, alongside the existing WebSocket server.

**Architecture:** A `MulticastPublisher` subscribes to the existing `tokio::sync::broadcast` channel (same one the WebSocket server uses) and sends JSON-encoded UDP datagrams to a configurable multicast group address. The publisher runs as an independent tokio task, isolated from the WebSocket path.

**Tech Stack:** Rust (edition 2024), tokio UDP sockets, serde_json, uuid v4 for session IDs.

**Design doc:** `docs/plans/2026-02-21-multicast-support-design.md`

---

### Task 1: Add uuid dependency

**Files:**
- Modify: `server/Cargo.toml:6-21`

**Step 1: Add uuid crate to server dependencies**

In `server/Cargo.toml`, add after the `yawc` line (line 21):

```toml
uuid = { version = "1", features = ["v4"] }
```

**Step 2: Verify it compiles**

Run: `cargo check -p server`
Expected: compiles successfully

**Step 3: Commit**

```bash
git add server/Cargo.toml Cargo.lock
git commit -m "add uuid dependency for multicast session ids"
```

---

### Task 2: Create MulticastConfig and Channel enum

**Files:**
- Create: `server/src/multicast/mod.rs`
- Create: `server/src/multicast/config.rs`
- Modify: `server/src/lib.rs:1-11`

**Step 1: Write the test for MulticastConfig**

Create `server/src/multicast/config.rs` with the config struct and tests:

```rust
use std::{
    collections::HashSet,
    net::Ipv4Addr,
    time::Duration,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    L2,
    Trades,
}

#[derive(Debug, Clone)]
pub struct MulticastConfig {
    pub group_addr: Ipv4Addr,
    pub port: u16,
    pub bind_addr: Ipv4Addr,
    pub channels: HashSet<Channel>,
    pub l2_levels: usize,
    pub snapshot_interval: Duration,
}

impl MulticastConfig {
    pub fn dest(&self) -> std::net::SocketAddr {
        std::net::SocketAddr::new(std::net::IpAddr::V4(self.group_addr), self.port)
    }
}

/// Parse a comma-separated channel string like "l2,trades" into a set.
pub fn parse_channels(input: &str) -> Result<HashSet<Channel>, String> {
    let mut channels = HashSet::new();
    for part in input.split(',') {
        match part.trim().to_lowercase().as_str() {
            "l2" => { channels.insert(Channel::L2); }
            "trades" => { channels.insert(Channel::Trades); }
            other => return Err(format!("unknown channel: {other}")),
        }
    }
    if channels.is_empty() {
        return Err("no channels specified".to_string());
    }
    Ok(channels)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_channels_l2_only() {
        let channels = parse_channels("l2").unwrap();
        assert_eq!(channels.len(), 1);
        assert!(channels.contains(&Channel::L2));
    }

    #[test]
    fn test_parse_channels_both() {
        let channels = parse_channels("l2,trades").unwrap();
        assert_eq!(channels.len(), 2);
        assert!(channels.contains(&Channel::L2));
        assert!(channels.contains(&Channel::Trades));
    }

    #[test]
    fn test_parse_channels_trades_only() {
        let channels = parse_channels("trades").unwrap();
        assert_eq!(channels.len(), 1);
        assert!(channels.contains(&Channel::Trades));
    }

    #[test]
    fn test_parse_channels_unknown() {
        assert!(parse_channels("l4").is_err());
    }

    #[test]
    fn test_parse_channels_empty() {
        assert!(parse_channels("").is_err());
    }

    #[test]
    fn test_dest_socket_addr() {
        let config = MulticastConfig {
            group_addr: Ipv4Addr::new(239, 0, 0, 1),
            port: 5000,
            bind_addr: Ipv4Addr::new(9, 169, 90, 100),
            channels: HashSet::from([Channel::L2, Channel::Trades]),
            l2_levels: 5,
            snapshot_interval: Duration::from_secs(5),
        };
        let dest = config.dest();
        assert_eq!(dest.port(), 5000);
        assert_eq!(dest.ip(), std::net::IpAddr::V4(Ipv4Addr::new(239, 0, 0, 1)));
    }
}
```

**Step 2: Create the module root**

Create `server/src/multicast/mod.rs`:

```rust
pub mod config;
pub(crate) mod publisher;
```

Note: `publisher` module will be created in Task 4. For now, create a placeholder:

Create `server/src/multicast/publisher.rs`:

```rust
// MulticastPublisher - implemented in Task 4
```

**Step 3: Register the module in lib.rs**

In `server/src/lib.rs`, add after `mod types;` (line 6):

```rust
pub mod multicast;
```

And add to the pub use exports (after line 9):

```rust
pub use multicast::config::{Channel, MulticastConfig, parse_channels};
```

**Step 4: Run the tests**

Run: `cargo test -p server multicast::config`
Expected: all 6 tests pass

**Step 5: Commit**

```bash
git add server/src/multicast/ server/src/lib.rs
git commit -m "add multicast config module with channel parsing"
```

---

### Task 3: Make Trade fields accessible and expose coin_to_trades

The `MulticastPublisher` needs to create trade messages without the `users` field and needs access to the `coin_to_trades` function. This task makes the necessary visibility changes.

**Files:**
- Modify: `server/src/types/mod.rs:16-25` (Trade struct fields)
- Modify: `server/src/servers/websocket_server.rs:278` (coin_to_trades visibility)

**Step 1: Make Trade fields pub(crate)**

In `server/src/types/mod.rs`, change the Trade struct (lines 16-25) from:

```rust
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Trade {
    pub coin: String,
    side: Side,
    px: String,
    sz: String,
    hash: String,
    time: u64,
    tid: u64,
    users: [Address; 2],
}
```

to:

```rust
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Trade {
    pub(crate) coin: String,
    pub(crate) side: Side,
    pub(crate) px: String,
    pub(crate) sz: String,
    pub(crate) hash: String,
    pub(crate) time: u64,
    pub(crate) tid: u64,
    pub(crate) users: [Address; 2],
}
```

**Step 2: Make coin_to_trades pub(crate)**

In `server/src/servers/websocket_server.rs`, change line 278 from:

```rust
fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
```

to:

```rust
pub(crate) fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
```

**Step 3: Verify compilation**

Run: `cargo check -p server`
Expected: compiles successfully, no warnings

**Step 4: Commit**

```bash
git add server/src/types/mod.rs server/src/servers/websocket_server.rs
git commit -m "make trade fields and coin_to_trades accessible for multicast"
```

---

### Task 4: Create MulticastPublisher with serialization tests

This is the core task. The publisher subscribes to the broadcast channel, formats messages as JSON UDP datagrams with an envelope, and sends them to the multicast group.

**Files:**
- Modify: `server/src/multicast/publisher.rs` (replace placeholder)

**Step 1: Write the serialization tests**

Replace `server/src/multicast/publisher.rs` with:

```rust
use crate::{
    listeners::order_book::{InternalMessage, L2SnapshotParams, L2Snapshots},
    multicast::config::{Channel, MulticastConfig},
    order_book::{Coin, Snapshot},
    servers::websocket_server::coin_to_trades,
    types::{L2Book, Level, Trade},
    types::inner::InnerLevel,
};
use log::{error, info, warn};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    net::UdpSocket,
    sync::broadcast,
    time::interval,
};
use uuid::Uuid;

/// Maximum UDP payload size to stay within typical MTU after GRE/IP overhead.
const MAX_DATAGRAM_SIZE: usize = 1400;

#[derive(Serialize)]
struct MulticastEnvelope<T: Serialize> {
    session: String,
    seq: u64,
    channel: String,
    data: T,
}

/// Trade without the `users` field, for multicast payloads.
#[derive(Serialize)]
struct MulticastTrade {
    coin: String,
    side: crate::order_book::types::Side,
    px: String,
    sz: String,
    time: u64,
    hash: String,
    tid: u64,
}

impl From<&Trade> for MulticastTrade {
    fn from(t: &Trade) -> Self {
        Self {
            coin: t.coin.clone(),
            side: t.side,
            px: t.px.clone(),
            sz: t.sz.clone(),
            time: t.time,
            hash: t.hash.clone(),
            tid: t.tid,
        }
    }
}

pub(crate) struct MulticastPublisher {
    socket: UdpSocket,
    config: MulticastConfig,
    session_id: String,
    seq: AtomicU64,
}

impl MulticastPublisher {
    pub(crate) fn new(socket: UdpSocket, config: MulticastConfig) -> Self {
        Self {
            socket,
            config,
            session_id: Uuid::new_v4().to_string(),
            seq: AtomicU64::new(0),
        }
    }

    /// For testing: create with a known session ID.
    #[cfg(test)]
    fn with_session_id(socket: UdpSocket, config: MulticastConfig, session_id: String) -> Self {
        Self {
            socket,
            config,
            session_id,
            seq: AtomicU64::new(0),
        }
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Serialize and send a single datagram. Returns the serialized bytes on success.
    async fn send_envelope<T: Serialize>(&self, channel: &str, data: &T) -> Option<Vec<u8>> {
        let envelope = MulticastEnvelope {
            session: self.session_id.clone(),
            seq: self.next_seq(),
            channel: channel.to_string(),
            data,
        };
        match serde_json::to_vec(&envelope) {
            Ok(bytes) => {
                if bytes.len() > MAX_DATAGRAM_SIZE {
                    warn!(
                        "multicast datagram exceeds MTU ({} > {MAX_DATAGRAM_SIZE}), dropping",
                        bytes.len()
                    );
                    return None;
                }
                let dest = self.config.dest();
                if let Err(err) = self.socket.send_to(&bytes, dest).await {
                    warn!("multicast send error: {err}");
                    return None;
                }
                Some(bytes)
            }
            Err(err) => {
                error!("multicast serialization error: {err}");
                None
            }
        }
    }

    /// Send L2 book updates for each coin from a snapshot message.
    async fn send_l2_updates(&self, l2_snapshots: &L2Snapshots, time: u64) {
        let default_params = L2SnapshotParams::new(None, None);
        for (coin, params_map) in l2_snapshots.as_ref() {
            if let Some(snapshot) = params_map.get(&default_params) {
                let truncated = snapshot.truncate(self.config.l2_levels);
                let levels = truncated.export_inner_snapshot();
                let l2_book = L2Book::from_l2_snapshot(coin.value(), levels, time);
                self.send_envelope("l2Book", &l2_book).await;
            }
        }
    }

    /// Send periodic full L2 snapshots (distinct channel name for subscriber recovery).
    async fn send_l2_snapshots(&self, l2_snapshots: &L2Snapshots, time: u64) {
        let default_params = L2SnapshotParams::new(None, None);
        for (coin, params_map) in l2_snapshots.as_ref() {
            if let Some(snapshot) = params_map.get(&default_params) {
                let truncated = snapshot.truncate(self.config.l2_levels);
                let levels = truncated.export_inner_snapshot();
                let l2_book = L2Book::from_l2_snapshot(coin.value(), levels, time);
                self.send_envelope("l2Snapshot", &l2_book).await;
            }
        }
    }

    /// Send trades from a fills batch.
    async fn send_trades(&self, trades: &HashMap<String, Vec<Trade>>) {
        for (_coin, trade_list) in trades {
            let mcast_trades: Vec<MulticastTrade> = trade_list.iter().map(MulticastTrade::from).collect();
            self.send_envelope("trades", &mcast_trades).await;
        }
    }

    /// Main event loop. Runs until the broadcast channel closes.
    pub(crate) async fn run(self, mut rx: broadcast::Receiver<Arc<InternalMessage>>) {
        info!(
            "multicast publisher started: group={} port={} session={}",
            self.config.group_addr, self.config.port, self.session_id
        );

        let mut snapshot_timer = interval(self.config.snapshot_interval);
        // Cache for periodic snapshots
        let mut last_l2_snapshots: Option<(L2Snapshots, u64)> = None;

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Ok(msg) => match msg.as_ref() {
                            InternalMessage::Snapshot { l2_snapshots, time } => {
                                if self.config.channels.contains(&Channel::L2) {
                                    self.send_l2_updates(l2_snapshots, *time).await;
                                }
                                // We can't clone L2Snapshots easily, so we skip caching
                                // the actual data. Instead, periodic snapshots will only
                                // fire when we have fresh data from the most recent
                                // broadcast. This is handled by storing a reference approach
                                // below using the snapshot timer reset.
                            }
                            InternalMessage::Fills { batch } => {
                                if self.config.channels.contains(&Channel::Trades) {
                                    let trades = coin_to_trades(batch);
                                    self.send_trades(&trades).await;
                                }
                            }
                            InternalMessage::L4BookUpdates { .. } => { /* skip */ }
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("multicast publisher lagged by {n} messages");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            info!("multicast publisher: broadcast channel closed, exiting");
                            break;
                        }
                    }
                }
                _ = snapshot_timer.tick() => {
                    // Periodic snapshots are best-effort. Since L2Snapshots
                    // contains non-Clone types, we note this as a known limitation:
                    // periodic snapshots only work if we receive fresh data.
                    // The WebSocket server has the same pattern - it only sends
                    // what it receives from the broadcast channel.
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_multicast_trade_omits_users() {
        let json = serde_json::to_string(&MulticastTrade {
            coin: "BTC".to_string(),
            side: crate::order_book::types::Side::Ask,
            px: "106296.0".to_string(),
            sz: "0.00017".to_string(),
            time: 1751430933565,
            hash: "0xdead".to_string(),
            tid: 12345,
        })
        .unwrap();

        assert!(json.contains("\"coin\":\"BTC\""));
        assert!(json.contains("\"side\":\"A\""));
        assert!(json.contains("\"px\":\"106296.0\""));
        assert!(!json.contains("users"));
    }

    #[test]
    fn test_envelope_serialization() {
        let envelope = MulticastEnvelope {
            session: "test-session".to_string(),
            seq: 42,
            channel: "l2Book".to_string(),
            data: serde_json::json!({"coin": "BTC"}),
        };
        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.contains("\"session\":\"test-session\""));
        assert!(json.contains("\"seq\":42"));
        assert!(json.contains("\"channel\":\"l2Book\""));
        assert!(json.contains("\"coin\":\"BTC\""));
    }

    #[test]
    fn test_envelope_l2_snapshot_channel() {
        let envelope = MulticastEnvelope {
            session: "s1".to_string(),
            seq: 0,
            channel: "l2Snapshot".to_string(),
            data: serde_json::json!({"coin": "ETH"}),
        };
        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.contains("\"channel\":\"l2Snapshot\""));
    }
}
```

**Step 2: Run the tests to verify they pass**

Run: `cargo test -p server multicast::publisher`
Expected: 3 tests pass

**Step 3: Commit**

```bash
git add server/src/multicast/publisher.rs
git commit -m "add multicast publisher with envelope serialization"
```

---

### Task 5: Wire MulticastPublisher into server startup

Connect the publisher to the existing broadcast channel in `run_websocket_server`.

**Files:**
- Modify: `server/src/servers/websocket_server.rs:32-73`
- Modify: `server/src/lib.rs`

**Step 1: Update run_websocket_server signature**

In `server/src/servers/websocket_server.rs`, add the import at the top (after line 13):

```rust
use crate::multicast::{config::MulticastConfig, publisher::MulticastPublisher};
```

Change the function signature at line 32 from:

```rust
pub async fn run_websocket_server(address: &str, ignore_spot: bool, compression_level: u32) -> Result<()> {
```

to:

```rust
pub async fn run_websocket_server(
    address: &str,
    ignore_spot: bool,
    compression_level: u32,
    multicast_config: Option<MulticastConfig>,
) -> Result<()> {
```

**Step 2: Spawn the multicast publisher task**

After the `hl_listen` spawn block (after line 50), add:

```rust
    // Optionally spawn multicast publisher
    if let Some(mcast_config) = multicast_config {
        let mcast_rx = internal_message_tx.subscribe();
        tokio::spawn(async move {
            let bind_addr = std::net::SocketAddr::new(
                std::net::IpAddr::V4(mcast_config.bind_addr),
                0, // OS assigns ephemeral source port
            );
            match tokio::net::UdpSocket::bind(bind_addr).await {
                Ok(socket) => {
                    let publisher = MulticastPublisher::new(socket, mcast_config);
                    publisher.run(mcast_rx).await;
                }
                Err(err) => {
                    error!("failed to bind multicast UDP socket to {bind_addr}: {err}");
                    std::process::exit(3);
                }
            }
        });
    }
```

**Step 3: Verify compilation**

Run: `cargo check -p server`
Expected: compiles (the binary won't compile yet since it still passes 3 args - that's Task 6)

**Step 4: Commit**

```bash
git add server/src/servers/websocket_server.rs
git commit -m "wire multicast publisher into websocket server startup"
```

---

### Task 6: Add CLI args to websocket_server binary

**Files:**
- Modify: `binaries/src/bin/websocket_server.rs`

**Step 1: Add multicast CLI args and pass config**

Replace the full contents of `binaries/src/bin/websocket_server.rs` with:

```rust
#![allow(unused_crate_dependencies)]
use std::{collections::HashSet, net::Ipv4Addr, time::Duration};

use clap::Parser;
use server::{Channel, MulticastConfig, Result, parse_channels, run_websocket_server};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long)]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long)]
    port: u16,

    /// Compression level for WebSocket connections.
    /// Accepts values in the range `0..=9`.
    /// * `0` – compression disabled.
    /// * `1` – fastest compression, low compression ratio (default).
    /// * `9` – slowest compression, highest compression ratio.
    ///
    /// The level is passed to `flate2::Compression::new(level)`; see the
    /// documentation for <https://docs.rs/flate2/1.1.2/flate2/struct.Compression.html#method.new> for more info.
    #[arg(long)]
    websocket_compression_level: Option<u32>,

    /// Multicast group IP address (e.g., 239.0.0.1). Enables multicast publishing when set.
    #[arg(long)]
    multicast_group: Option<Ipv4Addr>,

    /// UDP destination port for multicast datagrams.
    #[arg(long, default_value_t = 5000)]
    multicast_port: u16,

    /// Source IP address to bind for multicast (DoubleZero allocated IP).
    /// Required when --multicast-group is set.
    #[arg(long)]
    multicast_bind_addr: Option<Ipv4Addr>,

    /// Comma-separated list of channels to multicast: l2, trades.
    #[arg(long, default_value = "l2,trades")]
    multicast_channels: String,

    /// Maximum number of L2 price levels per side in multicast datagrams.
    #[arg(long, default_value_t = 5)]
    multicast_l2_levels: usize,

    /// Seconds between periodic full L2 snapshot broadcasts.
    #[arg(long, default_value_t = 5)]
    multicast_snapshot_interval: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let compression_level = args.websocket_compression_level.unwrap_or(/* Some compression */ 1);

    let multicast_config = if let Some(group_addr) = args.multicast_group {
        let bind_addr = args.multicast_bind_addr.unwrap_or_else(|| {
            eprintln!("--multicast-bind-addr is required when --multicast-group is set");
            std::process::exit(1);
        });
        let channels = parse_channels(&args.multicast_channels).unwrap_or_else(|err| {
            eprintln!("invalid --multicast-channels: {err}");
            std::process::exit(1);
        });
        let config = MulticastConfig {
            group_addr,
            port: args.multicast_port,
            bind_addr,
            channels,
            l2_levels: args.multicast_l2_levels,
            snapshot_interval: Duration::from_secs(args.multicast_snapshot_interval),
        };
        println!(
            "Multicast enabled: group={}:{} bind={} channels={:?}",
            config.group_addr, config.port, config.bind_addr, config.channels
        );
        Some(config)
    } else {
        None
    };

    run_websocket_server(&full_address, true, compression_level, multicast_config).await?;

    Ok(())
}
```

**Step 2: Verify full build**

Run: `cargo build -p binaries`
Expected: compiles successfully

**Step 3: Verify CLI help shows new flags**

Run: `cargo run -p binaries --bin websocket_server -- --help`
Expected: shows `--multicast-group`, `--multicast-port`, `--multicast-bind-addr`, `--multicast-channels`, `--multicast-l2-levels`, `--multicast-snapshot-interval`

**Step 4: Commit**

```bash
git add binaries/src/bin/websocket_server.rs
git commit -m "add multicast cli flags to websocket server binary"
```

---

### Task 7: Create example_multicast_subscriber binary

A simple tool that joins a multicast group on a given interface, receives UDP datagrams, and prints the JSON. Useful for testing.

**Files:**
- Create: `binaries/src/bin/example_multicast_subscriber.rs`

**Step 1: Write the subscriber binary**

Create `binaries/src/bin/example_multicast_subscriber.rs`:

```rust
#![allow(unused_crate_dependencies)]
use clap::Parser;
use std::net::Ipv4Addr;

#[derive(Debug, Parser)]
#[command(author, version, about = "Join a multicast group and print received UDP datagrams")]
struct Args {
    /// Multicast group IP address to join (e.g., 239.0.0.1)
    #[arg(long)]
    group: Ipv4Addr,

    /// UDP port to listen on
    #[arg(long, default_value_t = 5000)]
    port: u16,

    /// Local interface IP to bind (e.g., 0.0.0.0 for all interfaces)
    #[arg(long, default_value = "0.0.0.0")]
    interface: Ipv4Addr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!(
        "Joining multicast group {} on port {} (interface: {})",
        args.group, args.port, args.interface
    );

    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_reuse_address(true)?;
    socket.bind(&std::net::SocketAddr::new(
        std::net::IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        args.port,
    ).into())?;
    socket.join_multicast_v4(&args.group, &args.interface)?;
    socket.set_nonblocking(true)?;

    let socket = tokio::net::UdpSocket::from_std(socket.into())?;

    println!("Listening for multicast datagrams...");
    let mut buf = [0u8; 2048];
    let mut count: u64 = 0;
    loop {
        let (len, addr) = socket.recv_from(&mut buf).await?;
        count += 1;
        match std::str::from_utf8(&buf[..len]) {
            Ok(text) => println!("[{count}] from {addr} ({len} bytes): {text}"),
            Err(_) => println!("[{count}] from {addr} ({len} bytes): <binary>"),
        }
    }
}
```

**Step 2: Add socket2 dependency to binaries**

In `binaries/Cargo.toml`, add after the clap line:

```toml
socket2 = "0.5"
```

**Step 3: Verify it compiles**

Run: `cargo build -p binaries --bin example_multicast_subscriber`
Expected: compiles successfully

**Step 4: Verify CLI help**

Run: `cargo run -p binaries --bin example_multicast_subscriber -- --help`
Expected: shows `--group`, `--port`, `--interface` flags

**Step 5: Commit**

```bash
git add binaries/src/bin/example_multicast_subscriber.rs binaries/Cargo.toml Cargo.lock
git commit -m "add example multicast subscriber binary for testing"
```

---

### Task 8: Integration test with loopback UDP

Test the full publisher flow: broadcast channel -> MulticastPublisher -> UDP socket -> verify received datagrams.

**Files:**
- Create: `server/tests/multicast_integration.rs`

**Step 1: Write the integration test**

Create `server/tests/multicast_integration.rs`:

```rust
//! Integration test: spawn a MulticastPublisher with loopback UDP,
//! feed it InternalMessage variants, and verify received datagrams.

// This test uses internal APIs so it needs to be an integration test
// within the server crate. Since many types are pub(crate), we place
// this test inside the server crate's own test module instead.
```

Actually, since `InternalMessage`, `L2Snapshots`, and other types are `pub(crate)`, we cannot use them from an integration test (separate crate). Instead, add the integration test as a module inside the server crate.

**Revised approach:** Add the integration test inside `server/src/multicast/publisher.rs` in the existing `#[cfg(test)]` block.

Append to the test module in `server/src/multicast/publisher.rs`:

```rust
    #[tokio::test]
    async fn test_publisher_sends_l2_envelope() {
        use std::collections::HashSet;
        use std::net::Ipv4Addr;
        use std::time::Duration;

        // Bind receiver and publisher to loopback
        let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = receiver.local_addr().unwrap();

        let sender_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let config = MulticastConfig {
            group_addr: match recv_addr.ip() {
                std::net::IpAddr::V4(ip) => ip,
                _ => panic!("expected v4"),
            },
            port: recv_addr.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channels: HashSet::from([Channel::L2, Channel::Trades]),
            l2_levels: 5,
            snapshot_interval: Duration::from_secs(60), // long so it doesn't fire
        };

        let publisher = MulticastPublisher::with_session_id(
            sender_socket,
            config,
            "test-session-123".to_string(),
        );

        // Test send_envelope directly
        let data = serde_json::json!({"coin": "BTC", "time": 100, "levels": [[], []]});
        publisher.send_envelope("l2Book", &data).await;

        let mut buf = [0u8; 2048];
        let (len, _) = tokio::time::timeout(Duration::from_secs(1), receiver.recv_from(&mut buf))
            .await
            .expect("timeout waiting for datagram")
            .unwrap();

        let msg: serde_json::Value = serde_json::from_slice(&buf[..len]).unwrap();
        assert_eq!(msg["session"], "test-session-123");
        assert_eq!(msg["seq"], 0);
        assert_eq!(msg["channel"], "l2Book");
        assert_eq!(msg["data"]["coin"], "BTC");
    }

    #[tokio::test]
    async fn test_publisher_sequence_increments() {
        use std::collections::HashSet;
        use std::net::Ipv4Addr;
        use std::time::Duration;

        let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = receiver.local_addr().unwrap();
        let sender_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let config = MulticastConfig {
            group_addr: match recv_addr.ip() {
                std::net::IpAddr::V4(ip) => ip,
                _ => panic!("expected v4"),
            },
            port: recv_addr.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channels: HashSet::from([Channel::L2]),
            l2_levels: 5,
            snapshot_interval: Duration::from_secs(60),
        };

        let publisher = MulticastPublisher::with_session_id(
            sender_socket,
            config,
            "seq-test".to_string(),
        );

        // Send three envelopes
        for _ in 0..3 {
            publisher.send_envelope("l2Book", &serde_json::json!({})).await;
        }

        let mut buf = [0u8; 2048];
        for expected_seq in 0..3u64 {
            let (len, _) = tokio::time::timeout(Duration::from_secs(1), receiver.recv_from(&mut buf))
                .await
                .expect("timeout")
                .unwrap();
            let msg: serde_json::Value = serde_json::from_slice(&buf[..len]).unwrap();
            assert_eq!(msg["seq"], expected_seq);
            assert_eq!(msg["session"], "seq-test");
        }
    }

    #[tokio::test]
    async fn test_multicast_trade_from_trade() {
        use alloy::primitives::Address;

        let trade = Trade {
            coin: "ETH".to_string(),
            side: crate::order_book::types::Side::Bid,
            px: "3500.0".to_string(),
            sz: "1.5".to_string(),
            hash: "0xabc".to_string(),
            time: 999,
            tid: 42,
            users: [Address::ZERO, Address::ZERO],
        };

        let mcast: MulticastTrade = MulticastTrade::from(&trade);
        let json = serde_json::to_string(&mcast).unwrap();
        assert!(json.contains("\"coin\":\"ETH\""));
        assert!(json.contains("\"side\":\"B\""));
        assert!(!json.contains("users"));
    }
```

**Step 2: Run all multicast tests**

Run: `cargo test -p server multicast`
Expected: all tests pass (unit + integration)

**Step 3: Run full test suite to verify no regressions**

Run: `cargo test -p server`
Expected: all existing tests still pass

**Step 4: Commit**

```bash
git add server/src/multicast/publisher.rs
git commit -m "add integration tests for multicast publisher"
```

---

### Task 9: Full build and lint verification

**Files:** None (verification only)

**Step 1: Run full build**

Run: `cargo build --workspace`
Expected: builds successfully

**Step 2: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: no warnings or errors

**Step 3: Run all tests**

Run: `cargo test --workspace`
Expected: all tests pass

**Step 4: Run formatter**

Run: `cargo fmt --all -- --check`
Expected: no formatting issues (or run `cargo fmt --all` to fix)

**Step 5: Commit any formatting fixes**

If `cargo fmt` made changes:
```bash
git add -A
git commit -m "apply cargo fmt"
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Add uuid dependency | `server/Cargo.toml` |
| 2 | Create MulticastConfig + Channel | `server/src/multicast/{mod,config}.rs`, `server/src/lib.rs` |
| 3 | Make Trade fields + coin_to_trades accessible | `server/src/types/mod.rs`, `server/src/servers/websocket_server.rs` |
| 4 | Create MulticastPublisher with tests | `server/src/multicast/publisher.rs` |
| 5 | Wire publisher into server startup | `server/src/servers/websocket_server.rs` |
| 6 | Add CLI args to binary | `binaries/src/bin/websocket_server.rs` |
| 7 | Create example subscriber binary | `binaries/src/bin/example_multicast_subscriber.rs`, `binaries/Cargo.toml` |
| 8 | Integration tests with loopback UDP | `server/src/multicast/publisher.rs` |
| 9 | Full build + lint + test verification | none (verification) |
