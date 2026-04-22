# Multicast Support for Order Book Server

## Context

We are running a non-validating Hyperliquid node in Tokyo and want to multicast book updates into the DoubleZero network. The order book server currently reads node data from the file system and serves it over WebSocket. This design adds multicast publishing as an additional output path.

## Decisions

- **Dual output**: WebSocket server continues to run alongside multicast publisher in the same process
- **Channels**: L2 book and trades over multicast (configurable). L4 stays WebSocket-only (too large for UDP datagrams)
- **Wire format**: JSON, same structure as WebSocket messages, wrapped in an envelope with session ID and sequence number
- **Payload size**: Capped to fit within UDP MTU (~1400 bytes after GRE overhead). L2 levels limited to a configurable max (default 5 per side)
- **DoubleZero integration**: Assumes `doublezerod` is running separately and managing the GRE tunnel, PIM, and heartbeats. The order book server just sends UDP to the multicast group address
- **Group mapping**: Single multicast group for all channels, with a `channel` field in each message for subscriber-side filtering
- **Sequencing**: Monotonic sequence number + session UUID (regenerated on restart) so subscribers can detect gaps and restarts
- **Snapshots**: Periodic full L2 snapshots over multicast (configurable interval, default 5s) for gap recovery without needing WebSocket fallback
- **Coin filtering**: All coins published (no filtering)

## Architecture

```
Hyperliquid Node (file system)
        |
        v
  OrderBookListener
        |
        v
  broadcast channel (InternalMessage)
        |
        |--- WebSocket Server --- WS clients
        |
        '--- MulticastPublisher --- UDP socket --- DoubleZero multicast group
                                                    (via doublezerod GRE tunnel)
```

The `MulticastPublisher` subscribes to the existing `tokio::sync::broadcast` channel as an independent consumer, parallel to the WebSocket server. No changes to the listener or WebSocket server code.

## Message Format

Each UDP datagram contains a single JSON message:

```json
{
  "session": "a1b2c3d4-e5f6-...",
  "seq": 42,
  "channel": "l2Book",
  "data": { ... }
}
```

### L2 Book (`channel: "l2Book"`)

```json
{
  "session": "a1b2c3d4-...",
  "seq": 42,
  "channel": "l2Book",
  "data": {
    "coin": "BTC",
    "time": 1751427259657,
    "levels": [
      [{"px": "106217.0", "sz": "0.001", "n": 1}],
      [{"px": "106233.0", "sz": "0.267", "n": 3}]
    ]
  }
}
```

- One datagram per coin per snapshot cycle
- Number of levels capped to fit within MTU (configurable, default 5 per side)

### Trades (`channel: "trades"`)

```json
{
  "session": "a1b2c3d4-...",
  "seq": 43,
  "channel": "trades",
  "data": [
    {
      "coin": "BTC",
      "side": "A",
      "px": "106296.0",
      "sz": "0.00017",
      "time": 1751430933565,
      "hash": "0xde93...",
      "tid": 293353986402527
    }
  ]
}
```

- One datagram per batch of fills for a single coin
- Split across multiple datagrams if batch exceeds MTU
- `users` field omitted to reduce payload size

### Periodic L2 Snapshot (`channel: "l2Snapshot"`)

Same format as `l2Book` but sent on a timer (configurable, default every 5 seconds). Distinct channel name so subscribers can differentiate snapshots from incremental updates. Allows gap recovery without WebSocket fallback.

## Configuration

New CLI flags (multicast is opt-in, only active when `--multicast-group` is set):

```
--multicast-group 239.0.0.1          # Multicast group IP (required to enable)
--multicast-port 5000                # UDP destination port (default: 5000)
--multicast-bind-addr 9.169.90.100   # Source IP (DoubleZero allocated IP, required)
--multicast-channels l2,trades       # Channels to publish (default: l2,trades)
--multicast-l2-levels 5              # Max L2 levels per side (default: 5)
--multicast-snapshot-interval 5      # Seconds between full L2 snapshots (default: 5)
```

## Component Design

### New modules

- `server/src/multicast/mod.rs` - module root
- `server/src/multicast/publisher.rs` - `MulticastPublisher` struct and tokio task
- `server/src/multicast/config.rs` - `MulticastConfig` struct

### MulticastConfig

```rust
struct MulticastConfig {
    group_addr: Ipv4Addr,
    port: u16,
    bind_addr: Ipv4Addr,
    channels: HashSet<Channel>,
    l2_levels: usize,
    snapshot_interval: Duration,
}
```

### MulticastPublisher

```rust
struct MulticastPublisher {
    socket: UdpSocket,
    config: MulticastConfig,
    session_id: String,
    seq: AtomicU64,
}
```

The publisher runs as a tokio task with a select loop over the broadcast channel and a snapshot timer:

```rust
async fn run(self, mut rx: broadcast::Receiver<InternalMessage>) {
    let mut snapshot_timer = interval(self.config.snapshot_interval);
    let mut last_l2_snapshots = HashMap::new();

    loop {
        select! {
            msg = rx.recv() => {
                match msg {
                    Ok(InternalMessage::Snapshot(snapshots)) => {
                        last_l2_snapshots = snapshots;
                        if self.config.channels.contains(&L2) {
                            self.send_l2_updates(&snapshots).await;
                        }
                    }
                    Ok(InternalMessage::Fills(fills)) => {
                        if self.config.channels.contains(&Trades) {
                            self.send_trades(&fills).await;
                        }
                    }
                    Ok(InternalMessage::L4BookUpdates(_)) => { /* skip */ }
                    Err(RecvError::Lagged(n)) => {
                        warn!("multicast publisher lagged by {n} messages");
                    }
                    Err(RecvError::Closed) => break,
                }
            }
            _ = snapshot_timer.tick() => {
                if self.config.channels.contains(&L2) {
                    self.send_l2_snapshots(&last_l2_snapshots).await;
                }
            }
        }
    }
}
```

### Changes to existing code

1. `binaries/src/bin/websocket_server.rs` - add CLI args, construct config, spawn publisher task
2. `server/src/lib.rs` - export `multicast` module, modify `run_websocket_server` to optionally accept multicast config

No changes to `OrderBookListener`, `OrderBookState`, or WebSocket server logic.

### Error handling

- UDP `send_to` errors: logged at `warn`, message dropped
- Broadcast channel lag: logged, skipped messages lost (subscriber recovers via periodic snapshots)
- Serialization errors: logged at `error`

## Testing

### Unit tests

- JSON serialization matches expected format for L2 and trades messages
- Sequence numbering is monotonic with consistent session ID
- MTU enforcement: messages capped to configured max datagram size
- Config parsing from CLI args
- Channel filtering: only configured channels produce output

### Integration test

- Spawn publisher with loopback UDP socket
- Feed `InternalMessage` variants through broadcast channel
- Receive datagrams on listener socket, verify content, sequence numbers, session ID
- Verify periodic snapshots arrive at configured interval
- Verify lag handling

### Example binary

`binaries/src/bin/example_multicast_subscriber.rs` - joins a multicast group and prints received UDP datagrams. For testing and debugging.

## Deployment

Prerequisites:
1. Hyperliquid non-validating node running in Tokyo with block-batched output
2. DoubleZero multicast group created on-chain (`doublezero multicast group create`)
3. `doublezerod` running as publisher for the multicast group (manages GRE tunnel, PIM, heartbeats)
4. Order book server started with `--multicast-group` pointing to the allocated multicast IP

Subscribers:
1. Create DoubleZero user with subscriber role for the multicast group
2. Run `doublezerod` as subscriber
3. Application joins multicast group via standard socket API and receives UDP datagrams
