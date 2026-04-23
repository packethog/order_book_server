//! DZ Depth-of-Book multicast emitter.
//!
//! Receives `DobEvent`s from the L4 apply step via a bounded MPSC, packs them into
//! DoB frames per the wire spec, and sends on the mktdata / refdata sockets.
//! Phase 1 covers mktdata + refdata; the snapshot port is wired in phase 2.

use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::interval;

use crate::protocol::dob::constants::{
    BATCH_BOUNDARY_SIZE, DEFAULT_MTU, FRAME_HEADER_SIZE, HEARTBEAT_SIZE,
    ORDER_ADD_SIZE, ORDER_CANCEL_SIZE, ORDER_EXECUTE_SIZE,
};
use crate::protocol::dob::frame::DobFrameBuilder;
use crate::protocol::dob::messages::{
    BatchBoundary, OrderAdd, OrderCancel, OrderExecute,
    encode_batch_boundary, encode_heartbeat, encode_order_add, encode_order_cancel,
    encode_order_execute,
};

/// Events produced by the L4 apply step, consumed by the DoB emitter.
#[derive(Debug, Clone)]
pub enum DobEvent {
    OrderAdd(OrderAdd),
    OrderCancel(OrderCancel),
    OrderExecute(OrderExecute),
    BatchBoundary(BatchBoundary),
    /// Request a `Heartbeat` emission on mktdata (driven by the quiet-period timer).
    HeartbeatTick,
    /// Signals the emitter to flush and emit `EndOfSession` on shutdown.
    Shutdown,
}

/// Sender half handed to the L4 apply step and control tasks.
pub type DobEventSender = mpsc::Sender<DobEvent>;

/// Receiver half consumed by the emitter task.
pub type DobEventReceiver = mpsc::Receiver<DobEvent>;

/// Construct the bounded MPSC used between L4 apply and the DoB emitter.
/// Bound is a configuration knob; overflow drops and bumps `Reset Count`.
#[must_use]
pub fn channel(bound: usize) -> (DobEventSender, DobEventReceiver) {
    mpsc::channel(bound)
}

#[derive(Debug, Clone)]
pub struct DobMktdataConfig {
    pub group_addr: Ipv4Addr,
    pub port: u16,
    pub bind_addr: Ipv4Addr,
    pub channel_id: u8,
    pub mtu: u16,
    pub heartbeat_interval: Duration,
}

pub struct DobEmitter {
    config: DobMktdataConfig,
    socket: UdpSocket,
    seq: u64,
    reset_count: u8,
    current_frame: Option<DobFrameBuilder>,
}

impl DobEmitter {
    pub async fn bind(config: DobMktdataConfig) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(SocketAddrV4::new(config.bind_addr, 0)).await?;
        socket.set_multicast_loop_v4(false)?;
        socket.set_multicast_ttl_v4(1)?;
        // Connect so send() pushes to the configured multicast group:port
        socket.connect(SocketAddrV4::new(config.group_addr, config.port)).await?;
        Ok(Self {
            config,
            socket,
            seq: 0,
            reset_count: 0,
            current_frame: None,
        })
    }

    fn next_seq(&mut self) -> u64 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
    }

    fn now_ns() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }

    fn start_frame(&mut self) -> &mut DobFrameBuilder {
        let seq = self.next_seq();
        let ts = Self::now_ns();
        let fb = DobFrameBuilder::new(
            self.config.channel_id, seq, ts, self.reset_count, self.config.mtu,
        );
        self.current_frame = Some(fb);
        self.current_frame.as_mut().unwrap()
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        if let Some(mut fb) = self.current_frame.take() {
            if !fb.is_empty() {
                let frame = fb.finalize();
                self.socket.send(frame).await?;
            }
        }
        Ok(())
    }

    /// Write one event into the current frame; flush and open a new frame
    /// when the event wouldn't fit.
    async fn append(&mut self, event: &DobEvent) -> std::io::Result<()> {
        let size = event_size(event);
        if size == 0 {
            return Ok(()); // HeartbeatTick and Shutdown don't produce bytes here
        }

        // Ensure we have a frame, and that the event fits.
        if self.current_frame.as_ref().map_or(true, |fb| fb.remaining() < size) {
            self.flush().await?;
            self.start_frame();
        }

        let fb = self.current_frame.as_mut().unwrap();
        let buf = fb.message_buffer(size).expect("size checked above");
        match event {
            DobEvent::OrderAdd(msg) => encode_order_add(buf, msg),
            DobEvent::OrderCancel(msg) => encode_order_cancel(buf, msg),
            DobEvent::OrderExecute(msg) => encode_order_execute(buf, msg),
            DobEvent::BatchBoundary(msg) => encode_batch_boundary(buf, msg),
            _ => unreachable!("size filter should have returned"),
        }
        fb.commit_message();
        Ok(())
    }
}

fn event_size(event: &DobEvent) -> usize {
    match event {
        DobEvent::OrderAdd(_) => ORDER_ADD_SIZE,
        DobEvent::OrderCancel(_) => ORDER_CANCEL_SIZE,
        DobEvent::OrderExecute(_) => ORDER_EXECUTE_SIZE,
        DobEvent::BatchBoundary(_) => BATCH_BOUNDARY_SIZE,
        DobEvent::HeartbeatTick | DobEvent::Shutdown => 0,
    }
}

/// Runs the emitter loop until `rx` is closed or a `Shutdown` event is received.
pub async fn run_dob_emitter(
    mut emitter: DobEmitter,
    mut rx: DobEventReceiver,
) -> std::io::Result<()> {
    let mut heartbeat = interval(emitter.config.heartbeat_interval);
    heartbeat.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                if matches!(event, DobEvent::Shutdown) {
                    // Emit EndOfSession on mktdata before exiting so subscribers
                    // see a clean session close rather than a silent timeout.
                    use crate::protocol::dob::constants::END_OF_SESSION_SIZE;
                    if emitter.current_frame.as_ref().map_or(true, |fb| fb.remaining() < END_OF_SESSION_SIZE) {
                        emitter.flush().await?;
                        emitter.start_frame();
                    }
                    let fb = emitter.current_frame.as_mut().unwrap();
                    let buf = fb.message_buffer(END_OF_SESSION_SIZE).expect("size checked");
                    crate::protocol::dob::messages::encode_end_of_session(buf, DobEmitter::now_ns());
                    fb.commit_message();
                    emitter.flush().await?;
                    return Ok(());
                }
                if matches!(event, DobEvent::HeartbeatTick) {
                    // Writing a Heartbeat into the current frame, flushing first if needed.
                    if emitter.current_frame.as_ref().map_or(true, |fb| fb.remaining() < HEARTBEAT_SIZE) {
                        emitter.flush().await?;
                        emitter.start_frame();
                    }
                    let fb = emitter.current_frame.as_mut().unwrap();
                    let buf = fb.message_buffer(HEARTBEAT_SIZE).expect("size checked");
                    encode_heartbeat(buf, emitter.config.channel_id, DobEmitter::now_ns());
                    fb.commit_message();
                    emitter.flush().await?;
                    continue;
                }
                emitter.append(&event).await?;
            }
            _ = heartbeat.tick() => {
                // Opportunistic flush if we have pending messages; the quiet-period
                // heartbeat is emitted explicitly via HeartbeatTick, so this branch
                // just keeps the MTU-sized frame from sitting idle.
                emitter.flush().await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn channel_round_trip() {
        let (tx, mut rx) = channel(4);
        let msg = DobEvent::HeartbeatTick;
        tx.send(msg.clone()).await.unwrap();
        let received = rx.recv().await.unwrap();
        assert!(matches!(received, DobEvent::HeartbeatTick));
    }
}

#[cfg(test)]
mod emitter_tests {
    use super::*;

    fn sample_order_add(seq: u32) -> OrderAdd {
        OrderAdd {
            instrument_id: 42, source_id: 1, side: 0, order_flags: 0,
            per_instrument_seq: seq, order_id: seq as u64,
            enter_timestamp_ns: 1_700_000_000_000_000_000,
            price: 100, quantity: 1,
        }
    }

    async fn bind_receiver() -> (UdpSocket, SocketAddrV4) {
        let sock = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = match sock.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        (sock, addr)
    }

    #[tokio::test]
    async fn single_order_add_roundtrip_via_udp() {
        let (recv, recv_addr) = bind_receiver().await;
        let config = DobMktdataConfig {
            group_addr: *recv_addr.ip(),
            port: recv_addr.port(),
            bind_addr: Ipv4Addr::LOCALHOST,
            channel_id: 3,
            mtu: DEFAULT_MTU,
            heartbeat_interval: Duration::from_secs(60),
        };
        let mut emitter = DobEmitter::bind(config).await.unwrap();
        emitter.start_frame();
        let buf = emitter.current_frame.as_mut().unwrap().message_buffer(ORDER_ADD_SIZE).unwrap();
        encode_order_add(buf, &sample_order_add(1));
        emitter.current_frame.as_mut().unwrap().commit_message();
        emitter.flush().await.unwrap();

        let mut rxbuf = [0u8; 1500];
        let (n, _) = tokio::time::timeout(Duration::from_secs(1), recv.recv_from(&mut rxbuf))
            .await.unwrap().unwrap();
        assert_eq!(n, FRAME_HEADER_SIZE + ORDER_ADD_SIZE);
        assert_eq!(&rxbuf[0..2], &[0x44, 0x44], "DoB magic on wire");
        assert_eq!(rxbuf[FRAME_HEADER_SIZE], 0x10, "OrderAdd type");
    }
}
