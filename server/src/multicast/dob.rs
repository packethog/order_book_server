//! DZ Depth-of-Book multicast emitter.
//!
//! Receives `DobEvent`s from the L4 apply step via a bounded MPSC, packs them into
//! DoB frames per the wire spec, and sends on the mktdata / refdata sockets.
//! Phase 1 covers mktdata + refdata; the snapshot port is wired in phase 2.

use tokio::sync::mpsc;

use crate::protocol::dob::messages::{BatchBoundary, OrderAdd, OrderCancel, OrderExecute};

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
