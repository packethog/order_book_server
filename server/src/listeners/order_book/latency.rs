use log::info;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

/// Lightweight latency tracker for the inotify → process → broadcast pipeline.
///
/// All operations are lock-free (atomics only). Hot-path cost is one
/// `clock_gettime(CLOCK_MONOTONIC)` (~20 ns via vDSO) plus relaxed atomic
/// stores.
///
/// Call [`LatencyStats::report`] periodically to log a summary and reset.
pub(super) struct LatencyStats {
    // gossip: local_time - block_time (ms) — validator → hl-node write
    gossip_min: AtomicU64,
    gossip_max: AtomicU64,
    gossip_sum: AtomicU64,

    // hl_to_wake: t_wakeup - local_time (ms) — hl-node disk write → our inotify wakeup
    hl_to_wake_min: AtomicU64,
    hl_to_wake_max: AtomicU64,
    hl_to_wake_sum: AtomicU64,

    // process: t_done - t_wakeup (µs, monotonic) — lock + file read + deser + book apply
    process_min: AtomicU64,
    process_max: AtomicU64,
    process_sum: AtomicU64,

    // e2e: t_done - block_time (ms) — consensus → our output ready
    e2e_min: AtomicU64,
    e2e_max: AtomicU64,
    e2e_sum: AtomicU64,

    count: AtomicU64,
    last_report_ms: AtomicU64,
}

const SENTINEL_MIN: u64 = u64::MAX;
const REPORT_INTERVAL_MS: u64 = 10_000;

impl LatencyStats {
    pub(super) fn new() -> Self {
        Self {
            gossip_min: AtomicU64::new(SENTINEL_MIN),
            gossip_max: AtomicU64::new(0),
            gossip_sum: AtomicU64::new(0),
            hl_to_wake_min: AtomicU64::new(SENTINEL_MIN),
            hl_to_wake_max: AtomicU64::new(0),
            hl_to_wake_sum: AtomicU64::new(0),
            process_min: AtomicU64::new(SENTINEL_MIN),
            process_max: AtomicU64::new(0),
            process_sum: AtomicU64::new(0),
            e2e_min: AtomicU64::new(SENTINEL_MIN),
            e2e_max: AtomicU64::new(0),
            e2e_sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            last_report_ms: AtomicU64::new(now_epoch_ms()),
        }
    }

    /// Record one observation.
    ///
    /// - `t_wakeup` / `t_done`: monotonic instants bracketing our processing
    /// - `block_time_ms`: validator consensus timestamp (ms since epoch)
    /// - `local_time_ms`: hl-node wall-clock when it wrote the batch (ms since epoch)
    pub(super) fn record(&self, t_wakeup: Instant, t_done: Instant, block_time_ms: u64, local_time_ms: u64) {
        let gossip_ms = local_time_ms.saturating_sub(block_time_ms);

        // Wall-clock at wakeup for comparison with local_time.
        // We reconstruct it from t_done (which is ~now) minus the monotonic span.
        let process_duration = t_done.duration_since(t_wakeup);
        let now_ms = now_epoch_ms();
        let wakeup_ms = now_ms.saturating_sub(process_duration.as_millis() as u64);
        let hl_to_wake_ms = wakeup_ms.saturating_sub(local_time_ms);

        let process_us = process_duration.as_micros() as u64;
        let e2e_ms = now_ms.saturating_sub(block_time_ms);

        update_bucket(&self.gossip_min, &self.gossip_max, &self.gossip_sum, gossip_ms);
        update_bucket(&self.hl_to_wake_min, &self.hl_to_wake_max, &self.hl_to_wake_sum, hl_to_wake_ms);
        update_bucket(&self.process_min, &self.process_max, &self.process_sum, process_us);
        update_bucket(&self.e2e_min, &self.e2e_max, &self.e2e_sum, e2e_ms);

        self.count.fetch_add(1, Relaxed);

        // Auto-report every REPORT_INTERVAL_MS so we don't depend on the
        // ticker branch of select! (which can be starved by event floods).
        let last = self.last_report_ms.load(Relaxed);
        if now_ms.saturating_sub(last) >= REPORT_INTERVAL_MS {
            // CAS to avoid duplicate reports from concurrent calls
            if self.last_report_ms.compare_exchange(last, now_ms, Relaxed, Relaxed).is_ok() {
                self.report();
            }
        }
    }

    /// Log a summary line and reset all counters.  Returns sample count.
    pub(super) fn report(&self) -> u64 {
        let n = self.count.swap(0, Relaxed);
        if n == 0 {
            return 0;
        }

        let gossip = take_bucket(&self.gossip_min, &self.gossip_max, &self.gossip_sum);
        let hl_to_wake = take_bucket(&self.hl_to_wake_min, &self.hl_to_wake_max, &self.hl_to_wake_sum);
        let process = take_bucket(&self.process_min, &self.process_max, &self.process_sum);
        let e2e = take_bucket(&self.e2e_min, &self.e2e_max, &self.e2e_sum);

        info!(
            "latency n={n} | \
             gossip ms min/avg/max={}/{}/{} | \
             hl→wake ms min/avg/max={}/{}/{} | \
             process µs min/avg/max={}/{}/{} | \
             e2e ms min/avg/max={}/{}/{}",
            gossip.0,
            gossip.2 / n,
            gossip.1,
            hl_to_wake.0,
            hl_to_wake.2 / n,
            hl_to_wake.1,
            process.0,
            process.2 / n,
            process.1,
            e2e.0,
            e2e.2 / n,
            e2e.1,
        );

        n
    }
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn update_bucket(min: &AtomicU64, max: &AtomicU64, sum: &AtomicU64, val: u64) {
    let _ = min.fetch_update(Relaxed, Relaxed, |cur| if val < cur { Some(val) } else { None });
    let _ = max.fetch_update(Relaxed, Relaxed, |cur| if val > cur { Some(val) } else { None });
    sum.fetch_add(val, Relaxed);
}

fn take_bucket(min: &AtomicU64, max: &AtomicU64, sum: &AtomicU64) -> (u64, u64, u64) {
    let mn = min.swap(SENTINEL_MIN, Relaxed);
    let mx = max.swap(0, Relaxed);
    let s = sum.swap(0, Relaxed);
    // If min was never set (still sentinel), report 0
    (if mn == SENTINEL_MIN { 0 } else { mn }, mx, s)
}
