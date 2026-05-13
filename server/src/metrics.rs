use std::{net::SocketAddr, sync::OnceLock, time::Duration};

use axum::{
    Router,
    http::{StatusCode, header::CONTENT_TYPE},
    response::IntoResponse,
    routing::get as axum_get,
};
use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
};
use tokio::net::TcpListener;

pub struct Metrics {
    registry: Registry,
    listener_latency_seconds: HistogramVec,
    ingest_source_gossip_seconds: HistogramVec,
    ingest_file_tail_lag_seconds: GaugeVec,
    ingest_file_mtime_lag_seconds: HistogramVec,
    ingest_row_file_visibility_lag_seconds: HistogramVec,
    ingest_backlog_bytes: IntGaugeVec,
    tob_snapshot_compute_seconds: HistogramVec,
    tob_snapshot_enqueue_lag_seconds: HistogramVec,
    tob_snapshot_source_block_lag_seconds: HistogramVec,
    tob_snapshot_validator_write_lag_seconds: HistogramVec,
    tob_snapshot_listener_to_publisher_seconds: HistogramVec,
    tob_queue_delay_seconds: HistogramVec,
    tob_source_lag_seconds: HistogramVec,
    tob_socket_send_seconds: HistogramVec,
    tob_packets_total: IntCounterVec,
    tob_suppressed_total: IntCounterVec,
    tob_receiver_lag_total: IntCounterVec,
    dob_queue_delay_seconds: HistogramVec,
    dob_encode_seconds: HistogramVec,
    dob_socket_send_seconds: HistogramVec,
    dob_channel_drops_total: IntCounterVec,
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

fn register_histogram(registry: &Registry, opts: HistogramOpts, labels: &[&str]) -> HistogramVec {
    let metric = HistogramVec::new(opts, labels).expect("valid histogram metric");
    registry.register(Box::new(metric.clone())).expect("metric registered once");
    metric
}

fn register_counter(registry: &Registry, opts: Opts, labels: &[&str]) -> IntCounterVec {
    let metric = IntCounterVec::new(opts, labels).expect("valid counter metric");
    registry.register(Box::new(metric.clone())).expect("metric registered once");
    metric
}

pub fn get() -> &'static Metrics {
    METRICS.get_or_init(|| {
        let registry = Registry::new();
        let listener_latency_seconds = register_histogram(
            &registry,
            HistogramOpts::new("orderbook_listener_latency_seconds", "Listener-side latency by phase."),
            &["phase"],
        );
        let ingest_source_gossip_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "orderbook_ingest_source_gossip_seconds",
                "HL block timestamp to hl-node row local_time, by ingest source.",
            ),
            &["source"],
        );
        let ingest_file_tail_lag_seconds = GaugeVec::new(
            Opts::new(
                "orderbook_ingest_file_tail_lag_seconds",
                "Current wall-clock lag of the newest processed row local_time, by ingest source.",
            ),
            &["source"],
        )
        .expect("valid gauge metric");
        registry.register(Box::new(ingest_file_tail_lag_seconds.clone())).expect("metric registered once");
        let ingest_file_mtime_lag_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "orderbook_ingest_file_mtime_lag_seconds",
                "Observed lag between file modification time and publisher processing, by ingest source.",
            ),
            &["source"],
        );
        let ingest_row_file_visibility_lag_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "orderbook_ingest_row_file_visibility_lag_seconds",
                "File modification time minus row local_time, by ingest source.",
            ),
            &["source"],
        );
        let ingest_backlog_bytes = IntGaugeVec::new(
            Opts::new(
                "orderbook_ingest_backlog_bytes",
                "Current unread bytes remaining after a file drain, by ingest source.",
            ),
            &["source"],
        )
        .expect("valid gauge metric");
        registry.register(Box::new(ingest_backlog_bytes.clone())).expect("metric registered once");
        let tob_snapshot_compute_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "orderbook_tob_snapshot_compute_seconds",
                "Time spent computing L2 snapshots before TOB enqueue.",
            ),
            &["source"],
        );
        let tob_snapshot_enqueue_lag_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "orderbook_tob_snapshot_enqueue_lag_seconds",
                "TOB snapshot source lag at listener enqueue, before publisher queueing.",
            ),
            &["source"],
        );
        let tob_snapshot_source_block_lag_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "orderbook_tob_snapshot_source_block_lag_seconds",
                "Block-time lag between the emitted TOB snapshot height and the source row that triggered enqueue.",
            ),
            &["source"],
        );
        let tob_snapshot_validator_write_lag_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "orderbook_tob_snapshot_validator_write_lag_seconds",
                "HL source-row local_time minus block_time for rows that trigger TOB snapshot enqueue.",
            ),
            &["source"],
        );
        let tob_snapshot_listener_to_publisher_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "orderbook_tob_snapshot_listener_to_publisher_seconds",
                "Source-row local_time to TOB publisher receive time for snapshot messages.",
            ),
            &["source"],
        );
        let tob_queue_delay_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "orderbook_tob_queue_delay_seconds",
                "Time TOB messages spend in the internal publisher queue.",
            ),
            &["message_type"],
        );
        let tob_source_lag_seconds = register_histogram(
            &registry,
            HistogramOpts::new("orderbook_tob_source_lag_seconds", "TOB source lag at the freshness decision."),
            &["message_type", "decision"],
        );
        let tob_socket_send_seconds = register_histogram(
            &registry,
            HistogramOpts::new("orderbook_tob_socket_send_seconds", "Time spent sending TOB UDP packets."),
            &["channel"],
        );
        let tob_packets_total = register_counter(
            &registry,
            Opts::new("orderbook_tob_packets_total", "TOB multicast packets sent."),
            &["message_type"],
        );
        let tob_suppressed_total = register_counter(
            &registry,
            Opts::new("orderbook_tob_suppressed_total", "TOB marketdata messages suppressed by freshness checks."),
            &["message_type"],
        );
        let tob_receiver_lag_total = register_counter(
            &registry,
            Opts::new("orderbook_tob_receiver_lag_total", "TOB internal broadcast receiver lag."),
            &["kind"],
        );
        let dob_queue_delay_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "orderbook_dob_queue_delay_seconds",
                "Time DOB events spend in the internal publisher queue.",
            ),
            &["event_type"],
        );
        let dob_encode_seconds = register_histogram(
            &registry,
            HistogramOpts::new("orderbook_dob_encode_seconds", "Time spent encoding DOB events into frames."),
            &["event_type"],
        );
        let dob_socket_send_seconds = register_histogram(
            &registry,
            HistogramOpts::new("orderbook_dob_socket_send_seconds", "Time spent sending DOB UDP packets."),
            &["stream"],
        );
        let dob_channel_drops_total = register_counter(
            &registry,
            Opts::new("orderbook_dob_channel_drops_total", "DOB events dropped before reaching the emitter."),
            &["reason", "event_type"],
        );

        Metrics {
            registry,
            listener_latency_seconds,
            ingest_source_gossip_seconds,
            ingest_file_tail_lag_seconds,
            ingest_file_mtime_lag_seconds,
            ingest_row_file_visibility_lag_seconds,
            ingest_backlog_bytes,
            tob_snapshot_compute_seconds,
            tob_snapshot_enqueue_lag_seconds,
            tob_snapshot_source_block_lag_seconds,
            tob_snapshot_validator_write_lag_seconds,
            tob_snapshot_listener_to_publisher_seconds,
            tob_queue_delay_seconds,
            tob_source_lag_seconds,
            tob_socket_send_seconds,
            tob_packets_total,
            tob_suppressed_total,
            tob_receiver_lag_total,
            dob_queue_delay_seconds,
            dob_encode_seconds,
            dob_socket_send_seconds,
            dob_channel_drops_total,
        }
    })
}

pub fn observe_listener_latency(phase: &'static str, seconds: f64) {
    get().listener_latency_seconds.with_label_values(&[phase]).observe(seconds);
}

pub fn observe_ingest_source_gossip(source: &'static str, duration: Duration) {
    get().ingest_source_gossip_seconds.with_label_values(&[source]).observe(duration.as_secs_f64());
}

pub fn set_ingest_file_tail_lag(source: &'static str, duration: Duration) {
    get().ingest_file_tail_lag_seconds.with_label_values(&[source]).set(duration.as_secs_f64());
}

pub fn observe_ingest_file_mtime_lag(source: &'static str, duration: Duration) {
    get().ingest_file_mtime_lag_seconds.with_label_values(&[source]).observe(duration.as_secs_f64());
}

pub fn observe_ingest_row_file_visibility_lag(source: &'static str, duration: Duration) {
    get().ingest_row_file_visibility_lag_seconds.with_label_values(&[source]).observe(duration.as_secs_f64());
}

pub fn set_ingest_backlog_bytes(source: &'static str, bytes: u64) {
    let value = i64::try_from(bytes).unwrap_or(i64::MAX);
    get().ingest_backlog_bytes.with_label_values(&[source]).set(value);
}

pub fn observe_tob_snapshot_compute(source: &'static str, duration: Duration) {
    get().tob_snapshot_compute_seconds.with_label_values(&[source]).observe(duration.as_secs_f64());
}

pub fn observe_tob_snapshot_enqueue_lag(source: &'static str, duration: Duration) {
    get().tob_snapshot_enqueue_lag_seconds.with_label_values(&[source]).observe(duration.as_secs_f64());
}

pub fn observe_tob_snapshot_source_block_lag(source: &'static str, duration: Duration) {
    get().tob_snapshot_source_block_lag_seconds.with_label_values(&[source]).observe(duration.as_secs_f64());
}

pub fn observe_tob_snapshot_validator_write_lag(source: &'static str, duration: Duration) {
    get().tob_snapshot_validator_write_lag_seconds.with_label_values(&[source]).observe(duration.as_secs_f64());
}

pub fn observe_tob_snapshot_listener_to_publisher(source: &'static str, duration: Duration) {
    get().tob_snapshot_listener_to_publisher_seconds.with_label_values(&[source]).observe(duration.as_secs_f64());
}

pub fn observe_tob_queue_delay(message_type: &'static str, duration: Duration) {
    get().tob_queue_delay_seconds.with_label_values(&[message_type]).observe(duration.as_secs_f64());
}

pub fn observe_tob_source_lag(message_type: &'static str, decision: &'static str, duration: Duration) {
    get().tob_source_lag_seconds.with_label_values(&[message_type, decision]).observe(duration.as_secs_f64());
}

pub fn observe_tob_socket_send(channel: &'static str, duration: Duration) {
    get().tob_socket_send_seconds.with_label_values(&[channel]).observe(duration.as_secs_f64());
}

pub fn inc_tob_packet(message_type: &'static str) {
    get().tob_packets_total.with_label_values(&[message_type]).inc();
}

pub fn inc_tob_suppressed(message_type: &'static str) {
    get().tob_suppressed_total.with_label_values(&[message_type]).inc();
}

pub fn inc_tob_receiver_lag(kind: &'static str, count: u64) {
    get().tob_receiver_lag_total.with_label_values(&[kind]).inc_by(count);
}

pub fn observe_dob_queue_delay(event_type: &'static str, duration: Duration) {
    get().dob_queue_delay_seconds.with_label_values(&[event_type]).observe(duration.as_secs_f64());
}

pub fn observe_dob_encode(event_type: &'static str, duration: Duration) {
    get().dob_encode_seconds.with_label_values(&[event_type]).observe(duration.as_secs_f64());
}

pub fn observe_dob_socket_send(stream: &'static str, duration: Duration) {
    get().dob_socket_send_seconds.with_label_values(&[stream]).observe(duration.as_secs_f64());
}

pub fn inc_dob_channel_drop(reason: &'static str, event_type: &'static str) {
    get().dob_channel_drops_total.with_label_values(&[reason, event_type]).inc();
}

pub fn render() -> Result<String, prometheus::Error> {
    let encoder = TextEncoder::new();
    let metric_families = get().registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(String::from_utf8(buffer).expect("prometheus text encoder emits utf-8"))
}

async fn metrics_handler() -> impl IntoResponse {
    match render() {
        Ok(body) => ([(CONTENT_TYPE, TextEncoder::new().format_type().to_string())], body).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

pub async fn run_metrics_server(address: SocketAddr) -> crate::Result<()> {
    let app = Router::new().route("/metrics", axum_get(metrics_handler));
    let listener = TcpListener::bind(address).await?;
    log::info!("Prometheus metrics listening on http://{address}/metrics");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_expected_metric_families() {
        observe_listener_latency("gossip", 0.001);
        observe_ingest_source_gossip("diffs", Duration::from_millis(2));
        set_ingest_file_tail_lag("diffs", Duration::from_millis(3));
        observe_ingest_file_mtime_lag("diffs", Duration::from_millis(4));
        observe_ingest_row_file_visibility_lag("diffs", Duration::from_millis(5));
        set_ingest_backlog_bytes("diffs", 5);
        observe_tob_snapshot_compute("diffs", Duration::from_millis(6));
        observe_tob_snapshot_enqueue_lag("diffs", Duration::from_millis(7));
        observe_tob_queue_delay("snapshot", Duration::from_millis(2));
        observe_tob_source_lag("snapshot", "published", Duration::from_millis(3));
        observe_tob_socket_send("marketdata", Duration::from_micros(10));
        inc_tob_packet("quote");
        observe_dob_queue_delay("order_add", Duration::from_millis(1));
        observe_dob_encode("order_add", Duration::from_micros(5));
        observe_dob_socket_send("mktdata", Duration::from_micros(7));
        inc_dob_channel_drop("full", "order_add");

        let body = render().expect("metrics render");
        assert!(body.contains("orderbook_listener_latency_seconds"));
        assert!(body.contains("orderbook_ingest_source_gossip_seconds"));
        assert!(body.contains("orderbook_ingest_file_tail_lag_seconds"));
        assert!(body.contains("orderbook_ingest_file_mtime_lag_seconds"));
        assert!(body.contains("orderbook_ingest_row_file_visibility_lag_seconds"));
        assert!(body.contains("orderbook_ingest_backlog_bytes"));
        assert!(body.contains("orderbook_tob_snapshot_compute_seconds"));
        assert!(body.contains("orderbook_tob_snapshot_enqueue_lag_seconds"));
        assert!(body.contains("orderbook_tob_queue_delay_seconds"));
        assert!(body.contains("orderbook_dob_channel_drops_total"));
    }
}
