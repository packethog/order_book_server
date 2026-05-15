use crate::types::node_data::EventSource;
use log::info;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

const REPORT_INTERVAL_MS: u64 = 10_000;

#[derive(Debug, Default)]
pub(super) struct IngestProgress {
    fills: SourceProgress,
    statuses: SourceProgress,
    diffs: SourceProgress,
    last_report_ms: u64,
}

#[derive(Debug, Default)]
struct SourceProgress {
    rows: u64,
    bytes: u64,
    parse_errors: u64,
    partial_deferrals: u64,
    unique_blocks: HashSet<u64>,
    latest_height: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceReport {
    rows: u64,
    blocks: usize,
    bytes: u64,
    parse_errors: u64,
    partial_deferrals: u64,
    latest_height: Option<u64>,
}

impl IngestProgress {
    pub(super) fn new() -> Self {
        Self { last_report_ms: now_epoch_ms(), ..Self::default() }
    }

    pub(super) const fn record_read(&mut self, source: EventSource, bytes: usize) {
        self.source_mut(source).bytes += bytes as u64;
    }

    pub(super) fn record_row(&mut self, source: EventSource, height: u64) {
        let stats = self.source_mut(source);
        stats.rows += 1;
        stats.unique_blocks.insert(height);
        stats.latest_height = Some(stats.latest_height.map_or(height, |current| current.max(height)));
    }

    pub(super) const fn record_jsonl_deferral(&mut self, source: EventSource) {
        let stats = self.source_mut(source);
        stats.parse_errors += 1;
        stats.partial_deferrals += 1;
    }

    pub(super) fn report_if_due(&mut self) -> bool {
        let now_ms = now_epoch_ms();
        if now_ms.saturating_sub(self.last_report_ms) < REPORT_INTERVAL_MS {
            return false;
        }
        self.last_report_ms = now_ms;
        self.report()
    }

    pub(super) fn report(&mut self) -> bool {
        let Some(line) = self.take_report_line() else {
            return false;
        };
        info!("{line}");
        true
    }

    fn take_report_line(&mut self) -> Option<String> {
        let statuses = self.statuses.take_report();
        let diffs = self.diffs.take_report();
        let fills = self.fills.take_report();
        if statuses.is_empty() && diffs.is_empty() && fills.is_empty() {
            return None;
        }
        Some(format!(
            "ingest progress | statuses {} | diffs {} | fills {}",
            format_source(statuses),
            format_source(diffs),
            format_source(fills),
        ))
    }

    const fn source_mut(&mut self, source: EventSource) -> &mut SourceProgress {
        match source {
            EventSource::Fills => &mut self.fills,
            EventSource::OrderStatuses => &mut self.statuses,
            EventSource::OrderDiffs => &mut self.diffs,
        }
    }
}

impl SourceProgress {
    fn take_report(&mut self) -> SourceReport {
        let report = SourceReport {
            rows: self.rows,
            blocks: self.unique_blocks.len(),
            bytes: self.bytes,
            parse_errors: self.parse_errors,
            partial_deferrals: self.partial_deferrals,
            latest_height: self.latest_height,
        };
        self.rows = 0;
        self.bytes = 0;
        self.parse_errors = 0;
        self.partial_deferrals = 0;
        self.unique_blocks.clear();
        report
    }
}

impl SourceReport {
    const fn is_empty(self) -> bool {
        self.rows == 0 && self.bytes == 0 && self.parse_errors == 0 && self.partial_deferrals == 0
    }
}

fn format_source(report: SourceReport) -> String {
    let latest = report.latest_height.map_or_else(|| "none".to_owned(), |height| height.to_string());
    format!(
        "rows={} blocks={} bytes={} latest={} parse_errors={} partial_deferrals={}",
        report.rows, report.blocks, report.bytes, latest, report.parse_errors, report.partial_deferrals,
    )
}

fn now_epoch_ms() -> u64 {
    u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::IngestProgress;
    use crate::types::node_data::EventSource;

    #[test]
    fn ingest_progress_counts_repeated_rows_without_losing_unique_blocks() {
        let mut progress = IngestProgress::new();

        progress.record_read(EventSource::OrderStatuses, 100);
        progress.record_row(EventSource::OrderStatuses, 42);
        progress.record_row(EventSource::OrderStatuses, 42);
        progress.record_row(EventSource::OrderStatuses, 43);
        progress.record_read(EventSource::OrderDiffs, 55);
        progress.record_row(EventSource::OrderDiffs, 41);
        progress.record_jsonl_deferral(EventSource::OrderDiffs);

        let line = progress.take_report_line().unwrap();

        assert!(line.contains("statuses rows=3 blocks=2 bytes=100 latest=43 parse_errors=0 partial_deferrals=0"));
        assert!(line.contains("diffs rows=1 blocks=1 bytes=55 latest=41 parse_errors=1 partial_deferrals=1"));
        assert!(line.contains("fills rows=0 blocks=0 bytes=0 latest=none parse_errors=0 partial_deferrals=0"));

        progress.record_row(EventSource::OrderStatuses, 44);
        let line = progress.take_report_line().unwrap();
        assert!(line.contains("statuses rows=1 blocks=1 bytes=0 latest=44"));
    }

    #[test]
    fn ingest_progress_preserves_latest_height_after_interval_reset() {
        let mut progress = IngestProgress::new();

        progress.record_row(EventSource::Fills, 10);
        let _line = progress.take_report_line().unwrap();
        progress.record_read(EventSource::Fills, 7);
        let line = progress.take_report_line().unwrap();

        assert!(line.contains("fills rows=0 blocks=0 bytes=7 latest=10 parse_errors=0 partial_deferrals=0"));
    }
}
