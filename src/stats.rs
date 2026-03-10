use std::sync::atomic::AtomicU64;

/// Per-logger write-path statistics (all counters are monotonically increasing).
#[derive(Debug, Default)]
pub struct Statistics {
    pub total_logs: AtomicU64,
    pub dropped_logs: AtomicU64,
    pub bytes_written: AtomicU64,
    pub write_errors: AtomicU64,
}

/// Uploader-side statistics.
#[derive(Debug, Default)]
pub struct UploaderStats {
    pub files_uploaded: AtomicU64,
    pub bytes_uploaded: AtomicU64,
    pub upload_errors: AtomicU64,
    pub retry_count: AtomicU64,
}
