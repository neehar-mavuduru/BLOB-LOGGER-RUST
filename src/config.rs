use std::path::PathBuf;
use std::time::Duration;

use crate::error::Error;

/// Top-level configuration for a logger instance.
#[derive(Debug, Clone)]
pub struct Config {
    /// Number of write shards for lock-free dispatch.
    pub num_shards: usize,
    /// Total buffer memory across all shards (bytes).
    pub buffer_size: usize,
    /// Maximum file size before rotation (bytes).
    pub max_file_size: u64,
    /// Base directory; `logs/` and `upload_ready/` are created inside.
    pub log_file_path: PathBuf,
    /// Timer-based flush interval (default: 5 minutes).
    pub flush_interval: Duration,
    /// Optional GCS upload configuration.
    pub gcs_config: Option<GcsUploadConfig>,
}

/// GCS upload settings used by the `Uploader`.
#[derive(Debug, Clone)]
pub struct GcsUploadConfig {
    pub bucket: String,
    /// Object key prefix, e.g. `"service-name/"`.
    pub object_prefix: String,
    /// Multipart chunk size (default: 32 MiB).
    pub chunk_size: usize,
    /// Maximum upload retry attempts (default: 3).
    pub max_retries: u32,
    /// Interval between `upload_ready/` directory scans (default: 3 s).
    pub poll_interval: Duration,
}

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(300);
const MIN_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const MIN_SHARD_CAPACITY: usize = 65536;
const ALIGNMENT: usize = 4096;
const DEFAULT_CHUNK_SIZE: usize = 32 * 1024 * 1024;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(3);

impl Config {
    /// Validates and normalises the configuration, returning an error for invalid values.
    pub fn validate(&mut self) -> Result<(), Error> {
        if self.num_shards == 0 {
            return Err(Error::Config("num_shards must be >= 1".into()));
        }
        if self.buffer_size == 0 {
            return Err(Error::Config("buffer_size must be > 0".into()));
        }
        if self.max_file_size == 0 {
            return Err(Error::Config("max_file_size must be > 0".into()));
        }
        if self.log_file_path.as_os_str().is_empty() {
            return Err(Error::Config("log_file_path must not be empty".into()));
        }

        // Halve num_shards until per-shard capacity >= MIN_SHARD_CAPACITY
        while self.num_shards > 1 && self.buffer_size / self.num_shards < MIN_SHARD_CAPACITY {
            let old = self.num_shards;
            self.num_shards /= 2;
            if self.num_shards == 0 {
                self.num_shards = 1;
            }
            tracing::warn!(
                old_shards = old,
                new_shards = self.num_shards,
                "reduced num_shards so per-shard capacity >= 64 KiB"
            );
        }

        // Round up per-shard capacity to 4096 alignment
        let per_shard = self.buffer_size / self.num_shards;
        let aligned = (per_shard + ALIGNMENT - 1) & !(ALIGNMENT - 1);
        self.buffer_size = aligned * self.num_shards;

        // Flush interval defaults
        if self.flush_interval.is_zero() {
            self.flush_interval = DEFAULT_FLUSH_INTERVAL;
        }
        if self.flush_interval < MIN_FLUSH_INTERVAL {
            return Err(Error::Config(
                "flush_interval must be >= 1 second".into(),
            ));
        }

        // GCS defaults
        if let Some(ref mut gcs) = self.gcs_config {
            if gcs.chunk_size == 0 {
                gcs.chunk_size = DEFAULT_CHUNK_SIZE;
            }
            if gcs.max_retries == 0 {
                gcs.max_retries = DEFAULT_MAX_RETRIES;
            }
            if gcs.poll_interval.is_zero() {
                gcs.poll_interval = DEFAULT_POLL_INTERVAL;
            }
        }

        Ok(())
    }
}
