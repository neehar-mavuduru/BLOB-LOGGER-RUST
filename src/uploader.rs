use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use object_store::{MultipartUpload, ObjectStore, PutMultipartOpts};
use regex::Regex;
use tokio::io::AsyncReadExt;
use tokio::sync::watch;

use crate::config::GcsUploadConfig;
use crate::error::Error;
use crate::stats::UploaderStats;

/// Poll-based uploader that discovers sealed `.log` files in the logs directory
/// and uploads them to GCS.
#[derive(Debug)]
pub struct Uploader {
    config: GcsUploadConfig,
    scan_dir: PathBuf,
    store: Arc<dyn ObjectStore>,
    stats: Arc<UploaderStats>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Uploader {
    /// Creates an uploader using ADC-based GCS credentials.
    pub fn new_gcs(
        scan_dir: PathBuf,
        config: GcsUploadConfig,
    ) -> Result<Self, Error> {
        let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(&config.bucket)
            .build()
            .map_err(|e| Error::Gcs(format!("failed to build GCS client: {e}")))?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Ok(Self {
            config,
            scan_dir,
            store: Arc::new(store),
            stats: Arc::new(UploaderStats::default()),
            shutdown_tx,
            shutdown_rx,
            task_handle: None,
        })
    }

    /// Creates an uploader with an injected object store (for testing).
    pub fn with_store(
        scan_dir: PathBuf,
        config: GcsUploadConfig,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Self {
            config,
            scan_dir,
            store,
            stats: Arc::new(UploaderStats::default()),
            shutdown_tx,
            shutdown_rx,
            task_handle: None,
        }
    }

    /// Starts the background poll worker.
    pub fn start(&mut self) {
        let config = self.config.clone();
        let scan_dir = self.scan_dir.clone();
        let store = self.store.clone();
        let stats = self.stats.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();

        let handle = tokio::spawn(async move {
            // Crash-recovery scan: pick up leftover .log files from previous shutdown
            Self::scan_and_upload_inner(&scan_dir, &store, &config, &stats).await;

            let mut interval = tokio::time::interval(config.poll_interval);
            interval.tick().await; // first tick is immediate, skip it

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::scan_and_upload_inner(&scan_dir, &store, &config, &stats).await;
                    }
                    _ = shutdown_rx.changed() => {
                        // Final drain
                        Self::scan_and_upload_inner(&scan_dir, &store, &config, &stats).await;
                        break;
                    }
                }
            }
        });

        self.task_handle = Some(handle);
    }

    async fn scan_and_upload_inner(
        scan_dir: &PathBuf,
        store: &Arc<dyn ObjectStore>,
        config: &GcsUploadConfig,
        stats: &Arc<UploaderStats>,
    ) {
        let entries = match std::fs::read_dir(scan_dir) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("failed to read scan dir: {}", e);
                return;
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();

            // Skip directories
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(true) {
                continue;
            }

            // Only process sealed .log files (skip .tmp files still being written)
            match path.extension().and_then(|e| e.to_str()) {
                Some("log") => {}
                _ => continue,
            }

            if let Err(e) =
                Self::upload_file_with_retry(&path, store, config, stats).await
            {
                tracing::error!("upload failed for {:?}: {}", path, e);
            }
        }
    }

    async fn upload_file_with_retry(
        file_path: &PathBuf,
        store: &Arc<dyn ObjectStore>,
        config: &GcsUploadConfig,
        stats: &Arc<UploaderStats>,
    ) -> Result<(), Error> {
        let file_size = match std::fs::metadata(file_path) {
            Ok(m) => m.len(),
            Err(e) => {
                tracing::warn!("cannot stat {:?}: {}, skipping", file_path, e);
                stats.upload_errors.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        };

        if file_size == 0 {
            tracing::warn!("skipping empty file: {:?}", file_path);
            let _ = std::fs::remove_file(file_path);
            return Ok(());
        }

        let filename = file_path
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| Error::Uploader("invalid filename".into()))?;

        let object_name = Self::derive_gcs_path(filename, &config.object_prefix)?;
        crate::metrics::count("blob_logger.upload_file", 1, &[]);

        for attempt in 0..config.max_retries {
            let upload_start = std::time::Instant::now();
            match Self::do_upload(file_path, &object_name, store, config).await {
                Ok(()) => {
                    crate::metrics::timing("blob_logger.upload_file_duration", upload_start.elapsed(), &[]);

                    // Verify size
                    let object_path = object_store::path::Path::from(object_name.as_str());
                    match store.head(&object_path).await {
                        Ok(meta) => {
                            if meta.size != file_size as usize {
                                tracing::warn!(
                                    "size mismatch: local={} remote={} for {:?}",
                                    file_size, meta.size, file_path
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!("head check failed: {}", e);
                        }
                    }

                    let _ = std::fs::remove_file(file_path);
                    stats.files_uploaded.fetch_add(1, Ordering::Relaxed);
                    stats.bytes_uploaded.fetch_add(file_size, Ordering::Relaxed);
                    crate::metrics::count("blob_logger.upload_bytes", file_size as i64, &[]);
                    tracing::info!("uploaded {:?} → {}", filename, object_name);
                    return Ok(());
                }
                Err(e) => {
                    stats.retry_count.fetch_add(1, Ordering::Relaxed);
                    let backoff = Duration::from_secs(1 << attempt);
                    tracing::warn!(
                        "upload attempt {} failed for {:?}: {}; retrying in {:?}",
                        attempt + 1, file_path, e, backoff
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }

        // Permanent failure
        stats.upload_errors.fetch_add(1, Ordering::Relaxed);
        crate::metrics::count("blob_logger.upload_file_failed", 1, &[]);
        tracing::error!(
            "upload permanently failed for {:?} after {} retries",
            file_path, config.max_retries
        );
        Ok(())
    }

    /// Uploads a file using streaming multipart upload.
    /// Reads the file in `chunk_size` chunks to avoid loading it entirely into memory.
    async fn do_upload(
        file_path: &PathBuf,
        object_name: &str,
        store: &Arc<dyn ObjectStore>,
        config: &GcsUploadConfig,
    ) -> Result<(), Error> {
        let object_path = object_store::path::Path::from(object_name);

        let mut file = tokio::fs::File::open(file_path)
            .await
            .map_err(Error::Io)?;

        let mut upload = store
            .put_multipart_opts(&object_path, PutMultipartOpts::default())
            .await
            .map_err(|e| Error::Gcs(format!("put_multipart failed: {e}")))?;

        let mut buf = vec![0u8; config.chunk_size];
        loop {
            let n = file.read(&mut buf).await.map_err(Error::Io)?;
            if n == 0 {
                break;
            }
            upload
                .put_part(bytes::Bytes::copy_from_slice(&buf[..n]).into())
                .await
                .map_err(|e| {
                    Error::Gcs(format!("put_part failed: {e}"))
                })?;
        }

        upload
            .complete()
            .await
            .map_err(|e| Error::Gcs(format!("complete failed: {e}")))?;

        Ok(())
    }

    /// Derives GCS object path from filename.
    /// Pattern: `{prefix}{event_name}/{date}/{hour}/{filename}`
    ///
    /// Supports two filename formats:
    ///   - With pod: `{event}--{podname}_{YYYY-MM-DD_HH-MM-SS}_{seq}.log`
    ///   - Without pod: `{event}_{YYYY-MM-DD_HH-MM-SS}_{seq}.log`
    fn derive_gcs_path(filename: &str, prefix: &str) -> Result<String, Error> {
        // Try pod-name format first: {event}--{pod}_{date}_{time}_{seq}.log
        let pod_re = Regex::new(r"^(.+?)--(.+?)_(\d{4}-\d{2}-\d{2})_(\d{2})-\d{2}-\d{2}(?:_\d+)?\.log$")
            .map_err(|e| Error::Uploader(format!("regex error: {e}")))?;

        if let Some(caps) = pod_re.captures(filename) {
            let event_name = &caps[1];
            let date = &caps[3];
            let hour = &caps[4];
            return Ok(format!("{prefix}{event_name}/{date}/{hour}/{filename}"));
        }

        // Fallback to no-pod format: {event}_{date}_{time}_{seq}.log
        let re = Regex::new(r"^(.+?)_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})(?:_\d+)?\.log$")
            .map_err(|e| Error::Uploader(format!("regex error: {e}")))?;

        if let Some(caps) = re.captures(filename) {
            let event_name = &caps[1];
            let ts_str = &caps[2];

            let dt = chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%d_%H-%M-%S");
            let (date, hour) = match dt {
                Ok(dt) => (
                    dt.format("%Y-%m-%d").to_string(),
                    dt.format("%H").to_string(),
                ),
                Err(_) => {
                    tracing::warn!("failed to parse timestamp from filename: {}", filename);
                    let now = chrono::Local::now();
                    (
                        now.format("%Y-%m-%d").to_string(),
                        now.format("%H").to_string(),
                    )
                }
            };

            Ok(format!("{prefix}{event_name}/{date}/{hour}/{filename}"))
        } else {
            tracing::warn!("filename does not match expected pattern: {}", filename);
            let now = chrono::Local::now();
            let date = now.format("%Y-%m-%d").to_string();
            let hour = now.format("%H").to_string();
            let event = filename
                .strip_suffix(".log")
                .unwrap_or(filename);
            Ok(format!("{prefix}{event}/{date}/{hour}/{filename}"))
        }
    }

    /// Signals shutdown and waits for the poll worker to drain.
    pub async fn stop(&mut self) -> Result<(), Error> {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.task_handle.take() {
            handle
                .await
                .map_err(|e| Error::Uploader(format!("join error: {e}")))?;
        }
        Ok(())
    }

    /// Returns uploader statistics.
    pub fn stats(&self) -> &Arc<UploaderStats> {
        &self.stats
    }

    /// Exposed for testing: derive GCS path from filename.
    pub fn gcs_path_from_filename(filename: &str, prefix: &str) -> Result<String, Error> {
        Self::derive_gcs_path(filename, prefix)
    }
}
