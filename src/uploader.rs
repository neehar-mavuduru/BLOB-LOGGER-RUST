use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use regex::Regex;
use tokio::io::AsyncReadExt;
use tokio::sync::watch;

use crate::config::GcsUploadConfig;
use crate::error::Error;
use crate::stats::UploaderStats;

/// Poll-based uploader that discovers sealed log files in `upload_ready/`
/// and streams them to GCS via `object_store::put_multipart`.
#[derive(Debug)]
pub struct Uploader {
    config: GcsUploadConfig,
    upload_ready_dir: PathBuf,
    store: Arc<dyn ObjectStore>,
    stats: Arc<UploaderStats>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Uploader {
    /// Creates an uploader using ADC-based GCS credentials.
    pub fn new_gcs(
        upload_ready_dir: PathBuf,
        config: GcsUploadConfig,
    ) -> Result<Self, Error> {
        let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(&config.bucket)
            .build()
            .map_err(|e| Error::Gcs(format!("failed to build GCS client: {e}")))?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Ok(Self {
            config,
            upload_ready_dir,
            store: Arc::new(store),
            stats: Arc::new(UploaderStats::default()),
            shutdown_tx,
            shutdown_rx,
            task_handle: None,
        })
    }

    /// Creates an uploader with an injected object store (for testing).
    pub fn with_store(
        upload_ready_dir: PathBuf,
        config: GcsUploadConfig,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Self {
            config,
            upload_ready_dir,
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
        let upload_ready_dir = self.upload_ready_dir.clone();
        let store = self.store.clone();
        let stats = self.stats.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();

        let handle = tokio::spawn(async move {
            // Crash-recovery scan
            Self::scan_and_upload_inner(&upload_ready_dir, &store, &config, &stats).await;

            let mut interval = tokio::time::interval(config.poll_interval);
            interval.tick().await; // first tick is immediate, skip it

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::scan_and_upload_inner(&upload_ready_dir, &store, &config, &stats).await;
                    }
                    _ = shutdown_rx.changed() => {
                        // Final drain
                        Self::scan_and_upload_inner(&upload_ready_dir, &store, &config, &stats).await;
                        break;
                    }
                }
            }
        });

        self.task_handle = Some(handle);
    }

    async fn scan_and_upload_inner(
        upload_ready_dir: &PathBuf,
        store: &Arc<dyn ObjectStore>,
        config: &GcsUploadConfig,
        stats: &Arc<UploaderStats>,
    ) {
        let entries = match std::fs::read_dir(upload_ready_dir) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("failed to read upload_ready dir: {}", e);
                return;
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();

            if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                continue;
            }

            let metadata = match std::fs::symlink_metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };

            if !metadata.file_type().is_symlink() {
                continue;
            }

            let real_path = match std::fs::read_link(&path) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("failed to read symlink {:?}: {}", path, e);
                    continue;
                }
            };

            if let Err(e) =
                Self::upload_file_with_retry(&real_path, &path, store, config, stats).await
            {
                tracing::error!("upload failed for {:?}: {}", real_path, e);
            }
        }
    }

    async fn upload_file_with_retry(
        real_path: &PathBuf,
        symlink_path: &PathBuf,
        store: &Arc<dyn ObjectStore>,
        config: &GcsUploadConfig,
        stats: &Arc<UploaderStats>,
    ) -> Result<(), Error> {
        // Bug 3 fix: skip empty files
        let file_size = match std::fs::metadata(real_path) {
            Ok(m) => m.len(),
            Err(e) => {
                tracing::warn!("cannot stat {:?}: {}, treating as dangling symlink", real_path, e);
                stats.upload_errors.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        };

        if file_size == 0 {
            tracing::warn!("skipping empty file: {:?}", real_path);
            let _ = std::fs::remove_file(real_path);
            let _ = std::fs::remove_file(symlink_path);
            return Ok(());
        }

        let filename = real_path
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| Error::Uploader("invalid filename".into()))?;

        let object_name = Self::derive_gcs_path(filename, &config.object_prefix)?;

        for attempt in 0..config.max_retries {
            match Self::do_upload(real_path, &object_name, store, config).await {
                Ok(()) => {
                    // Verify size
                    let object_path = object_store::path::Path::from(object_name.as_str());
                    match store.head(&object_path).await {
                        Ok(meta) => {
                            if meta.size != file_size as usize {
                                tracing::warn!(
                                    "size mismatch: local={} remote={} for {:?}",
                                    file_size, meta.size, real_path
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!("head check failed: {}", e);
                        }
                    }

                    let _ = std::fs::remove_file(symlink_path);
                    let _ = std::fs::remove_file(real_path);
                    stats.files_uploaded.fetch_add(1, Ordering::Relaxed);
                    stats.bytes_uploaded.fetch_add(file_size, Ordering::Relaxed);
                    tracing::info!("uploaded {:?} → {}", filename, object_name);
                    return Ok(());
                }
                Err(e) => {
                    stats.retry_count.fetch_add(1, Ordering::Relaxed);
                    let backoff = Duration::from_secs(1 << attempt);
                    tracing::warn!(
                        "upload attempt {} failed for {:?}: {}; retrying in {:?}",
                        attempt + 1, real_path, e, backoff
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }

        // Permanent failure
        stats.upload_errors.fetch_add(1, Ordering::Relaxed);
        tracing::error!(
            "upload permanently failed for {:?} after {} retries",
            real_path, config.max_retries
        );
        Ok(())
    }

    async fn do_upload(
        real_path: &PathBuf,
        object_name: &str,
        store: &Arc<dyn ObjectStore>,
        _config: &GcsUploadConfig,
    ) -> Result<(), Error> {
        let object_path = object_store::path::Path::from(object_name);

        let mut file = tokio::fs::File::open(real_path)
            .await
            .map_err(Error::Io)?;

        let file_size = file
            .metadata()
            .await
            .map_err(Error::Io)?
            .len();

        let mut data = Vec::with_capacity(file_size as usize);
        file.read_to_end(&mut data)
            .await
            .map_err(Error::Io)?;

        store
            .put(&object_path, bytes::Bytes::from(data).into())
            .await
            .map_err(|e| Error::Gcs(format!("put failed: {e}")))?;

        Ok(())
    }

    /// Derives GCS object path from filename.
    /// Pattern: `{prefix}{event_name}/{date}/{hour}/{filename}`
    fn derive_gcs_path(filename: &str, prefix: &str) -> Result<String, Error> {
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
