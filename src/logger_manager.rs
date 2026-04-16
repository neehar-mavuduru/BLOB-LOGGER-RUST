use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;

use crate::config::Config;
use crate::error::Error;
use crate::logger::Logger;
use crate::stats::{Statistics, UploaderStats};
use crate::uploader::Uploader;

/// Manages per-event loggers with concurrent routing via `DashMap`.
#[derive(Debug)]
pub struct LoggerManager {
    loggers: DashMap<String, Arc<parking_lot::Mutex<Logger>>>,
    #[allow(dead_code)]
    base_dir: PathBuf,
    logs_dir: PathBuf,
    config: Config,
    uploader: Option<Arc<tokio::sync::Mutex<Uploader>>>,
}

impl LoggerManager {
    /// Creates a new manager, ensuring directory structure exists.
    pub async fn new(mut config: Config) -> Result<Self, Error> {
        config.validate()?;

        if let Some(ref mc) = config.metrics_config {
            let tags: Vec<(&str, &str)> = mc.global_tags
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            crate::metrics::init_metrics(&mc.telegraf_host, &mc.telegraf_port, &tags, mc.sampling_rate);
        }

        let base_dir = config.log_file_path.clone();
        let logs_dir = base_dir.join("logs");

        std::fs::create_dir_all(&logs_dir)?;

        let uploader = if let Some(ref gcs_config) = config.gcs_config {
            let mut up = Uploader::new_gcs(logs_dir.clone(), gcs_config.clone())?;
            up.start();
            Some(Arc::new(tokio::sync::Mutex::new(up)))
        } else {
            None
        };

        Ok(Self {
            loggers: DashMap::new(),
            base_dir,
            logs_dir,
            config,
            uploader,
        })
    }

    /// Creates a manager with a custom uploader (for testing with FakeObjectStore).
    pub fn with_uploader(
        config: Config,
        uploader: Option<Uploader>,
    ) -> Result<Self, Error> {
        let base_dir = config.log_file_path.clone();
        let logs_dir = base_dir.join("logs");

        std::fs::create_dir_all(&logs_dir)?;

        let uploader = uploader.map(|u| Arc::new(tokio::sync::Mutex::new(u)));

        Ok(Self {
            loggers: DashMap::new(),
            base_dir,
            logs_dir,
            config,
            uploader,
        })
    }

    /// Sanitises an event name: replaces unsafe characters, truncates to 255 chars.
    pub fn sanitize_event_name(name: &str) -> Result<String, Error> {
        let sanitized: String = name
            .chars()
            .map(|c| {
                if "\\/:*?\"<>| ".contains(c) {
                    '_'
                } else {
                    c
                }
            })
            .take(255)
            .collect();

        if sanitized.is_empty() {
            return Err(Error::InvalidEventName(
                "event name is empty after sanitization".into(),
            ));
        }

        Ok(sanitized)
    }

    fn get_or_create_logger(&self, event_name: &str) -> Result<Arc<parking_lot::Mutex<Logger>>, Error> {
        let sanitized = Self::sanitize_event_name(event_name)?;

        // Fast path
        if let Some(entry) = self.loggers.get(&sanitized) {
            return Ok(entry.value().clone());
        }

        // Slow path — DashMap handles the race
        let entry = self.loggers.entry(sanitized.clone()).or_try_insert_with(|| -> Result<_, Error> {
            let logger = Logger::new(
                &sanitized,
                &self.logs_dir,
                self.config.clone(),
            )?;
            Ok(Arc::new(parking_lot::Mutex::new(logger)))
        })?;

        Ok(entry.value().clone())
    }

    /// Logs raw bytes to the logger for `event_name`.
    #[inline]
    pub async fn log_bytes_with_event(&self, event_name: &str, data: &[u8]) -> Result<(), Error> {
        let logger = self.get_or_create_logger(event_name)?;
        let result = logger.lock().log_bytes(data);
        result
    }

    /// Logs a string message to the logger for `event_name`.
    #[inline]
    pub async fn log_with_event(&self, event_name: &str, msg: &str) -> Result<(), Error> {
        let logger = self.get_or_create_logger(event_name)?;
        let result = logger.lock().log(msg);
        result
    }

    /// Closes a specific event's logger.
    pub async fn close_event_logger(&self, event_name: &str) -> Result<(), Error> {
        let sanitized = Self::sanitize_event_name(event_name)?;
        if let Some((_, logger)) = self.loggers.remove(&sanitized) {
            logger.lock().close()?;
        }
        Ok(())
    }

    /// Returns uploader statistics if GCS upload is enabled, otherwise `None`.
    pub async fn get_uploader_stats(&self) -> Option<Arc<UploaderStats>> {
        let uploader = self.uploader.as_ref()?;
        Some(uploader.lock().await.stats().clone())
    }

    /// Returns per-event statistics.
    pub fn get_stats(&self) -> HashMap<String, Arc<Statistics>> {
        self.loggers
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().lock().get_stats().clone()))
            .collect()
    }

    /// Closes all loggers and stops the uploader.
    pub async fn close(&self) -> Result<(), Error> {
        for entry in self.loggers.iter() {
            entry.value().lock().close()?;
        }

        if let Some(ref uploader) = self.uploader {
            uploader.lock().await.stop().await?;
        }

        Ok(())
    }
}
