//! GCS load test: two events, 1000 RPS total, ~50 KB per log line.
//!
//! Uses a fixed set of log payloads (no random generation). Configure via environment
//! variables or built-in defaults; see README or docs for parameters.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use blob_logger_rust::config::{Config, GcsUploadConfig};
use blob_logger_rust::logger_manager::LoggerManager;

/// Target size per log line (bytes).
const LOG_LINE_BYTES: usize = 50 * 1024; // 50 KB

/// Number of fixed payload variants to cycle through.
const NUM_PAYLOADS: usize = 10;

/// Builds a fixed set of log payloads, each ~50 KB (log-line style with padding).
fn fixed_payloads() -> Vec<Vec<u8>> {
    let template = r##"{"ts":"2025-01-15T12:00:00Z","event":"image_search","request_id":"req-00000000-0000-0000-0000-000000000000","user_id":"user_123","query":"sample query text","result_count":42,"latency_ms":12,"extra":"#PAD#"}"##;
    let pad_len = LOG_LINE_BYTES.saturating_sub(template.len());
    let pad = "#".repeat(pad_len.max(0));

    (0..NUM_PAYLOADS)
        .map(|i| {
            let mut s = template.replace("#PAD#", &pad);
            // Vary request_id so payloads differ
            s = s.replace("00000000-0000-0000", &format!("{:08x}-{:04x}-{:04x}", i * 0x11111111, i, i * 17));
            let b = s.into_bytes();
            // Truncate or pad to exactly LOG_LINE_BYTES for consistent size
            if b.len() >= LOG_LINE_BYTES {
                b[..LOG_LINE_BYTES].to_vec()
            } else {
                let mut v = b;
                v.extend(std::iter::repeat(b' ').take(LOG_LINE_BYTES - v.len()));
                v
            }
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,blob_logger_rust=info".into()),
        )
        .init();

    let log_file_path: PathBuf = std::env::var("LOG_FILE_PATH")
        .unwrap_or_else(|_| "/mnt/localdisk/rustlogger/".into())
        .into();
    let gcs_bucket = std::env::var("GCS_BUCKET").unwrap_or_else(|_| "gcs-dsci-srch-search-prd".into());
    let gcs_prefix = std::env::var("GCS_PREFIX")
        .unwrap_or_else(|_| "Image_search/gcs-flush/".into());
    let run_seconds: u64 = std::env::var("RUN_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let target_rps: u64 = std::env::var("TARGET_RPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    println!("Load test config:");
    println!("  LOG_FILE_PATH   = {:?}", log_file_path);
    println!("  GCS_BUCKET      = {}", gcs_bucket);
    println!("  GCS_PREFIX      = {}", gcs_prefix);
    println!("  RUN_SECONDS     = {}", run_seconds);
    println!("  TARGET_RPS      = {}", target_rps);
    println!("  Events          = event_a, event_b (50% each)");
    println!("  Payload size    = ~{} KB (fixed set of {} variants)", LOG_LINE_BYTES / 1024, NUM_PAYLOADS);

    let payloads = fixed_payloads();
    for (i, p) in payloads.iter().enumerate() {
        println!("  Payload {} length = {} bytes", i, p.len());
    }

    let mut config = Config {
        num_shards: 8,
        buffer_size: 64 * 1024 * 1024, // 64 MiB
        max_file_size: 64 * 1024 * 1024, // 64 MiB per file
        log_file_path: log_file_path.clone(),
        flush_interval: Duration::from_secs(10),
        gcs_config: Some(GcsUploadConfig {
            bucket: gcs_bucket.clone(),
            object_prefix: gcs_prefix.clone(),
            chunk_size: 0, // use default
            max_retries: 0, // use default
            poll_interval: Duration::from_secs(3),
        }),
    };
    config.validate()?;

    let mgr = LoggerManager::new(config).await?;

    let interval = Duration::from_secs_f64(1.0 / target_rps as f64);
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let events = ["event_a", "event_b"];
    let sent = AtomicU64::new(0);
    let start = Instant::now();
    let deadline = start + Duration::from_secs(run_seconds);

    println!("\nRunning load test for {} seconds at {} RPS...", run_seconds, target_rps);

    while Instant::now() < deadline {
        tick.tick().await;
        let i = sent.fetch_add(1, Ordering::Relaxed) as usize;
        let event = events[i % 2];
        let payload = &payloads[i % NUM_PAYLOADS];
        if let Err(e) = mgr.log_bytes_with_event(event, payload).await {
            tracing::error!("log_bytes_with_event failed: {}", e);
        }
    }

    let elapsed = start.elapsed();
    let total_sent = sent.load(Ordering::Relaxed);
    let actual_rps = total_sent as f64 / elapsed.as_secs_f64();

    println!("\nStopping: closing loggers and uploader...");
    mgr.close().await?;

    println!("\n--- Stats ---");
    let stats = mgr.get_stats();
    for (event_name, s) in &stats {
        let total = s.total_logs.load(Ordering::Relaxed);
        let dropped = s.dropped_logs.load(Ordering::Relaxed);
        let bytes = s.bytes_written.load(Ordering::Relaxed);
        let errors = s.write_errors.load(Ordering::Relaxed);
        println!(
            "  {}: total_logs={} dropped={} bytes_written={} write_errors={}",
            event_name, total, dropped, bytes, errors
        );
    }

    if let Some(su) = mgr.get_uploader_stats().await {
        println!(
            "  Uploader: files_uploaded={} bytes_uploaded={} upload_errors={} retry_count={}",
            su.files_uploaded.load(Ordering::Relaxed),
            su.bytes_uploaded.load(Ordering::Relaxed),
            su.upload_errors.load(Ordering::Relaxed),
            su.retry_count.load(Ordering::Relaxed),
        );
    }

    println!(
        "\n  Elapsed: {:.2}s | Sent: {} | Actual RPS: {:.1}",
        elapsed.as_secs_f64(),
        total_sent,
        actual_rps
    );
    println!("\nLoad test finished.");
    Ok(())
}
