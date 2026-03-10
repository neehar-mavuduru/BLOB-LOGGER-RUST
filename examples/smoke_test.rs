use std::time::Duration;

use blob_logger_rust::config::Config;
use blob_logger_rust::logger_manager::LoggerManager;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let tmp = tempfile::TempDir::new().unwrap();
    println!("Using temp dir: {:?}", tmp.path());

    let mut config = Config {
        num_shards: 4,
        buffer_size: 4 * 65536,
        max_file_size: 64 * 1024,
        log_file_path: tmp.path().to_path_buf(),
        flush_interval: Duration::from_secs(5),
        gcs_config: None,
    };
    config.validate().unwrap();

    let mgr = LoggerManager::new(config).await.unwrap();

    println!("Writing 500 messages to 'payment' event...");
    for i in 0..500 {
        mgr.log_with_event("payment", &format!("transaction_{i:04}: amount=99.99"))
            .await
            .unwrap();
    }

    println!("Writing 200 messages to 'audit' event...");
    for i in 0..200 {
        mgr.log_with_event("audit", &format!("audit_entry_{i:04}: action=login"))
            .await
            .unwrap();
    }

    println!("Closing all loggers...");
    mgr.close().await.unwrap();

    let stats = mgr.get_stats();
    for (event, s) in &stats {
        println!(
            "Event '{}': total={}, written_bytes={}, dropped={}",
            event,
            s.total_logs.load(std::sync::atomic::Ordering::Relaxed),
            s.bytes_written.load(std::sync::atomic::Ordering::Relaxed),
            s.dropped_logs.load(std::sync::atomic::Ordering::Relaxed),
        );
    }

    // List files
    let logs_dir = tmp.path().join("logs");
    println!("\nFiles in logs/:");
    for entry in std::fs::read_dir(&logs_dir).unwrap() {
        let entry = entry.unwrap();
        let meta = std::fs::metadata(entry.path()).unwrap();
        println!("  {:?} ({} bytes)", entry.file_name(), meta.len());
    }

    let upload_dir = tmp.path().join("upload_ready");
    println!("\nSymlinks in upload_ready/:");
    for entry in std::fs::read_dir(&upload_dir).unwrap() {
        let entry = entry.unwrap();
        let meta = std::fs::symlink_metadata(entry.path()).unwrap();
        if meta.file_type().is_symlink() {
            let target = std::fs::read_link(entry.path()).unwrap();
            println!("  {:?} → {:?}", entry.file_name(), target);
        }
    }

    println!("\nSmoke test passed!");
}
