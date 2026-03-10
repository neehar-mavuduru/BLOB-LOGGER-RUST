use std::path::PathBuf;
use std::time::Duration;

use blob_logger_rust::config::{Config, GcsUploadConfig};

fn base_config() -> Config {
    Config {
        num_shards: 4,
        buffer_size: 4 * 65536,
        max_file_size: 1024 * 1024,
        log_file_path: PathBuf::from("/tmp/test"),
        flush_interval: Duration::from_secs(300),
        gcs_config: None,
    }
}

#[test]
fn test_valid_config() {
    let mut cfg = base_config();
    assert!(cfg.validate().is_ok());
}

#[test]
fn test_zero_num_shards() {
    let mut cfg = base_config();
    cfg.num_shards = 0;
    let err = cfg.validate().unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("num_shards"), "error should mention num_shards: {}", msg);
}

#[test]
fn test_zero_buffer_size() {
    let mut cfg = base_config();
    cfg.buffer_size = 0;
    let err = cfg.validate().unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("buffer_size"), "error should mention buffer_size: {}", msg);
}

#[test]
fn test_zero_max_file_size() {
    let mut cfg = base_config();
    cfg.max_file_size = 0;
    let err = cfg.validate().unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("max_file_size"), "error should mention max_file_size: {}", msg);
}

#[test]
fn test_empty_log_file_path() {
    let mut cfg = base_config();
    cfg.log_file_path = PathBuf::from("");
    let err = cfg.validate().unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("log_file_path"), "error should mention log_file_path: {}", msg);
}

#[test]
fn test_shard_auto_reduction() {
    let mut cfg = base_config();
    cfg.num_shards = 64;
    cfg.buffer_size = 65536;
    cfg.validate().expect("validate should succeed");
    assert_eq!(cfg.num_shards, 1, "should reduce to 1 shard");
}

#[test]
fn test_gcs_defaults_applied() {
    let mut cfg = base_config();
    cfg.gcs_config = Some(GcsUploadConfig {
        bucket: "test-bucket".into(),
        object_prefix: "prefix/".into(),
        chunk_size: 0,
        max_retries: 0,
        poll_interval: Duration::ZERO,
    });
    cfg.validate().expect("validate");
    let gcs = cfg.gcs_config.as_ref().unwrap();
    assert_eq!(gcs.chunk_size, 32 * 1024 * 1024);
    assert_eq!(gcs.max_retries, 3);
    assert_eq!(gcs.poll_interval, Duration::from_secs(3));
}

#[test]
fn test_flush_interval_default() {
    let mut cfg = base_config();
    cfg.flush_interval = Duration::ZERO;
    cfg.validate().expect("validate");
    assert_eq!(cfg.flush_interval, Duration::from_secs(300));
}

#[test]
fn test_shard_capacity_4096_alignment() {
    let mut cfg = base_config();
    cfg.buffer_size = 1_000_000;
    cfg.num_shards = 3;
    cfg.validate().expect("validate");
    let per_shard = cfg.buffer_size / cfg.num_shards;
    assert_eq!(per_shard % 4096, 0, "per-shard capacity must be 4096-aligned, got {}", per_shard);
}
