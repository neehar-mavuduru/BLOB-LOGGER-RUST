use std::path::PathBuf;
use std::sync::atomic::Ordering;

use blob_logger_rust::config::Config;
use blob_logger_rust::error::Error;
use blob_logger_rust::logger::Logger;
use tempfile::TempDir;

mod common;

fn setup() -> (TempDir, PathBuf, Config) {
    let tmp = TempDir::new().expect("create temp dir");
    let logs_dir = tmp.path().join("logs");
    std::fs::create_dir_all(&logs_dir).expect("mkdir");

    let mut config = common::make_config(tmp.path());
    config.validate().expect("validate");
    (tmp, logs_dir, config)
}

#[test]
fn test_log_bytes_basic() {
    let (_tmp, logs_dir, config) = setup();
    let mut logger = Logger::new("basic_test", &logs_dir, config).expect("create logger");

    logger.log_bytes(b"hello world").expect("log");
    logger.close().expect("close");

    let files = common::find_log_files(&logs_dir);
    assert!(!files.is_empty(), "should have log file after close");

    let records = common::read_all_records(&files[0]);
    assert!(records.iter().any(|r| r == b"hello world"), "should find the logged record");
}

#[test]
fn test_stats_tracking() {
    let (_tmp, logs_dir, config) = setup();
    let mut logger = Logger::new("stats_test", &logs_dir, config).expect("create logger");

    for _ in 0..1000 {
        logger.log_bytes(b"stats payload").expect("log");
    }

    let stats = logger.get_stats();
    assert_eq!(stats.total_logs.load(Ordering::Relaxed), 1000);
    assert!(stats.bytes_written.load(Ordering::Relaxed) > 0);
    assert_eq!(stats.dropped_logs.load(Ordering::Relaxed), 0);

    logger.close().expect("close");
}

#[test]
fn test_log_after_close_returns_error() {
    let (_tmp, logs_dir, config) = setup();
    let mut logger = Logger::new("close_test", &logs_dir, config).expect("create logger");

    logger.close().expect("close");
    let result = logger.log_bytes(b"after close");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::LoggerClosed));
    assert_eq!(logger.get_stats().dropped_logs.load(Ordering::Relaxed), 1);
}

#[test]
fn test_shutdown_no_data_loss() {
    let (_tmp, logs_dir, config) = setup();
    let mut logger = Logger::new("shutdown_test", &logs_dir, config).expect("create logger");

    for i in 0..500 {
        logger.log_bytes(format!("msg_{i:04}").as_bytes()).expect("log");
    }

    logger.close().expect("close");

    let files = common::find_log_files(&logs_dir);
    let mut all_records = Vec::new();
    for f in &files {
        all_records.extend(common::read_all_records(f));
    }

    assert_eq!(all_records.len(), 500, "all 500 messages should be present");
}

#[test]
fn test_close_idempotent() {
    let (_tmp, logs_dir, config) = setup();
    let mut logger = Logger::new("idempotent_test", &logs_dir, config).expect("create logger");

    logger.close().expect("close 1");
    logger.close().expect("close 2");
    logger.close().expect("close 3");
}
