use blob_logger_rust::config::Config;
use blob_logger_rust::logger_manager::LoggerManager;
use tempfile::TempDir;

mod common;

fn make_manager_config(base_dir: &std::path::Path) -> Config {
    let mut config = common::make_config(base_dir);
    config.validate().expect("validate");
    config
}

#[tokio::test]
async fn test_creates_directories() {
    let tmp = TempDir::new().expect("temp dir");
    let config = make_manager_config(tmp.path());
    let _mgr = LoggerManager::new(config).await.expect("create manager");

    assert!(tmp.path().join("logs").exists());
    assert!(tmp.path().join("upload_ready").exists());
}

#[tokio::test]
async fn test_lazy_logger_creation() {
    let tmp = TempDir::new().expect("temp dir");
    let config = make_manager_config(tmp.path());
    let mgr = LoggerManager::new(config).await.expect("create manager");

    // No files before first log
    let files: Vec<_> = std::fs::read_dir(tmp.path().join("logs"))
        .expect("read")
        .flatten()
        .collect();
    assert!(files.is_empty());

    mgr.log_bytes_with_event("test_event", b"hello").await.expect("log");

    let files: Vec<_> = std::fs::read_dir(tmp.path().join("logs"))
        .expect("read")
        .flatten()
        .collect();
    assert!(!files.is_empty(), "should have .tmp file after logging");
}

#[tokio::test]
async fn test_sanitizes_event_name_path_traversal() {
    let result = LoggerManager::sanitize_event_name("../../etc/passwd");
    assert!(result.is_ok());
    let sanitized = result.unwrap();
    assert!(!sanitized.contains('/'), "should not contain /");
}

#[tokio::test]
async fn test_sanitizes_all_special_chars() {
    let result = LoggerManager::sanitize_event_name(r#"a/b\c:d*e?f"g<h>i|j k"#);
    assert!(result.is_ok());
    let sanitized = result.unwrap();
    for c in &['/', '\\', ':', '*', '?', '"', '<', '>', '|', ' '] {
        assert!(!sanitized.contains(*c), "should not contain '{}'", c);
    }
}

#[tokio::test]
async fn test_sanitizes_truncation() {
    let long_name: String = "a".repeat(300);
    let result = LoggerManager::sanitize_event_name(&long_name);
    assert!(result.is_ok());
    let sanitized = result.unwrap();
    assert!(sanitized.len() <= 255, "should be truncated to 255");
}

#[tokio::test]
async fn test_close_flushes_all_events() {
    let tmp = TempDir::new().expect("temp dir");
    let config = make_manager_config(tmp.path());
    let mgr = LoggerManager::new(config).await.expect("create manager");

    for i in 0..5 {
        let event = format!("event_{i}");
        mgr.log_bytes_with_event(&event, b"data").await.expect("log");
    }

    mgr.close().await.expect("close");

    let log_files = common::find_log_files(&tmp.path().join("logs"));
    assert_eq!(log_files.len(), 5, "should have 5 log files");

    let symlinks = common::find_symlinks(&tmp.path().join("upload_ready"));
    assert_eq!(symlinks.len(), 5, "should have 5 symlinks");
}
