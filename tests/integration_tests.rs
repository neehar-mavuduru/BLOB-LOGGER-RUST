use std::sync::Arc;
use std::time::Duration;

use blob_logger_rust::config::{Config, GcsUploadConfig};
use blob_logger_rust::logger_manager::LoggerManager;
use blob_logger_rust::uploader::Uploader;
use tempfile::TempDir;

mod common;

fn make_integration_config(base_dir: &std::path::Path) -> Config {
    let mut config = Config {
        num_shards: 4,
        buffer_size: 4 * 65536,
        max_file_size: 1024 * 1024,
        log_file_path: base_dir.to_path_buf(),
        flush_interval: Duration::from_secs(300),
        gcs_config: None,
    };
    config.validate().expect("validate");
    config
}

fn make_small_file_config(base_dir: &std::path::Path) -> Config {
    let mut config = Config {
        num_shards: 4,
        buffer_size: 8 * 1024 * 1024,
        max_file_size: 512 * 1024,
        log_file_path: base_dir.to_path_buf(),
        flush_interval: Duration::from_secs(300),
        gcs_config: None,
    };
    config.validate().expect("validate");
    config
}

#[tokio::test]
async fn test_write_and_close_produces_log_files() {
    let tmp = TempDir::new().expect("temp dir");
    let config = make_integration_config(tmp.path());

    let mgr = LoggerManager::with_uploader(config, None).expect("create manager");

    for i in 0..500 {
        let msg = format!("msg_{i:06}");
        mgr.log_bytes_with_event("payment", msg.as_bytes())
            .await
            .expect("log");
    }

    mgr.close().await.expect("close");

    let logs_dir = tmp.path().join("logs");
    let upload_ready = tmp.path().join("upload_ready");

    // Should have sealed log files
    let log_files = common::find_log_files(&logs_dir);
    assert!(
        !log_files.is_empty(),
        "should have at least one .log file"
    );

    // Should have symlinks
    let symlinks = common::find_symlinks(&upload_ready);
    assert!(
        !symlinks.is_empty(),
        "should have at least one symlink"
    );

    // No .tmp files should remain
    common::assert_no_tmp_files(&logs_dir);

    // Verify records are recoverable
    let mut total_records = 0;
    for f in &log_files {
        let records = common::read_all_records(f);
        total_records += records.len();
    }

    assert_eq!(
        total_records, 500,
        "all 500 messages should be recoverable from log files"
    );
}

#[tokio::test]
async fn test_upload_picks_up_sealed_files() {
    let tmp = TempDir::new().expect("temp dir");
    let logs_dir = tmp.path().join("logs");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&logs_dir).expect("mkdir");
    std::fs::create_dir_all(&upload_ready).expect("mkdir");

    // Pre-populate with a file and symlink
    let filename = "test_event_2026-03-07_14-00-00.log";
    let file_path = logs_dir.join(filename);
    std::fs::write(&file_path, b"some test data content here").expect("write");
    let sym_path = upload_ready.join(filename);
    std::os::unix::fs::symlink(&file_path, &sym_path).expect("symlink");

    let fake_store = Arc::new(common::FakeObjectStore::new());
    let gcs_config = GcsUploadConfig {
        bucket: "test-bucket".into(),
        object_prefix: "prefix/".into(),
        chunk_size: 1024 * 1024,
        max_retries: 3,
        poll_interval: Duration::from_millis(100),
    };

    let mut uploader = Uploader::with_store(upload_ready.clone(), gcs_config, fake_store.clone());
    uploader.start();

    tokio::time::sleep(Duration::from_secs(3)).await;
    uploader.stop().await.expect("stop");

    let state = fake_store.state.lock();
    assert_eq!(state.objects.len(), 1, "should have uploaded 1 object");

    // Verify GCS path format
    let key = state.objects.keys().next().unwrap();
    assert!(
        key.contains("test_event/2026-03-07/14/"),
        "GCS path should contain event/date/hour: {}",
        key
    );
}

#[tokio::test]
async fn test_multi_event_isolation() {
    let tmp = TempDir::new().expect("temp dir");
    let config = make_integration_config(tmp.path());

    let mgr = LoggerManager::with_uploader(config, None).expect("create manager");

    for event_id in 0..5 {
        for i in 0..50 {
            let event = format!("event_{event_id}");
            let msg = format!("e{event_id}_m{i:04}");
            mgr.log_bytes_with_event(&event, msg.as_bytes())
                .await
                .expect("log");
        }
    }

    mgr.close().await.expect("close");

    let logs_dir = tmp.path().join("logs");
    let log_files = common::find_log_files(&logs_dir);
    assert_eq!(
        log_files.len(),
        5,
        "should have 5 log files (one per event)"
    );

    // Verify each file has records
    for f in &log_files {
        let records = common::read_all_records(f);
        assert_eq!(records.len(), 50, "each event should have 50 records in {:?}", f);
    }

    // No .tmp files should remain
    common::assert_no_tmp_files(&logs_dir);
}

#[tokio::test]
async fn test_write_rotate_upload() {
    let tmp = TempDir::new().expect("temp dir");
    let upload_ready = tmp.path().join("upload_ready");
    let logs_dir = tmp.path().join("logs");
    let config = make_small_file_config(tmp.path());

    let mgr = LoggerManager::with_uploader(config, None).expect("create manager");

    let payload = vec![0xCCu8; 600];
    for _ in 0..1000 {
        mgr.log_bytes_with_event("rotate_test", &payload)
            .await
            .expect("log");
    }

    mgr.close().await.expect("close");

    let log_files = common::find_log_files(&logs_dir);
    assert!(!log_files.is_empty(), "should have log files after rotation");

    let mut total_records = 0;
    for f in &log_files {
        total_records += common::read_all_records(f).len();
    }
    assert!(
        total_records >= 1000,
        "at least 1000 records should be recoverable from log files, found {total_records}"
    );

    let fake_store = Arc::new(common::FakeObjectStore::new());
    let gcs_config = GcsUploadConfig {
        bucket: "test-bucket".into(),
        object_prefix: "prefix/".into(),
        chunk_size: 128 * 1024,
        max_retries: 3,
        poll_interval: Duration::from_millis(100),
    };

    let mut uploader = Uploader::with_store(upload_ready.clone(), gcs_config, fake_store.clone());
    uploader.start();
    tokio::time::sleep(Duration::from_secs(3)).await;
    uploader.stop().await.expect("stop");

    let state = fake_store.state.lock();
    assert!(!state.objects.is_empty(), "should have uploaded objects to GCS");

    for key in state.objects.keys() {
        assert!(
            key.contains("rotate_test/"),
            "GCS path should contain event name: {key}"
        );
    }
    drop(state);

    let remaining = common::find_symlinks(&upload_ready);
    assert!(
        remaining.is_empty(),
        "upload_ready should be empty after upload, found: {:?}",
        remaining
    );

    let state = fake_store.state.lock();
    let mut gcs_total = 0;
    for data in state.objects.values() {
        gcs_total += common::decode_records(data).len();
    }
    assert!(
        gcs_total >= 1000,
        "at least 1000 records should be in GCS objects, found {gcs_total}"
    );
}

#[tokio::test]
async fn test_no_data_loss_under_max_concurrency() {
    let tmp = TempDir::new().expect("temp dir");
    let mut config = Config {
        num_shards: 8,
        buffer_size: 8 * 65536,
        max_file_size: 2 * 1024 * 1024,
        log_file_path: tmp.path().to_path_buf(),
        flush_interval: Duration::from_secs(300),
        gcs_config: None,
    };
    config.validate().expect("validate");

    let mgr = Arc::new(LoggerManager::with_uploader(config, None).expect("create manager"));

    let mut handles = Vec::new();
    for task_id in 0..100u32 {
        let mgr = mgr.clone();
        handles.push(tokio::spawn(async move {
            for event_id in 0..5u32 {
                let event = format!("event_{event_id}");
                for msg_id in 0..50u32 {
                    let msg = format!("t{task_id:03}_e{event_id}_m{msg_id:03}");
                    mgr.log_bytes_with_event(&event, msg.as_bytes())
                        .await
                        .expect("log");
                }
            }
        }));
    }

    for h in handles {
        h.await.expect("join");
    }

    mgr.close().await.expect("close");

    let logs_dir = tmp.path().join("logs");
    let log_files = common::find_log_files(&logs_dir);

    let mut total_records = 0;
    let mut all_records = std::collections::HashSet::new();
    for f in &log_files {
        for r in common::read_all_records(f) {
            let s = String::from_utf8_lossy(&r).to_string();
            all_records.insert(s);
            total_records += 1;
        }
    }

    assert_eq!(total_records, 25_000, "all 25,000 records should be present");
    assert_eq!(all_records.len(), 25_000, "no duplicate records");
}

#[tokio::test]
async fn test_crash_recovery() {
    let tmp = TempDir::new().expect("temp dir");
    let logs_dir = tmp.path().join("logs");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&logs_dir).expect("mkdir");
    std::fs::create_dir_all(&upload_ready).expect("mkdir");

    // Pre-populate with leftover symlinks (simulating crash)
    for i in 0..3 {
        let filename = format!("crash_event_{i}_2026-03-07_14-{i:02}-00.log");
        let file_path = logs_dir.join(&filename);
        std::fs::write(&file_path, format!("crash recovery data {i}")).expect("write");
        let sym_path = upload_ready.join(&filename);
        std::os::unix::fs::symlink(&file_path, &sym_path).expect("symlink");
    }

    let fake_store = Arc::new(common::FakeObjectStore::new());
    let gcs_config = GcsUploadConfig {
        bucket: "test-bucket".into(),
        object_prefix: "recovery/".into(),
        chunk_size: 1024 * 1024,
        max_retries: 3,
        poll_interval: Duration::from_millis(100),
    };

    let mut uploader = Uploader::with_store(upload_ready.clone(), gcs_config, fake_store.clone());
    uploader.start();

    tokio::time::sleep(Duration::from_secs(3)).await;
    uploader.stop().await.expect("stop");

    let state = fake_store.state.lock();
    assert_eq!(
        state.objects.len(),
        3,
        "all 3 leftover files should be uploaded"
    );
}
