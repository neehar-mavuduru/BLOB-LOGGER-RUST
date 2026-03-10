use std::sync::Arc;
use std::time::Duration;

use blob_logger_rust::config::GcsUploadConfig;
use blob_logger_rust::uploader::Uploader;
use tempfile::TempDir;

mod common;

fn make_gcs_config() -> GcsUploadConfig {
    GcsUploadConfig {
        bucket: "test-bucket".into(),
        object_prefix: "test-prefix/".into(),
        chunk_size: 1024 * 1024,
        max_retries: 3,
        poll_interval: Duration::from_millis(100),
    }
}

#[test]
fn test_gcs_object_path_from_filename_timestamp() {
    let path = Uploader::gcs_path_from_filename(
        "payment_2026-03-07_14-30-00.log",
        "prefix/",
    )
    .expect("derive path");

    assert_eq!(
        path,
        "prefix/payment/2026-03-07/14/payment_2026-03-07_14-30-00.log"
    );
}

#[test]
fn test_gcs_object_path_event_with_underscores() {
    let path = Uploader::gcs_path_from_filename(
        "payment_retry_2026-03-07_14-30-00.log",
        "prefix/",
    )
    .expect("derive path");

    assert_eq!(
        path,
        "prefix/payment_retry/2026-03-07/14/payment_retry_2026-03-07_14-30-00.log"
    );
}

#[tokio::test]
async fn test_empty_file_skipped() {
    let tmp = TempDir::new().expect("temp dir");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&upload_ready).expect("mkdir");

    // Create an empty .log file and symlink
    let log_file = tmp.path().join("empty_2026-03-07_14-30-00.log");
    std::fs::write(&log_file, b"").expect("write empty");
    let symlink = upload_ready.join("empty_2026-03-07_14-30-00.log");
    std::os::unix::fs::symlink(&log_file, &symlink).expect("symlink");

    let fake_store = Arc::new(common::FakeObjectStore::new());
    let config = make_gcs_config();

    let mut uploader = Uploader::with_store(upload_ready.clone(), config, fake_store.clone());
    uploader.start();

    tokio::time::sleep(Duration::from_millis(500)).await;
    uploader.stop().await.expect("stop");

    let state = fake_store.state.lock();
    assert_eq!(state.put_call_count, 0, "should not upload empty file");
}

#[tokio::test]
async fn test_crash_recovery_picks_up_leftover_symlinks() {
    let tmp = TempDir::new().expect("temp dir");
    let upload_ready = tmp.path().join("upload_ready");
    let logs_dir = tmp.path().join("logs");
    std::fs::create_dir_all(&upload_ready).expect("mkdir");
    std::fs::create_dir_all(&logs_dir).expect("mkdir");

    // Pre-populate with 3 symlinks
    for i in 0..3 {
        let filename = format!("event_{i}_2026-03-07_14-{i:02}-00.log");
        let log_path = logs_dir.join(&filename);
        std::fs::write(&log_path, format!("data for event {i}")).expect("write");
        let sym_path = upload_ready.join(&filename);
        std::os::unix::fs::symlink(&log_path, &sym_path).expect("symlink");
    }

    let fake_store = Arc::new(common::FakeObjectStore::new());
    let config = make_gcs_config();

    let mut uploader = Uploader::with_store(upload_ready.clone(), config, fake_store.clone());
    uploader.start();

    // Wait for uploads to complete
    tokio::time::sleep(Duration::from_secs(3)).await;
    uploader.stop().await.expect("stop");

    let state = fake_store.state.lock();
    assert_eq!(state.objects.len(), 3, "all 3 files should be uploaded");
}

#[tokio::test]
async fn test_ignores_tmp_files() {
    let tmp = TempDir::new().expect("temp dir");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&upload_ready).expect("mkdir");

    // Place a .tmp file directly in upload_ready
    let tmp_file = upload_ready.join("test.log.tmp");
    std::fs::write(&tmp_file, b"not ready").expect("write");

    let fake_store = Arc::new(common::FakeObjectStore::new());
    let config = make_gcs_config();

    let mut uploader = Uploader::with_store(upload_ready.clone(), config, fake_store.clone());
    uploader.start();

    tokio::time::sleep(Duration::from_millis(500)).await;
    uploader.stop().await.expect("stop");

    let state = fake_store.state.lock();
    assert_eq!(state.put_call_count, 0, "should not upload .tmp files");
}

#[tokio::test]
async fn test_stop_idempotent() {
    let tmp = TempDir::new().expect("temp dir");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&upload_ready).expect("mkdir");

    let fake_store = Arc::new(common::FakeObjectStore::new());
    let config = make_gcs_config();

    let mut uploader = Uploader::with_store(upload_ready, config, fake_store);
    uploader.start();

    uploader.stop().await.expect("stop 1");
    uploader.stop().await.expect("stop 2");
    uploader.stop().await.expect("stop 3");
}
