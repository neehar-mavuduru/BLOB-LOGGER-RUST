use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use blob_logger_rust::buffer::{Buffer, HEADER_OFFSET};
use blob_logger_rust::config::{Config, GcsUploadConfig};
use blob_logger_rust::error::Error;
use blob_logger_rust::file_writer::{FileWriter, SizeFileWriter};
use blob_logger_rust::logger::Logger;
use blob_logger_rust::logger_manager::LoggerManager;
use blob_logger_rust::shard::Shard;
use blob_logger_rust::uploader::Uploader;
use tempfile::TempDir;

mod common;

/// Mock FileWriter that stalls write_vectored for a configurable duration.
#[derive(Debug)]
struct StalledFileWriter {
    stall_duration: Duration,
    write_count: AtomicU64,
    close_count: AtomicU64,
}

impl StalledFileWriter {
    fn new(stall_duration: Duration) -> Self {
        Self {
            stall_duration,
            write_count: AtomicU64::new(0),
            close_count: AtomicU64::new(0),
        }
    }
}

impl FileWriter for StalledFileWriter {
    fn write_vectored(&self, buffers: &[&[u8]]) -> Result<usize, Error> {
        std::thread::sleep(self.stall_duration);
        self.write_count.fetch_add(1, Ordering::Relaxed);
        let total: usize = buffers.iter().map(|b| b.len()).sum();
        Ok(total)
    }

    fn last_pwritev_duration(&self) -> Duration {
        Duration::from_nanos(0)
    }

    fn close(&mut self) -> Result<(), Error> {
        self.close_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[test]
fn test_adversarial_cas_high_contention() {
    for run in 0..5 {
        let buf = Arc::new(Buffer::new(8 * 1024 * 1024, 0).expect("create 8MB buffer"));
        let mut handles = Vec::new();

        for tid in 0u32..500 {
            let buf = buf.clone();
            handles.push(std::thread::spawn(move || {
                let payload = format!("t{tid:04}_{:0>11}", "x"); // 16 bytes
                let _ = buf.write(payload.as_bytes());
            }));
        }

        for h in handles {
            h.join().expect("join");
        }

        let slice = buf.data_slice();
        let offset = buf.offset().load(Ordering::Relaxed) as usize;
        let mut pos = HEADER_OFFSET;
        let mut records = std::collections::HashSet::new();

        while pos + 4 <= offset {
            let rec_len = u32::from_le_bytes(slice[pos..pos + 4].try_into().unwrap()) as usize;
            if pos + 4 + rec_len > offset {
                break;
            }
            let data = String::from_utf8_lossy(&slice[pos + 4..pos + 4 + rec_len]).to_string();
            assert!(
                records.insert(data.clone()),
                "run {run}: duplicate record: {data}"
            );
            pos += 4 + rec_len;
        }

        assert_eq!(
            records.len(),
            500,
            "run {run}: all 500 records should be present"
        );
    }
}

#[test]
fn test_adversarial_double_swap_one_winner() {
    for iteration in 0..100 {
        let (tx, rx) = crossbeam_channel::bounded(16);
        let shard = Arc::new(Shard::new(65536, 0, tx).expect("create shard"));

        shard.write(b"pre-swap data").expect("write");

        let barrier = Arc::new(Barrier::new(100));
        let mut handles = Vec::new();

        for _ in 0..100 {
            let shard = shard.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                shard.try_swap();
            }));
        }

        for h in handles {
            h.join().expect("join");
        }

        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert!(
            count >= 1,
            "iteration {iteration}: at least one swap should succeed"
        );
        assert!(
            !shard.swapping_flag().load(Ordering::Relaxed),
            "iteration {iteration}: swapping should be false"
        );
    }
}

#[test]
fn test_adversarial_file_offset_alignment() {
    let tmp = TempDir::new().expect("temp dir");
    let logs_dir = tmp.path().join("logs");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&logs_dir).expect("mkdir");
    std::fs::create_dir_all(&upload_ready).expect("mkdir");

    let writer = SizeFileWriter::new("align_test", &logs_dir, &upload_ready, 100 * 1024 * 1024)
        .expect("create writer");

    for i in 0..50 {
        let buf_count = (i % 4) + 1;
        let buffers: Vec<Vec<u8>> = (0..buf_count).map(|_| vec![0xAAu8; 4096]).collect();
        let slices: Vec<&[u8]> = buffers.iter().map(|b| b.as_slice()).collect();
        writer.write_vectored(&slices).expect("write_vectored");
    }
}

#[tokio::test]
async fn test_adversarial_concurrent_logger_creation() {
    let tmp = TempDir::new().expect("temp dir");
    let mut config = common::make_config(tmp.path());
    config.validate().expect("validate");

    let mgr = Arc::new(LoggerManager::with_uploader(config, None).expect("create manager"));

    let mut handles = Vec::new();
    for _ in 0..200 {
        let mgr = mgr.clone();
        handles.push(tokio::spawn(async move {
            mgr.log_bytes_with_event("brand_new", b"concurrent data")
                .await
                .expect("log");
        }));
    }

    for h in handles {
        h.await.expect("join");
    }

    // Should have exactly one .tmp file for "brand_new"
    let logs_dir = tmp.path().join("logs");
    let files: Vec<_> = std::fs::read_dir(&logs_dir)
        .expect("read")
        .flatten()
        .filter(|e| {
            e.path()
                .to_string_lossy()
                .contains("brand_new")
        })
        .collect();
    assert_eq!(files.len(), 1, "should have exactly one file for brand_new");
}

#[tokio::test]
async fn test_adversarial_close_while_writing() {
    let tmp = TempDir::new().expect("temp dir");
    let mut config = common::make_config(tmp.path());
    config.buffer_size = 4 * 65536;
    config.validate().expect("validate");

    let mgr = Arc::new(LoggerManager::with_uploader(config, None).expect("create manager"));

    let mgr_write = mgr.clone();
    let write_handle = tokio::spawn(async move {
        for i in 0..1000 {
            let _ = mgr_write
                .log_bytes_with_event("close_test", format!("msg_{i}").as_bytes())
                .await;
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let close_result = tokio::time::timeout(Duration::from_secs(5), mgr.close()).await;
    assert!(close_result.is_ok(), "close should complete within 5 seconds");

    let _ = write_handle.await;
}

#[tokio::test]
async fn test_adversarial_multiple_close_concurrent() {
    let tmp = TempDir::new().expect("temp dir");
    let mut config = common::make_config(tmp.path());
    config.validate().expect("validate");

    let mgr = Arc::new(LoggerManager::with_uploader(config, None).expect("create manager"));
    mgr.log_bytes_with_event("test", b"data")
        .await
        .expect("log");

    let mut handles = Vec::new();
    for _ in 0..10 {
        let mgr = mgr.clone();
        handles.push(tokio::spawn(async move {
            let _ = mgr.close().await;
        }));
    }

    for h in handles {
        h.await.expect("join");
    }
}

#[test]
fn test_adversarial_symlink_already_exists() {
    let tmp = TempDir::new().expect("temp dir");
    let logs_dir = tmp.path().join("logs");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&logs_dir).expect("mkdir");
    std::fs::create_dir_all(&upload_ready).expect("mkdir");

    // Pre-create a stale symlink
    let stale_target = logs_dir.join("stale_target.log");
    std::fs::write(&stale_target, b"stale").expect("write stale");
    let stale_symlink = upload_ready.join("stale_target.log");
    std::os::unix::fs::symlink(&stale_target, &stale_symlink).expect("symlink");

    // The writer should be able to create files without panicking
    let mut writer = SizeFileWriter::new("symlink_test", &logs_dir, &upload_ready, 8192)
        .expect("create writer");

    let buf = vec![0xBBu8; 4096];
    writer.write_vectored(&[&buf]).expect("write");

    // The writer may warn about the stale symlink but should not panic
    writer.close().expect("close");
}

#[tokio::test]
async fn test_adversarial_write_after_swap_before_flush() {
    let (tx, rx) = crossbeam_channel::bounded(32);
    let shard = Arc::new(Shard::new(65536, 0, tx).expect("create shard"));

    let payload = vec![0xAAu8; 60];
    for _ in 0..50 {
        let _ = shard.write(&payload);
    }

    let active_before = shard.active_index().load(Ordering::Relaxed);
    shard.try_swap();
    let active_after = shard.active_index().load(Ordering::Relaxed);
    assert_ne!(active_before, active_after, "active buffer should have swapped");

    // Write from another thread into the new active buffer
    let shard2 = shard.clone();
    let handle = tokio::task::spawn_blocking(move || {
        shard2.write(b"post_swap_data").expect("write after swap")
    });

    let (n, _) = handle.await.unwrap();
    assert!(n > 0, "write after swap should succeed in new active buffer");

    // The first swapped buffer should contain our original data
    let flushed_buf = rx.try_recv().expect("should have received flushed buffer");
    let has_data = flushed_buf.data_slice()[HEADER_OFFSET..].iter().any(|&b| b != 0);
    assert!(has_data, "flushed buffer should contain original data");

    // Simulate flush worker: mark as flushed so the next swap can proceed
    flushed_buf.mark_flushed();

    // Now a second swap should work (inactive buffer marked flushed)
    shard.try_swap();

    let second_buf = rx.try_recv().expect("second swap should produce a buffer");
    let has_post_data = second_buf.data_slice()[HEADER_OFFSET..].iter().any(|&b| b != 0);
    assert!(has_post_data, "second buffer should contain post-swap data");
}

#[tokio::test]
async fn test_adversarial_stalled_disk() {
    let stalled_writer = StalledFileWriter::new(Duration::from_secs(2));
    let mut config = common::make_config(std::path::Path::new("/tmp/stalled_test"));
    config.buffer_size = 4 * 4096;
    config.num_shards = 1;
    config.validate().unwrap();

    let mut logger = Logger::with_writer(Box::new(stalled_writer), config).unwrap();

    let payload = vec![0xBBu8; 64];
    let mut buffer_full_count = 0;
    let start = std::time::Instant::now();

    for _ in 0..10000 {
        match logger.log_bytes(&payload) {
            Ok(()) => {}
            Err(Error::BufferFull) => {
                buffer_full_count += 1;
                if buffer_full_count > 3 {
                    break;
                }
            }
            Err(_) => {}
        }
        if start.elapsed() > Duration::from_secs(10) {
            break;
        }
    }

    assert!(
        buffer_full_count > 0,
        "should get BufferFull when disk is stalled"
    );

    let stats = logger.get_stats();
    assert_eq!(
        stats.write_errors.load(Ordering::Relaxed),
        0,
        "write_errors should not be incremented (drop path, not write error)"
    );

    logger.close().unwrap();
}

#[test]
fn test_adversarial_upload_ready_missing() {
    let tmp = TempDir::new().expect("temp dir");
    let logs_dir = tmp.path().join("logs");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&logs_dir).expect("mkdir");
    std::fs::create_dir_all(&upload_ready).expect("mkdir");

    let mut writer =
        SizeFileWriter::new("missing_dir_test", &logs_dir, &upload_ready, 8192).expect("create writer");

    std::fs::remove_dir_all(&upload_ready).expect("remove upload_ready");

    let buf = vec![0xBBu8; 4096];
    writer.write_vectored(&[&buf]).expect("write");

    // Write enough to trigger rotation (8192 byte max)
    writer.write_vectored(&[&buf]).expect("write again");

    // Rotation tries to create symlink in missing dir - should log error, not panic
    writer.close().expect("close should not panic");

    // Log file should still exist
    let entries: Vec<_> = std::fs::read_dir(&logs_dir)
        .expect("read logs dir")
        .flatten()
        .collect();
    assert!(!entries.is_empty(), ".log file should still be created");
}

#[tokio::test]
async fn test_adversarial_partial_upload_retry() {
    let tmp = TempDir::new().expect("temp dir");
    let logs_dir = tmp.path().join("logs");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&logs_dir).expect("mkdir");
    std::fs::create_dir_all(&upload_ready).expect("mkdir");

    let filename = "fail_event_2026-03-07_14-00-00.log";
    let file_path = logs_dir.join(filename);
    std::fs::write(&file_path, b"upload retry data").expect("write");
    let sym_path = upload_ready.join(filename);
    std::os::unix::fs::symlink(&file_path, &sym_path).expect("symlink");

    let fake_store = Arc::new(common::FakeObjectStore::new());
    let gcs_config = GcsUploadConfig {
        bucket: "test".into(),
        object_prefix: "p/".into(),
        chunk_size: 1024 * 1024,
        max_retries: 3,
        poll_interval: Duration::from_millis(100),
    };

    let mut uploader =
        Uploader::with_store(upload_ready.clone(), gcs_config, fake_store.clone());
    uploader.start();

    tokio::time::sleep(Duration::from_secs(2)).await;
    uploader.stop().await.expect("stop");

    let state = fake_store.state.lock();
    assert_eq!(
        state.objects.len(),
        1,
        "file should be uploaded after retries"
    );

    // Symlink and original file should be cleaned up after successful upload
    assert!(
        !sym_path.exists(),
        "symlink should be removed after upload"
    );
}

#[tokio::test]
async fn test_adversarial_poll_during_rotation() {
    let tmp = TempDir::new().expect("temp dir");
    let upload_ready = tmp.path().join("upload_ready");

    let mut config = Config {
        num_shards: 2,
        buffer_size: 2 * 65536,
        max_file_size: 32 * 1024,
        log_file_path: tmp.path().to_path_buf(),
        flush_interval: Duration::from_secs(300),
        gcs_config: None,
    };
    config.validate().expect("validate");

    let fake_store = Arc::new(common::FakeObjectStore::new());
    let gcs_config = GcsUploadConfig {
        bucket: "test".into(),
        object_prefix: "p/".into(),
        chunk_size: 1024 * 1024,
        max_retries: 1,
        poll_interval: Duration::from_millis(10),
    };

    let mut uploader =
        Uploader::with_store(upload_ready.clone(), gcs_config, fake_store.clone());
    uploader.start();

    let mgr = LoggerManager::with_uploader(config, None).expect("create manager");

    let payload = vec![0xDDu8; 600];
    for _ in 0..200 {
        let _ = mgr.log_bytes_with_event("poll_rotation", &payload).await;
    }

    mgr.close().await.expect("close");
    tokio::time::sleep(Duration::from_secs(2)).await;
    uploader.stop().await.expect("stop");

    // Verify no .tmp files were uploaded
    let state = fake_store.state.lock();
    for key in state.objects.keys() {
        assert!(
            !key.ends_with(".tmp"),
            ".tmp file should never appear in uploads: {key}"
        );
    }
}

#[tokio::test]
async fn test_adversarial_dangling_symlink() {
    let tmp = TempDir::new().expect("temp dir");
    let upload_ready = tmp.path().join("upload_ready");
    let logs_dir = tmp.path().join("logs");
    std::fs::create_dir_all(&upload_ready).expect("mkdir");
    std::fs::create_dir_all(&logs_dir).expect("mkdir");

    // Create a dangling symlink (target doesn't exist)
    let nonexistent = logs_dir.join("ghost_2026-03-07_14-00-00.log");
    let symlink = upload_ready.join("ghost_2026-03-07_14-00-00.log");
    std::os::unix::fs::symlink(&nonexistent, &symlink).expect("symlink");

    // Also create a valid file + symlink
    let valid_file = logs_dir.join("valid_2026-03-07_14-01-00.log");
    std::fs::write(&valid_file, b"valid data").expect("write");
    let valid_sym = upload_ready.join("valid_2026-03-07_14-01-00.log");
    std::os::unix::fs::symlink(&valid_file, &valid_sym).expect("symlink");

    let fake_store = Arc::new(common::FakeObjectStore::new());
    let gcs_config = blob_logger_rust::config::GcsUploadConfig {
        bucket: "test".into(),
        object_prefix: "p/".into(),
        chunk_size: 1024 * 1024,
        max_retries: 1,
        poll_interval: Duration::from_millis(100),
    };

    let mut uploader =
        blob_logger_rust::uploader::Uploader::with_store(upload_ready.clone(), gcs_config, fake_store.clone());
    uploader.start();

    tokio::time::sleep(Duration::from_secs(2)).await;
    uploader.stop().await.expect("stop");

    let state = fake_store.state.lock();
    // The valid file should be uploaded
    assert!(
        !state.objects.is_empty(),
        "at least the valid file should be uploaded"
    );
}
