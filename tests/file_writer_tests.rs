use std::path::PathBuf;

use blob_logger_rust::file_writer::{FileWriter, SizeFileWriter};
use tempfile::TempDir;

mod common;

fn setup() -> (TempDir, PathBuf) {
    let tmp = TempDir::new().expect("create temp dir");
    let logs_dir = tmp.path().join("logs");
    std::fs::create_dir_all(&logs_dir).expect("mkdir logs");
    (tmp, logs_dir)
}

fn make_4096_buffer(fill: u8) -> Vec<u8> {
    vec![fill; 4096]
}

#[test]
fn test_tmp_suffix_on_create() {
    let (_tmp, logs_dir) = setup();
    let _writer = SizeFileWriter::new("test_event", &logs_dir, 1024 * 1024)
        .expect("create writer");

    let entries: Vec<_> = std::fs::read_dir(&logs_dir)
        .expect("read dir")
        .flatten()
        .collect();
    assert_eq!(entries.len(), 1);
    let path = entries[0].path();
    assert!(
        path.to_string_lossy().ends_with(".tmp"),
        "file should have .tmp suffix: {:?}",
        path
    );

    // No .log file yet
    let log_files: Vec<_> = std::fs::read_dir(&logs_dir)
        .expect("read dir")
        .flatten()
        .filter(|e| e.path().extension().and_then(|e| e.to_str()) == Some("log"))
        .collect();
    assert!(log_files.is_empty(), "no .log files should exist yet");
}

#[test]
fn test_write_vectored_multiple_buffers() {
    let (_tmp, logs_dir) = setup();
    let mut writer = SizeFileWriter::new("test_event", &logs_dir, 1024 * 1024)
        .expect("create writer");

    let buf1 = make_4096_buffer(0x01);
    let buf2 = make_4096_buffer(0x02);
    let buf3 = make_4096_buffer(0x03);
    let buf4 = make_4096_buffer(0x04);

    let buffers: Vec<&[u8]> = vec![&buf1, &buf2, &buf3, &buf4];
    let written = writer.write_vectored(&buffers).expect("write_vectored");
    assert!(written > 0);

    writer.close().expect("close");
}

#[test]
fn test_file_offset_stays_aligned() {
    let (_tmp, logs_dir) = setup();
    let writer = SizeFileWriter::new("test_event", &logs_dir, 10 * 1024 * 1024)
        .expect("create writer");

    let buf = make_4096_buffer(0xAA);

    for _ in 0..20 {
        let buffers: Vec<&[u8]> = vec![&buf];
        writer.write_vectored(&buffers).expect("write_vectored");
    }
}

#[test]
fn test_close_no_tmp_remains() {
    let (_tmp, logs_dir) = setup();
    let mut writer = SizeFileWriter::new("test_event", &logs_dir, 1024 * 1024)
        .expect("create writer");

    let buf = make_4096_buffer(0xBB);
    writer.write_vectored(&[&buf[..]]).expect("write");
    writer.close().expect("close");

    common::assert_no_tmp_files(&logs_dir);

    let log_files = common::find_log_files(&logs_dir);
    assert!(!log_files.is_empty(), "should have at least one .log file");
}

#[test]
fn test_empty_file_not_sealed() {
    let (_tmp, logs_dir) = setup();
    let mut writer = SizeFileWriter::new("test_event", &logs_dir, 1024 * 1024)
        .expect("create writer");

    // Close without writing anything
    writer.close().expect("close");

    // Bug 2 fix: no 0-byte .log files should exist
    let log_files = common::find_log_files(&logs_dir);
    assert!(log_files.is_empty(), "no .log files should exist for empty file");
}
