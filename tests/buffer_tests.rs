use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use blob_logger_rust::buffer::{Buffer, HEADER_OFFSET};

const TIMESTAMP_SIZE: usize = 8;

#[test]
fn test_initial_offset() {
    let buf = Buffer::new(4096, 0).expect("create buffer");
    assert_eq!(buf.offset().load(Ordering::Relaxed), HEADER_OFFSET as i32);
    buf.free().expect("free buffer");
}

#[test]
fn test_write_length_prefix() {
    let buf = Buffer::new(4096, 0).expect("create buffer");
    let (n, _) = buf.write(b"hello").expect("write");
    assert_eq!(n, 4 + TIMESTAMP_SIZE + 5); // 4 byte prefix + 8 byte timestamp + 5 bytes data

    let slice = buf.data_slice();
    // Length prefix should be timestampSize + len(payload) = 8 + 5 = 13
    let len = u32::from_le_bytes(slice[HEADER_OFFSET..HEADER_OFFSET + 4].try_into().unwrap());
    assert_eq!(len, (TIMESTAMP_SIZE + 5) as u32);

    // Timestamp should be non-zero
    let ts = u64::from_le_bytes(slice[HEADER_OFFSET + 4..HEADER_OFFSET + 12].try_into().unwrap());
    assert!(ts > 0, "timestamp should be non-zero");

    // Payload follows the timestamp
    assert_eq!(&slice[HEADER_OFFSET + 4 + TIMESTAMP_SIZE..HEADER_OFFSET + 4 + TIMESTAMP_SIZE + 5], b"hello");

    buf.free().expect("free");
}

#[test]
fn test_write_offset_advances() {
    let buf = Buffer::new(4096, 0).expect("create buffer");
    let record_size = 4 + TIMESTAMP_SIZE + 5; // prefix + ts + "hello"

    let (n1, _) = buf.write(b"hello").expect("write 1");
    assert_eq!(n1, record_size);
    let off1 = buf.offset().load(Ordering::Relaxed);
    assert_eq!(off1, (HEADER_OFFSET + record_size) as i32);

    let (n2, _) = buf.write(b"world").expect("write 2");
    assert_eq!(n2, record_size);
    let off2 = buf.offset().load(Ordering::Relaxed);
    assert_eq!(off2, (HEADER_OFFSET + 2 * record_size) as i32);

    buf.free().expect("free");
}

#[test]
fn test_write_full_buffer() {
    let buf = Buffer::new(4096, 0).expect("create buffer");
    let payload = vec![0xABu8; 100];
    let rec_len_expected = TIMESTAMP_SIZE + 100; // what's stored in the length prefix
    let mut count = 0;

    loop {
        match buf.write(&payload) {
            Ok((0, true)) => break,
            Ok((n, _)) => {
                assert!(n > 0);
                count += 1;
            }
            Err(e) => panic!("unexpected error: {}", e),
        }
    }

    assert!(count > 0, "should have written at least one record");

    // Decode all records
    let slice = buf.data_slice();
    let offset = buf.offset().load(Ordering::Relaxed) as usize;
    let mut pos = HEADER_OFFSET;
    let mut decoded = 0;
    while pos + 4 <= offset {
        let rec_len = u32::from_le_bytes(slice[pos..pos + 4].try_into().unwrap()) as usize;
        assert_eq!(rec_len, rec_len_expected);
        // Skip 4B prefix + 8B timestamp, verify payload
        assert_eq!(&slice[pos + 4 + TIMESTAMP_SIZE..pos + 4 + rec_len], &payload[..]);
        pos += 4 + rec_len;
        decoded += 1;
    }
    assert_eq!(decoded, count);

    buf.free().expect("free");
}

#[test]
fn test_write_proactive_swap_signal() {
    let buf = Buffer::new(4096, 0).expect("create buffer");
    let payload = vec![0xCDu8; 100];
    let mut got_proactive = false;

    loop {
        match buf.write(&payload) {
            Ok((0, true)) => break,
            Ok((_n, true)) => {
                got_proactive = true;
                break;
            }
            Ok((_n, false)) => continue,
            Err(e) => panic!("unexpected error: {}", e),
        }
    }

    assert!(got_proactive, "should have received proactive swap signal at 90%");
    buf.free().expect("free");
}

#[test]
fn test_write_cas_retry_correctness() {
    let buf = Arc::new(Buffer::new(1024 * 1024, 0).expect("create 1MB buffer"));
    let mut handles = Vec::new();

    for tid in 0u32..50 {
        let buf = buf.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0u32..20 {
                let payload = format!("tid={tid:04},i={i:04}");
                let _ = buf.write(payload.as_bytes());
            }
        }));
    }

    for h in handles {
        h.join().expect("thread join");
    }

    // Decode all records and check uniqueness
    let slice = buf.data_slice();
    let offset = buf.offset().load(Ordering::Relaxed) as usize;
    let mut pos = HEADER_OFFSET;
    let mut records = std::collections::HashSet::new();

    while pos + 4 <= offset {
        let rec_len = u32::from_le_bytes(slice[pos..pos + 4].try_into().unwrap()) as usize;
        if pos + 4 + rec_len > offset {
            break;
        }
        // Skip timestamp, extract payload
        let data = String::from_utf8_lossy(&slice[pos + 4 + TIMESTAMP_SIZE..pos + 4 + rec_len]).to_string();
        assert!(records.insert(data.clone()), "duplicate record: {}", data);
        pos += 4 + rec_len;
    }

    assert_eq!(records.len(), 50 * 20, "all 1000 records should be present");
}

#[test]
fn test_write_header() {
    let buf = Buffer::new(4096, 0).expect("create buffer");
    buf.write(b"test data").expect("write");
    buf.write_header();

    let slice = buf.data_slice();
    let block_size = u32::from_le_bytes(slice[0..4].try_into().unwrap());
    let valid_offset = u32::from_le_bytes(slice[4..8].try_into().unwrap());

    assert_eq!(block_size, 4096, "block_size should equal capacity");
    let expected_valid = buf.offset().load(Ordering::Relaxed) as u32 - HEADER_OFFSET as u32;
    assert_eq!(valid_offset, expected_valid);

    buf.free().expect("free");
}

#[test]
fn test_reset() {
    let buf = Buffer::new(4096, 0).expect("create buffer");
    buf.write(b"some data").expect("write");
    buf.reset();

    assert_eq!(buf.offset().load(Ordering::Relaxed), HEADER_OFFSET as i32);
    let slice = buf.data_slice();
    assert_eq!(&slice[0..8], &[0u8; 8], "header should be zeroed");

    buf.free().expect("free");
}

#[test]
fn test_wait_for_inflight_releases() {
    let buf = Arc::new(Buffer::new(4096, 0).expect("create buffer"));
    buf.inflight().fetch_add(1, Ordering::Release);

    let buf2 = buf.clone();
    let handle = std::thread::spawn(move || {
        buf2.wait_for_inflight();
    });

    std::thread::sleep(Duration::from_millis(50));
    assert!(!handle.is_finished(), "should still be waiting");

    buf.inflight().fetch_sub(1, Ordering::Release);
    handle.join().expect("thread should complete");
}
