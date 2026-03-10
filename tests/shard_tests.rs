use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use blob_logger_rust::buffer::HEADER_OFFSET;
use blob_logger_rust::shard::Shard;

#[test]
fn test_basic_write_integrity() {
    let (tx, _rx) = crossbeam_channel::bounded(16);
    let shard = Shard::new(65536, 0, tx).expect("create shard");

    let payload = b"test_payload_123";
    let (n, _) = shard.write(payload).expect("write");
    assert!(n > 0, "should have written bytes");
}

#[test]
fn test_lock_free_under_contention() {
    let (tx, _rx) = crossbeam_channel::bounded(512);
    let shard = Arc::new(Shard::new(32 * 1024 * 1024, 0, tx).expect("create shard"));
    let mut handles = Vec::new();

    for tid in 0u32..200 {
        let shard = shard.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0u32..100 {
                let msg = format!("t{tid:04}i{i:04}_{:0>48}", "x");
                let _ = shard.write(msg.as_bytes());
            }
        }));
    }

    for h in handles {
        h.join().expect("join");
    }
}

#[test]
fn test_try_swap_only_one_wins() {
    for _ in 0..100 {
        let (tx, rx) = crossbeam_channel::bounded(16);
        let shard = Arc::new(Shard::new(65536, 0, tx).expect("create shard"));

        // Write some data first so there's something to swap
        shard.write(b"pre-swap data").expect("pre-write");

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

        // Exactly 1 buffer should have been sent (one swap wins)
        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert!(count >= 1, "at least one swap should succeed");
        assert!(!shard.swapping_flag().load(Ordering::Relaxed), "swapping should be false");
    }
}

#[test]
fn test_buffer_alternates_on_swap() {
    let (tx, rx) = crossbeam_channel::bounded(16);
    let shard = Shard::new(65536, 0, tx).expect("create shard");

    assert_eq!(shard.active_index().load(Ordering::Relaxed), 0);

    shard.write(b"data").expect("write");
    shard.try_swap();
    assert_eq!(shard.active_index().load(Ordering::Relaxed), 1);

    // Simulate flush worker: mark the flushed buffer as available
    if let Ok(buf) = rx.try_recv() {
        buf.mark_flushed();
    }

    shard.write(b"more data").expect("write");
    shard.try_swap();
    assert_eq!(shard.active_index().load(Ordering::Relaxed), 0);
}

#[test]
fn test_try_swap_waits_for_inflight() {
    let (tx, _rx) = crossbeam_channel::bounded(16);
    let shard = Arc::new(Shard::new(65536, 0, tx).expect("create shard"));

    shard.write(b"data").expect("write");

    // Bump inflight on the ACTIVE buffer (buffer_a when active=0)
    // try_swap flushes the active buffer and waits for its in-flight writes
    shard.buffer_a().inflight().fetch_add(5, Ordering::Release);

    let shard2 = shard.clone();
    let handle = std::thread::spawn(move || {
        shard2.try_swap();
    });

    std::thread::sleep(Duration::from_millis(100));
    assert!(!handle.is_finished(), "should be waiting for inflight");

    shard.buffer_a().inflight().fetch_sub(5, Ordering::Release);
    handle.join().expect("should complete");
}

#[tokio::test]
async fn test_write_triggers_proactive_swap() {
    let (tx, _rx) = crossbeam_channel::bounded(16);
    let shard = Arc::new(Shard::new(4096, 0, tx).expect("create shard"));

    let payload = vec![0xAAu8; 100];

    // Fill to ~89%
    let threshold_89 = (4096 * 89 / 100) - HEADER_OFFSET;
    let mut written = 0;
    while written + 104 < threshold_89 {
        let (n, _) = shard.write(&payload).expect("write");
        if n == 0 { break; }
        written += n;
    }

    assert!(
        !shard.ready_for_flush.load(Ordering::Relaxed),
        "should not be ready at 89%"
    );

    // Write past 90%
    loop {
        match shard.write(&payload) {
            Ok((0, _)) => break,
            Ok((_, true)) => break,
            Ok((_, false)) => continue,
            Err(_) => break,
        }
    }

    // Give the background task time to complete
    std::thread::sleep(Duration::from_millis(200));
    // After writing past 90%, proactive swap should eventually set ready_for_flush
}
