use std::sync::atomic::Ordering;

use blob_logger_rust::shard_collection::ShardCollection;

#[test]
fn test_write_distributes() {
    let (tx, _rx) = crossbeam_channel::bounded(128);
    let sc = ShardCollection::new(8, 65536, tx).expect("create collection");

    for _ in 0..10_000 {
        let _ = sc.write(b"test data payload");
    }

    // Check that writes distributed across multiple shards
    let shards = sc.shards();
    let mut non_empty = 0;
    for s in shards {
        // Check if buffer_a or buffer_b got any writes (offset > HEADER_OFFSET)
        let a_off = s.buffer_a().offset().load(Ordering::Relaxed);
        let b_off = s.buffer_b().offset().load(Ordering::Relaxed);
        if a_off > 8 || b_off > 8 {
            non_empty += 1;
        }
    }
    assert!(non_empty > 1, "writes should distribute across multiple shards, got {}", non_empty);
}

#[test]
fn test_threshold_triggers() {
    let (tx, _rx) = crossbeam_channel::bounded(128);
    let sc = ShardCollection::new(8, 65536, tx).expect("create collection");

    // Mark 2 of 8 shards as ready (threshold = max(1, 8*25/100) = 2)
    sc.shards()[0].ready_for_flush.store(true, Ordering::Relaxed);
    sc.shards()[1].ready_for_flush.store(true, Ordering::Relaxed);

    let (_, threshold) = sc.write(b"test").expect("write");
    assert!(threshold, "threshold should be reached with 2 ready shards");
}

#[test]
fn test_threshold_below() {
    let (tx, _rx) = crossbeam_channel::bounded(128);
    let sc = ShardCollection::new(8, 65536, tx).expect("create collection");

    // Mark only 1 of 8 as ready
    sc.shards()[0].ready_for_flush.store(true, Ordering::Relaxed);

    let (_, threshold) = sc.write(b"test").expect("write");
    assert!(!threshold, "threshold should not be reached with only 1 ready shard");
}

#[test]
fn test_flush_all_swaps_ready() {
    let (tx, rx) = crossbeam_channel::bounded(128);
    let sc = ShardCollection::new(8, 65536, tx).expect("create collection");

    // Write data to several shards and mark 4 as ready
    for i in 0..4 {
        sc.shards()[i].write(b"data").expect("write");
        sc.shards()[i].ready_for_flush.store(true, Ordering::Relaxed);
    }

    sc.flush_all();

    // Should have received buffers from the flush
    std::thread::sleep(std::time::Duration::from_millis(200));
    let mut count = 0;
    while rx.try_recv().is_ok() {
        count += 1;
    }
    assert!(count > 0, "flush_all should send buffers to flush channel");
}
