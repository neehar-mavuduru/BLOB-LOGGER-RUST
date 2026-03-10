use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_channel::Sender;
use rand::Rng;

use crate::buffer::Buffer;
use crate::error::Error;
use crate::shard::Shard;

/// Collection of write shards with random dispatch and threshold-based flush signalling.
#[derive(Debug)]
pub struct ShardCollection {
    shards: Vec<Arc<Shard>>,
    /// Number of ready shards that triggers a file-level flush.
    threshold: usize,
    #[allow(dead_code)]
    flush_tx: Sender<Arc<Buffer>>,
}

impl ShardCollection {
    /// Creates `num_shards` shards, each with `per_shard_capacity` bytes.
    pub fn new(
        num_shards: usize,
        per_shard_capacity: usize,
        flush_tx: Sender<Arc<Buffer>>,
    ) -> Result<Self, Error> {
        let mut shards = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            shards.push(Arc::new(Shard::new(
                per_shard_capacity,
                i as u32,
                flush_tx.clone(),
            )?));
        }

        let threshold = std::cmp::max(1, num_shards * 25 / 100);

        Ok(Self {
            shards,
            threshold,
            flush_tx,
        })
    }

    /// Writes `data` to a randomly selected shard.
    /// Returns `(bytes_reserved, threshold_reached)`.
    pub fn write(&self, data: &[u8]) -> Result<(usize, bool), Error> {
        let idx = rand::thread_rng().gen_range(0..self.shards.len());
        let (n, _) = self.shards[idx].write(data)?;

        let ready_count = self.ready_shard_count();
        Ok((n, ready_count >= self.threshold))
    }

    /// Flushes all shards unconditionally.
    pub fn flush_all(&self) {
        for shard in &self.shards {
            shard.flush_all();
        }
    }

    /// Counts shards with `ready_for_flush == true`.
    pub fn ready_shard_count(&self) -> usize {
        self.shards
            .iter()
            .filter(|s| s.ready_for_flush.load(Ordering::Relaxed))
            .count()
    }

    /// Returns a reference to all shards (for testing / stats).
    pub fn shards(&self) -> &[Arc<Shard>] {
        &self.shards
    }
}
