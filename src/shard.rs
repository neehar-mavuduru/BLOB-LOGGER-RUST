use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_channel::Sender;

use crate::buffer::Buffer;
use crate::error::Error;

/// Double-buffered write shard. Writers target the active buffer; on swap the
/// inactive buffer is sent to the flush channel for disk write.
#[derive(Debug)]
pub struct Shard {
    buffer_a: Arc<Buffer>,
    buffer_b: Arc<Buffer>,
    /// 0 = buffer_a active, 1 = buffer_b active.
    active: Arc<AtomicUsize>,
    swapping: Arc<AtomicBool>,
    /// Set when the shard has been swapped and the inactive buffer is pending flush.
    pub ready_for_flush: Arc<AtomicBool>,
    flush_tx: Sender<Arc<Buffer>>,
    id: u32,
}

impl Shard {
    /// Creates a new shard with two buffers of `capacity` bytes each.
    pub fn new(
        capacity: usize,
        id: u32,
        flush_tx: Sender<Arc<Buffer>>,
    ) -> Result<Self, Error> {
        let buffer_a = Arc::new(Buffer::new(capacity, id)?);
        let buffer_b = Arc::new(Buffer::new(capacity, id)?);

        Ok(Self {
            buffer_a,
            buffer_b,
            active: Arc::new(AtomicUsize::new(0)),
            swapping: Arc::new(AtomicBool::new(false)),
            ready_for_flush: Arc::new(AtomicBool::new(false)),
            flush_tx,
            id,
        })
    }

    fn active_buffer(&self) -> &Arc<Buffer> {
        if self.active.load(Ordering::Acquire) == 0 {
            &self.buffer_a
        } else {
            &self.buffer_b
        }
    }

    #[allow(dead_code)]
    fn inactive_buffer(&self) -> &Arc<Buffer> {
        if self.active.load(Ordering::Acquire) == 0 {
            &self.buffer_b
        } else {
            &self.buffer_a
        }
    }

    /// Writes `data` to the active buffer. Returns `(bytes_reserved, threshold_hit)`.
    pub fn write(&self, data: &[u8]) -> Result<(usize, bool), Error> {
        let buf = self.active_buffer();
        let (n, swap_signal) = buf.write(data)?;

        if n == 0 && swap_signal {
            // Buffer full — swap synchronously
            self.try_swap();
            return Ok((0, true));
        }

        if swap_signal {
            // 90% full — proactive swap in background
            let shard_active = self.active.clone();
            let shard_swapping = self.swapping.clone();
            let shard_ready = self.ready_for_flush.clone();
            let buf_a = self.buffer_a.clone();
            let buf_b = self.buffer_b.clone();
            let tx = self.flush_tx.clone();

            tokio::task::spawn_blocking(move || {
                if shard_swapping
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return;
                }

                let active_idx = shard_active.load(Ordering::Acquire);
                let new_idx = 1 - active_idx;
                let to_flush = if active_idx == 0 { &buf_a } else { &buf_b };
                let next_active = if active_idx == 0 { &buf_b } else { &buf_a };

                // Background path: wait a bit longer for the inactive buffer
                let mut available = false;
                for _ in 0..100_000 {
                    if next_active.is_flushed() {
                        available = true;
                        break;
                    }
                    std::hint::spin_loop();
                }

                if !available {
                    shard_swapping.store(false, Ordering::Release);
                    return;
                }

                shard_active.store(new_idx, Ordering::Release);
                to_flush.wait_for_inflight();
                to_flush.write_header();
                to_flush.set_pending_flush();

                if let Err(e) = tx.try_send(to_flush.clone()) {
                    tracing::warn!("flush channel full on proactive swap: {}", e);
                }

                shard_ready.store(true, Ordering::Release);
                shard_swapping.store(false, Ordering::Release);
            });
        }

        Ok((n, false))
    }

    /// Atomically swaps active/inactive buffers and sends the old active buffer
    /// (containing data) to the flush channel. Only one thread wins the CAS.
    pub fn try_swap(&self) {
        if self
            .swapping
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let active_idx = self.active.load(Ordering::Acquire);
        let new_idx = 1 - active_idx;
        let to_flush = if active_idx == 0 {
            &self.buffer_a
        } else {
            &self.buffer_b
        };
        let next_active = if active_idx == 0 {
            &self.buffer_b
        } else {
            &self.buffer_a
        };

        // Brief wait for next active buffer to be available (flushed)
        let mut available = next_active.is_flushed();
        if !available {
            for _ in 0..1000 {
                std::hint::spin_loop();
                if next_active.is_flushed() {
                    available = true;
                    break;
                }
            }
        }

        if !available {
            // Flush worker hasn't processed the inactive buffer yet; give up
            self.swapping.store(false, Ordering::Release);
            return;
        }

        self.active.store(new_idx, Ordering::Release);
        to_flush.wait_for_inflight();
        to_flush.write_header();
        to_flush.set_pending_flush();

        if let Err(e) = self.flush_tx.try_send(to_flush.clone()) {
            tracing::warn!("flush channel full on try_swap: {}", e);
        }

        self.ready_for_flush.store(true, Ordering::Release);
        self.swapping.store(false, Ordering::Release);
    }

    /// Flushes the shard if it has pending data, regardless of ready_for_flush state.
    pub fn flush_all(&self) {
        self.try_swap();
    }

    /// Shard identifier.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Access to the active index (for testing).
    pub fn active_index(&self) -> &Arc<AtomicUsize> {
        &self.active
    }

    /// Access to the swapping flag (for testing).
    pub fn swapping_flag(&self) -> &Arc<AtomicBool> {
        &self.swapping
    }

    /// Access to buffer A (for testing).
    pub fn buffer_a(&self) -> &Arc<Buffer> {
        &self.buffer_a
    }

    /// Access to buffer B (for testing).
    pub fn buffer_b(&self) -> &Arc<Buffer> {
        &self.buffer_b
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        self.buffer_a.reset();
        self.buffer_b.reset();
    }
}
