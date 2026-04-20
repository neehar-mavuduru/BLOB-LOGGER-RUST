use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;

use crate::error::Error;
use crate::unsafe_io;

/// Bytes reserved at the start of each buffer for the block header.
pub const HEADER_OFFSET: usize = 8;
/// Byte offset of the `block_size` field (u32 LE) within the header.
pub const HEADER_BLOCK_SIZE_OFFSET: usize = 0;
/// Byte offset of the `valid_offset` field (u32 LE) within the header.
pub const HEADER_VALID_OFFSET_OFFSET: usize = 4;

/// Lock-free, mmap-backed write buffer with CAS-based offset reservation.
///
/// Each record is stored as a 4-byte LE length prefix followed by the payload.
/// Before flush, an 8-byte block header is written at offset 0.
pub struct Buffer {
    data_ptr: *mut u8,
    capacity: usize,
    offset: Arc<AtomicI32>,
    inflight: Arc<AtomicI64>,
    /// True when the buffer is available for reuse (flushed to disk and reset).
    flushed: Arc<std::sync::atomic::AtomicBool>,
    shard_id: u32,
}

// SAFETY: Buffer is Send + Sync because:
// - data_ptr writes are gated by atomic CAS on offset, ensuring non-overlapping regions
// - All concurrent writes target disjoint byte ranges by construction
// - The mmap lifetime is managed by the owning Shard; free() is explicit and consuming
unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("capacity", &self.capacity)
            .field("offset", &self.offset.load(Ordering::Relaxed))
            .field("shard_id", &self.shard_id)
            .finish()
    }
}

impl Buffer {
    /// Allocates a new mmap-backed buffer of `capacity` bytes.
    pub fn new(capacity: usize, shard_id: u32) -> Result<Self, Error> {
        let (data_ptr, _actual_size) = unsafe_io::mmap_alloc(capacity)?;
        unsafe_io::zero_region(data_ptr, capacity);

        Ok(Self {
            data_ptr,
            capacity,
            offset: Arc::new(AtomicI32::new(HEADER_OFFSET as i32)),
            inflight: Arc::new(AtomicI64::new(0)),
            flushed: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            shard_id,
        })
    }

    /// Attempts to write `data` into the buffer using CAS-based offset reservation.
    ///
    /// On-disk record layout: `[4-byte recLen (LE)][8-byte timestamp (LE)][payload]`
    /// where `recLen = 8 + len(payload)` (matches Go's format).
    ///
    /// Returns `(bytes_reserved, should_swap)`:
    /// - `(0, true)` means the buffer is full.
    /// - `(n, true)` means the write succeeded but the buffer is >=90% full (proactive swap).
    /// - `(n, false)` means normal success.
    pub fn write(&self, data: &[u8]) -> Result<(usize, bool), Error> {
        const TIMESTAMP_SIZE: usize = 8;
        let total_size = 4 + TIMESTAMP_SIZE + data.len();

        loop {
            let current = self.offset.load(Ordering::Acquire);
            if current as usize + total_size >= self.capacity {
                return Ok((0, true));
            }

            match self.offset.compare_exchange_weak(
                current,
                current + total_size as i32,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.inflight.fetch_add(1, Ordering::AcqRel);

                    let off = current as usize;
                    // Length prefix: timestampSize + len(payload)
                    unsafe_io::write_u32_le(self.data_ptr, off, (TIMESTAMP_SIZE + data.len()) as u32);
                    // 8-byte nanosecond timestamp
                    let ts_nanos = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64;
                    unsafe_io::write_u64_le(self.data_ptr, off + 4, ts_nanos);
                    // Payload
                    unsafe_io::write_bytes(self.data_ptr, off + 4 + TIMESTAMP_SIZE, data);

                    self.inflight.fetch_sub(1, Ordering::AcqRel);

                    let new_offset = off + total_size;
                    let proactive = new_offset >= self.capacity * 9 / 10;
                    return Ok((total_size, proactive));
                }
                Err(_) => continue,
            }
        }
    }

    /// Writes the 8-byte block header: `[block_size: u32 LE, valid_offset: u32 LE]`.
    ///
    /// `block_size` is always set to `capacity` so every block on disk is a fixed,
    /// predictable size. This simplifies parsing and inherently satisfies 4096 alignment.
    /// `valid_offset` records how many bytes of actual record data the block contains.
    /// Must be called immediately before flushing via `pwritev`.
    pub fn write_header(&self) {
        let valid = (self.offset.load(Ordering::Acquire) - HEADER_OFFSET as i32) as u32;
        unsafe_io::write_u32_le(self.data_ptr, HEADER_BLOCK_SIZE_OFFSET, self.capacity as u32);
        unsafe_io::write_u32_le(self.data_ptr, HEADER_VALID_OFFSET_OFFSET, valid);
    }

    /// Returns the full buffer content as a byte slice (capacity bytes).
    pub fn data_slice(&self) -> &[u8] {
        unsafe_io::as_slice(self.data_ptr as *const u8, self.capacity)
    }

    /// Returns true if the buffer has no record data (only the header region).
    pub fn is_empty(&self) -> bool {
        self.offset.load(Ordering::Acquire) as usize <= HEADER_OFFSET
    }

    /// Zeros the buffer and resets offset to `HEADER_OFFSET`.
    pub fn reset(&self) {
        unsafe_io::zero_region(self.data_ptr, self.capacity);
        self.offset.store(HEADER_OFFSET as i32, Ordering::Release);
    }

    /// Spin-waits until all in-flight writes complete.
    pub fn wait_for_inflight(&self) {
        while self.inflight.load(Ordering::Acquire) > 0 {
            std::hint::spin_loop();
        }
    }

    /// Marks the buffer as flushed: resets data and sets the flushed flag.
    /// Called by the flush worker after writing to disk.
    pub fn mark_flushed(&self) {
        self.reset();
        self.flushed.store(true, Ordering::Release);
    }

    /// Returns true if the buffer has been flushed and is available for reuse.
    pub fn is_flushed(&self) -> bool {
        self.flushed.load(Ordering::Acquire)
    }

    /// Marks the buffer as pending flush (not available for reuse).
    pub fn set_pending_flush(&self) {
        self.flushed.store(false, Ordering::Release);
    }

    /// Releases the underlying mmap allocation. Consuming — takes ownership.
    pub fn free(self) -> Result<(), Error> {
        unsafe_io::mmap_free(self.data_ptr, self.capacity)
    }

    /// Current write offset.
    pub fn offset(&self) -> &Arc<AtomicI32> {
        &self.offset
    }

    /// In-flight write counter.
    pub fn inflight(&self) -> &Arc<AtomicI64> {
        &self.inflight
    }

    /// Buffer capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The shard id this buffer belongs to.
    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    /// Raw data pointer (for testing only).
    #[cfg(test)]
    pub fn data_ptr(&self) -> *mut u8 {
        self.data_ptr
    }
}
