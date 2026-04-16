//! Low-level I/O primitives. **All `unsafe` code in the crate lives here.**
//!
//! On Linux: uses `mmap`, `pwritev`, `fallocate`, `O_DIRECT` via `nix`.
//! On other platforms: safe fallbacks via `std::fs`.

use crate::error::Error;
use std::path::Path;

// ─── Memory helpers (used by Buffer) ─────────────────────────────────────────

/// Writes a little-endian `u32` at `base + offset`.
///
/// # Safety contract (internal)
/// Caller guarantees `offset + 4 <= allocated region size` and that no other
/// writer is concurrently writing to the same 4 bytes.
pub fn write_u32_le(base: *mut u8, offset: usize, value: u32) {
    let bytes = value.to_le_bytes();
    // SAFETY: caller guarantees `offset + 4` is within the allocated mmap region
    // and that the 4-byte target range is not concurrently written by another thread.
    // The base pointer comes from mmap_alloc which returns a valid, page-aligned region.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), base.add(offset), 4);
    }
}

/// Writes a little-endian `u64` at `base + offset`.
pub fn write_u64_le(base: *mut u8, offset: usize, value: u64) {
    let bytes = value.to_le_bytes();
    // SAFETY: same contract as write_u32_le — caller guarantees `offset + 8` is within bounds.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), base.add(offset), 8);
    }
}

/// Copies `data` into the mmap region at `base + offset`.
pub fn write_bytes(base: *mut u8, offset: usize, data: &[u8]) {
    if data.is_empty() {
        return;
    }
    // SAFETY: caller guarantees `offset + data.len()` is within the mmap region
    // and the target byte range does not overlap with any concurrent writer.
    // CAS-based offset reservation in Buffer::write ensures non-overlapping regions.
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), base.add(offset), data.len());
    }
}

/// Zeros `size` bytes starting at `base`.
pub fn zero_region(base: *mut u8, size: usize) {
    // SAFETY: caller guarantees `base` points to a valid allocation of at least `size` bytes.
    // Called only when no concurrent readers/writers are active (after wait_for_inflight).
    unsafe {
        std::ptr::write_bytes(base, 0, size);
    }
}

/// Returns a shared slice over `[base, base+len)`.
///
/// # Safety contract (internal)
/// The caller must ensure the region remains valid for the lifetime `'a` and
/// that no mutable alias exists for the returned range during `'a`.
pub fn as_slice<'a>(base: *const u8, len: usize) -> &'a [u8] {
    // SAFETY: base is a valid mmap pointer, len <= allocated size,
    // and the buffer is not being freed during the lifetime of the returned slice.
    // The flush path holds references only while the buffer is in the "inactive" state.
    unsafe { std::slice::from_raw_parts(base, len) }
}

// ─── Platform-specific I/O ──────────────────────────────────────────────────

#[cfg(target_os = "linux")]
mod platform {
    use super::*;
    use nix::fcntl::OFlag;
    use nix::sys::stat::Mode;
    use nix::unistd;
    use std::os::fd::AsFd;

    const PAGE: usize = 4096;

    /// Allocates an anonymous private mmap of `size` bytes (rounded up to 4096).
    pub fn mmap_alloc(size: usize) -> Result<(*mut u8, usize), Error> {
        let rounded = (size + PAGE - 1) & !(PAGE - 1);
        let len = std::num::NonZeroUsize::new(rounded)
            .ok_or_else(|| Error::Mmap("size must be > 0".into()))?;
        // SAFETY: Anonymous mapping with no file backing; len is non-zero and page-aligned.
        let ptr = unsafe {
            nix::sys::mman::mmap_anonymous(
                None,
                len,
                nix::sys::mman::ProtFlags::PROT_READ | nix::sys::mman::ProtFlags::PROT_WRITE,
                nix::sys::mman::MapFlags::MAP_PRIVATE,
            )
            .map_err(|e| Error::Mmap(format!("mmap failed: {e}")))?
        };
        Ok((ptr.as_ptr() as *mut u8, rounded))
    }

    /// Releases a previously allocated mmap region.
    pub fn mmap_free(ptr: *mut u8, size: usize) -> Result<(), Error> {
        // SAFETY: `ptr` was returned by a prior mmap_alloc with the same `size`.
        // After munmap the pointer is invalid; the caller must not dereference it.
        unsafe {
            let nn = std::num::NonZeroUsize::new(size)
                .ok_or_else(|| Error::Mmap("munmap size must be > 0".into()))?;
            nix::sys::mman::munmap(
                std::ptr::NonNull::new(ptr as *mut _).unwrap(),
                nn.get(),
            )
            .map_err(|e| Error::Mmap(format!("munmap failed: {e}")))?;
        }
        Ok(())
    }

    /// Writes byte slices to `fd` at `offset` using `pwritev(2)`.
    pub fn pwritev(fd: i32, bufs: &[&[u8]], offset: i64) -> Result<usize, Error> {
        use std::io::IoSlice;

        for (i, buf) in bufs.iter().enumerate() {
            debug_assert_eq!(
                buf.len() % PAGE,
                0,
                "pwritev buffer {i} length {} is not 4096-aligned",
                buf.len()
            );
        }

        let io_slices: Vec<IoSlice<'_>> = bufs.iter().map(|b| IoSlice::new(b)).collect();

        // SAFETY: fd is a valid open file descriptor obtained from open_direct.
        // The IoSlice references are valid for the duration of the syscall.
        // UB would occur if fd is closed concurrently or if the slices point to freed memory.
        let written = unsafe {
            let fd_borrowed = std::os::fd::BorrowedFd::borrow_raw(fd);
            nix::sys::uio::pwritev(fd_borrowed, &io_slices, offset)
                .map_err(|e| Error::Pwritev(format!("pwritev failed: {e}")))?
        };
        Ok(written)
    }

    /// Preallocates `size` bytes on `fd` using `fallocate(2)`.
    pub fn fallocate(fd: i32, size: i64) -> Result<(), Error> {
        use nix::fcntl::FallocateFlags;
        // SAFETY: fd is a valid open file descriptor. fallocate does not affect memory safety.
        nix::fcntl::fallocate(fd, FallocateFlags::empty(), 0, size).map_err(|e| {
            Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;
        Ok(())
    }

    /// `fsync(2)` wrapper.
    pub fn fsync(fd: i32) -> Result<(), Error> {
        nix::unistd::fsync(fd)
            .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
        Ok(())
    }

    /// `ftruncate(2)` wrapper.
    pub fn ftruncate(fd: i32, size: i64) -> Result<(), Error> {
        // SAFETY: fd is a valid open file descriptor.
        unsafe {
            let fd_borrowed = std::os::fd::BorrowedFd::borrow_raw(fd);
            unistd::ftruncate(fd_borrowed.as_fd(), size)
                .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
        }
        Ok(())
    }

    /// Opens a file with `O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT | O_DSYNC`.
    pub fn open_direct(path: &Path) -> Result<i32, Error> {
        use std::os::fd::IntoRawFd;

        let flags = OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_TRUNC | OFlag::O_DIRECT | OFlag::O_DSYNC;
        let mode = Mode::from_bits_truncate(0o644);

        let fd = nix::fcntl::open(path, flags, mode)
            .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("open_direct: {e}"))))?;
        Ok(fd.into_raw_fd())
    }

    /// Closes a raw file descriptor.
    pub fn close_fd(fd: i32) -> Result<(), Error> {
        // SAFETY: fd is a valid open file descriptor that has not been closed yet.
        // After this call, the fd value must not be reused by the caller.
        unsafe {
            let owned = std::os::fd::OwnedFd::from_raw_fd(fd);
            drop(owned);
        }
        Ok(())
    }

    use std::os::fd::FromRawFd;
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::*;
    const PAGE: usize = 4096;

    pub fn mmap_alloc(size: usize) -> Result<(*mut u8, usize), Error> {
        let rounded = (size + PAGE - 1) & !(PAGE - 1);
        let layout = std::alloc::Layout::from_size_align(rounded, PAGE)
            .map_err(|e| Error::Mmap(format!("layout error: {e}")))?;
        // SAFETY: layout has non-zero size (validated upstream) and PAGE alignment.
        // The allocated memory is zeroed, valid for `rounded` bytes, and must be
        // freed with mmap_free (which uses dealloc with the same layout).
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(Error::Mmap("allocation failed".into()));
        }
        Ok((ptr, rounded))
    }

    pub fn mmap_free(ptr: *mut u8, size: usize) -> Result<(), Error> {
        let rounded = (size + PAGE - 1) & !(PAGE - 1);
        let layout = std::alloc::Layout::from_size_align(rounded, PAGE)
            .map_err(|e| Error::Mmap(format!("layout error: {e}")))?;
        // SAFETY: ptr was allocated by mmap_alloc with the same layout.
        // After dealloc the pointer is invalid.
        unsafe { std::alloc::dealloc(ptr, layout) };
        Ok(())
    }

    pub fn pwritev(fd: i32, bufs: &[&[u8]], offset: i64) -> Result<usize, Error> {

        // No O_DIRECT on non-Linux; use standard write_at
        let mut total = 0usize;
        let mut current_offset = offset;

        for buf in bufs {
            let written = write_at(fd, buf, current_offset)?;
            total += written;
            current_offset += written as i64;
        }
        Ok(total)
    }

    fn write_at(fd: i32, buf: &[u8], offset: i64) -> Result<usize, Error> {
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::fs::FileExt;
            use std::os::unix::io::FromRawFd;

            // SAFETY: We borrow the fd without taking ownership by immediately
            // forgetting the File. The fd remains valid.
            let file = unsafe { std::fs::File::from_raw_fd(fd) };
            let result = file.write_at(buf, offset as u64);
            std::mem::forget(file); // don't close the fd
            result.map_err(Error::Io)
        }
        #[cfg(not(target_os = "macos"))]
        {
            use std::os::unix::fs::FileExt;
            use std::os::unix::io::FromRawFd;

            let file = unsafe { std::fs::File::from_raw_fd(fd) };
            let result = file.write_at(buf, offset as u64);
            std::mem::forget(file);
            result.map_err(Error::Io)
        }
    }

    pub fn fallocate(fd: i32, size: i64) -> Result<(), Error> {
        // No fallocate on non-Linux; use ftruncate as approximation
        ftruncate(fd, size)
    }

    pub fn fsync(fd: i32) -> Result<(), Error> {
        use std::os::unix::io::FromRawFd;
        // SAFETY: borrowing fd without taking ownership
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        let result = file.sync_all();
        std::mem::forget(file);
        result.map_err(Error::Io)
    }

    pub fn ftruncate(fd: i32, size: i64) -> Result<(), Error> {
        use std::os::unix::io::FromRawFd;
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        let result = file.set_len(size as u64);
        std::mem::forget(file);
        result.map_err(Error::Io)
    }

    pub fn open_direct(path: &Path) -> Result<i32, Error> {
        use std::os::unix::io::IntoRawFd;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(file.into_raw_fd())
    }

    pub fn close_fd(fd: i32) -> Result<(), Error> {
        use std::os::unix::io::FromRawFd;
        // SAFETY: fd is a valid open file descriptor. Taking ownership to close it.
        let _file = unsafe { std::fs::File::from_raw_fd(fd) };
        // File dropped here → fd closed
        Ok(())
    }
}

// Re-export platform functions at module level
pub use platform::*;
