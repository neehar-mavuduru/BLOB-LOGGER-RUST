use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::error::Error;
use crate::unsafe_io;

static FILE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Abstraction over vectored file writing with rotation support.
pub trait FileWriter: Send + Sync + std::fmt::Debug {
    /// Writes multiple 4096-aligned buffers at the current file offset.
    fn write_vectored(&self, buffers: &[&[u8]]) -> Result<usize, Error>;
    /// Duration of the last pwritev call.
    fn last_pwritev_duration(&self) -> Duration;
    /// Closes the writer, sealing the current file.
    fn close(&mut self) -> Result<(), Error>;
}

/// Mutable state protected by a Mutex for interior mutability.
struct WriterState {
    current_fd: Option<i32>,
    current_tmp_path: PathBuf,
    next_fd: Option<i32>,
    next_tmp_path: PathBuf,
    file_offset: i64,
}

/// Size-based rotating file writer using O_DIRECT-aligned I/O.
///
/// Implements Bug 1 (alignment), Bug 2 (empty file) fixes from the Go reference.
pub struct SizeFileWriter {
    base_name: String,
    logs_dir: PathBuf,
    upload_ready_dir: PathBuf,
    max_file_size: i64,
    state: Mutex<WriterState>,
    last_pwritev_ns: AtomicU64,
    next_file_ready: AtomicBool,
}

impl std::fmt::Debug for SizeFileWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        f.debug_struct("SizeFileWriter")
            .field("base_name", &self.base_name)
            .field("file_offset", &state.file_offset)
            .field("max_file_size", &self.max_file_size)
            .finish()
    }
}

impl SizeFileWriter {
    /// Creates the first `.tmp` file and preallocates space.
    pub fn new(
        base_name: &str,
        logs_dir: &std::path::Path,
        upload_ready_dir: &std::path::Path,
        max_file_size: u64,
    ) -> Result<Self, Error> {
        let tmp_path = Self::make_tmp_path(base_name, logs_dir);
        let fd = unsafe_io::open_direct(&tmp_path)?;

        let aligned_max = Self::align_up(max_file_size as i64);
        unsafe_io::fallocate(fd, aligned_max)?;

        Ok(Self {
            base_name: base_name.to_string(),
            logs_dir: logs_dir.to_path_buf(),
            upload_ready_dir: upload_ready_dir.to_path_buf(),
            max_file_size: max_file_size as i64,
            state: Mutex::new(WriterState {
                current_fd: Some(fd),
                current_tmp_path: tmp_path,
                next_fd: None,
                next_tmp_path: PathBuf::new(),
                file_offset: 0,
            }),
            last_pwritev_ns: AtomicU64::new(0),
            next_file_ready: AtomicBool::new(false),
        })
    }

    fn make_tmp_path(base_name: &str, logs_dir: &std::path::Path) -> PathBuf {
        let ts = chrono::Local::now().format("%Y-%m-%d_%H-%M-%S");
        let seq = FILE_SEQ.fetch_add(1, Ordering::Relaxed);
        logs_dir.join(format!("{base_name}_{ts}_{seq}.log.tmp"))
    }

    fn align_up(v: i64) -> i64 {
        (v + 4095) & !4095
    }

    fn pre_create_next_file(&self) -> Result<(i32, PathBuf), Error> {
        let path = Self::make_tmp_path(&self.base_name, &self.logs_dir);
        let fd = unsafe_io::open_direct(&path)?;
        let aligned_max = Self::align_up(self.max_file_size);
        unsafe_io::fallocate(fd, aligned_max)?;
        Ok((fd, path))
    }

    fn strip_tmp_extension(path: &std::path::Path) -> PathBuf {
        let s = path.to_string_lossy();
        if let Some(stripped) = s.strip_suffix(".tmp") {
            PathBuf::from(stripped)
        } else {
            path.to_path_buf()
        }
    }

    /// Rotates the current file: seal, rename, symlink, and prepare next file.
    /// Must be called with `state` already locked.
    fn rotate_inner(&self, state: &mut WriterState) -> Result<(), Error> {
        let fd = state
            .current_fd
            .take()
            .ok_or_else(|| Error::Io(std::io::Error::other("no current fd")))?;

        unsafe_io::fsync(fd)?;

        if state.file_offset == 0 {
            // Bug 2 fix: skip 0-byte files
            unsafe_io::close_fd(fd)?;
            let _ = std::fs::remove_file(&state.current_tmp_path);
            state.current_tmp_path = PathBuf::new();
        } else {
            unsafe_io::ftruncate(fd, state.file_offset)?;
            unsafe_io::close_fd(fd)?;

            let final_path = Self::strip_tmp_extension(&state.current_tmp_path);
            std::fs::rename(&state.current_tmp_path, &final_path)?;

            let symlink_path = self.upload_ready_dir.join(
                final_path
                    .file_name()
                    .ok_or_else(|| Error::Io(std::io::Error::other("no filename")))?,
            );
            // Remove stale symlink if it exists, then create a new one
            let _ = std::fs::remove_file(&symlink_path);
            if let Err(e) = std::os::unix::fs::symlink(&final_path, &symlink_path) {
                tracing::error!("failed to create symlink {:?} → {:?}: {}", symlink_path, final_path, e);
            }
        }

        state.file_offset = 0;

        if state.next_fd.is_some() && self.next_file_ready.load(Ordering::Relaxed) {
            state.current_fd = state.next_fd.take();
            state.current_tmp_path = state.next_tmp_path.clone();
            self.next_file_ready.store(false, Ordering::Relaxed);
        } else {
            let (fd, path) = self.pre_create_next_file()?;
            state.current_fd = Some(fd);
            state.current_tmp_path = path;
        }

        tracing::info!(base_name = %self.base_name, "file rotated");
        Ok(())
    }
}

impl FileWriter for SizeFileWriter {
    fn write_vectored(&self, buffers: &[&[u8]]) -> Result<usize, Error> {
        for (i, buf) in buffers.iter().enumerate() {
            if buf.len() % 4096 != 0 {
                return Err(Error::Pwritev(format!(
                    "buffer {i} length {} is not 4096-aligned",
                    buf.len()
                )));
            }
        }

        let mut state = self.state.lock();

        let fd = state.current_fd.ok_or_else(|| {
            Error::Io(std::io::Error::other("no current fd"))
        })?;

        // Bug 1 fix: assert alignment before every pwritev
        debug_assert_eq!(
            state.file_offset % 4096,
            0,
            "file_offset misalignment detected"
        );

        let t0 = Instant::now();
        let written = unsafe_io::pwritev(fd, buffers, state.file_offset)?;
        self.last_pwritev_ns
            .store(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // Bug 1 fix: advance by padded total, not by bytes written
        let padded_total: i64 = buffers.iter().map(|b| b.len() as i64).sum();
        state.file_offset += padded_total;

        debug_assert_eq!(state.file_offset % 4096, 0);

        let file_offset = state.file_offset;
        let max_file_size = self.max_file_size;

        // Proactive next-file creation at 90%
        if file_offset >= max_file_size * 9 / 10
            && !self.next_file_ready.load(Ordering::Relaxed)
            && state.next_fd.is_none()
        {
            match self.pre_create_next_file() {
                Ok((next_fd, next_path)) => {
                    state.next_fd = Some(next_fd);
                    state.next_tmp_path = next_path;
                    self.next_file_ready.store(true, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::error!("failed to pre-create next file: {}", e);
                }
            }
        }

        // Rotation at max
        if file_offset >= max_file_size {
            self.rotate_inner(&mut state)?;
        }

        Ok(written)
    }

    fn last_pwritev_duration(&self) -> Duration {
        Duration::from_nanos(self.last_pwritev_ns.load(Ordering::Relaxed))
    }

    fn close(&mut self) -> Result<(), Error> {
        let mut state = self.state.lock();

        if let Some(fd) = state.current_fd.take() {
            unsafe_io::fsync(fd)?;

            if state.file_offset == 0 {
                // Bug 2 fix: skip 0-byte files
                unsafe_io::close_fd(fd)?;
                let _ = std::fs::remove_file(&state.current_tmp_path);
            } else {
                unsafe_io::ftruncate(fd, state.file_offset)?;
                unsafe_io::close_fd(fd)?;

                let final_path = Self::strip_tmp_extension(&state.current_tmp_path);
                std::fs::rename(&state.current_tmp_path, &final_path)?;

                let symlink_path = self.upload_ready_dir.join(
                    final_path.file_name().unwrap_or_default(),
                );
                let _ = std::fs::remove_file(&symlink_path);
                if let Err(e) = std::os::unix::fs::symlink(&final_path, &symlink_path) {
                    tracing::error!("failed to create symlink on close: {}", e);
                }
            }
        }

        // Clean up pre-allocated next file
        if let Some(fd) = state.next_fd.take() {
            unsafe_io::close_fd(fd)?;
            let _ = std::fs::remove_file(&state.next_tmp_path);
        }

        Ok(())
    }
}
