use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{self, Receiver, Sender};

use crate::buffer::Buffer;
use crate::config::Config;
use crate::error::Error;
use crate::file_writer::{FileWriter, SizeFileWriter};
use crate::shard_collection::ShardCollection;
use crate::stats::Statistics;

/// High-performance logger with lock-free sharded writes and background flush.
#[derive(Debug)]
pub struct Logger {
    shard_collection: Arc<ShardCollection>,
    file_writer: Arc<parking_lot::Mutex<Box<dyn FileWriter>>>,
    #[allow(dead_code)]
    flush_rx: Receiver<Arc<Buffer>>,
    #[allow(dead_code)]
    flush_tx: Sender<Arc<Buffer>>,
    shutdown_tx: Sender<()>,
    flush_task: Option<std::thread::JoinHandle<()>>,
    stats: Arc<Statistics>,
    #[allow(dead_code)]
    config: Config,
    closed: Arc<AtomicBool>,
}

impl Logger {
    /// Creates a new logger and starts the background flush worker.
    pub fn new(
        base_name: &str,
        logs_dir: &std::path::Path,
        upload_ready_dir: &std::path::Path,
        config: Config,
    ) -> Result<Self, Error> {
        let num_shards = config.num_shards;
        let per_shard_capacity = config.buffer_size / num_shards;

        let (flush_tx, flush_rx) = crossbeam_channel::bounded(num_shards * 2);

        let shard_collection = Arc::new(ShardCollection::new(
            num_shards,
            per_shard_capacity,
            flush_tx.clone(),
        )?);

        let file_writer: Box<dyn FileWriter> = Box::new(SizeFileWriter::new(
            base_name,
            logs_dir,
            upload_ready_dir,
            config.max_file_size,
        )?);
        let file_writer = Arc::new(parking_lot::Mutex::new(file_writer));

        let stats = Arc::new(Statistics::default());
        let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded::<()>(1);

        let flush_task = Self::spawn_flush_worker(
            flush_rx.clone(),
            file_writer.clone(),
            stats.clone(),
            shard_collection.clone(),
            shutdown_rx,
            config.flush_interval,
        );

        Ok(Self {
            shard_collection,
            file_writer,
            flush_rx,
            flush_tx,
            shutdown_tx,
            flush_task: Some(flush_task),
            stats,
            config,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Creates a logger with an injected FileWriter (for testing).
    pub fn with_writer(
        writer: Box<dyn FileWriter>,
        config: Config,
    ) -> Result<Self, Error> {
        let num_shards = config.num_shards;
        let per_shard_capacity = config.buffer_size / num_shards;

        let (flush_tx, flush_rx) = crossbeam_channel::bounded(num_shards * 2);

        let shard_collection = Arc::new(ShardCollection::new(
            num_shards,
            per_shard_capacity,
            flush_tx.clone(),
        )?);

        let file_writer = Arc::new(parking_lot::Mutex::new(writer));
        let stats = Arc::new(Statistics::default());
        let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded::<()>(1);

        let flush_task = Self::spawn_flush_worker(
            flush_rx.clone(),
            file_writer.clone(),
            stats.clone(),
            shard_collection.clone(),
            shutdown_rx,
            config.flush_interval,
        );

        Ok(Self {
            shard_collection,
            file_writer,
            flush_rx,
            flush_tx,
            shutdown_tx,
            flush_task: Some(flush_task),
            stats,
            config,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    fn spawn_flush_worker(
        flush_rx: Receiver<Arc<Buffer>>,
        file_writer: Arc<parking_lot::Mutex<Box<dyn FileWriter>>>,
        stats: Arc<Statistics>,
        shard_collection: Arc<ShardCollection>,
        shutdown_rx: Receiver<()>,
        flush_interval: Duration,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(flush_interval);

            loop {
                crossbeam_channel::select! {
                    recv(flush_rx) -> buf_result => {
                        let buf = match buf_result {
                            Ok(b) => b,
                            Err(_) => break,
                        };

                        let mut buffers = vec![buf];
                        while let Ok(b) = flush_rx.try_recv() {
                            buffers.push(b);
                        }

                        let slices: Vec<&[u8]> = buffers.iter().map(|b| b.data_slice()).collect();

                        match file_writer.lock().write_vectored(&slices) {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!("write_vectored failed: {}", e);
                                stats.write_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        for buf in &buffers {
                            buf.mark_flushed();
                        }
                    }
                    recv(ticker) -> _ => {
                        shard_collection.flush_all();
                    }
                    recv(shutdown_rx) -> _ => {
                        // Final drain: flush all shards, then drain flush channel
                        shard_collection.flush_all();

                        // Small delay to let background swaps complete
                        std::thread::sleep(Duration::from_millis(10));

                        while let Ok(buf) = flush_rx.try_recv() {
                            let slices = vec![buf.data_slice()];
                            match file_writer.lock().write_vectored(&slices) {
                                Ok(_) => {}
                                Err(e) => {
                                    tracing::error!("write_vectored failed during shutdown: {}", e);
                                    stats.write_errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            buf.mark_flushed();
                        }
                        break;
                    }
                }
            }
        })
    }

    /// Logs raw bytes. Zero-copy on the hot path.
    #[inline]
    pub fn log_bytes(&self, data: &[u8]) -> Result<(), Error> {
        self.stats.total_logs.fetch_add(1, Ordering::Relaxed);

        if self.closed.load(Ordering::Acquire) {
            self.stats.dropped_logs.fetch_add(1, Ordering::Relaxed);
            return Err(Error::LoggerClosed);
        }

        match self.shard_collection.write(data)? {
            (n, _) if n > 0 => {
                self.stats
                    .bytes_written
                    .fetch_add(n as u64, Ordering::Relaxed);
                Ok(())
            }
            (0, _) => {
                // Buffer full — brief back-pressure
                std::thread::sleep(Duration::from_millis(50));

                match self.shard_collection.write(data)? {
                    (n, _) if n > 0 => {
                        self.stats
                            .bytes_written
                            .fetch_add(n as u64, Ordering::Relaxed);
                        Ok(())
                    }
                    _ => {
                        self.shard_collection.flush_all();
                        match self.shard_collection.write(data)? {
                            (n, _) if n > 0 => {
                                self.stats
                                    .bytes_written
                                    .fetch_add(n as u64, Ordering::Relaxed);
                                Ok(())
                            }
                            _ => {
                                self.stats.dropped_logs.fetch_add(1, Ordering::Relaxed);
                                Err(Error::BufferFull)
                            }
                        }
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    /// Logs a string message (zero-copy via `as_bytes()`).
    #[inline]
    pub fn log(&self, msg: &str) -> Result<(), Error> {
        self.log_bytes(msg.as_bytes())
    }

    /// Shuts down the logger: flushes all pending data and seals files.
    pub fn close(&mut self) -> Result<(), Error> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(()); // already closed
        }

        self.shard_collection.flush_all();
        let _ = self.shutdown_tx.try_send(());

        if let Some(handle) = self.flush_task.take() {
            let _ = handle.join();
        }

        self.file_writer.lock().close()?;
        Ok(())
    }

    /// Returns a reference to the logger's statistics.
    pub fn get_stats(&self) -> &Arc<Statistics> {
        &self.stats
    }

    /// Returns whether the logger is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}
