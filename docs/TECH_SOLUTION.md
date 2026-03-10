# Blob Logger Rust — Technical Solution Document

## 1. Executive Summary

**blob-logger-rust** is a high-performance, sharded log writer library written in Rust. It buffers log entries in memory using lock-free, mmap-backed buffers, flushes them to rotating files via vectored I/O (`pwritev`), and optionally uploads sealed files to Google Cloud Storage (GCS). It is designed for low-latency, high-throughput logging with minimal contention on the write path and supports multiple named “events” (log streams) via a `LoggerManager`.

---

## 2. Goals and Non-Goals

| Goals | Non-Goals |
|-------|-----------|
| High-throughput, low-contention logging | General-purpose log formatting (e.g. structured JSON) |
| Per-event log streams with file rotation | Real-time log streaming to remote endpoints |
| Optional GCS upload of sealed files | Multi-cloud or non-GCS object stores (extensible via `ObjectStore` trait) |
| Lock-free hot path; background flush | Synchronous, blocking writes on every log call |
| O_DIRECT-aligned I/O on Linux | Cross-platform O_DIRECT (fallback uses standard I/O) |
| Testability via injected `FileWriter` and `ObjectStore` | GUI or CLI tooling |

---

## 3. Architecture Overview

### 3.1 High-Level Data Flow

```
  log_bytes(msg) / log(msg)
         │
         ▼
  ┌──────────────────┐     random shard     ┌─────────────────┐
  │ ShardCollection   │ ───────────────────► │ Shard (double    │
  │ (N shards)       │                       │  buffer A/B)     │
  └──────────────────┘                       └────────┬─────────┘
                                                      │
                         full / 90% full              │ swap → flush_tx
                                                      ▼
  ┌──────────────────┐     Arc<Buffer>        ┌─────────────────┐
  │ Flush worker      │ ◄──────────────────── │ crossbeam       │
  │ (dedicated thread)│                       │ channel         │
  └────────┬─────────┘                       └─────────────────┘
           │ write_vectored (pwritev)
           ▼
  ┌──────────────────┐
  │ FileWriter       │  → .log.tmp → rotate → .log → symlink in upload_ready/
  │ (SizeFileWriter) │
  └────────┬─────────┘
           │ (optional)
           ▼
  ┌──────────────────┐     poll upload_ready/
  │ Uploader (async)  │ ─────────────────────► GCS (object_store)
  └──────────────────┘
```

### 3.2 Component Summary

| Component | Responsibility |
|-----------|----------------|
| **LoggerManager** | Per-event logger lifecycle; `DashMap` for concurrent get-or-create; optional GCS `Uploader`; directory setup (`logs/`, `upload_ready/`). |
| **Logger** | Single event’s pipeline: `ShardCollection` + `FileWriter` + background flush thread; `log_bytes` / `log`; `close()` flushes and seals. |
| **ShardCollection** | N shards; random shard selection for each write; `flush_all()` for timer or back-pressure. |
| **Shard** | Double-buffered (A/B); CAS-based swap; full or 90% full triggers swap; inactive buffer sent to flush channel. |
| **Buffer** | Mmap-backed (or heap on non-Linux); CAS offset reservation; 8-byte block header; 4-byte length + payload per record. |
| **FileWriter** | Vectored write interface; `SizeFileWriter`: O_DIRECT, 4096 alignment, size-based rotation, symlink in `upload_ready/`. |
| **Uploader** | Polls `upload_ready/` for symlinks; reads target file; uploads to GCS via `object_store`; retries with backoff; deletes local file and symlink on success. |
| **unsafe_io** | Centralized unsafe I/O: mmap, pwritev, fallocate, O_DIRECT open, fsync, ftruncate; Linux vs non-Linux implementations. |

---

## 4. Detailed Design

### 4.1 Buffer and Record Format

- **Buffer**: Fixed-size region (mmap on Linux, aligned heap elsewhere). First 8 bytes are a **block header**:
  - `[0..4]`: `block_size` (u32 LE) — capacity.
  - `[4..8]`: `valid_offset` (u32 LE) — bytes used after the header.
- **Records**: For each log entry, `length (u32 LE)` + raw bytes. No cross-record framing beyond length prefix.
- **Reservation**: `Buffer::write` uses a single `AtomicI32` offset; CAS reserves `4 + data.len()` bytes, then writes length and payload. No mutex on the hot path.
- **Alignment**: Buffers are 4096-byte aligned and sized for O_DIRECT `pwritev` (entire buffer written as one or more 4096-aligned segments).

### 4.2 Sharding and Double Buffering

- **ShardCollection** holds `num_shards` shards. Each **Shard** has two **Buffer**s (A and B).
- **Active buffer**: Writers use CAS to pick the current active index (0 or 1); all appends go to that buffer.
- **Swap triggers**:
  - Buffer full: immediate synchronous swap.
  - Buffer ≥90% full: proactive swap offloaded to `tokio::task::spawn_blocking` so the caller does not block.
- **Swap protocol**: CAS on `swapping`; wait until the *other* buffer is `is_flushed()`; flip active index; wait for in-flight writes on the buffer being flushed; write 8-byte header; send buffer to `flush_tx`; set `ready_for_flush`.
- **Flush worker**: Dedicated OS thread; receives `Arc<Buffer>` from channel; batches available buffers; calls `file_writer.write_vectored(slices)`; then `mark_flushed()` on each buffer so the shard can reuse it.

### 4.3 File Writer and Rotation

- **SizeFileWriter**:
  - Writes only 4096-aligned data; advances `file_offset` by the padded total to keep alignment.
  - Files created as `{base_name}_{timestamp}_{seq}.log.tmp` with `O_DIRECT | O_DSYNC` (Linux).
  - Preallocates `max_file_size` (aligned) via `fallocate`.
  - At **max_file_size** (or on close): fsync, ftruncate to actual size, close, rename `.tmp` → `.log`, create symlink in `upload_ready/` pointing to the final path.
  - **Empty file handling**: If `file_offset == 0` at rotation/close, the `.tmp` file is removed and no symlink is created (avoids uploading empty blobs).
- **Proactive next file**: When current file reaches 90% of `max_file_size`, the next `.tmp` file is pre-created and preallocated so rotation does not stall the flush worker.

### 4.4 Uploader (GCS)

- **Discovery**: Polls `upload_ready/` at `poll_interval`. Only follows **symlinks**; skips `.tmp` and non-symlinks.
- **Path derivation**: Filename pattern `{event_name}_{YYYY-MM-DD_HH-MM-SS}[_seq].log`. Object path: `{object_prefix}{event_name}/{date}/{hour}/{filename}`.
- **Upload**: Read entire file into memory; `store.put(object_path, data)`; on success, remove symlink and target file.
- **Retries**: Configurable `max_retries` with exponential backoff (1s, 2s, 4s, …). Empty files are skipped (no upload).
- **Credentials**: GCS client built via `object_store::gcp::GoogleCloudStorageBuilder::from_env()` (ADC).

### 4.5 Concurrency and Threading

- **Write path**: No global lock. Writers choose a shard at random; each shard uses lock-free CAS in `Buffer` and CAS-based swap in `Shard`. Optional `spawn_blocking` for proactive swap.
- **Flush path**: One dedicated thread per `Logger`; receives buffers from a bounded `crossbeam_channel`; batches and calls `write_vectored` under `file_writer.lock()`.
- **Timer**: Same thread uses `crossbeam_channel::select!` with a tick channel; on tick, calls `shard_collection.flush_all()`.
- **Shutdown**: `close()` sends a unit on `shutdown_tx`; flush thread drains the channel, flushes all shards, then joins. `LoggerManager::close()` closes all loggers and then stops the uploader task.

### 4.6 Configuration and Validation

- **Config**: `num_shards`, `buffer_size`, `max_file_size`, `log_file_path`, `flush_interval`, optional `gcs_config`.
- **Validation**: `num_shards >= 1`; `buffer_size` and `max_file_size > 0`; non-empty `log_file_path`; per-shard capacity at least 64 KiB (else reduce shards); buffer size rounded to 4096 alignment; `flush_interval` default 300s, minimum 1s; GCS defaults for `chunk_size`, `max_retries`, `poll_interval`.

### 4.7 Event Names and LoggerManager

- **Event names**: Sanitized (unsafe chars `\ / : * ? " < > | space` → `_`), truncated to 255 chars. Empty after sanitization is an error.
- **LoggerManager**: `get_or_create_logger(event_name)` via `DashMap`; each key is a `Logger` wrapped in `Arc<Mutex<Logger>>`. `log_with_event` / `log_bytes_with_event` acquire the lock and call `logger.log` / `log_bytes`.

---

## 5. Technology Stack

| Area | Choice | Notes |
|------|--------|--------|
| Language | Rust 2021, 1.75+ | No unsafe on hot path except in `unsafe_io` and `Buffer` (documented). |
| Async runtime | Tokio | Used for `Uploader` poll loop and `spawn_blocking` for proactive swap. |
| Channels | crossbeam-channel | Bounded channel for flush queue; tick for timer. |
| Concurrency | parking_lot::Mutex, DashMap | Logger and FileWriter state; per-event logger map. |
| Object storage | object_store 0.11 (gcp) | GCS put; ADC from env. |
| Low-level I/O | nix 0.29 (fs, mman) | mmap, pwritev, fallocate, O_DIRECT open (Linux). |
| Time / formatting | chrono 0.4 | Timestamps in filenames and GCS path. |
| Errors | thiserror | Unified `Error` enum. |
| Logging | tracing, tracing-subscriber | Internal diagnostics. |
| Testing | tokio-test, tempfile, criterion | Unit/integration and benchmarks. |

---

## 6. Data Layout and I/O Assumptions

- **Block header**: 8 bytes at start of every buffer flush: `block_size`, `valid_offset` (both u32 LE). Consumers can use this to parse valid data and ignore padding.
- **Record layout**: `[len: u32 LE][payload: len bytes]` repeated. No delimiter between records except length.
- **Files**: All written buffers are 4096-aligned in length; `file_offset` is always 4096-aligned. Linux uses O_DIRECT; non-Linux uses standard `write_at`-style writes.
- **Directories**: Under `log_file_path`: `logs/` (actual `.tmp` and `.log` files), `upload_ready/` (symlinks to `.log` files for upload).

---

## 7. Error Handling and Observability

- **Errors**: `Error` enum covers I/O, config, buffer full, logger closed, mmap, pwritev, GCS, invalid event name, uploader. Propagated via `Result`; no panics on expected failures.
- **Back-pressure**: When a write cannot reserve space (buffer full), the logger briefly sleeps, retries, then flushes all shards and retries once more; if still full, returns `Error::BufferFull` and increments `dropped_logs`.
- **Statistics**: `Statistics` (total_logs, dropped_logs, bytes_written, write_errors); `UploaderStats` (files_uploaded, bytes_uploaded, upload_errors, retry_count). All atomic; exposed via `Logger::get_stats()` and `Uploader::stats()`.
- **Tracing**: Errors and important events (e.g. file rotation, upload success/failure) logged via `tracing`.

---

## 8. Testing and Benchmarks

- **Unit / integration**: Tests for buffer, shard, shard collection, file writer, logger, logger manager, uploader, config; adversarial and integration tests present.
- **Benchmarks** (criterion): Hot-path `log_bytes` (64 B and 4096 B); parallel 8/64 threads; shard write with and without swap; event name sanitization.
- **Example**: `smoke_test` creates a temp dir, runs `LoggerManager` with two events, writes messages, closes, and prints stats and directory listing.

---

## 9. Security and Operational Notes

- **Paths**: Event names sanitized; file paths built from config and sanitized event names (no user-controlled raw paths in open/rename).
- **GCS**: No built-in URL or bucket from user input in a way that would allow SSRF; bucket and prefix are from config. Credentials from environment (ADC).
- **Concurrency**: No unsafe sharing of mutable state without synchronization; Buffer’s unsafe Send/Sync documented and justified by CAS and non-overlapping writes.
- **Platform**: Linux gets O_DIRECT and mmap; other platforms get heap buffers and non-O_DIRECT I/O. Same API everywhere.

---

## 10. Deployment and Usage

- **Embedding**: Use `LoggerManager::new(config).await` (or `with_uploader` for tests). Call `log_with_event` / `log_bytes_with_event`; call `close()` on shutdown.
- **Config example**: Set `log_file_path`, `num_shards`, `buffer_size`, `max_file_size`, `flush_interval`; optionally set `gcs_config` (bucket, object_prefix, poll_interval, max_retries).
- **GCS**: Ensure ADC is configured (e.g. service account key file or workload identity). Uploader runs in a Tokio task; ensure runtime is kept alive until `close()`.

---

## 11. Possible Extensions

- **Other object stores**: Same `Uploader` logic with a different `ObjectStore` implementation (e.g. S3, Azure) via the existing trait.
- **Structured logging**: Callers can serialize to JSON (or other format) and pass the bytes to `log_bytes`.
- **Metrics**: Export `Statistics` and `UploaderStats` to Prometheus/OpenTelemetry via a thin adapter.
- **Compression**: Compress payloads before `log_bytes` or add an optional compression step in the flush path or before upload.
- **Read path**: No current API to read back logs; could add a separate reader that parses block header + length-prefixed records from `.log` files.

---

## 12. Document History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-03-11 | Initial technical solution document. |
