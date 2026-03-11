# GCS Load Test

The `load_test_gcs` example runs a configurable load test against the blob logger with optional GCS upload. It uses **two events** (`event_a`, `event_b`), a **fixed set of ~50 KB log payloads** (no random generation), and targets a configurable **RPS** and **duration**.

## Default configuration (built-in)

| Parameter    | Default value              | Description                          |
|-------------|----------------------------|--------------------------------------|
| Log path    | `/mnt/localdisk/rustlogger/`| Base directory for `logs/` and `upload_ready/` |
| GCS bucket  | `gcs-dsci-srch-search-prd`  | GCS bucket name                      |
| GCS prefix  | `Image_search/gcs-flush/`  | Object key prefix (include trailing `/`) |
| Run duration| `60` seconds               | How long to send logs                |
| Target RPS  | `1000`                     | Target logs per second (split 50/50 between the two events) |
| Buffer      | 64 MiB total               | Total in-memory buffer               |
| Shards      | 8                          | Number of write shards               |
| Flush interval | 10 s                    | Timer-based flush interval           |

Payload: **~50 KB** per log line; **10 fixed variants** (JSON-like, varied `request_id`) cycled through.

## Environment variables

Override defaults by setting:

| Variable        | Example                    | Description |
|----------------|----------------------------|-------------|
| `LOG_FILE_PATH`| `/mnt/localdisk/rustlogger/`| Base path for log files (default: `/mnt/localdisk/rustlogger/`) |
| `GCS_BUCKET`   | `gcs-dsci-srch-search-prd` | GCS bucket name |
| `GCS_PREFIX`   | `Image_search/gcs-flush/` | Object key prefix; use trailing `/` |
| `RUN_SECONDS`  | `120`                      | Load test duration in seconds (default: 60) |
| `TARGET_RPS`   | `1000`                     | Target logs per second (default: 1000) |
| `RUST_LOG`     | `info,blob_logger_rust=info`| Log level (optional) |

## Prerequisites (Linux VM)

1. **Service account for GCS**  
   Application Default Credentials (ADC) must be configured so the VM can upload to the bucket, e.g.:
   - Service account key file: `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`
   - Or workload identity / metadata server for GCE/GKE

2. **Directory**  
   The log path must exist and be writable (the example does not create the base path; `LoggerManager` creates `logs/` and `upload_ready/` under it):
   ```bash
   sudo mkdir -p /mnt/localdisk/rustlogger
   sudo chown "$USER" /mnt/localdisk/rustlogger
   ```

3. **Build** (from repo root):
   ```bash
   cargo build --release --example load_test_gcs
   ```

## Run command

**Using defaults** (log path `/mnt/localdisk/rustlogger/`, bucket `gcs-dsci-srch-search-prd`, prefix `Image_search/gcs-flush/`, 1000 RPS, 60 s):

```bash
cargo run --release --example load_test_gcs
```

Or run the built binary:

```bash
./target/release/examples/load_test_gcs
```

**Override configuration** (example: 2 minutes, 500 RPS, custom path):

```bash
LOG_FILE_PATH=/mnt/localdisk/rustlogger/ \
GCS_BUCKET=gcs-dsci-srch-search-prd \
GCS_PREFIX=Image_search/gcs-flush/ \
RUN_SECONDS=120 \
TARGET_RPS=500 \
cargo run --release --example load_test_gcs
```

**Your exact parameters** (path, bucket, prefix; 1000 RPS, two events, 64 MB buffer, 8 shards, 10 s flush are already defaults in the example):

```bash
LOG_FILE_PATH=/mnt/localdisk/rustlogger/ \
GCS_BUCKET=gcs-dsci-srch-search-prd \
GCS_PREFIX=Image_search/gcs-flush/ \
RUN_SECONDS=60 \
TARGET_RPS=1000 \
cargo run --release --example load_test_gcs
```

No need to set the last four if you’re happy with 60 s and 1000 RPS; the example already uses your path, bucket, and prefix as defaults.

## Output

- At startup: config (path, bucket, prefix, duration, RPS, payload size, payload variant lengths).
- While running: logs at `RUST_LOG` level (e.g. rotations, uploads).
- At the end: per-event stats (total_logs, dropped, bytes_written, write_errors) and uploader stats (files_uploaded, bytes_uploaded, upload_errors, retry_count), plus elapsed time, total sent, and actual RPS.
