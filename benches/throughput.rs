use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};

use blob_logger_rust::config::Config;
use blob_logger_rust::logger::Logger;
use blob_logger_rust::logger_manager::LoggerManager;
use blob_logger_rust::shard::Shard;

fn make_config(base_dir: &std::path::Path) -> Config {
    let mut config = Config {
        num_shards: 8,
        buffer_size: 8 * 1024 * 1024,
        max_file_size: 64 * 1024 * 1024,
        log_file_path: base_dir.to_path_buf(),
        flush_interval: Duration::from_secs(300),
        gcs_config: None,
        metrics_config: None,
    };
    config.validate().unwrap();
    config
}

fn bench_log_bytes_hot_path(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    let tmp = tempfile::TempDir::new().unwrap();
    let logs_dir = tmp.path().join("logs");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&logs_dir).unwrap();
    std::fs::create_dir_all(&upload_ready).unwrap();

    let config = make_config(tmp.path());
    let logger = Logger::new("bench_event", &logs_dir, &upload_ready, config).unwrap();

    let payload = vec![0xAAu8; 64];

    c.bench_function("log_bytes_hot_path_64B", |b| {
        b.iter(|| {
            let _ = logger.log_bytes(&payload);
        })
    });
}

fn bench_log_bytes_parallel_8(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    let tmp = tempfile::TempDir::new().unwrap();
    let logs_dir = tmp.path().join("logs");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&logs_dir).unwrap();
    std::fs::create_dir_all(&upload_ready).unwrap();

    let config = make_config(tmp.path());
    let logger = Arc::new(parking_lot::Mutex::new(
        Logger::new("bench_par8", &logs_dir, &upload_ready, config).unwrap(),
    ));

    let payload = vec![0xAAu8; 64];

    let handle = rt.handle().clone();
    c.bench_function("log_bytes_parallel_8", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let logger = logger.clone();
                    let payload = payload.clone();
                    let handle = handle.clone();
                    std::thread::spawn(move || {
                        let _guard = handle.enter();
                        for _ in 0..10 {
                            let _ = logger.lock().log_bytes(&payload);
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        })
    });
}

fn bench_log_bytes_parallel_64(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    let tmp = tempfile::TempDir::new().unwrap();
    let logs_dir = tmp.path().join("logs");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&logs_dir).unwrap();
    std::fs::create_dir_all(&upload_ready).unwrap();

    let config = make_config(tmp.path());
    let logger = Arc::new(parking_lot::Mutex::new(
        Logger::new("bench_par64", &logs_dir, &upload_ready, config).unwrap(),
    ));

    let payload = vec![0xAAu8; 64];

    let handle = rt.handle().clone();
    c.bench_function("log_bytes_parallel_64", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..64)
                .map(|_| {
                    let logger = logger.clone();
                    let payload = payload.clone();
                    let handle = handle.clone();
                    std::thread::spawn(move || {
                        let _guard = handle.enter();
                        for _ in 0..10 {
                            let _ = logger.lock().log_bytes(&payload);
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        })
    });
}

fn bench_shard_write_no_swap(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    let (tx, _rx) = crossbeam_channel::bounded(1024);
    let shard = Shard::new(32 * 1024 * 1024, 0, tx).unwrap();

    let payload = vec![0xBBu8; 64];

    c.bench_function("shard_write_no_swap_64B", |b| {
        b.iter(|| {
            let _ = shard.write(&payload);
        })
    });
}

fn bench_shard_write_with_swap(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    let (tx, rx) = crossbeam_channel::bounded(4096);
    let shard = Shard::new(8192, 0, tx).unwrap();

    let payload = vec![0xBBu8; 64];

    c.bench_function("shard_write_with_swap_64B", |b| {
        b.iter(|| {
            let _ = shard.write(&payload);
            while let Ok(buf) = rx.try_recv() {
                buf.mark_flushed();
            }
        })
    });
}

fn bench_sanitize_event_name(c: &mut Criterion) {
    let name = "a]b_c_d_e_f_g_h_i_j_k_l_m_n_o_p_q_r_s_t_u_v_w_x_y_z_0123456789ab";

    c.bench_function("sanitize_event_name_64char", |b| {
        b.iter(|| {
            let _ = LoggerManager::sanitize_event_name(name);
        })
    });
}

fn bench_log_bytes_large_payload(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    let tmp = tempfile::TempDir::new().unwrap();
    let logs_dir = tmp.path().join("logs");
    let upload_ready = tmp.path().join("upload_ready");
    std::fs::create_dir_all(&logs_dir).unwrap();
    std::fs::create_dir_all(&upload_ready).unwrap();

    let config = make_config(tmp.path());
    let logger = Logger::new("bench_large", &logs_dir, &upload_ready, config).unwrap();

    let payload = vec![0xCCu8; 4096];

    c.bench_function("log_bytes_hot_path_4096B", |b| {
        b.iter(|| {
            let _ = logger.log_bytes(&payload);
        })
    });
}

criterion_group!(
    benches,
    bench_log_bytes_hot_path,
    bench_log_bytes_parallel_8,
    bench_log_bytes_parallel_64,
    bench_shard_write_no_swap,
    bench_shard_write_with_swap,
    bench_sanitize_event_name,
    bench_log_bytes_large_payload,
);
criterion_main!(benches);
