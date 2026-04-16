//! Cadence/StatsD/DogStatsD metrics module.
//!
//! Adapted from inferflow-rust's metrics module. Uses `try_state()` instead of
//! `state()` so that metrics emission is a no-op when `init_metrics()` has not
//! been called — safe for library use in tests and local dev.

use std::sync::OnceLock;
use cadence::{BufferedUdpMetricSink, QueuingMetricSink, MetricSink};
use std::net::UdpSocket;
use std::time::Duration;

struct MetricsState {
    sink: QueuingMetricSink,
    global_tags: Vec<(String, String)>,
    sampling_rate: f64,
}

static STATE: OnceLock<MetricsState> = OnceLock::new();

pub fn init_metrics(
    telegraf_host: &str,
    telegraf_port: &str,
    global_tags: &[(&str, &str)],
    sampling_rate: f64,
) {
    let addr = format!("{}:{}", telegraf_host, telegraf_port);
    let socket = UdpSocket::bind("0.0.0.0:0").expect("bind udp");
    socket.set_nonblocking(true).ok();

    let buffered = BufferedUdpMetricSink::from(addr.as_str(), socket).expect("buffered udp sink");
    let queued = QueuingMetricSink::from(buffered);

    let tags: Vec<(String, String)> = global_tags
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    let sampling_rate = sampling_rate.clamp(0.0, 1.0);

    STATE
        .set(MetricsState { sink: queued, global_tags: tags, sampling_rate })
        .ok();
}

#[inline]
fn should_sample(sampling_rate: f64) -> bool {
    if sampling_rate <= 0.0 {
        return false;
    }
    if sampling_rate >= 1.0 {
        return true;
    }
    fastrand::f64() < sampling_rate
}

/// Returns the global metrics state, or `None` if `init_metrics()` was never called.
/// This makes all metric functions no-ops when metrics are not initialized — safe for
/// library use where the caller may not set up Telegraf.
#[inline]
fn try_state() -> Option<&'static MetricsState> {
    STATE.get()
}

fn sample_suffix(sampling_rate: f64) -> String {
    if sampling_rate > 0.0 && sampling_rate < 1.0 {
        format!("|@{}", sampling_rate)
    } else {
        String::new()
    }
}

/// Build DogStatsD tag string. All tags use standard `key:value` format.
fn build_tags(global: &[(String, String)], local: &[(&str, &str)]) -> String {
    let cap = global.len() + local.len();
    let mut parts = Vec::with_capacity(cap);
    for (k, v) in global {
        parts.push(format!("{}:{}", k, v));
    }
    for (k, v) in local {
        parts.push(format!("{}:{}", k, v));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("|#{}", parts.join(","))
    }
}

pub fn timing(name: &str, value: Duration, tags: &[(&str, &str)]) {
    let Some(s) = try_state() else { return };
    if !should_sample(s.sampling_rate) {
        return;
    }
    let sample_str = sample_suffix(s.sampling_rate);
    let tag_str = build_tags(&s.global_tags, tags);
    let line = format!("{}:{}|ms{}{}", name, value.as_millis(), sample_str, tag_str);
    let _ = s.sink.emit(&line);
}

pub fn count(name: &str, value: i64, tags: &[(&str, &str)]) {
    let Some(s) = try_state() else { return };
    if !should_sample(s.sampling_rate) {
        return;
    }
    let sample_str = sample_suffix(s.sampling_rate);
    let tag_str = build_tags(&s.global_tags, tags);
    let line = format!("{}:{}|c{}{}", name, value, sample_str, tag_str);
    let _ = s.sink.emit(&line);
}

pub fn gauge(name: &str, value: i64, tags: &[(&str, &str)]) {
    let Some(s) = try_state() else { return };
    if !should_sample(s.sampling_rate) {
        return;
    }
    let sample_str = sample_suffix(s.sampling_rate);
    let tag_str = build_tags(&s.global_tags, tags);
    let line = format!("{}:{}|g{}{}", name, value, sample_str, tag_str);
    let _ = s.sink.emit(&line);
}

// ─── Pre-sampled variants for correlated metric groups ────────────────────

/// Expose the sampling decision for callers that need to sample once for a
/// group of correlated metrics.
pub fn should_sample_check() -> bool {
    let Some(s) = try_state() else { return false };
    should_sample(s.sampling_rate)
}

pub fn count_sampled(name: &str, value: i64, tags: &[(&str, &str)]) {
    let Some(s) = try_state() else { return };
    let sample_str = sample_suffix(s.sampling_rate);
    let tag_str = build_tags(&s.global_tags, tags);
    let line = format!("{}:{}|c{}{}", name, value, sample_str, tag_str);
    let _ = s.sink.emit(&line);
}

pub fn timing_sampled(name: &str, value: Duration, tags: &[(&str, &str)]) {
    let Some(s) = try_state() else { return };
    let sample_str = sample_suffix(s.sampling_rate);
    let tag_str = build_tags(&s.global_tags, tags);
    let line = format!("{}:{}|ms{}{}", name, value.as_millis(), sample_str, tag_str);
    let _ = s.sink.emit(&line);
}
