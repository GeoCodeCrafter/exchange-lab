use std::sync::Arc;

use axum::{routing::get, Router};
use once_cell::sync::Lazy;
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, Opts, Registry, TextEncoder};
use thiserror::Error;

pub static DEFAULT_METRICS: Lazy<Arc<ExchangeMetrics>> =
    Lazy::new(|| Arc::new(ExchangeMetrics::new().expect("metrics registry")));

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("prometheus error: {0}")]
    Prometheus(#[from] prometheus::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

#[derive(Debug)]
pub struct ExchangeMetrics {
    registry: Registry,
    pub events_ingested_total: IntCounter,
    pub events_replayed_total: IntCounter,
    pub parse_latency_ns: Histogram,
    pub replay_latency_ns: Histogram,
    pub invariant_failures_total: IntCounter,
}

impl ExchangeMetrics {
    pub fn new() -> Result<Self, MetricsError> {
        let registry = Registry::new();
        let events_ingested_total =
            IntCounter::with_opts(Opts::new("events_ingested_total", "Total ingested events"))?;
        let events_replayed_total =
            IntCounter::with_opts(Opts::new("events_replayed_total", "Total replayed events"))?;
        let parse_latency_ns = Histogram::with_opts(
            HistogramOpts::new("parse_latency_ns", "Parse latency in nanoseconds")
                .buckets(latency_buckets()),
        )?;
        let replay_latency_ns = Histogram::with_opts(
            HistogramOpts::new("replay_latency_ns", "Replay latency in nanoseconds")
                .buckets(latency_buckets()),
        )?;
        let invariant_failures_total = IntCounter::with_opts(Opts::new(
            "invariant_failures_total",
            "Invariant check failures",
        ))?;

        registry.register(Box::new(events_ingested_total.clone()))?;
        registry.register(Box::new(events_replayed_total.clone()))?;
        registry.register(Box::new(parse_latency_ns.clone()))?;
        registry.register(Box::new(replay_latency_ns.clone()))?;
        registry.register(Box::new(invariant_failures_total.clone()))?;

        Ok(Self {
            registry,
            events_ingested_total,
            events_replayed_total,
            parse_latency_ns,
            replay_latency_ns,
            invariant_failures_total,
        })
    }

    pub fn record_ingest(&self, count: usize, latency_ns: u64) {
        self.events_ingested_total.inc_by(count as u64);
        self.parse_latency_ns.observe(latency_ns as f64);
    }

    pub fn record_replay(&self, count: usize, latency_ns: u64, invariant_failures: usize) {
        self.events_replayed_total.inc_by(count as u64);
        self.replay_latency_ns.observe(latency_ns as f64);
        self.invariant_failures_total
            .inc_by(invariant_failures as u64);
    }

    pub fn render(&self) -> Result<String, MetricsError> {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }

    pub fn router(metrics: Arc<Self>) -> Router {
        Router::new().route(
            "/metrics",
            get(move || {
                let metrics = metrics.clone();
                async move {
                    metrics.render().unwrap_or_else(|error| {
                        format!("metrics_error{{message=\"{}\"}} 1\n", error)
                    })
                }
            }),
        )
    }
}

fn latency_buckets() -> Vec<f64> {
    vec![
        100.0,
        500.0,
        1_000.0,
        10_000.0,
        100_000.0,
        1_000_000.0,
        10_000_000.0,
        100_000_000.0,
    ]
}

#[cfg(test)]
mod tests {
    use super::ExchangeMetrics;

    #[test]
    fn renders_prometheus_metrics() {
        let metrics = ExchangeMetrics::new().expect("metrics");
        metrics.record_ingest(2, 10);
        let body = metrics.render().expect("render");
        assert!(body.contains("events_ingested_total"));
        assert!(body.contains("parse_latency_ns"));
    }
}
