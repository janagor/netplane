use crate::config::Scenario;
use crate::fixture::ProbeError;
use hdrhistogram::Histogram;
use serde::Serialize;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

/// Lowest representable latency sample (1 ns).
pub const HISTOGRAM_LOW_NS: u64 = 1;
/// Highest representable latency sample (60 s). Values above this clamp.
pub const HISTOGRAM_HIGH_NS: u64 = 60_000_000_000;
/// Significant figures retained across the histogram range (~0.1% relative).
pub const HISTOGRAM_SIG_FIGS: u8 = 3;
/// Independent open-loop histogram shards merged after the workload.
pub const OPEN_HISTOGRAM_SHARDS: usize = 16;

#[derive(Default)]
pub struct Counters {
    pub attempted: AtomicU64,
    pub admitted: AtomicU64,
    pub rejected_at_capacity: AtomicU64,
    pub admission_other_errors: AtomicU64,
    pub client_dropped: AtomicU64,
    pub runtime_errors: AtomicU64,
    pub completed: AtomicU64,
    pub completed_in_window: AtomicU64,
    pub completed_after_window: AtomicU64,
    pub drain_cancelled: AtomicU64,
    pub outstanding: AtomicU64,
    pub peak_outstanding: AtomicU64,
    pub nominal_arrivals: AtomicU64,
    pub scheduled_arrivals: AtomicU64,
    pub generated_attempts: AtomicU64,
}

impl Counters {
    pub fn reset(&self) {
        for counter in [
            &self.attempted,
            &self.admitted,
            &self.rejected_at_capacity,
            &self.admission_other_errors,
            &self.client_dropped,
            &self.runtime_errors,
            &self.completed,
            &self.completed_in_window,
            &self.completed_after_window,
            &self.drain_cancelled,
            &self.outstanding,
            &self.peak_outstanding,
            &self.nominal_arrivals,
            &self.scheduled_arrivals,
            &self.generated_attempts,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

#[derive(Clone, Copy, Serialize)]
pub struct Latency {
    pub p50: Option<u64>,
    pub p95: Option<u64>,
    pub p99: Option<u64>,
}

impl Latency {
    pub fn micros(self) -> Self {
        Self {
            p50: self.p50.map(|value| value / 1000),
            p95: self.p95.map(|value| value / 1000),
            p99: self.p99.map(|value| value / 1000),
        }
    }

    pub const fn empty() -> Self {
        Self {
            p50: None,
            p95: None,
            p99: None,
        }
    }
}

pub fn histogram() -> Result<Histogram<u64>, ProbeError> {
    Ok(Histogram::new_with_bounds(
        HISTOGRAM_LOW_NS,
        HISTOGRAM_HIGH_NS,
        HISTOGRAM_SIG_FIGS,
    )?)
}

pub fn record(hist: &mut Histogram<u64>, nanos: u64) {
    let value = nanos.clamp(HISTOGRAM_LOW_NS, HISTOGRAM_HIGH_NS);
    let _ = hist.record(value);
}

pub fn percentile(hist: &Histogram<u64>) -> Latency {
    if hist.is_empty() {
        return Latency::empty();
    }
    Latency {
        p50: Some(hist.value_at_percentile(50.0)),
        p95: Some(hist.value_at_percentile(95.0)),
        p99: Some(hist.value_at_percentile(99.0)),
    }
}

pub fn merge(into: &mut Histogram<u64>, from: &Histogram<u64>) -> Result<(), ProbeError> {
    into.add(from)?;
    Ok(())
}

/// Per-shard histograms so open-loop recording does not share one mutex.
pub struct LatencySink {
    shards: Vec<Mutex<Histogram<u64>>>,
}

impl LatencySink {
    pub fn new(shard_count: usize) -> Result<Self, ProbeError> {
        let count = shard_count.max(1);
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(Mutex::new(histogram()?));
        }
        Ok(Self { shards })
    }

    pub fn record(&self, shard: usize, nanos: u64) {
        let index = shard % self.shards.len();
        let mut hist = self.shards[index]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        record(&mut hist, nanos);
    }

    pub fn merged(&self) -> Result<Histogram<u64>, ProbeError> {
        let mut out = histogram()?;
        for shard in &self.shards {
            let hist = shard
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            merge(&mut out, &hist)?;
        }
        Ok(out)
    }
}

#[derive(Serialize)]
pub struct Metrics {
    pub attempted: u64,
    pub admitted: u64,
    pub completed: u64,
    pub completed_in_window: u64,
    pub completed_after_window: u64,
    pub drain_cancelled: u64,
    pub cancelled_requests: u64,
    pub rejected_at_capacity: u64,
    pub admission_other_errors: u64,
    pub client_dropped: u64,
    pub runtime_errors: u64,
    pub throughput_rps: Option<f64>,
    pub publication_rate: Option<f64>,
    pub latency_ns: Latency,
    pub latency_us: Latency,
    pub latency_samples: u64,
    pub missing_latency_samples: u64,
    pub measurement_complete: bool,
    pub completed_by_backend: Vec<u64>,
    pub peak_outstanding: u64,
    pub requested_publications: u64,
    pub successful_publications: u64,
    pub publication_errors: u64,
    pub publications_after_window: u64,
    pub publication_latency_us: Latency,
    pub candidate_build_latency_us: Latency,
    pub nominal_arrivals: Option<u64>,
    pub scheduled_arrivals: Option<u64>,
    pub generated_attempts: Option<u64>,
    pub offered_rps_achieved: Option<f64>,
    pub histogram_significant_figures: u8,
    pub histogram_max_ns: u64,
    pub peak_rss_bytes: Option<u64>,
    pub peak_live_resources: Option<u64>,
    pub final_live_resources: Option<u64>,
}

impl Metrics {
    pub fn validate(&self, scenario: Scenario) -> Result<(), ProbeError> {
        if scenario == Scenario::Retention {
            if self.completed != 0
                || self.completed_in_window != 0
                || self.throughput_rps.is_some()
                || self.cancelled_requests != self.admitted
                || self.publication_rate.is_none_or(|rate| !rate.is_finite())
                || self.successful_publications != self.admitted
                || self.latency_samples != 0
                || self.missing_latency_samples != 0
            {
                return Err("invalid retention result counters".into());
            }
            return Ok(());
        }
        if self.completed > self.admitted
            || self.completed_in_window > self.completed
            || self.completed_after_window > self.completed
            || self.completed_in_window + self.completed_after_window != self.completed
            || self.completed + self.runtime_errors + self.drain_cancelled != self.admitted
            || self.attempted
                != self.admitted + self.rejected_at_capacity + self.admission_other_errors
            || self.completed_by_backend.iter().sum::<u64>() != self.completed
            || self.throughput_rps.is_none_or(|rate| !rate.is_finite())
            || self.successful_publications > self.requested_publications
            || self.successful_publications
                + self.publication_errors
                + self.publications_after_window
                > self.requested_publications
            || self.latency_samples + self.missing_latency_samples != self.completed
            || self.measurement_complete
                != (self.drain_cancelled == 0
                    && self.missing_latency_samples == 0
                    && self.runtime_errors == 0
                    && self.admission_other_errors == 0)
        {
            return Err("invalid result counters".into());
        }
        Ok(())
    }
}
