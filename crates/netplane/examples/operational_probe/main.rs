//! Reproducible in-memory operational workloads for Netplane and matched Tower.

mod config;
mod environment;
mod fixture;
mod metrics;
mod retention;
mod workload;

use clap::Parser;
use config::{Cli, Config, Implementation, Load, Scenario};
use environment::{Environment, environment, rss};
use fixture::ProbeError;
use metrics::{
    Counters, HISTOGRAM_HIGH_NS, HISTOGRAM_SIG_FIGS, Latency, LatencySink, Metrics,
    OPEN_HISTOGRAM_SHARDS, percentile,
};
use serde::Serialize;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use workload::{call, drive, publisher};

#[derive(Serialize)]
struct Report {
    schema_version: u32,
    implementation: Implementation,
    scenario: Scenario,
    config: Config,
    environment: Environment,
    metrics: Metrics,
}

impl Report {
    fn validate(&self) -> Result<(), ProbeError> {
        self.metrics.validate(self.scenario)
    }
}

async fn verify_routing(config: &Config, call: &workload::Call) -> Result<(), ProbeError> {
    for index in 0..16 {
        let request = fixture::request(index, config);
        match call(request) {
            workload::Admit::Ready(future) => {
                let response = future.await?;
                if response.0 != request.backend {
                    return Err("routing mismatch during probe setup".into());
                }
            }
            workload::Admit::AtCapacity | workload::Admit::Failed(_) => {
                return Err("setup request was not admitted".into());
            }
        }
    }
    Ok(())
}

fn collect_metrics(
    config: &Config,
    counters: &Counters,
    by_backend: &[AtomicU64],
    sink: &LatencySink,
    measured: f64,
    publication: (u64, u64, u64, u64, Latency, Latency),
) -> Result<Metrics, ProbeError> {
    let (
        requested_publications,
        successful_publications,
        publication_errors,
        publications_after_window,
        publish_us,
        build_us,
    ) = publication;
    let admitted = counters.admitted.load(Ordering::Relaxed);
    let completed = counters.completed.load(Ordering::Relaxed);
    let errors = counters.runtime_errors.load(Ordering::Relaxed);
    let cancelled = counters.drain_cancelled.load(Ordering::Relaxed);
    if completed + errors + cancelled != admitted {
        return Err("result has undrained accepted requests".into());
    }
    if counters.outstanding.load(Ordering::Relaxed) != 0 && cancelled == 0 {
        return Err("outstanding requests remain".into());
    }
    let hist = sink.merged()?;
    let latency_samples = hist.len();
    let missing_latency_samples = completed.saturating_sub(latency_samples);
    let latency = percentile(&hist);
    let completed_in_window = counters.completed_in_window.load(Ordering::Relaxed);
    let admission_other = counters.admission_other_errors.load(Ordering::Relaxed);
    let open = config.load == Load::Open;
    Ok(Metrics {
        attempted: counters.attempted.load(Ordering::Relaxed),
        admitted,
        completed,
        completed_in_window,
        completed_after_window: counters.completed_after_window.load(Ordering::Relaxed),
        drain_cancelled: cancelled,
        cancelled_requests: 0,
        rejected_at_capacity: counters.rejected_at_capacity.load(Ordering::Relaxed),
        admission_other_errors: admission_other,
        client_dropped: counters.client_dropped.load(Ordering::Relaxed),
        runtime_errors: errors,
        throughput_rps: Some(f64::from(u32::try_from(completed_in_window)?) / measured),
        publication_rate: if config.scenario == Scenario::Publication {
            Some(f64::from(u32::try_from(successful_publications)?) / measured)
        } else {
            None
        },
        latency_ns: latency,
        latency_us: latency.micros(),
        latency_samples,
        missing_latency_samples,
        measurement_complete: cancelled == 0
            && missing_latency_samples == 0
            && errors == 0
            && admission_other == 0,
        completed_by_backend: by_backend
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect(),
        peak_outstanding: counters.peak_outstanding.load(Ordering::Relaxed),
        requested_publications,
        successful_publications,
        publication_errors,
        publications_after_window,
        publication_latency_us: publish_us,
        candidate_build_latency_us: build_us,
        nominal_arrivals: open.then_some(counters.nominal_arrivals.load(Ordering::Relaxed)),
        scheduled_arrivals: open.then_some(counters.scheduled_arrivals.load(Ordering::Relaxed)),
        generated_attempts: open.then_some(counters.generated_attempts.load(Ordering::Relaxed)),
        offered_rps_achieved: if open {
            Some(
                f64::from(u32::try_from(
                    counters.generated_attempts.load(Ordering::Relaxed),
                )?) / measured,
            )
        } else {
            None
        },
        histogram_significant_figures: HISTOGRAM_SIG_FIGS,
        histogram_max_ns: HISTOGRAM_HIGH_NS,
        peak_rss_bytes: rss(),
        peak_live_resources: None,
        final_live_resources: None,
    })
}

async fn run(config: Config) -> Result<Report, ProbeError> {
    if config.scenario == Scenario::Retention {
        return retention::run(config).await;
    }
    let (call, controller) = call(&config)?;
    verify_routing(&config, &call).await?;
    let counters = Arc::new(Counters::default());
    let by_backend = Arc::new(
        (0..config.scenario.backends())
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );
    if config.warmup_secs > 0 {
        let warmup_end = Instant::now() + Duration::from_secs(config.warmup_secs);
        drive(&config, &call, &counters, None, &by_backend, warmup_end).await?;
        counters.reset();
        for count in by_backend.iter() {
            count.store(0, Ordering::Relaxed);
        }
    }
    let shard_count = match config.load {
        Load::Closed => config.clients.max(1),
        Load::Open => OPEN_HISTOGRAM_SHARDS,
    };
    let sink = Arc::new(LatencySink::new(shard_count)?);
    let started = Instant::now();
    let end = started + Duration::from_secs(config.duration_secs);
    let publisher = publisher(&config, controller.as_ref(), started, end)?;
    drive(&config, &call, &counters, Some(&sink), &by_backend, end).await?;
    let publication = if let Some(publisher) = publisher {
        let (requested, successful, errors, after_window, mut publish, mut build) =
            publisher.await??;
        (
            requested,
            successful,
            errors,
            after_window,
            publish_percentiles(&mut publish),
            publish_percentiles(&mut build),
        )
    } else {
        (0, 0, 0, 0, Latency::empty(), Latency::empty())
    };
    let measured = started
        .elapsed()
        .min(Duration::from_secs(config.duration_secs))
        .as_secs_f64();
    let metrics = collect_metrics(
        &config,
        &counters,
        &by_backend,
        &sink,
        measured,
        publication,
    )?;
    if let Some(controller) = controller {
        match controller.shutdown(Duration::from_secs(5)).await {
            Ok(()) => {}
            Err(netplane::DrainError::TimedOut { .. }) if metrics.drain_cancelled > 0 => {}
            Err(error) => return Err(Box::new(error)),
        }
    }
    let report = Report {
        schema_version: 2,
        implementation: config.implementation,
        scenario: config.scenario,
        config,
        environment: environment(),
        metrics,
    };
    report.validate()?;
    Ok(report)
}

pub(crate) fn publish_percentiles(samples: &mut [u64]) -> Latency {
    if samples.is_empty() {
        return Latency::empty();
    }
    samples.sort_unstable();
    let rank = |percent: usize| {
        samples[samples
            .len()
            .saturating_mul(percent)
            .div_ceil(100)
            .saturating_sub(1)]
    };
    Latency {
        p50: Some(rank(50)),
        p95: Some(rank(95)),
        p99: Some(rank(99)),
    }
}

fn main() -> Result<(), ProbeError> {
    let cli = Cli::parse();
    let config = cli.validate()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let report = runtime.block_on(run(config))?;
    let json = serde_json::to_string_pretty(&report)?;
    if let Some(path) = cli.output {
        std::fs::write(path, format!("{json}\n"))?;
    } else {
        println!("{json}");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
