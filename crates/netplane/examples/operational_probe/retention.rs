use crate::{
    Report,
    config::{Config, RetentionMode},
    environment,
    fixture::{ProbeError, Request, Response},
    metrics::{HISTOGRAM_HIGH_NS, HISTOGRAM_SIG_FIGS, Latency, Metrics},
    rss,
};
use netplane::{
    Capacity, ControllerOptions, GraphDraft, Netplane, ReconfigurationController, RuntimeLimits,
};
use std::{
    convert::Infallible,
    future::{Ready, ready},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tower::Service;

struct CountedService {
    live: Arc<AtomicU64>,
    generation: u64,
}

impl CountedService {
    fn new(live: &Arc<AtomicU64>, generation: u64) -> Self {
        live.fetch_add(1, Ordering::Relaxed);
        Self {
            live: Arc::clone(live),
            generation,
        }
    }
}

impl Drop for CountedService {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Service<Request> for CountedService {
    type Response = Response;
    type Error = Infallible;
    type Future = Ready<Result<Response, Infallible>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: Request) -> Self::Future {
        ready(Ok((0, self.generation)))
    }
}

async fn wait_for(live: &AtomicU64, expected: u64) -> Result<(), ProbeError> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while live.load(Ordering::Relaxed) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

pub async fn run(config: Config) -> Result<Report, ProbeError> {
    let held_limit = match config.retention_mode {
        RetentionMode::Paced => 1,
        RetentionMode::BurstShort => 10.min(config.publications),
        RetentionMode::BurstLong => config.publications,
    };
    if config.publications == 0 {
        return Err("retention requires a positive publication count".into());
    }
    if config.max_outstanding < held_limit {
        return Err("max_outstanding must cover held requests for this retention mode".into());
    }
    let live = Arc::new(AtomicU64::new(0));
    let capacity = Capacity {
        max_in_flight: config.outer_in_flight,
        buffer_capacity: config.outer_buffer,
    };
    let first = GraphDraft::new(CountedService::new(&live, 0))
        .capacity(capacity)
        .validate()?;
    let runtime = Netplane::new(
        first,
        RuntimeLimits {
            max_outstanding: config.max_outstanding,
        },
    )?;
    let controller = ReconfigurationController::new(
        runtime,
        ControllerOptions {
            max_retained_versions: 0,
        },
    );
    let handle = controller.handle();
    let mut held = Vec::new();
    let mut peak_live = live.load(Ordering::Relaxed);
    let mut peak_outstanding = 0_u64;
    let mut publication_us = Vec::new();
    let mut build_us = Vec::new();
    let mut cancelled_requests = 0_u64;
    let started = Instant::now();
    for generation in 1..=config.publications {
        let old = handle.try_execute(Request { backend: 0 })?;
        let token = controller
            .current_token()
            .ok_or("retention controller closed")?;
        let build_start = Instant::now();
        let candidate = GraphDraft::new(CountedService::new(&live, u64::try_from(generation)?))
            .capacity(capacity)
            .validate()?;
        build_us.push(u64::try_from(build_start.elapsed().as_micros())?);
        let publish_start = Instant::now();
        controller.publish_if_current(token, candidate)?;
        publication_us.push(u64::try_from(publish_start.elapsed().as_micros())?);
        peak_live = peak_live.max(live.load(Ordering::Relaxed));
        held.push(old);
        peak_outstanding = peak_outstanding.max(u64::try_from(held.len())?);
        match config.retention_mode {
            RetentionMode::Paced => {
                cancelled_requests += u64::try_from(held.len())?;
                held.clear();
                wait_for(&live, 1).await?;
            }
            RetentionMode::BurstShort if held.len() >= 10 => {
                cancelled_requests += u64::try_from(held.len())?;
                held.clear();
                wait_for(&live, 1).await?;
            }
            RetentionMode::BurstShort | RetentionMode::BurstLong => {}
        }
    }
    cancelled_requests += u64::try_from(held.len())?;
    peak_outstanding = peak_outstanding.max(u64::try_from(held.len())?);
    held.clear();
    wait_for(&live, 1).await?;
    controller.shutdown(Duration::from_secs(5)).await?;
    wait_for(&live, 0).await?;
    let metrics = retention_metrics(
        &config,
        cancelled_requests,
        peak_outstanding,
        started.elapsed().as_secs_f64(),
        peak_live,
        live.load(Ordering::Relaxed),
        &mut publication_us,
        &mut build_us,
    )?;
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

fn retention_metrics(
    config: &Config,
    cancelled_requests: u64,
    peak_outstanding: u64,
    elapsed: f64,
    peak_live: u64,
    final_live: u64,
    publication_us: &mut [u64],
    build_us: &mut [u64],
) -> Result<Metrics, ProbeError> {
    let publications = u64::try_from(config.publications)?;
    Ok(Metrics {
        attempted: cancelled_requests,
        admitted: cancelled_requests,
        completed: 0,
        completed_in_window: 0,
        completed_after_window: 0,
        drain_cancelled: 0,
        cancelled_requests,
        rejected_at_capacity: 0,
        admission_other_errors: 0,
        client_dropped: 0,
        runtime_errors: 0,
        throughput_rps: None,
        publication_rate: Some(f64::from(u32::try_from(publications)?) / elapsed),
        latency_ns: Latency::empty(),
        latency_us: Latency::empty(),
        latency_samples: 0,
        missing_latency_samples: 0,
        measurement_complete: true,
        completed_by_backend: vec![0],
        peak_outstanding,
        requested_publications: publications,
        successful_publications: publications,
        publication_errors: 0,
        publications_after_window: 0,
        publication_latency_us: crate::publish_percentiles(publication_us),
        candidate_build_latency_us: crate::publish_percentiles(build_us),
        nominal_arrivals: None,
        scheduled_arrivals: None,
        generated_attempts: None,
        offered_rps_achieved: None,
        histogram_significant_figures: HISTOGRAM_SIG_FIGS,
        histogram_max_ns: HISTOGRAM_HIGH_NS,
        peak_rss_bytes: rss(),
        peak_live_resources: Some(peak_live),
        final_live_resources: Some(final_live),
    })
}
