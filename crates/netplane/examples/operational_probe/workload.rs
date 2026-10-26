use crate::config::{Config, Load, Scenario};
use crate::fixture::{self, ProbeError, Request, Response};
use crate::metrics::{Counters, LatencySink, OPEN_HISTOGRAM_SHARDS};
use netplane::{AdmissionError, Handle};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::Barrier, task::JoinSet};

pub const DRAIN_SECS: u64 = 5;

pub enum Admit {
    Ready(Pin<Box<dyn Future<Output = Result<Response, ProbeError>> + Send>>),
    AtCapacity,
    Failed(ProbeError),
}

impl std::fmt::Debug for Admit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ready(_) => formatter.write_str("Ready(..)"),
            Self::AtCapacity => formatter.write_str("AtCapacity"),
            Self::Failed(error) => formatter.debug_tuple("Failed").field(error).finish(),
        }
    }
}

pub type Call = Arc<dyn Fn(Request) -> Admit + Send + Sync>;

pub fn call(
    config: &Config,
) -> Result<
    (
        Call,
        Option<Arc<netplane::ReconfigurationController<Request, Response>>>,
    ),
    ProbeError,
> {
    match config.implementation {
        crate::config::Implementation::Netplane => {
            let controller = Arc::new(fixture::netplane(config)?);
            let handle: Handle<Request, Response> = controller.handle();
            let execute: Call = Arc::new(move |request| match handle.try_execute(request) {
                Ok(future) => Admit::Ready(Box::pin(future)),
                Err(AdmissionError::AtCapacity { .. }) => Admit::AtCapacity,
                Err(error) => Admit::Failed(Box::new(error)),
            });
            Ok((execute, Some(controller)))
        }
        crate::config::Implementation::Tower => {
            let tower = fixture::tower(config);
            let permits = Arc::new(tokio::sync::Semaphore::new(config.max_outstanding));
            let execute: Call = Arc::new(move |request| {
                let permit = match Arc::clone(&permits).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(tokio::sync::TryAcquireError::NoPermits) => return Admit::AtCapacity,
                    Err(error) => return Admit::Failed(Box::new(error)),
                };
                let future = tower(request);
                Admit::Ready(Box::pin(async move {
                    let response = future.await?;
                    drop(permit);
                    Ok(response)
                }))
            });
            Ok((execute, None))
        }
    }
}

fn observe(
    request: Request,
    elapsed_ns: u64,
    completed_at: Instant,
    window_end: Instant,
    result: &Result<Response, ProbeError>,
    counters: &Counters,
    sink: Option<(&LatencySink, usize)>,
    by_backend: &[AtomicU64],
) {
    match result {
        Ok((backend, _)) if *backend == request.backend => {
            counters.completed.fetch_add(1, Ordering::Relaxed);
            by_backend[*backend].fetch_add(1, Ordering::Relaxed);
            if completed_at <= window_end {
                counters.completed_in_window.fetch_add(1, Ordering::Relaxed);
            } else {
                counters
                    .completed_after_window
                    .fetch_add(1, Ordering::Relaxed);
            }
            if let Some((sink, shard)) = sink {
                sink.record(shard, elapsed_ns);
            }
        }
        _ => {
            counters.runtime_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
    counters.outstanding.fetch_sub(1, Ordering::Relaxed);
}

async fn admit_and_run(
    call: &Call,
    request: Request,
    counters: &Counters,
    sink: Option<(&LatencySink, usize)>,
    by_backend: &[AtomicU64],
    window_end: Instant,
) {
    // Admission is only counted when it still falls inside the measurement window.
    if Instant::now() >= window_end {
        return;
    }
    counters.attempted.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let future = match call(request) {
        Admit::Ready(future) => future,
        Admit::AtCapacity => {
            counters
                .rejected_at_capacity
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        Admit::Failed(_) => {
            counters
                .admission_other_errors
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    counters.admitted.fetch_add(1, Ordering::Relaxed);
    let active = counters.outstanding.fetch_add(1, Ordering::Relaxed) + 1;
    counters
        .peak_outstanding
        .fetch_max(active, Ordering::Relaxed);
    let result = future.await;
    let completed_at = Instant::now();
    let elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    observe(
        request,
        elapsed_ns,
        completed_at,
        window_end,
        &result,
        counters,
        sink,
        by_backend,
    );
}

pub fn publisher(
    config: &Config,
    controller: Option<&Arc<netplane::ReconfigurationController<Request, Response>>>,
    started: Instant,
    end: Instant,
) -> Result<
    Option<tokio::task::JoinHandle<Result<(u64, u64, u64, u64, Vec<u64>, Vec<u64>), ProbeError>>>,
    ProbeError,
> {
    if config.scenario != Scenario::Publication {
        return Ok(None);
    }
    let requested = u64::try_from(config.publications)?;
    if requested == 0 {
        return Ok(Some(tokio::spawn(async move {
            Ok::<_, ProbeError>((requested, 0, 0, 0, Vec::new(), Vec::new()))
        })));
    }
    let controller = Arc::clone(controller.ok_or("publication requires a controller")?);
    let config = config.clone();
    Ok(Some(tokio::spawn(async move {
        let mut successful = 0_u64;
        let mut errors = 0_u64;
        let mut after_window = 0_u64;
        let mut publish_samples = Vec::new();
        let mut build_samples = Vec::new();
        let spacing_ns = Duration::from_secs(config.duration_secs).as_nanos()
            / u128::try_from(config.publications)?;
        for generation in 0..config.publications {
            let due = started
                + Duration::from_nanos(u64::try_from(spacing_ns * u128::try_from(generation)?)?);
            if due > end {
                break;
            }
            tokio::time::sleep_until(due.into()).await;
            // A scheduled deadline inside the window can still wake after `end`.
            if Instant::now() > end {
                after_window += u64::try_from(config.publications - generation)?;
                break;
            }
            let Some(token) = controller.current_token() else {
                errors += 1;
                break;
            };
            let build_start = Instant::now();
            let candidate = fixture::graph(&config, u64::try_from(generation + 1)?)?;
            build_samples.push(u64::try_from(build_start.elapsed().as_micros())?);
            // Candidate construction can itself push past the measurement window.
            if Instant::now() > end {
                after_window += u64::try_from(config.publications - generation)?;
                break;
            }
            let publish_start = Instant::now();
            if controller.publish_if_current(token, candidate).is_ok() {
                successful += 1;
            } else {
                errors += 1;
            }
            publish_samples.push(u64::try_from(publish_start.elapsed().as_micros())?);
        }
        Ok((
            requested,
            successful,
            errors,
            after_window,
            publish_samples,
            build_samples,
        ))
    })))
}

fn join_error(error: &tokio::task::JoinError) -> ProbeError {
    format!("workload task failed: {error}").into()
}

fn account_cancelled(counters: &Counters) {
    let admitted = counters.admitted.load(Ordering::Relaxed);
    let finished = counters.completed.load(Ordering::Relaxed)
        + counters.runtime_errors.load(Ordering::Relaxed);
    counters
        .drain_cancelled
        .store(admitted.saturating_sub(finished), Ordering::Relaxed);
    counters.outstanding.store(0, Ordering::Relaxed);
}

async fn drain_unit(
    tasks: &mut JoinSet<()>,
    counters: &Counters,
    drain_deadline: Instant,
) -> Result<(), ProbeError> {
    loop {
        match tokio::time::timeout_at(drain_deadline.into(), tasks.join_next()).await {
            Ok(None) => return Ok(()),
            Ok(Some(Ok(()))) => {}
            Ok(Some(Err(error))) => return Err(join_error(&error)),
            Err(_) => break,
        }
    }
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => return Err(join_error(&error)),
        }
    }
    account_cancelled(counters);
    Ok(())
}

async fn closed_loop(
    config: &Config,
    call: &Call,
    counters: &Arc<Counters>,
    sink: Option<&Arc<LatencySink>>,
    by_backend: &Arc<Vec<AtomicU64>>,
    end: Instant,
) -> Result<(), ProbeError> {
    let barrier = Arc::new(Barrier::new(config.clients + 1));
    let sequence = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();
    for shard in 0..config.clients {
        let (call, config, counters, by_backend, barrier, sequence, sink) = (
            Arc::clone(call),
            config.clone(),
            Arc::clone(counters),
            Arc::clone(by_backend),
            Arc::clone(&barrier),
            Arc::clone(&sequence),
            sink.map(Arc::clone),
        );
        tasks.spawn(async move {
            barrier.wait().await;
            while Instant::now() < end {
                let index = sequence.fetch_add(1, Ordering::Relaxed);
                admit_and_run(
                    &call,
                    fixture::request(index, &config),
                    &counters,
                    sink.as_ref().map(|sink| (sink.as_ref(), shard)),
                    &by_backend,
                    end,
                )
                .await;
            }
            Ok::<_, ProbeError>(())
        });
    }
    barrier.wait().await;
    // Wait for the full measurement window, then allow a bounded drain.
    let drain_deadline = end + Duration::from_secs(DRAIN_SECS);
    loop {
        match tokio::time::timeout_at(drain_deadline.into(), tasks.join_next()).await {
            Ok(None) => break,
            Ok(Some(Ok(Ok(())))) => {}
            Ok(Some(Ok(Err(error)))) => return Err(error),
            Ok(Some(Err(error))) => return Err(join_error(&error)),
            Err(_) => {
                tasks.abort_all();
                while let Some(result) = tasks.join_next().await {
                    match result {
                        Ok(Ok(()) | Err(_)) => {}
                        Err(error) if error.is_cancelled() => {}
                        Err(error) => return Err(join_error(&error)),
                    }
                }
                account_cancelled(counters);
                break;
            }
        }
    }
    Ok(())
}

async fn open_loop(
    config: &Config,
    call: &Call,
    counters: &Arc<Counters>,
    sink: Option<&Arc<LatencySink>>,
    by_backend: &Arc<Vec<AtomicU64>>,
    end: Instant,
) -> Result<(), ProbeError> {
    let rate = config.offered_rps.ok_or("missing offered rate")?;
    let interval = Duration::from_nanos(1_000_000_000 / rate);
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut tasks = JoinSet::new();
    let mut sequence = 0_u64;
    let mut scheduled = 0_u64;
    let nominal = rate.saturating_mul(config.duration_secs);
    counters.nominal_arrivals.store(nominal, Ordering::Relaxed);
    while Instant::now() < end {
        tick.tick().await;
        scheduled += 1;
        if Instant::now() >= end {
            break;
        }
        while let Some(result) = tasks.try_join_next() {
            result.map_err(|error| join_error(&error))?;
        }
        if tasks.len() >= config.generator_capacity {
            counters.client_dropped.fetch_add(1, Ordering::Relaxed);
            sequence += 1;
            continue;
        }
        counters.generated_attempts.fetch_add(1, Ordering::Relaxed);
        let request = fixture::request(sequence, config);
        let shard = usize::try_from(sequence)? % OPEN_HISTOGRAM_SHARDS;
        sequence += 1;
        let (call, counters, sink, by_backend) = (
            Arc::clone(call),
            Arc::clone(counters),
            sink.map(Arc::clone),
            Arc::clone(by_backend),
        );
        tasks.spawn(async move {
            admit_and_run(
                &call,
                request,
                &counters,
                sink.as_ref().map(|sink| (sink.as_ref(), shard)),
                &by_backend,
                end,
            )
            .await;
        });
    }
    counters
        .scheduled_arrivals
        .store(scheduled, Ordering::Relaxed);
    drain_unit(&mut tasks, counters, end + Duration::from_secs(DRAIN_SECS)).await
}

pub async fn drive(
    config: &Config,
    call: &Call,
    counters: &Arc<Counters>,
    sink: Option<&Arc<LatencySink>>,
    by_backend: &Arc<Vec<AtomicU64>>,
    end: Instant,
) -> Result<(), ProbeError> {
    match config.load {
        Load::Closed => closed_loop(config, call, counters, sink, by_backend, end).await,
        Load::Open => open_loop(config, call, counters, sink, by_backend, end).await,
    }
}
