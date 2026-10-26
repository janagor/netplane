//! Closed-loop and open-loop measurement with latency percentiles and CPU share.

use crate::config::Config;
use crate::fixtures::{Call, ProbeError};
use serde::Serialize;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Barrier;

/// Latency percentiles in nanoseconds.
#[derive(Serialize)]
pub struct Latency {
    pub mean: f64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
}

/// Open-loop latency components. All values are nanoseconds.
#[derive(Serialize)]
pub struct OpenLoopLatency {
    /// `invoke_start - scheduled_arrival` (generator/scheduling lateness).
    pub arrival_lateness: Latency,
    /// `completion - invoke_start` (Handle path only).
    pub service: Latency,
    /// `completion - scheduled_arrival` (end-to-end including lateness).
    pub end_to_end: Latency,
}

#[derive(Serialize)]
pub struct RunMetrics {
    pub completed: u64,
    pub elapsed_ns: u64,
    pub throughput_rps: f64,
    /// Closed-loop: invoke-to-completion. Open-loop: end-to-end for compatibility.
    pub latency: Latency,
    /// Present only for open-loop runs.
    pub open_loop: Option<OpenLoopLatency>,
    /// Process CPU time / wall time. Values above 1.0 mean multiple cores.
    pub cpu_utilization: Option<f64>,
    /// Linux process scheduling delay; unavailable on systems without schedstat.
    pub scheduler_wait_ns: Option<u64>,
    /// Not collected: workspace forbids `unsafe`, which a counting allocator needs.
    pub allocations_per_request: Option<f64>,
}

fn percentile(sorted: &[u64], numerator: usize) -> u64 {
    let rank = sorted.len().saturating_mul(numerator).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len().saturating_sub(1))]
}

const fn as_f64(value: u64) -> f64 {
    // Probe counters stay well below the 53-bit mantissa in practice.
    #[expect(
        clippy::cast_precision_loss,
        reason = "JSON throughput and mean latency are reporting-only floats"
    )]
    {
        value as f64
    }
}

fn process_schedstat() -> Option<(u64, u64)> {
    // /proc/self/schedstat reports only the thread-group leader on some kernels.
    // These values are descriptive; avoid interpreting them as process-wide CPU.
    let text = std::fs::read_to_string("/proc/self/schedstat").ok()?;
    let mut fields = text.split_whitespace();
    Some((fields.next()?.parse().ok()?, fields.next()?.parse().ok()?))
}

fn latency_of(mut samples: Vec<u64>) -> Result<Latency, ProbeError> {
    if samples.is_empty() {
        return Err("no latency samples".into());
    }
    samples.sort_unstable();
    let sum: u128 = samples.iter().map(|&value| u128::from(value)).sum();
    let completed = u64::try_from(samples.len()).map_err(|_| "sample count overflow")?;
    Ok(Latency {
        mean: as_f64(u64::try_from(sum / u128::from(completed)).unwrap_or(u64::MAX)),
        p50: percentile(&samples, 50),
        p95: percentile(&samples, 95),
        p99: percentile(&samples, 99),
    })
}

pub async fn warm(call: &Call, count: usize) -> Result<(), ProbeError> {
    for index in 0..count {
        let response = call(u64::try_from(index)?).await?;
        if response != u64::try_from(index)?.wrapping_add(1) {
            return Err("warmup response mismatch".into());
        }
    }
    Ok(())
}

pub async fn measure(config: &Config, call: &Call) -> Result<RunMetrics, ProbeError> {
    if let Some(rate) = config.offered_rps {
        return measure_open_loop(config, call, rate).await;
    }
    let callers = config.callers;
    let samples_per_caller = config.samples / callers;
    let armed = Arc::new(Barrier::new(callers + 1));
    let go = Arc::new(Barrier::new(callers + 1));
    let mut joins = Vec::with_capacity(callers);
    for worker in 0..callers {
        let armed = Arc::clone(&armed);
        let go = Arc::clone(&go);
        let call = Arc::clone(call);
        joins.push(tokio::spawn(async move {
            armed.wait().await;
            go.wait().await;
            let mut local = Vec::with_capacity(samples_per_caller);
            for index in 0..samples_per_caller {
                let request = u64::try_from(worker * samples_per_caller + index)?;
                let started = Instant::now();
                let response = call(request).await?;
                if response != request.wrapping_add(1) {
                    return Err("response mismatch".into());
                }
                local.push(
                    u64::try_from(started.elapsed().as_nanos()).map_err(|_| "latency overflow")?,
                );
            }
            Ok::<_, ProbeError>(local)
        }));
    }
    armed.wait().await;
    let sched_before = process_schedstat();
    let started = Instant::now();
    go.wait().await;
    let mut latencies = Vec::with_capacity(config.samples);
    for join in joins {
        latencies.extend(join.await??);
    }
    let elapsed = started.elapsed();
    let sched_after = process_schedstat();
    summarize(latencies, elapsed, sched_before, sched_after, None)
}

async fn measure_open_loop(
    config: &Config,
    call: &Call,
    rate: u64,
) -> Result<RunMetrics, ProbeError> {
    // Pace with `callers` generator tasks that spawn the request future and continue.
    // Awaiting completion on the pacer path turns open-loop into closed-loop under load
    // and folds queueing into arrival_lateness. Spawning every sleeper at t=0 caused
    // timer catch-up bursts that hit admission.
    let callers = config.callers.max(1);
    let next = Arc::new(AtomicUsize::new(0));
    let samples = config.samples;
    let armed = Arc::new(Barrier::new(callers + 1));
    let go = Arc::new(Barrier::new(callers + 1));
    let epoch_slot = Arc::new(std::sync::OnceLock::new());
    let (tx, mut rx) =
        tokio::sync::mpsc::unbounded_channel::<Result<(u64, u64, u64), ProbeError>>();
    let mut pacers = Vec::with_capacity(callers);
    for _ in 0..callers {
        let armed = Arc::clone(&armed);
        let go = Arc::clone(&go);
        let call = Arc::clone(call);
        let next = Arc::clone(&next);
        let epoch_slot = Arc::clone(&epoch_slot);
        let tx = tx.clone();
        pacers.push(tokio::spawn(async move {
            armed.wait().await;
            go.wait().await;
            let epoch = *epoch_slot
                .get()
                .ok_or("open-loop epoch missing before generators released")?;
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= samples {
                    break;
                }
                let offset_ns = u128::try_from(index)? * 1_000_000_000 / u128::from(rate);
                let scheduled = epoch + Duration::from_nanos(u64::try_from(offset_ns)?);
                wait_until(scheduled).await;
                let request = u64::try_from(index)?;
                let call = Arc::clone(&call);
                let tx = tx.clone();
                tokio::spawn(async move {
                    let result = async {
                        // Timestamps start when this task runs: lateness includes
                        // generator/scheduler delay; service is invoke→completion only.
                        let invoke = Instant::now();
                        let lateness =
                            u64::try_from(invoke.saturating_duration_since(scheduled).as_nanos())?;
                        let response = call(request).await?;
                        if response != request.wrapping_add(1) {
                            return Err("response mismatch".into());
                        }
                        let service = u64::try_from(invoke.elapsed().as_nanos())?;
                        let end_to_end = u64::try_from(scheduled.elapsed().as_nanos())?;
                        Ok((lateness, service, end_to_end))
                    }
                    .await;
                    let _ = tx.send(result);
                });
            }
            Ok::<_, ProbeError>(())
        }));
    }
    drop(tx);
    armed.wait().await;
    let sched_before = process_schedstat();
    let epoch = Instant::now();
    let _ = epoch_slot.set(epoch);
    go.wait().await;
    for pacer in pacers {
        pacer.await??;
    }
    let mut lateness = Vec::with_capacity(config.samples);
    let mut service = Vec::with_capacity(config.samples);
    let mut end_to_end = Vec::with_capacity(config.samples);
    while let Some(sample) = rx.recv().await {
        let (late, svc, e2e) = sample?;
        lateness.push(late);
        service.push(svc);
        end_to_end.push(e2e);
    }
    let elapsed = epoch.elapsed();
    let sched_after = process_schedstat();
    let open_loop = OpenLoopLatency {
        arrival_lateness: latency_of(lateness)?,
        service: latency_of(service)?,
        end_to_end: latency_of(end_to_end.clone())?,
    };
    summarize(
        end_to_end,
        elapsed,
        sched_before,
        sched_after,
        Some(open_loop),
    )
}

/// Wait until `deadline` without busy-spinning Tokio workers.
async fn wait_until(deadline: Instant) {
    let now = Instant::now();
    if now < deadline {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    }
}

fn summarize(
    latencies: Vec<u64>,
    elapsed: Duration,
    sched_before: Option<(u64, u64)>,
    sched_after: Option<(u64, u64)>,
    open_loop: Option<OpenLoopLatency>,
) -> Result<RunMetrics, ProbeError> {
    let completed = u64::try_from(latencies.len()).map_err(|_| "sample count overflow")?;
    let elapsed_ns = u64::try_from(elapsed.as_nanos()).map_err(|_| "elapsed overflow")?;
    let throughput_rps = if elapsed == Duration::ZERO {
        0.0
    } else {
        as_f64(completed) / elapsed.as_secs_f64()
    };
    let cpu_utilization = match (sched_before, sched_after, elapsed_ns) {
        (Some((before, _)), Some((after, _)), wall) if after >= before && wall > 0 => {
            Some(as_f64(after - before) / as_f64(wall))
        }
        _ => None,
    };
    let scheduler_wait_ns = match (sched_before, sched_after) {
        (Some((_, before)), Some((_, after))) => after.checked_sub(before),
        _ => None,
    };
    Ok(RunMetrics {
        completed,
        elapsed_ns,
        throughput_rps,
        latency: latency_of(latencies)?,
        open_loop,
        cpu_utilization,
        scheduler_wait_ns,
        allocations_per_request: None,
    })
}
