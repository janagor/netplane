//! Closed-loop multi-task measurement with latency percentiles and CPU share.

use crate::config::Config;
use crate::fixtures::{Call, ProbeError};
use serde::Serialize;
use std::{
    sync::Arc,
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

#[derive(Serialize)]
pub struct RunMetrics {
    pub completed: u64,
    pub elapsed_ns: u64,
    pub throughput_rps: f64,
    pub latency: Latency,
    /// Process CPU time / wall time. Values above 1.0 mean multiple cores.
    pub cpu_utilization: Option<f64>,
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

fn process_cpu_ns() -> Option<u64> {
    // schedstat field 1 is on-CPU time in nanoseconds (Linux).
    let text = std::fs::read_to_string("/proc/self/schedstat").ok()?;
    text.split_whitespace().next()?.parse().ok()
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
                    u64::try_from(started.elapsed().as_nanos())
                        .map_err(|_| "latency overflow")?,
                );
            }
            Ok::<_, ProbeError>(local)
        }));
    }
    armed.wait().await;
    let cpu_before = process_cpu_ns();
    let started = Instant::now();
    go.wait().await;
    let mut latencies = Vec::with_capacity(config.samples);
    for join in joins {
        latencies.extend(join.await??);
    }
    let elapsed = started.elapsed();
    let cpu_after = process_cpu_ns();
    latencies.sort_unstable();
    let sum: u128 = latencies.iter().map(|&value| u128::from(value)).sum();
    let completed = u64::try_from(latencies.len()).map_err(|_| "sample count overflow")?;
    let elapsed_ns = u64::try_from(elapsed.as_nanos()).map_err(|_| "elapsed overflow")?;
    let throughput_rps = if elapsed == Duration::ZERO {
        0.0
    } else {
        as_f64(completed) / elapsed.as_secs_f64()
    };
    let cpu_utilization = match (cpu_before, cpu_after, elapsed_ns) {
        (Some(before), Some(after), wall) if after >= before && wall > 0 => {
            Some(as_f64(after - before) / as_f64(wall))
        }
        _ => None,
    };
    let mean = as_f64(u64::try_from(sum / u128::from(completed.max(1))).unwrap_or(u64::MAX));
    Ok(RunMetrics {
        completed,
        elapsed_ns,
        throughput_rps,
        latency: Latency {
            mean,
            p50: percentile(&latencies, 50),
            p95: percentile(&latencies, 95),
            p99: percentile(&latencies, 99),
        },
        cpu_utilization,
        allocations_per_request: None,
    })
}
