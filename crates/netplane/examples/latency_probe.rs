//! Collect per-request latency percentiles for the in-memory Netplane path.
//!
//! Workload modes:
//! - `batched`: concurrent in-flight requests driven by `join_all` in one task
//! - `tasks`: independently scheduled caller tasks, each issuing sequential requests

use std::{
    convert::Infallible,
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::future::join_all;
use netplane::{Capacity, GraphDraft, Handle, Netplane, RuntimeLimits};
use tokio::sync::Barrier;
use tower::service_fn;

type ProbeError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Workload {
    Batched,
    Tasks,
}

impl Workload {
    fn parse(value: &str) -> Result<Self, ProbeError> {
        match value {
            "batched" => Ok(Self::Batched),
            "tasks" => Ok(Self::Tasks),
            _ => Err("workload must be batched or tasks".into()),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Batched => "batched",
            Self::Tasks => "tasks",
        }
    }
}

fn percentile(sorted: &[u128], numerator: usize) -> u128 {
    let rank = sorted.len().saturating_mul(numerator).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn parse_args() -> Result<(usize, usize, Workload), ProbeError> {
    let args: Vec<String> = std::env::args().collect();
    let callers = args
        .get(1)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(1);
    let samples = args
        .get(2)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(4096);
    let workload = args
        .get(3)
        .map(String::as_str)
        .map(Workload::parse)
        .transpose()?
        .unwrap_or(Workload::Batched);
    if !(1..=32).contains(&callers) || !(1..=1_000_000).contains(&samples) {
        return Err("callers must be 1..=32 and samples must be 1..=1000000".into());
    }
    if !samples.is_multiple_of(callers) {
        return Err("samples must be divisible by callers".into());
    }
    Ok((callers, samples, workload))
}

async fn measure_batched(
    handle: &Handle<u64, u64>,
    callers: usize,
    samples: usize,
) -> Result<(Duration, Vec<u128>), ProbeError> {
    // One handle clone per in-flight slot, matching task-mode per-worker cloning.
    let handles: Vec<_> = (0..callers).map(|_| handle.clone()).collect();
    let mut latencies = Vec::with_capacity(samples);
    let started = Instant::now();
    for offset in (0..samples).step_by(callers) {
        let batch_len = callers.min(samples - offset);
        let batch = handles[..batch_len]
            .iter()
            .enumerate()
            .map(|(slot, handle)| {
                let index = offset + slot;
                async move {
                    let started = Instant::now();
                    let response = handle.execute(u64::try_from(index)?).await?;
                    assert_eq!(response, u64::try_from(index)? + 1);
                    Ok::<_, ProbeError>(started.elapsed().as_nanos())
                }
            });
        for result in join_all(batch).await {
            latencies.push(result?);
        }
    }
    Ok((started.elapsed(), latencies))
}

async fn measure_tasks(
    handle: &Handle<u64, u64>,
    callers: usize,
    samples: usize,
) -> Result<(Duration, Vec<u128>), ProbeError> {
    let samples_per_caller = samples / callers;
    let armed = Arc::new(Barrier::new(callers + 1));
    let go = Arc::new(Barrier::new(callers + 1));
    let mut joins = Vec::with_capacity(callers);
    for worker in 0..callers {
        let armed = Arc::clone(&armed);
        let go = Arc::clone(&go);
        let handle = handle.clone();
        joins.push(tokio::spawn(async move {
            armed.wait().await;
            go.wait().await;
            let mut local = Vec::with_capacity(samples_per_caller);
            for index in 0..samples_per_caller {
                let request = u64::try_from(worker * samples_per_caller + index)?;
                let started = Instant::now();
                let response = handle.execute(request).await?;
                assert_eq!(response, request + 1);
                local.push(started.elapsed().as_nanos());
            }
            Ok::<_, ProbeError>(local)
        }));
    }
    armed.wait().await;
    let mut latencies = Vec::with_capacity(samples);
    let started = Instant::now();
    go.wait().await;
    for join in joins {
        latencies.extend(join.await??);
    }
    Ok((started.elapsed(), latencies))
}

fn main() -> Result<(), ProbeError> {
    let (callers, samples, workload) = parse_args()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let service = service_fn(|value: u64| std::future::ready(Ok::<_, Infallible>(value + 1)));
        let graph = GraphDraft::new(service)
            .capacity(Capacity {
                max_in_flight: 64,
                buffer_capacity: 128,
            })
            .validate()?;
        let graph = Netplane::new(graph, RuntimeLimits { max_outstanding: 256 })?;
        let handle = graph.handle();
        for value in 0..100 {
            assert_eq!(handle.execute(value).await?, value + 1);
        }

        let (elapsed, mut latencies) = match workload {
            Workload::Batched => measure_batched(&handle, callers, samples).await?,
            Workload::Tasks => measure_tasks(&handle, callers, samples).await?,
        };

        latencies.sort_unstable();
        let throughput = f64::from(u32::try_from(samples)?) / elapsed.as_secs_f64();
        println!(
            "workload={} callers={callers} samples={samples} throughput_rps={throughput:.0} p50_ns={} p95_ns={} p99_ns={}",
            workload.as_str(),
            percentile(&latencies, 50),
            percentile(&latencies, 95),
            percentile(&latencies, 99)
        );
        Ok(())
    })
}
