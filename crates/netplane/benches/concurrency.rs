#![expect(
    missing_docs,
    reason = "Criterion generates public benchmark entrypoints"
)]
#![expect(
    clippy::unwrap_used,
    reason = "Invalid benchmark fixtures must fail immediately"
)]
pub mod common;

use std::{sync::Arc, time::Duration, time::Instant};

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures_util::future::join_all;
use netplane::Handle;
use tokio::{sync::Barrier, task::JoinHandle};

const COMPLETED_REQUESTS: usize = 128;

/// Batched in-flight requests driven by `join_all` inside one parent Tokio task.
fn batched_inflight(c: &mut Criterion) {
    let runtime = common::runtime();
    let graph = runtime.block_on(async { common::netplane(512) });
    let handle = graph.handle();
    runtime.block_on(common::warm(&handle));
    let mut group = c.benchmark_group("concurrency/batched_inflight");
    for callers in [1, 2, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(COMPLETED_REQUESTS as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(callers),
            &callers,
            |b, &callers| {
                b.to_async(&runtime).iter(|| async {
                    for batch in 0..(COMPLETED_REQUESTS / callers) {
                        let futures = (0..callers).map(|index| {
                            handle
                                .execute(black_box(u64::try_from(batch * callers + index).unwrap()))
                        });
                        for result in join_all(futures).await {
                            black_box(result.unwrap());
                        }
                    }
                });
            },
        );
    }
    group.finish();
}

/// Spawn caller tasks and wait until every worker is parked on the release barrier.
async fn spawn_armed_workers(
    handle: &Handle<u64, u64>,
    callers: usize,
    ops_per_caller: usize,
) -> (Arc<Barrier>, Vec<JoinHandle<()>>) {
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
            for index in 0..ops_per_caller {
                let request = u64::try_from(worker * ops_per_caller + index).unwrap();
                black_box(handle.execute(black_box(request)).await.unwrap());
            }
        }));
    }
    armed.wait().await;
    (go, joins)
}

/// Independently scheduled caller tasks released together after arming.
///
/// Worker tasks are spawned and parked outside the timed interval. The timed
/// work releases them and waits for a fixed number of sequential completions.
fn multi_task_callers(c: &mut Criterion) {
    let runtime = common::runtime();
    let graph = runtime.block_on(async { common::netplane(512) });
    let handle = graph.handle();
    runtime.block_on(common::warm(&handle));
    let mut group = c.benchmark_group("concurrency/multi_task_callers");
    for callers in [1, 2, 4, 8, 16, 32] {
        let ops_per_caller = COMPLETED_REQUESTS / callers;
        group.throughput(Throughput::Elements(COMPLETED_REQUESTS as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(callers),
            &callers,
            |b, &callers| {
                b.to_async(&runtime).iter_custom(|iters| {
                    let handle = handle.clone();
                    async move {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let (go, joins) =
                                spawn_armed_workers(&handle, callers, ops_per_caller).await;
                            let started = Instant::now();
                            go.wait().await;
                            for join in joins {
                                join.await.unwrap();
                            }
                            total += started.elapsed();
                        }
                        total
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, batched_inflight, multi_task_callers);
criterion_main!(benches);
