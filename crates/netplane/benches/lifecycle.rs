#![expect(
    missing_docs,
    reason = "Criterion generates public benchmark entrypoints"
)]
#![expect(
    clippy::unwrap_used,
    reason = "Invalid benchmark fixtures must fail immediately"
)]
#![expect(
    clippy::semicolon_if_nothing_returned,
    reason = "Criterion closures return unit"
)]
#![expect(
    clippy::significant_drop_tightening,
    reason = "Criterion groups are explicitly finished"
)]
pub mod common;

use std::{
    convert::Infallible,
    future::{Future, Ready},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    thread,
    time::Duration,
};

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use netplane::{
    Capacity, ControllerOptions, GraphDraft, Netplane, ReconfigurationController, RuntimeLimits,
};
use tower::Service;

struct NeverReady(Arc<AtomicUsize>);

impl Service<u64> for NeverReady {
    type Response = u64;
    type Error = Infallible;
    type Future = Ready<Result<u64, Infallible>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Poll::Pending
    }

    fn call(&mut self, request: u64) -> Self::Future {
        std::future::ready(Ok(request))
    }
}

async fn poll_once_pending<F: Future>(future: &mut Pin<Box<F>>) {
    assert!(std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await);
}

fn wait_until_polled(runtime: &tokio::runtime::Runtime, polls: &AtomicUsize) {
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), async {
            while polls.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
}

fn never_ready_netplane(
    runtime: &tokio::runtime::Runtime,
    polls: Arc<AtomicUsize>,
) -> Netplane<u64, u64> {
    runtime.block_on(async {
        Netplane::new(
            GraphDraft::new(NeverReady(polls))
                .capacity(common::CAPACITY)
                .validate()
                .unwrap(),
            RuntimeLimits {
                max_outstanding: 64,
            },
        )
        .unwrap()
    })
}

fn lifecycle_ops(c: &mut Criterion) {
    let runtime = common::runtime();
    let mut graph = c.benchmark_group("graph");
    graph.bench_function("compile_single_root", |b| {
        b.iter(|| {
            runtime.block_on(async { black_box(common::draft()) });
        })
    });
    graph.bench_function("compile_router_4_backends", |b| {
        b.iter(|| {
            runtime.block_on(async { black_box(common::routed(4, false)) });
        })
    });
    graph.finish();

    let mut group = c.benchmark_group("lifecycle");
    group.bench_function("drop_admitted_unpolled", |b| {
        let graph = runtime.block_on(async { common::netplane(64) });
        let handle = graph.handle();
        b.iter(|| {
            let request = handle.try_execute(black_box(7)).unwrap();
            drop(request);
        });
    });
    group.bench_function("cancel_pending_readiness", |b| {
        b.iter_batched(
            || {
                let polls = Arc::new(AtomicUsize::new(0));
                let graph = never_ready_netplane(&runtime, Arc::clone(&polls));
                let mut execution = Box::pin(graph.handle().execute(7));
                runtime.block_on(poll_once_pending(&mut execution));
                wait_until_polled(&runtime, &polls);
                (graph, execution)
            },
            |(graph, execution)| {
                drop(execution);
                graph
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.bench_function("expired_deadline", |b| {
        let graph = runtime.block_on(async { common::netplane(64) });
        let handle = graph.handle();
        b.to_async(&runtime).iter(|| async {
            let result = handle
                .execute_until(
                    black_box(7),
                    tokio::time::Instant::now() - Duration::from_nanos(1),
                )
                .await;
            assert!(result.is_err());
        });
    });
    group.bench_function("construct_and_shutdown_empty", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let mut graph = common::netplane(64);
                graph.shutdown(Duration::from_secs(1)).await.unwrap();
            });
        })
    });
    group.bench_function("shutdown_preconstructed_empty", |b| {
        b.iter_batched(
            || runtime.block_on(async { common::netplane(64) }),
            |mut graph| {
                runtime.block_on(async {
                    graph.shutdown(Duration::from_secs(1)).await.unwrap();
                });
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.bench_function("drain_after_request_drop", |b| {
        b.iter_batched(
            || {
                let graph = runtime.block_on(async { common::netplane(64) });
                let request = graph.handle().try_execute(7).unwrap();
                (graph, request)
            },
            |(mut graph, request)| {
                let drain = graph.shutdown(Duration::from_secs(1));
                drop(request);
                runtime.block_on(drain).unwrap();
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

fn queue_cancellation(c: &mut Criterion) {
    let runtime = common::runtime();
    let polls = Arc::new(AtomicUsize::new(0));
    let graph = runtime.block_on(async {
        let version = GraphDraft::new(NeverReady(Arc::clone(&polls)))
            .capacity(Capacity {
                max_in_flight: 1,
                buffer_capacity: 1,
            })
            .validate()
            .unwrap();
        Netplane::new(
            version,
            RuntimeLimits {
                max_outstanding: 64,
            },
        )
        .unwrap()
    });
    let handle = graph.handle();
    let mut first = Box::pin(handle.execute(0));
    runtime.block_on(poll_once_pending(&mut first));
    wait_until_polled(&runtime, &polls);
    let mut second = Box::pin(handle.execute(1));
    runtime.block_on(poll_once_pending(&mut second));

    c.bench_function("lifecycle/cancel_waiting_queue", |b| {
        b.iter_batched(
            || {
                let mut waiter = Box::pin(handle.execute(2));
                runtime.block_on(poll_once_pending(&mut waiter));
                waiter
            },
            drop,
            criterion::BatchSize::PerIteration,
        );
    });
    drop(second);
    drop(first);
}

fn publication_ops(c: &mut Criterion) {
    let runtime = common::runtime();
    let mut publication = c.benchmark_group("publication");
    publication.bench_function("direct_with_displaced_drop", |b| {
        let mut graph = runtime.block_on(async { common::netplane(64) });
        b.iter_batched(
            || runtime.block_on(async { common::draft() }),
            |candidate| {
                graph.publish(candidate).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });
    for history in [0, 2, 8, 32] {
        let controller = runtime.block_on(async {
            ReconfigurationController::new(
                common::netplane(64),
                ControllerOptions {
                    max_retained_versions: history,
                },
            )
        });
        publication.bench_with_input(
            BenchmarkId::new("controller_history", history),
            &history,
            |b, _| {
                b.iter_batched(
                    || runtime.block_on(async { common::draft() }),
                    |candidate| {
                        let token = controller.current_token().unwrap();
                        black_box(controller.publish_if_current(token, candidate).unwrap());
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    publication.finish();

    let mut retained = c.benchmark_group("publication/retained");
    retained.bench_function("direct_with_old_request", |b| {
        let mut graph = runtime.block_on(async { common::netplane(64) });
        let handle = graph.handle();
        b.iter_batched(
            || {
                let old = handle.try_execute(0).unwrap();
                let candidate = runtime.block_on(async { common::draft() });
                (old, candidate)
            },
            |(old, candidate)| {
                graph.publish(candidate).unwrap();
                drop(old);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    retained.finish();

    let mut traffic = c.benchmark_group("publication/traffic");
    traffic.bench_function("direct_with_one_caller", |b| {
        let mut graph = runtime.block_on(async { common::netplane(64) });
        let handle = graph.handle();
        let runtime_handle = runtime.handle().clone();
        let running = Arc::new(AtomicBool::new(true));
        let worker_running = Arc::clone(&running);
        let worker = thread::spawn(move || {
            while worker_running.load(Ordering::Relaxed) {
                black_box(runtime_handle.block_on(handle.execute(7)).unwrap());
            }
        });
        b.iter_batched(
            || runtime.block_on(async { common::draft() }),
            |candidate| graph.publish(candidate).unwrap(),
            criterion::BatchSize::SmallInput,
        );
        running.store(false, Ordering::Relaxed);
        worker.join().unwrap();
    });
    traffic.finish();
}

criterion_group!(benches, lifecycle_ops, queue_cancellation, publication_ops);
criterion_main!(benches);
