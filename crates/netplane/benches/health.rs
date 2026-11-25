#![expect(
    missing_docs,
    reason = "Criterion generates public benchmark entrypoints"
)]
#![expect(clippy::unwrap_used, reason = "Benchmark fixtures must be valid")]
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use netplane::{
    HealthAwareRouting, WeightedRouting,
    health::{
        BackendIdentity, HealthHandle, HealthLayer, HealthOutcome, HealthPolicy, HealthRegistry,
    },
    routing::UniformRouting,
};
use std::{
    convert::Infallible,
    future::Future,
    num::NonZeroU32,
    pin::pin,
    sync::Barrier,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tower::{Layer, Service, service_fn};

fn selection(c: &mut Criterion) {
    let registry = HealthRegistry::default();
    let mut group = c.benchmark_group("health/selection");
    for count in [2_usize, 8, 32, 128] {
        // Integer percentages permit at most 100 positive targets.
        let positive = count.min(100);
        let policy = WeightedRouting::try_new((0..count).map(|i| {
            (
                i.to_string(),
                u8::try_from(100 / positive + usize::from(i < 100 % positive)).unwrap()
                    * u8::from(i < positive),
            )
        }))
        .unwrap();
        let handles: Vec<_> = (0..count)
            .map(|i| {
                registry
                    .register(
                        BackendIdentity {
                            service: count.to_string(),
                            instance: i.to_string(),
                            incarnation: "bench".into(),
                        },
                        HealthPolicy {
                            failure_threshold: NonZeroU32::MIN,
                            recovery_threshold: NonZeroU32::MIN,
                            recovery_delay: Duration::from_secs(3600),
                        },
                    )
                    .unwrap()
            })
            .collect();
        let health = HealthAwareRouting::try_new(
            policy.clone(),
            handles
                .iter()
                .enumerate()
                .map(|(i, h)| (i.to_string(), h.clone())),
        )
        .unwrap();
        group.bench_with_input(BenchmarkId::new("disabled", count), &count, |b, _| {
            b.iter(|| black_box(policy.select(black_box(b"customer-1"))));
        });
        group.bench_with_input(BenchmarkId::new("all_healthy", count), &count, |b, _| {
            b.iter(|| black_box(health.select(black_box(b"customer-1"))));
        });
        for handle in handles.iter().step_by(2) {
            handle.report(handle.begin_observation(), HealthOutcome::Failure);
        }
        group.bench_with_input(BenchmarkId::new("half_healthy", count), &count, |b, _| {
            b.iter(|| black_box(health.select(black_box(b"customer-1"))));
        });
        if count >= 8 {
            let stop = std::sync::atomic::AtomicBool::new(false);
            std::thread::scope(|scope| {
                let writer_stop = &stop;
                let writer_handles = &handles;
                let worker = scope.spawn(move || {
                    while !writer_stop.load(std::sync::atomic::Ordering::Relaxed) {
                        for h in writer_handles {
                            h.report(h.begin_observation(), HealthOutcome::Success);
                        }
                    }
                });
                group.bench_with_input(
                    BenchmarkId::new("concurrent_observations", count),
                    &count,
                    |b, _| b.iter(|| black_box(health.select(black_box(b"customer-1")))),
                );
                stop.store(true, std::sync::atomic::Ordering::Relaxed);
                worker.join().unwrap();
            });
        }
        group.bench_with_input(BenchmarkId::new("observation", count), &count, |b, _| {
            b.iter(|| {
                let h = &handles[1];
                h.report(h.begin_observation(), black_box(HealthOutcome::Success));
            });
        });
    }
    group.finish();
}
criterion_group!(benches, selection, uniform_selection, passive_wrapper);
criterion_main!(benches);

fn benchmark_handle(registry: &HealthRegistry, service: &str, index: usize) -> HealthHandle {
    registry
        .register(
            BackendIdentity {
                service: service.into(),
                instance: index.to_string(),
                incarnation: "bench".into(),
            },
            HealthPolicy {
                failure_threshold: NonZeroU32::MIN,
                recovery_threshold: NonZeroU32::MIN,
                recovery_delay: Duration::from_secs(3600),
            },
        )
        .unwrap()
}

fn uniform_selection(c: &mut Criterion) {
    let registry = HealthRegistry::default();
    let mut group = c.benchmark_group("health/uniform_selection");
    for count in [2_usize, 8, 32, 128] {
        let policy = UniformRouting::try_new((0..count).map(|index| index.to_string())).unwrap();
        let handles: Vec<_> = (0..count)
            .map(|index| benchmark_handle(&registry, &format!("uniform-{count}"), index))
            .collect();
        group.bench_with_input(BenchmarkId::new("disabled", count), &count, |b, _| {
            b.iter(|| black_box(policy.select(black_box(b"customer-1"))));
        });
        let select = || {
            let mut index = 0;
            policy.select_eligible(black_box(b"customer-1"), |_| {
                let eligible = handles[index].is_eligible();
                index += 1;
                eligible
            })
        };
        group.bench_with_input(BenchmarkId::new("all_healthy", count), &count, |b, _| {
            b.iter(|| black_box(select()));
        });
        for handle in handles.iter().step_by(2) {
            handle.report(handle.begin_observation(), HealthOutcome::Failure);
        }
        group.bench_with_input(BenchmarkId::new("half_healthy", count), &count, |b, _| {
            b.iter(|| black_box(select()));
        });
    }
    group.finish();
}
const fn classify(_: Result<&(), &Infallible>) -> HealthOutcome {
    HealthOutcome::Success
}
fn execute_ready<S: Service<(), Response = (), Error = Infallible>>(service: &mut S) {
    let mut context = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(matches!(
        service.poll_ready(&mut context),
        Poll::Ready(Ok(()))
    ));
    let future = service.call(());
    let mut future = pin!(future);
    assert!(matches!(
        black_box(future.as_mut().poll(&mut context)),
        Poll::Ready(Ok(()))
    ));
}
fn passive_wrapper(c: &mut Criterion) {
    let registry = HealthRegistry::default();
    let health = benchmark_handle(&registry, "passive", 0);
    let mut raw = service_fn(|()| std::future::ready(Ok::<_, Infallible>(())));
    let layer = HealthLayer::new(health, classify);
    let mut observed = layer.layer(raw);
    let mut group = c.benchmark_group("health/passive_wrapper");
    group.bench_function("disabled", |b| b.iter(|| execute_ready(&mut raw)));
    group.bench_function("single_worker", |b| b.iter(|| execute_ready(&mut observed)));
    for enabled in [false, true] {
        group.bench_function(
            if enabled {
                "four_workers"
            } else {
                "four_workers_disabled"
            },
            |b| {
                b.iter_custom(|iterations| {
                    let barrier = Barrier::new(5);
                    let start = Instant::now();
                    std::thread::scope(|scope| {
                        let workers: Vec<_> = (0..4)
                            .map(|worker| {
                                let layer = &layer;
                                let barrier = &barrier;
                                scope.spawn(move || {
                                    let mut raw = service_fn(|()| {
                                        std::future::ready(Ok::<_, Infallible>(()))
                                    });
                                    let mut observed = layer.layer(raw);
                                    barrier.wait();
                                    for _ in
                                        0..(iterations / 4 + u64::from(worker < iterations % 4))
                                    {
                                        if enabled {
                                            execute_ready(&mut observed);
                                        } else {
                                            execute_ready(&mut raw);
                                        }
                                    }
                                })
                            })
                            .collect();
                        barrier.wait();
                        for worker in workers {
                            worker.join().unwrap();
                        }
                    });
                    start.elapsed()
                });
            },
        );
    }
    group.finish();
}
