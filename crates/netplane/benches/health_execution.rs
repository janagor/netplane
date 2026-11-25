#![expect(
    missing_docs,
    reason = "Criterion generates public benchmark entrypoints"
)]
#![expect(
    clippy::unwrap_used,
    reason = "Invalid benchmark fixtures must fail immediately"
)]
pub mod common;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures_util::future::join_all;
use netplane::{
    GraphBuilder, Netplane, RuntimeLimits,
    health::{
        BackendIdentity, HealthHandle, HealthLayer, HealthOutcome, HealthPolicy, HealthRegistry,
    },
    routing::UniformRouting,
};
use std::{
    convert::Infallible,
    num::NonZeroU32,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tower::Layer;

#[derive(Clone, Copy)]
enum Scenario {
    Ordinary,
    Health,
    Passive,
    HalfUnhealthy,
    Changing,
}
impl Scenario {
    const fn name(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Health => "health_only",
            Self::Passive => "passive",
            Self::HalfUnhealthy => "half_unhealthy",
            Self::Changing => "changing",
        }
    }
}
fn fixture(
    count: usize,
    buffered: bool,
    scenario: Scenario,
) -> (Netplane<u64, u64>, Vec<HealthHandle>) {
    let registry = HealthRegistry::default();
    let handles: Vec<_> = (0..count)
        .map(|index| {
            registry
                .register(
                    BackendIdentity {
                        service: "benchmark".into(),
                        instance: index.to_string(),
                        incarnation: "one".into(),
                    },
                    HealthPolicy {
                        failure_threshold: NonZeroU32::MIN,
                        recovery_threshold: NonZeroU32::MIN,
                        recovery_delay: if matches!(scenario, Scenario::Changing) {
                            Duration::ZERO
                        } else {
                            Duration::from_secs(3600)
                        },
                    },
                )
                .unwrap()
        })
        .collect();
    let names: Vec<_> = (0..count).map(|index| format!("backend-{index}")).collect();
    let policy = UniformRouting::try_new(names.clone()).unwrap();
    let eligibility = handles.clone();
    let mut builder = GraphBuilder::new()
        .try_router("router", move |request: &u64| {
            let key = request.to_le_bytes();
            if matches!(scenario, Scenario::Ordinary) {
                return Ok(policy.select(&key).to_owned());
            }
            let mut index = 0;
            policy
                .select_eligible(&key, |_| {
                    let eligible = eligibility[index].is_eligible();
                    index += 1;
                    eligible
                })
                .map(str::to_owned)
        })
        .entry("router")
        .backend_capacity(common::CAPACITY)
        .fallback("router", names[0].clone());
    if buffered {
        builder = builder.capacity(common::CAPACITY);
    } else {
        builder = builder.without_outer_capacity();
    }
    for (name, health) in names.into_iter().zip(&handles) {
        builder = if matches!(scenario, Scenario::Passive | Scenario::Changing) {
            builder.service(
                name.clone(),
                HealthLayer::new(health.clone(), classify).layer(common::service()),
            )
        } else {
            builder.service(name.clone(), common::service())
        }
        .route("router", name.clone(), name);
    }
    if matches!(scenario, Scenario::HalfUnhealthy) {
        for handle in handles.iter().step_by(2) {
            handle.report(handle.begin_observation(), HealthOutcome::Failure);
        }
    }
    (
        Netplane::new(
            builder.validate().unwrap(),
            RuntimeLimits {
                max_outstanding: 512,
            },
        )
        .unwrap(),
        handles,
    )
}
const fn classify(_: Result<&u64, &Infallible>) -> HealthOutcome {
    HealthOutcome::Success
}

fn execution(c: &mut Criterion) {
    let runtime = common::runtime();
    for buffered in [false, true] {
        let mode = if buffered { "buffered" } else { "backend_only" };
        let mut group = c.benchmark_group(format!("health_execution/{mode}"));
        group.throughput(Throughput::Elements(1));
        for count in [2, 8, 32, 128] {
            for scenario in [
                Scenario::Ordinary,
                Scenario::Health,
                Scenario::Passive,
                Scenario::HalfUnhealthy,
            ] {
                let (graph, _handles) =
                    runtime.block_on(async { fixture(count, buffered, scenario) });
                let handle = graph.handle();
                runtime.block_on(common::warm(&handle));
                let mut key = 0_u64;
                group.bench_with_input(BenchmarkId::new(scenario.name(), count), &count, |b, _| {
                    b.to_async(&runtime).iter(|| {
                        key = key.wrapping_add(1);
                        let request = key;
                        let handle = &handle;
                        async move {
                            assert_eq!(
                                black_box(handle.execute(black_box(request)).await.unwrap()),
                                request + 1
                            );
                        }
                    });
                });
            }
        }
        group.finish();
    }
}

fn concurrent_changes(c: &mut Criterion) {
    const BATCH: u64 = 16;
    let runtime = common::runtime();
    let mut group = c.benchmark_group("health_execution/concurrent_changes");
    group.throughput(Throughput::Elements(BATCH));
    for buffered in [false, true] {
        let mode = if buffered { "buffered" } else { "backend_only" };
        for count in [2, 8, 32, 128] {
            for scenario in [Scenario::Ordinary, Scenario::Changing] {
                let (graph, handles) =
                    runtime.block_on(async { fixture(count, buffered, scenario) });
                let handle = graph.handle();
                runtime.block_on(common::warm(&handle));
                // Keep one healthy backend while the others fail and recover.
                let changing_handles = &handles[1..];
                let stop = AtomicBool::new(false);
                std::thread::scope(|scope| {
                    let stop_ref = &stop;
                    let worker = matches!(scenario, Scenario::Changing).then(|| {
                        scope.spawn(move || {
                            while !stop_ref.load(Ordering::Relaxed) {
                                for health in changing_handles {
                                    health
                                        .report(health.begin_observation(), HealthOutcome::Failure);
                                    health
                                        .report(health.begin_observation(), HealthOutcome::Success);
                                }
                                std::thread::yield_now();
                            }
                        })
                    });
                    group.bench_with_input(
                        BenchmarkId::new(format!("{mode}/{}", scenario.name()), count),
                        &count,
                        |b, _| {
                            b.to_async(&runtime).iter(|| async {
                                let futures =
                                    (0..BATCH).map(|request| handle.execute(black_box(request)));
                                for response in join_all(futures).await {
                                    black_box(response.unwrap());
                                }
                            });
                        },
                    );
                    stop.store(true, Ordering::Relaxed);
                    if let Some(worker) = worker {
                        worker.join().unwrap();
                    }
                });
            }
        }
    }
    group.finish();
}
criterion_group!(benches, execution, concurrent_changes);
criterion_main!(benches);
