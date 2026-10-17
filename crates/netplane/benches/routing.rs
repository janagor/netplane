#![expect(
    missing_docs,
    reason = "Criterion generates public benchmark entrypoints"
)]
#![expect(
    clippy::unwrap_used,
    reason = "Invalid benchmark fixtures must fail immediately"
)]
pub mod common;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use netplane::{Netplane, RuntimeLimits};
use tower::{Service, ServiceExt};

fn routing(c: &mut Criterion) {
    let runtime = common::runtime();
    let mut group = c.benchmark_group("routing/requests");
    for routes in [1, 2, 4, 8, 16, 32, 64] {
        for (label, dynamic) in [
            ("declared_borrowed", false),
            ("fallback_borrowed", false),
            ("declared_allocated", true),
        ] {
            let graph = runtime.block_on(async {
                Netplane::new(
                    common::routed(routes, dynamic),
                    RuntimeLimits {
                        max_outstanding: 256,
                    },
                )
                .unwrap()
            });
            let handle = graph.handle();
            runtime.block_on(common::warm(&handle));
            let request = if label == "fallback_borrowed" {
                u64::MAX
            } else {
                (routes - 1) as u64
            };
            group.bench_with_input(BenchmarkId::new(label, routes), &request, |b, &request| {
                b.to_async(&runtime).iter(|| async {
                    assert_eq!(
                        black_box(handle.execute(black_box(request)).await.unwrap()),
                        request.wrapping_add(1)
                    );
                });
            });
        }

        let mut tower_router =
            runtime.block_on(async { common::tower_double_buffered_router(routes) });
        runtime.block_on(async {
            for value in 0..100 {
                assert_eq!(
                    tower_router
                        .ready()
                        .await
                        .unwrap()
                        .call(value)
                        .await
                        .unwrap(),
                    value + 1
                );
            }
        });
        let request = (routes - 1) as u64;
        group.bench_with_input(
            BenchmarkId::new("tower_double_buffered", routes),
            &request,
            |b, &request| {
                b.to_async(&runtime).iter(|| {
                    let mut tower_router = tower_router.clone();
                    async move {
                        assert_eq!(
                            black_box(
                                tower_router
                                    .ready()
                                    .await
                                    .unwrap()
                                    .call(black_box(request))
                                    .await
                                    .unwrap()
                            ),
                            request.wrapping_add(1)
                        );
                    }
                });
            },
        );
    }
    group.finish();
}

fn route_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("routing/label_lookup");
    for routes in [1, 2, 4, 8, 16, 32, 64] {
        let table = common::route_table(routes);
        group.bench_with_input(
            BenchmarkId::from_parameter(routes),
            &routes,
            |b, &routes| {
                b.iter(|| {
                    let request = black_box((routes - 1) as u64);
                    let label = common::borrowed_route_label(request, routes);
                    black_box(table.get(label.as_ref()));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, routing, route_lookup);
criterion_main!(benches);
