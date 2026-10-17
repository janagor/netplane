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
pub mod common;

use std::{convert::Infallible, time::Duration};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use netplane::{Capacity, Endpoint, GraphDraft, Netplane, RuntimeLimits};
use tower::{Service, ServiceBuilder, ServiceExt, service_fn};

fn netplane_from_draft(
    runtime: &tokio::runtime::Runtime,
    draft: GraphDraft<
        impl tower::Service<
            u64,
            Response = u64,
            Error = Infallible,
            Future: std::future::Future<Output = Result<u64, Infallible>> + Send,
        > + Send
        + 'static,
    >,
) -> Netplane<u64, u64> {
    runtime.block_on(async {
        Netplane::new(
            draft.capacity(common::CAPACITY).validate().unwrap(),
            RuntimeLimits {
                max_outstanding: 256,
            },
        )
        .unwrap()
    })
}

async fn warm_tower(
    service: &mut (impl Service<u64, Response = u64, Error = tower::BoxError> + Clone),
) {
    for value in 0..100 {
        assert_eq!(
            service.ready().await.unwrap().call(value).await.unwrap(),
            value + 1
        );
    }
}

fn execution(c: &mut Criterion) {
    let runtime = common::runtime();
    let endpoint =
        runtime.block_on(async { Endpoint::new(common::service(), common::CAPACITY).unwrap() });
    let graph = netplane_from_draft(&runtime, GraphDraft::new(common::service()));
    let routed = runtime.block_on(async {
        Netplane::new(
            common::routed(1, false),
            RuntimeLimits {
                max_outstanding: 256,
            },
        )
        .unwrap()
    });
    let mut tower_router = runtime.block_on(async { common::tower_double_buffered_router(1) });
    let yielding = netplane_from_draft(&runtime, GraphDraft::new(common::yielding_service()));
    runtime.block_on(common::warm(&graph.handle()));
    runtime.block_on(common::warm(&routed.handle()));
    runtime.block_on(common::warm(&yielding.handle()));
    runtime.block_on(warm_tower(&mut tower_router));

    let mut group = c.benchmark_group("execution/serial");
    group.bench_function("tower_direct", |b| {
        b.to_async(&runtime).iter(|| async {
            let value = common::service().oneshot(black_box(7)).await.unwrap();
            assert_eq!(black_box(value), 8);
        })
    });
    let bounded = runtime.block_on(async {
        ServiceBuilder::new()
            .buffer(128)
            .concurrency_limit(64)
            .service(common::service())
    });
    group.bench_function("tower_bounded", |b| {
        b.to_async(&runtime).iter(|| async {
            let value = bounded.clone().oneshot(black_box(7)).await.unwrap();
            assert_eq!(black_box(value), 8);
        })
    });
    group.bench_function("endpoint", |b| {
        b.to_async(&runtime).iter(|| async {
            let value = endpoint.execute(black_box(7)).await.unwrap();
            assert_eq!(black_box(value), 8);
        })
    });
    let handle = graph.handle();
    group.bench_function("graph_draft", |b| {
        b.to_async(&runtime).iter(|| async {
            let value = handle.execute(black_box(7)).await.unwrap();
            assert_eq!(black_box(value), 8);
        })
    });
    let routed_handle = routed.handle();
    group.bench_function("graph_builder_one_route", |b| {
        b.to_async(&runtime).iter(|| async {
            let value = routed_handle.execute(black_box(7)).await.unwrap();
            assert_eq!(black_box(value), 8);
        })
    });
    group.bench_function("tower_double_buffered_one_route", |b| {
        b.to_async(&runtime).iter(|| {
            let mut tower_router = tower_router.clone();
            async move {
                let value = tower_router
                    .ready()
                    .await
                    .unwrap()
                    .call(black_box(7))
                    .await
                    .unwrap();
                assert_eq!(black_box(value), 8);
            }
        })
    });
    let yielding_handle = yielding.handle();
    group.bench_function("graph_draft_yield_once", |b| {
        b.to_async(&runtime).iter(|| async {
            let value = yielding_handle.execute(black_box(7)).await.unwrap();
            assert_eq!(black_box(value), 8);
        })
    });
    group.finish();
}

fn admission(c: &mut Criterion) {
    let runtime = common::runtime();
    let graph = runtime.block_on(async { common::netplane(256) });
    let handle = graph.handle();
    let mut admission = c.benchmark_group("admission");
    admission.bench_function("success_unpolled", |b| {
        b.iter(|| {
            let future = handle.try_execute(black_box(7)).unwrap();
            black_box(&future);
            drop(future);
        })
    });
    let full = runtime.block_on(async { common::netplane(1) });
    let full_handle = full.handle();
    let held = full_handle.try_execute(0).unwrap();
    admission.bench_function("rejected_at_capacity", |b| {
        b.iter(|| {
            assert!(matches!(
                full_handle.try_execute(black_box(7)),
                Err(netplane::AdmissionError::AtCapacity { .. })
            ));
        })
    });
    drop(held);
    let waiting = runtime.block_on(async {
        let version = GraphDraft::new(service_fn(|value: u64| async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            Ok::<_, Infallible>(value + 1)
        }))
        .capacity(Capacity {
            max_in_flight: 1,
            buffer_capacity: 1,
        })
        .validate()
        .unwrap();
        Netplane::new(version, RuntimeLimits { max_outstanding: 4 }).unwrap()
    });
    let waiting_handle = waiting.handle();
    admission.bench_function("accepted_waiting_capacity_two_completions", |b| {
        b.to_async(&runtime).iter(|| async {
            let (first, second) =
                tokio::join!(waiting_handle.execute(1), waiting_handle.execute(2));
            assert_eq!(first.unwrap(), 2);
            assert_eq!(second.unwrap(), 3);
        });
    });
    admission.finish();
}

criterion_group!(benches, execution, admission);
criterion_main!(benches);
