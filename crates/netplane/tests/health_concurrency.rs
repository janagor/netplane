//! Concurrent health observations, publication, admission and shutdown.
use netplane::{
    AdmissionError, Capacity, GraphBuilder, GraphVersion, HealthAwareRouting, Netplane,
    PublishError, RoutingError, RuntimeLimits, WeightedRouting,
    health::{
        BackendIdentity, HealthHandle, HealthMode, HealthOutcome, HealthPolicy, HealthRegistry,
    },
};
use std::{
    convert::Infallible,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Barrier;
use tower::{BoxError, service_fn};

fn register(registry: &HealthRegistry, instance: &str) -> Result<HealthHandle, BoxError> {
    Ok(registry.register_with_mode(
        BackendIdentity {
            service: "test".into(),
            instance: instance.into(),
            incarnation: "one".into(),
        },
        HealthPolicy {
            failure_threshold: NonZeroU32::MIN,
            recovery_threshold: NonZeroU32::MIN,
            recovery_delay: Duration::ZERO,
        },
        HealthMode::Active,
    )?)
}

fn graph(
    handles: &[HealthHandle; 2],
    version: usize,
    buffered: bool,
    peaks: &mut Vec<Arc<AtomicUsize>>,
) -> Result<GraphVersion<u64, (usize, usize)>, BoxError> {
    let policy = HealthAwareRouting::try_new(
        WeightedRouting::try_new([("a", 60), ("b", 40)])?,
        [
            ("a".into(), handles[0].clone()),
            ("b".into(), handles[1].clone()),
        ],
    )?;
    let mut builder = GraphBuilder::new();
    for (backend, name) in ["a", "b"].into_iter().enumerate() {
        let current = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        peaks.push(Arc::clone(&peak));
        builder = builder.service(
            name,
            service_fn(move |_: u64| {
                let count = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                let current = Arc::clone(&current);
                async move {
                    tokio::task::yield_now().await;
                    current.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, Infallible>((version, backend))
                }
            }),
        );
    }
    builder = builder
        .try_router("router", move |key: &u64| {
            policy.select(&key.to_le_bytes()).map(str::to_owned)
        })
        .route("router", "a", "a")
        .route("router", "b", "b")
        .fallback("router", "b")
        .entry("router")
        .backend_capacity(Capacity {
            max_in_flight: 2,
            buffer_capacity: 2,
        });
    if !buffered {
        builder = builder.without_outer_capacity();
    }
    Ok(builder.validate()?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_execution_survives_concurrent_health_publication_and_shutdown()
-> Result<(), BoxError> {
    tokio::time::timeout(Duration::from_secs(10), async {
        for buffered in [false, true] {
            let registry = HealthRegistry::default();
            let handles = [register(&registry, "a")?, register(&registry, "b")?];
            let mut peaks = Vec::new();
            let mut runtime = Netplane::new(
                graph(&handles, 0, buffered, &mut peaks)?,
                RuntimeLimits {
                    max_outstanding: 16,
                },
            )?;
            let execution = runtime.handle();
            let start = Arc::new(Barrier::new(8));
            let mut tasks = Vec::new();
            for worker in 0..4_u64 {
                let execution = execution.clone();
                let start = Arc::clone(&start);
                tasks.push(tokio::spawn(async move {
                    start.wait().await;
                    for key in 0..200_u64 {
                        assert!(execution.status().outstanding <= 16);
                        match execution.try_execute(worker * 200 + key) {
                            Ok(future) => match future.await {
                                Ok((version, backend)) => {
                                    assert!(version < 40);
                                    assert!(backend < 2);
                                }
                                Err(error) => assert_eq!(
                                    error.downcast_ref::<RoutingError>(),
                                    Some(&RoutingError::NoEligibleBackends)
                                ),
                            },
                            Err(AdmissionError::AtCapacity { .. } | AdmissionError::Closed) => {}
                        }
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, BoxError>(())
                }));
            }
            for handle in &handles {
                let handle = handle.clone();
                let start = Arc::clone(&start);
                tasks.push(tokio::spawn(async move {
                    start.wait().await;
                    for _ in 0..200 {
                        handle.report(handle.begin_observation(), HealthOutcome::Success);
                        tokio::task::yield_now().await;
                        handle.report(handle.begin_observation(), HealthOutcome::Failure);
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, BoxError>(())
                }));
            }
            let churn_registry = registry.clone();
            let churn_start = Arc::clone(&start);
            tasks.push(tokio::spawn(async move {
                churn_start.wait().await;
                for index in 0..200 {
                    let transient = register(&churn_registry, &format!("transient-{index}"))?;
                    assert!(churn_registry.snapshot().summary.tracked_backends <= 3);
                    drop(transient);
                    tokio::task::yield_now().await;
                }
                Ok::<_, BoxError>(())
            }));
            start.wait().await;
            for version in 1..40 {
                runtime.publish(graph(&handles, version, buffered, &mut peaks)?)?;
                tokio::task::yield_now().await;
            }
            // Shutdown races with both request producers and health observations.
            runtime.shutdown(Duration::from_secs(2)).await?;
            assert!(matches!(
                runtime.publish(graph(&handles, 40, buffered, &mut peaks)?),
                Err(PublishError::ShuttingDown)
            ));
            for task in tasks {
                task.await??;
            }
            assert_eq!(execution.status().outstanding, 0);
            assert!(peaks.iter().all(|peak| peak.load(Ordering::SeqCst) <= 2));
            assert!(registry.snapshot().summary.health_transitions > 0);
            drop(handles);
            tokio::time::timeout(Duration::from_secs(1), async {
                while registry.snapshot().summary.tracked_backends != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
        }
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}
