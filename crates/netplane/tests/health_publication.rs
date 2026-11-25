//! Live health changes are independent of immutable publication and dispatch.
use netplane::{
    ControllerOptions, GraphBuilder, GraphVersion, HealthAwareRouting, Netplane,
    ReconfigurationController, RoutingError, RuntimeLimits, WeightedRouting,
    health::{BackendIdentity, HealthHandle, HealthOutcome, HealthPolicy, HealthRegistry},
};
use std::{convert::Infallible, num::NonZeroU32, sync::Arc, time::Duration};
use tokio::sync::Notify;
use tower::{BoxError, service_fn};

type TestResult = Result<(), BoxError>;
fn register(
    registry: &HealthRegistry,
    instance: &str,
    incarnation: &str,
) -> Result<HealthHandle, BoxError> {
    Ok(registry.register(
        BackendIdentity {
            service: "test".into(),
            instance: instance.into(),
            incarnation: incarnation.into(),
        },
        HealthPolicy {
            failure_threshold: NonZeroU32::MIN,
            recovery_threshold: NonZeroU32::MIN,
            recovery_delay: Duration::from_secs(10),
        },
    )?)
}
fn graph(
    a: &HealthHandle,
    b: &HealthHandle,
    buffered: bool,
    started: &Arc<Notify>,
    release: &Arc<Notify>,
) -> Result<GraphVersion<bool, &'static str>, BoxError> {
    let policy = HealthAwareRouting::try_new(
        WeightedRouting::try_new([("a", 100), ("b", 0)])?,
        [("a".into(), a.clone()), ("b".into(), b.clone())],
    )?;
    let started = started.clone();
    let release = release.clone();
    let mut builder = GraphBuilder::new()
        .service(
            "a",
            service_fn(move |wait: bool| {
                let started = started.clone();
                let release = release.clone();
                async move {
                    if wait {
                        started.notify_one();
                        release.notified().await;
                    }
                    Ok::<_, Infallible>("a")
                }
            }),
        )
        .service("b", service_fn(|_| async { Ok::<_, Infallible>("b") }))
        .try_router("router", move |_: &bool| {
            policy.select(b"key").map(str::to_owned)
        })
        .route("router", "a", "a")
        .route("router", "b", "b")
        .fallback("router", "b")
        .entry("router");
    if !buffered {
        builder = builder.without_outer_capacity();
    }
    Ok(builder.validate()?)
}
#[tokio::test(start_paused = true)]
async fn health_never_republishes_or_reroutes_dispatched_work() -> TestResult {
    for buffered in [false, true] {
        let registry = HealthRegistry::default();
        let a = register(&registry, "a", "old")?;
        let b = register(&registry, "b", "old")?;
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let runtime = Netplane::new(
            graph(&a, &b, buffered, &started, &release)?,
            RuntimeLimits { max_outstanding: 8 },
        )?;
        let control = ReconfigurationController::new(runtime, ControllerOptions::default());
        let initial = control.inspect().active.ok_or("missing publication")?.id;
        let handle = control.handle();
        let dispatched = tokio::spawn(handle.execute(true));
        started.notified().await;
        let unpolled = handle.execute(false);
        a.report(a.begin_observation(), HealthOutcome::Failure);
        assert_eq!(
            control.inspect().active.ok_or("missing publication")?.id,
            initial
        );
        let error = unpolled
            .await
            .err()
            .ok_or("unhealthy route unexpectedly dispatched")?;
        assert_eq!(
            error.downcast_ref::<RoutingError>(),
            Some(&RoutingError::NoEligibleBackends)
        );
        // Republishing unchanged handles does not reset local health.
        control.publish_if_current(
            control.current_token().ok_or("missing token")?,
            graph(&a, &b, buffered, &started, &release)?,
        )?;
        assert!(handle.execute(false).await.is_err());
        let replacement = register(&registry, "a", "new")?;
        assert!(replacement.is_eligible());
        control.publish_if_current(
            control.current_token().ok_or("missing token")?,
            graph(&replacement, &b, buffered, &started, &release)?,
        )?;
        assert_eq!(handle.execute(false).await?, "a");
        control.rollback_if_current(control.current_token().ok_or("missing token")?, initial)?;
        assert!(handle.execute(false).await.is_err());
        release.notify_one();
        assert_eq!(dispatched.await??, "a");
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(a.is_eligible());
        a.report(a.begin_observation(), HealthOutcome::Success);
        assert_eq!(handle.execute(false).await?, "a");
        assert_eq!(handle.status().outstanding, 0);
        control.shutdown(Duration::from_secs(1)).await?;
    }
    Ok(())
}
