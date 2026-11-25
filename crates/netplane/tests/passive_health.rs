//! Tower readiness, result preservation and cancellation boundaries.
use netplane::health::{
    BackendIdentity, HealthLayer, HealthOutcome, HealthPolicy, HealthRegistry, HealthState,
    OutcomeClassifier,
};
use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tower::{Layer, Service, ServiceExt};

#[derive(Clone)]
struct Classifier(Arc<AtomicUsize>);
impl OutcomeClassifier<usize, &'static str> for Classifier {
    fn classify(&self, result: Result<&usize, &&'static str>) -> HealthOutcome {
        self.0.fetch_add(1, Ordering::Relaxed);
        match result {
            Ok(_) => HealthOutcome::Success,
            Err(_) => HealthOutcome::Failure,
        }
    }
}
fn handle() -> Result<netplane::health::HealthHandle, netplane::health::HealthError> {
    HealthRegistry::default().register(
        BackendIdentity {
            service: "service".into(),
            instance: "one".into(),
            incarnation: "one".into(),
        },
        HealthPolicy {
            failure_threshold: NonZeroU32::MIN,
            recovery_threshold: NonZeroU32::MIN,
            recovery_delay: Duration::from_secs(10),
        },
    )
}

#[tokio::test]
async fn successful_and_failed_calls_report_once_and_preserve_results()
-> Result<(), Box<dyn std::error::Error>> {
    let handle = handle()?;
    let classified = Arc::new(AtomicUsize::new(0));
    let mut service = HealthLayer::new(handle.clone(), Classifier(Arc::clone(&classified))).layer(
        tower::service_fn(|request: bool| async move {
            if request {
                Ok(42)
            } else {
                Err("backend failed")
            }
        }),
    );
    assert_eq!(service.ready().await?.call(true).await, Ok(42));
    assert_eq!(classified.load(Ordering::Relaxed), 1);
    assert_eq!(
        service.ready().await?.call(false).await,
        Err("backend failed")
    );
    assert_eq!(classified.load(Ordering::Relaxed), 2);
    assert_eq!(handle.snapshot().state, HealthState::Unhealthy);
    Ok(())
}

#[tokio::test]
async fn cancellation_and_unpolled_execution_do_not_report()
-> Result<(), netplane::health::HealthError> {
    let handle = handle()?;
    let classified = Arc::new(AtomicUsize::new(0));
    let mut service = HealthLayer::new(handle.clone(), Classifier(Arc::clone(&classified))).layer(
        tower::service_fn(|(): ()| std::future::pending::<Result<usize, &'static str>>()),
    );
    drop(service.call(()));
    let mut executing = Box::pin(service.call(()));
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(std::future::Future::poll(executing.as_mut(), &mut cx).is_pending());
    drop(executing);
    assert_eq!(classified.load(Ordering::Relaxed), 0);
    assert_eq!(handle.snapshot().last_observation, None);
    Ok(())
}

struct ReadinessService {
    polls: Arc<AtomicUsize>,
}
impl Service<()> for ReadinessService {
    type Response = usize;
    type Error = &'static str;
    type Future = std::future::Ready<Result<usize, &'static str>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.polls.fetch_add(1, Ordering::Relaxed) == 0 {
            Poll::Pending
        } else {
            Poll::Ready(Err("readiness failed"))
        }
    }
    fn call(&mut self, (): ()) -> Self::Future {
        std::future::ready(Ok(7))
    }
}
#[test]
fn readiness_pending_and_errors_forward_without_observations()
-> Result<(), netplane::health::HealthError> {
    let handle = handle()?;
    let classified = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let mut service = HealthLayer::new(handle.clone(), Classifier(Arc::clone(&classified))).layer(
        ReadinessService {
            polls: Arc::clone(&polls),
        },
    );
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert_eq!(service.poll_ready(&mut cx), Poll::Pending);
    assert_eq!(
        service.poll_ready(&mut cx),
        Poll::Ready(Err("readiness failed"))
    );
    assert_eq!(polls.load(Ordering::Relaxed), 2);
    assert_eq!(classified.load(Ordering::Relaxed), 0);
    assert_eq!(handle.snapshot().last_observation, None);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn backend_timeout_is_observed_as_failure() -> Result<(), Box<dyn std::error::Error>> {
    let handle = handle()?;
    let classified = Arc::new(AtomicUsize::new(0));
    let mut service = HealthLayer::new(handle.clone(), Classifier(Arc::clone(&classified))).layer(
        tower::service_fn(|(): ()| async {
            tokio::time::timeout(Duration::from_secs(1), std::future::pending::<usize>())
                .await
                .map_err(|_| "timeout")
        }),
    );
    assert_eq!(service.ready().await?.call(()).await, Err("timeout"));
    assert_eq!(classified.load(Ordering::Relaxed), 1);
    assert!(!handle.is_eligible());
    Ok(())
}

#[tokio::test]
async fn runtime_admission_rejection_does_not_observe_backend_failure()
-> Result<(), tower::BoxError> {
    let handle = handle()?;
    let classified = Arc::new(AtomicUsize::new(0));
    let service = HealthLayer::new(handle.clone(), Classifier(Arc::clone(&classified))).layer(
        tower::service_fn(|(): ()| std::future::ready(Ok::<_, &'static str>(7))),
    );
    let graph = netplane::GraphDraft::new(service).validate()?;
    let mut runtime =
        netplane::Netplane::new(graph, netplane::RuntimeLimits { max_outstanding: 1 })?;
    let execution = runtime.handle().execute(());
    assert!(matches!(
        runtime.handle().try_execute(()),
        Err(netplane::AdmissionError::AtCapacity { .. })
    ));
    assert_eq!(classified.load(Ordering::Relaxed), 0);
    assert_eq!(handle.snapshot().last_observation, None);
    drop(execution);
    runtime.shutdown(Duration::from_secs(1)).await?;
    assert_eq!(classified.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn queued_execution_after_cooldown_cannot_supply_active_recovery_evidence()
-> Result<(), tower::BoxError> {
    use netplane::{Capacity, GraphBuilder, Netplane, RuntimeLimits, health::HealthMode};
    use tokio::sync::Notify;
    let registry = HealthRegistry::default();
    let health = registry.register_with_mode(
        BackendIdentity {
            service: "test".into(),
            instance: "queued".into(),
            incarnation: "one".into(),
        },
        HealthPolicy {
            failure_threshold: NonZeroU32::MIN,
            recovery_threshold: NonZeroU32::MIN,
            recovery_delay: Duration::from_secs(10),
        },
        HealthMode::Active,
    )?;
    health.report(health.begin_probe(), HealthOutcome::Success);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let selected = Arc::new(Notify::new());
    let backend_entered = entered.clone();
    let backend_release = release.clone();
    let selector_selected = selected.clone();
    let service = tower::service_fn(move |request: usize| {
        let entered = backend_entered.clone();
        let release = backend_release.clone();
        async move {
            if request == 1 {
                entered.notify_one();
                release.notified().await;
            }
            Ok::<_, &'static str>(request)
        }
    });
    let service =
        HealthLayer::new(health.clone(), Classifier(Arc::new(AtomicUsize::new(0)))).layer(service);
    let graph = GraphBuilder::new()
        .service_with_capacity(
            "a",
            service,
            Capacity {
                max_in_flight: 1,
                buffer_capacity: 2,
            },
        )
        .router("root", move |request: &usize| {
            if *request == 2 {
                selector_selected.notify_one();
            }
            "a"
        })
        .route("root", "a", "a")
        .fallback("root", "a")
        .entry("root")
        .without_outer_capacity()
        .validate()?;
    let mut runtime = Netplane::new(graph, RuntimeLimits { max_outstanding: 2 })?;
    let first = tokio::spawn(runtime.handle().execute(1));
    entered.notified().await;
    let queued = tokio::spawn(runtime.handle().execute(2));
    selected.notified().await;
    health.report(health.begin_observation(), HealthOutcome::Failure);
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(health.snapshot().state, HealthState::Recovering);
    release.notify_one();
    assert_eq!(first.await??, 1);
    assert_eq!(queued.await??, 2);
    let snapshot = health.snapshot();
    assert_eq!(snapshot.state, HealthState::Recovering);
    assert_eq!(snapshot.consecutive_successes, 0);
    assert!(!snapshot.eligible);
    health.report(health.begin_probe(), HealthOutcome::Success);
    assert!(health.is_eligible());
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
