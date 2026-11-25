//! Active probes have a separate bounded task budget and explicit ownership.
use netplane::health::{
    ActiveHealthConfig, BackendIdentity, HealthHandle, HealthMode, HealthOutcome, HealthPolicy,
    HealthProbe, HealthRegistry, HealthScheduler, HealthState, ProbeError,
};
use std::{
    future::{Future, pending},
    num::{NonZeroU32, NonZeroUsize},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tower::BoxError;

type TestResult = Result<(), BoxError>;
#[derive(Clone)]
struct Probe {
    calls: Arc<AtomicUsize>,
    outcome: Option<HealthOutcome>,
}
impl HealthProbe for Probe {
    fn identity(&self) -> netplane::health::ProbeIdentity {
        netplane::health::ProbeIdentity {
            configuration_key: format!("fixture:{:?}", self.outcome),
        }
    }
    type Future = Pin<Box<dyn Future<Output = HealthOutcome> + Send>>;
    fn check(&self) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let outcome = self.outcome;
        Box::pin(async move {
            match outcome {
                Some(outcome) => outcome,
                None => pending().await,
            }
        })
    }
}
fn config() -> ActiveHealthConfig {
    ActiveHealthConfig {
        interval: Duration::from_secs(5),
        timeout: Duration::from_secs(2),
        max_concurrent_checks: NonZeroUsize::MIN,
        max_registered_backends: NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
    }
}
fn backend(registry: &HealthRegistry, instance: &str) -> Result<HealthHandle, BoxError> {
    Ok(registry.register_with_mode(
        BackendIdentity {
            service: "test".into(),
            instance: instance.into(),
            incarnation: "one".into(),
        },
        HealthPolicy {
            failure_threshold: NonZeroU32::MIN,
            recovery_threshold: NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
            recovery_delay: Duration::from_secs(10),
        },
        HealthMode::Active,
    )?)
}
async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn active_success_requires_recovery_evidence_and_idle_backend_recovers() -> TestResult {
    let registry = HealthRegistry::default();
    let health = backend(&registry, "a")?;
    let scheduler = HealthScheduler::new(config())?;
    let control = scheduler.handle();
    let calls = Arc::new(AtomicUsize::new(0));
    let _registration = control.register(
        health.clone(),
        Probe {
            calls: Arc::clone(&calls),
            outcome: Some(HealthOutcome::Success),
        },
    )?;
    let task = tokio::spawn(scheduler.run());
    settle().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(health.snapshot().state, HealthState::Recovering);
    assert!(!health.is_eligible());
    tokio::time::advance(Duration::from_secs(5)).await;
    settle().await;
    assert!(health.is_eligible());
    health.report(health.begin_observation(), HealthOutcome::Failure);
    assert!(!health.is_eligible());
    tokio::time::advance(Duration::from_secs(10)).await;
    settle().await;
    assert!(!health.is_eligible());
    tokio::time::advance(Duration::from_secs(5)).await;
    settle().await;
    assert!(health.is_eligible());
    control.shutdown();
    task.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn slow_checks_bound_global_work_skip_backlog_and_cancel_on_removal() -> TestResult {
    let registry = HealthRegistry::default();
    let a = backend(&registry, "a")?;
    let b = backend(&registry, "b")?;
    let mut limits = config();
    limits.timeout = Duration::from_secs(100);
    let scheduler = HealthScheduler::new(limits)?;
    let control = scheduler.handle();
    let a_calls = Arc::new(AtomicUsize::new(0));
    let b_calls = Arc::new(AtomicUsize::new(0));
    let registration = control.register(
        a.clone(),
        Probe {
            calls: Arc::clone(&a_calls),
            outcome: None,
        },
    )?;
    let _other = control.register(
        b,
        Probe {
            calls: Arc::clone(&b_calls),
            outcome: None,
        },
    )?;
    let task = tokio::spawn(scheduler.run());
    settle().await;
    assert_eq!(a_calls.load(Ordering::SeqCst), 1);
    assert_eq!(b_calls.load(Ordering::SeqCst), 0);
    assert_eq!(registry.snapshot().summary.active_probes, 1);
    tokio::time::advance(Duration::from_secs(30)).await;
    settle().await;
    assert_eq!(a_calls.load(Ordering::SeqCst), 1);
    drop(registration);
    settle().await;
    assert_eq!(b_calls.load(Ordering::SeqCst), 1);
    assert!(a.snapshot().last_observation.is_none());
    assert_eq!(registry.snapshot().summary.active_probes, 1);
    control.shutdown();
    task.await?;
    assert_eq!(registry.snapshot().summary.active_probes, 0);
    assert!(matches!(
        control.register(
            a,
            Probe {
                calls: a_calls,
                outcome: None
            }
        ),
        Err(ProbeError::Closed)
    ));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn timeout_is_failure_and_unhealthy_backend_does_not_overlap() -> TestResult {
    let registry = HealthRegistry::default();
    let health = backend(&registry, "a")?;
    let scheduler = HealthScheduler::new(config())?;
    let control = scheduler.handle();
    let calls = Arc::new(AtomicUsize::new(0));
    let _registration = control.register(
        health.clone(),
        Probe {
            calls: Arc::clone(&calls),
            outcome: None,
        },
    )?;
    let task = tokio::spawn(scheduler.run());
    settle().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    settle().await;
    assert_eq!(health.snapshot().state, HealthState::Unhealthy);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(registry.snapshot().summary.active_probes, 0);
    control.shutdown();
    task.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn registration_budget_shared_lifetimes_and_scheduler_drop() -> TestResult {
    let registry = HealthRegistry::default();
    let a = backend(&registry, "a")?;
    let b = backend(&registry, "b")?;
    let mut limits = config();
    limits.max_registered_backends = NonZeroUsize::MIN;
    let scheduler = HealthScheduler::new(limits)?;
    let control = scheduler.handle();
    let calls = Arc::new(AtomicUsize::new(0));
    let probe = Probe {
        calls: Arc::clone(&calls),
        outcome: Some(HealthOutcome::Success),
    };
    let registration = control.register(a.clone(), probe.clone())?;
    let retained = control.register(a, probe.clone())?;
    assert!(matches!(
        control.register(b.clone(), probe.clone()),
        Err(ProbeError::AtCapacity)
    ));
    drop(registration);
    assert!(matches!(
        control.register(b.clone(), probe.clone()),
        Err(ProbeError::AtCapacity)
    ));
    drop(retained);
    let _registration = control.register(b, probe)?;
    drop(scheduler);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn invalid_schedule_is_rejected_without_starting_tasks() {
    let mut limits = config();
    limits.interval = Duration::ZERO;
    assert!(matches!(
        HealthScheduler::new(limits),
        Err(ProbeError::InvalidDuration)
    ));
    limits = config();
    limits.timeout = Duration::MAX;
    assert!(matches!(
        HealthScheduler::new(limits),
        Err(ProbeError::InvalidDuration)
    ));
}

#[tokio::test(start_paused = true)]
async fn many_backends_share_strict_parallel_budget_and_make_progress() -> TestResult {
    let registry = HealthRegistry::default();
    let mut limits = config();
    limits.max_concurrent_checks = NonZeroUsize::new(3).unwrap_or(NonZeroUsize::MIN);
    let scheduler = HealthScheduler::new(limits)?;
    let control = scheduler.handle();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registrations = Vec::new();
    for index in 0..8 {
        registrations.push(control.register(
            backend(&registry, &index.to_string())?,
            Probe {
                calls: Arc::clone(&calls),
                outcome: None,
            },
        )?);
    }
    assert_eq!(registrations.len(), 8);
    let task = tokio::spawn(scheduler.run());
    settle().await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(registry.snapshot().summary.active_probes, 3);
    tokio::time::advance(Duration::from_secs(2)).await;
    settle().await;
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    assert_eq!(registry.snapshot().summary.active_probes, 3);
    tokio::time::advance(Duration::from_secs(2)).await;
    settle().await;
    assert_eq!(calls.load(Ordering::SeqCst), 8);
    assert_eq!(registry.snapshot().summary.active_probes, 2);
    control.shutdown();
    task.await?;
    assert_eq!(registry.snapshot().summary.active_probes, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dropping_scheduler_future_aborts_checks_and_closes_registration() -> TestResult {
    let registry = HealthRegistry::default();
    let health = backend(&registry, "a")?;
    let scheduler = HealthScheduler::new(config())?;
    let control = scheduler.handle();
    let calls = Arc::new(AtomicUsize::new(0));
    let _registration = control.register(
        health.clone(),
        Probe {
            calls: Arc::clone(&calls),
            outcome: None,
        },
    )?;
    let task = tokio::spawn(scheduler.run());
    settle().await;
    assert_eq!(registry.snapshot().summary.active_probes, 1);
    task.abort();
    assert!(task.await.is_err());
    settle().await;
    assert_eq!(registry.snapshot().summary.active_probes, 0);
    assert!(health.snapshot().last_observation.is_none());
    assert!(matches!(
        control.register(
            health,
            Probe {
                calls,
                outcome: None
            }
        ),
        Err(ProbeError::Closed)
    ));
    Ok(())
}

#[derive(Clone)]
struct CountedProbe {
    current: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}
struct ActiveCheck(Arc<AtomicUsize>);
impl Drop for ActiveCheck {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl HealthProbe for CountedProbe {
    fn identity(&self) -> netplane::health::ProbeIdentity {
        netplane::health::ProbeIdentity {
            configuration_key: "counted-v1".into(),
        }
    }
    type Future = Pin<Box<dyn Future<Output = HealthOutcome> + Send>>;
    fn check(&self) -> Self::Future {
        let active = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let guard = ActiveCheck(Arc::clone(&self.current));
        Box::pin(async move {
            let _guard = guard;
            pending().await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn immediate_reregistration_never_overlaps_cancelled_probe() -> TestResult {
    let registry = HealthRegistry::default();
    let health = backend(&registry, "a")?;
    let mut limits = config();
    limits.max_concurrent_checks = NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN);
    let scheduler = HealthScheduler::new(limits)?;
    let control = scheduler.handle();
    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let probe = CountedProbe {
        current: Arc::clone(&current),
        peak: Arc::clone(&peak),
    };
    let task = tokio::spawn(scheduler.run());
    for _ in 0..1000 {
        let registration = control.register(health.clone(), probe.clone())?;
        settle().await;
        drop(registration);
    }
    control.shutdown();
    task.await?;
    assert_eq!(current.load(Ordering::SeqCst), 0);
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert!(health.snapshot().last_observation.is_none());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn status_distinguishes_registrations_from_cancelled_probe_futures() -> TestResult {
    let registry = HealthRegistry::default();
    let health = backend(&registry, "status")?;
    let scheduler = HealthScheduler::new(config())?;
    let handle = scheduler.handle();
    let initial = handle.status();
    assert_eq!(initial.registered_backends, 0);
    assert_eq!(initial.running_checks, 0);
    assert_eq!(initial.max_registered_backends, 8);
    assert_eq!(initial.max_concurrent_checks, 1);
    let registration = handle.register(
        health,
        Probe {
            calls: Arc::new(AtomicUsize::new(0)),
            outcome: None,
        },
    )?;
    let duplicate = registration.clone();
    assert_eq!(handle.status().registered_backends, 1);
    assert_eq!(handle.status().running_checks, 0);
    let task = tokio::spawn(scheduler.run());
    settle().await;
    assert_eq!(handle.status().running_checks, 1);
    drop(duplicate);
    assert_eq!(handle.status().registered_backends, 1);
    drop(registration);
    // Cancellation releases membership now, but not a still-owned probe future.
    assert_eq!(handle.status().registered_backends, 0);
    assert_eq!(handle.status().running_checks, 1);
    settle().await;
    assert_eq!(handle.status().running_checks, 0);
    handle.shutdown();
    task.await?;
    assert_eq!(handle.status().running_checks, 0);
    Ok(())
}
