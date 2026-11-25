use super::*;
use crate::health::{HealthPolicy, HealthRegistry, HealthState};
use std::{
    future::{Ready, ready},
    num::NonZeroU32,
    sync::mpsc,
};
use tower::BoxError;

struct Probe(&'static str);
impl HealthProbe for Probe {
    type Future = Ready<HealthOutcome>;
    fn identity(&self) -> ProbeIdentity {
        ProbeIdentity {
            configuration_key: self.0.into(),
        }
    }
    fn check(&self) -> Self::Future {
        ready(HealthOutcome::Success)
    }
}
fn fixture() -> Result<(HealthScheduler, HealthHandle), BoxError> {
    Ok((
        HealthScheduler::new(ActiveHealthConfig {
            interval: Duration::from_secs(1),
            timeout: Duration::from_secs(1),
            max_concurrent_checks: NonZeroUsize::MIN,
            max_registered_backends: NonZeroUsize::MIN,
        })?,
        HealthRegistry::default().register_with_mode(
            BackendIdentity {
                service: "service".into(),
                instance: "endpoint".into(),
                incarnation: "pod".into(),
            },
            HealthPolicy {
                failure_threshold: NonZeroU32::MIN,
                recovery_threshold: NonZeroU32::MIN,
                recovery_delay: Duration::from_secs(60),
            },
            HealthMode::Active,
        )?,
    ))
}
fn completed(scheduler: &HealthScheduler, health: &HealthHandle) -> Completed {
    let registrations = scheduler
        .shared
        .registrations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Completed {
        id: *registrations.entries.keys().next().unwrap_or(&u64::MAX),
        token: health.begin_probe(),
        outcome: HealthOutcome::Success,
    }
}

#[test]
fn accepted_completion_updates_before_replacement_even_when_callbacks_are_delayed()
-> Result<(), BoxError> {
    let (scheduler, health) = fixture()?;
    let handle = scheduler.handle();
    let old = handle.register(health.clone(), Probe("old"))?;
    let completed = completed(&scheduler, &health);
    std::thread::scope(|scope| -> Result<(), BoxError> {
        let (accepted, wait_accepted) = mpsc::sync_channel(1);
        let (replaced, wait_replaced) = mpsc::sync_channel(1);
        let scheduler = &scheduler;
        let worker = scope.spawn(move || {
            let events = scheduler.accept_completed(Ok(completed));
            if accepted.send(()).is_err()
                || wait_replaced.recv_timeout(Duration::from_secs(10)).is_err()
            {
                return;
            }
            if let Some(events) = events {
                events.emit();
            }
        });
        wait_accepted.recv_timeout(Duration::from_secs(10))?;
        // Acceptance has already committed before deregistration can return.
        assert_eq!(health.snapshot().state, HealthState::Healthy);
        drop(old);
        let _new = handle.register(health.clone(), Probe("new"))?;
        health.report(health.begin_probe(), HealthOutcome::Failure);
        let after_replacement = health.snapshot();
        // An early return or panic drops this sender and wakes the worker.
        replaced.send(())?;
        assert!(worker.join().is_ok());
        // Delayed old callbacks cannot modify the replacement's observation.
        assert_eq!(health.snapshot(), after_replacement);
        assert_eq!(health.snapshot().state, HealthState::Unhealthy);
        Ok(())
    })
}

#[test]
fn removed_registration_discards_completed_old_probe_before_acceptance() -> Result<(), BoxError> {
    let (scheduler, health) = fixture()?;
    let handle = scheduler.handle();
    let old = handle.register(health.clone(), Probe("old"))?;
    let completed = completed(&scheduler, &health);
    drop(old);
    let _new = handle.register(health.clone(), Probe("new"))?;
    let before = health.snapshot();
    scheduler.observe_completed(Ok(completed));
    assert_eq!(health.snapshot(), before);
    assert!(!health.is_eligible());
    Ok(())
}

#[test]
fn discarded_completion_releases_last_record_outside_scheduler_lock() -> Result<(), BoxError> {
    use tracing_subscriber::prelude::*;
    struct Inspect(HealthSchedulerHandle);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Inspect {
        fn on_event(&self, _: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            // A failed assertion diagnoses lock reentrancy rather than hanging.
            assert!(self.0.shared.registrations.try_lock().is_ok());
        }
    }
    let (scheduler, health) = fixture()?;
    let handle = scheduler.handle();
    let _scope = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(Inspect(handle.clone())),
    );
    let registration = handle.register(health.clone(), Probe("old"))?;
    let accepted = completed(&scheduler, &health);
    scheduler.observe_completed(Ok(accepted));
    assert!(health.is_eligible());
    let completed = completed(&scheduler, &health);
    drop(registration);
    drop(health);
    // This token is the last owner; rejecting it destroys the record and emits
    // backend_removed, whose callback is allowed to inspect this scheduler.
    scheduler.observe_completed(Ok(completed));
    Ok(())
}
