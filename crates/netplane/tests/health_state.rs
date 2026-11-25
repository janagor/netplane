//! Backend health generations, cooldowns and lifecycle.
use netplane::health::{
    BackendIdentity, HealthError, HealthMode, HealthOutcome, HealthPolicy, HealthRegistry,
    HealthState,
};
use std::{num::NonZeroU32, time::Duration};

fn identity(incarnation: &str) -> BackendIdentity {
    BackendIdentity {
        service: "payments".into(),
        instance: "endpoint-a".into(),
        incarnation: incarnation.into(),
    }
}
const fn policy() -> HealthPolicy {
    HealthPolicy {
        failure_threshold: NonZeroU32::MIN.saturating_add(2),
        recovery_threshold: NonZeroU32::MIN.saturating_add(1),
        recovery_delay: Duration::from_secs(10),
    }
}
fn report(handle: &netplane::health::HealthHandle, outcome: HealthOutcome) {
    handle.report(handle.begin_observation(), outcome);
}

#[tokio::test(start_paused = true)]
async fn thresholds_cooldown_and_passive_recovery() -> Result<(), HealthError> {
    let registry = HealthRegistry::default();
    let handle = registry.register(identity("one"), policy())?;
    assert_eq!(handle.snapshot().state, HealthState::Healthy);
    report(&handle, HealthOutcome::Failure);
    assert!(handle.is_eligible());
    report(&handle, HealthOutcome::Success);
    assert_eq!(handle.snapshot().consecutive_failures, 0);
    for _ in 0..3 {
        report(&handle, HealthOutcome::Failure);
    }
    assert!(!handle.is_eligible());
    let before = handle.snapshot();
    report(&handle, HealthOutcome::Ignored);
    assert_eq!(before, handle.snapshot());
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(!handle.is_eligible());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(handle.snapshot().state, HealthState::Recovering);
    assert!(handle.is_eligible());
    report(&handle, HealthOutcome::Success);
    assert_eq!(handle.snapshot().state, HealthState::Recovering);
    report(&handle, HealthOutcome::Failure);
    assert_eq!(handle.snapshot().state, HealthState::Unhealthy);
    tokio::time::advance(Duration::from_secs(10)).await;
    report(&handle, HealthOutcome::Success);
    report(&handle, HealthOutcome::Success);
    assert_eq!(handle.snapshot().state, HealthState::Healthy);
    assert_eq!(registry.snapshot().summary.health_transitions, 5);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn active_recovery_requires_evidence_and_rejects_delayed_success() -> Result<(), HealthError>
{
    let registry = HealthRegistry::default();
    let handle = registry.register_with_mode(identity("one"), policy(), HealthMode::Active)?;
    assert_eq!(handle.snapshot().state, HealthState::Recovering);
    assert!(!handle.is_eligible());
    handle.report(handle.begin_probe(), HealthOutcome::Success);
    handle.report(handle.begin_probe(), HealthOutcome::Success);
    let stale = handle.begin_observation();
    for _ in 0..3 {
        report(&handle, HealthOutcome::Failure);
    }
    handle.report(stale, HealthOutcome::Success);
    assert_eq!(handle.snapshot().state, HealthState::Unhealthy);
    handle.report(handle.begin_probe(), HealthOutcome::Success);
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(!handle.is_eligible());
    handle.report(handle.begin_probe(), HealthOutcome::Success);
    assert!(!handle.is_eligible());
    handle.report(handle.begin_probe(), HealthOutcome::Success);
    assert!(handle.is_eligible());
    Ok(())
}

#[test]
fn registry_reuses_state_isolates_replacements_and_reclaims_tokens() -> Result<(), HealthError> {
    let registry = HealthRegistry::default();
    let handle = registry.register(identity("one"), policy())?;
    let same = registry.register(identity("one"), policy())?;
    for _ in 0..3 {
        report(&handle, HealthOutcome::Failure);
    }
    assert!(!same.is_eligible());
    let replacement = registry.register(identity("two"), policy())?;
    replacement.report(handle.begin_observation(), HealthOutcome::Failure);
    assert_eq!(replacement.snapshot().consecutive_failures, 0);
    let token = handle.begin_observation();
    drop(handle);
    drop(same);
    drop(replacement);
    assert_eq!(registry.snapshot().summary.tracked_backends, 1);
    drop(token);
    assert_eq!(registry.snapshot().summary.tracked_backends, 0);
    Ok(())
}

#[test]
fn invalid_and_conflicting_registrations_do_not_change_state() -> Result<(), HealthError> {
    let registry = HealthRegistry::default();
    assert!(matches!(
        registry.register(identity(""), policy()),
        Err(HealthError::InvalidIdentity)
    ));
    let mut invalid = policy();
    invalid.recovery_delay = Duration::MAX;
    assert!(matches!(
        registry.register(identity("one"), invalid),
        Err(HealthError::InvalidRecoveryDelay)
    ));
    let handle = registry.register(identity("one"), policy())?;
    assert!(matches!(
        registry.register_with_mode(identity("one"), policy(), HealthMode::Active),
        Err(HealthError::IncompatibleRegistration)
    ));
    assert!(handle.is_eligible());
    Ok(())
}

#[test]
fn concurrent_failures_transition_once_and_old_generations_are_discarded() -> Result<(), HealthError>
{
    let registry = HealthRegistry::default();
    let handle = registry.register(identity("one"), policy())?;
    let tokens: Vec<_> = (0..32).map(|_| handle.begin_observation()).collect();
    std::thread::scope(|scope| {
        for token in tokens {
            let handle = &handle;
            scope.spawn(move || handle.report(token, HealthOutcome::Failure));
        }
    });
    assert_eq!(handle.snapshot().consecutive_failures, 3);
    assert_eq!(registry.snapshot().summary.health_transitions, 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn old_execution_cannot_supply_recovery_evidence_after_cooldown() -> Result<(), HealthError> {
    let registry = HealthRegistry::default();
    let handle = registry.register(identity("one"), policy())?;
    let stale = handle.begin_observation();
    for _ in 0..3 {
        report(&handle, HealthOutcome::Failure);
    }
    tokio::time::advance(Duration::from_secs(10)).await;
    handle.report(stale, HealthOutcome::Success);
    let snapshot = handle.snapshot();
    assert_eq!(snapshot.state, HealthState::Recovering);
    assert_eq!(snapshot.consecutive_successes, 0);
    report(&handle, HealthOutcome::Success);
    assert_eq!(handle.snapshot().consecutive_successes, 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn active_recovery_counts_only_probe_successes_but_any_failure_excludes()
-> Result<(), HealthError> {
    let registry = HealthRegistry::default();
    let handle = registry.register_with_mode(identity("one"), policy(), HealthMode::Active)?;
    report(&handle, HealthOutcome::Success);
    assert_eq!(handle.snapshot().consecutive_successes, 0);
    handle.report(handle.begin_probe(), HealthOutcome::Success);
    assert_eq!(handle.snapshot().consecutive_successes, 1);
    report(&handle, HealthOutcome::Failure);
    assert_eq!(handle.snapshot().state, HealthState::Unhealthy);
    tokio::time::advance(Duration::from_secs(10)).await;
    report(&handle, HealthOutcome::Success);
    assert_eq!(handle.snapshot().consecutive_successes, 0);
    handle.report(handle.begin_probe(), HealthOutcome::Success);
    handle.report(handle.begin_probe(), HealthOutcome::Success);
    assert!(handle.is_eligible());
    Ok(())
}

#[test]
fn concurrent_reporting_timestamps_never_move_backwards() -> Result<(), HealthError> {
    let registry = HealthRegistry::default();
    let handle = registry.register(identity("one"), policy())?;
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let handle = &handle;
                scope.spawn(move || {
                    let mut previous = None;
                    for _ in 0..5000 {
                        report(handle, HealthOutcome::Success);
                        let now = handle.snapshot().last_observation;
                        assert!(now >= previous);
                        previous = now;
                    }
                })
            })
            .collect();
        for worker in workers {
            assert!(worker.join().is_ok());
        }
    });
    Ok(())
}

#[test]
fn large_registry_reclaims_independent_records_and_survives_same_identity_churn()
-> Result<(), HealthError> {
    let registry = HealthRegistry::default();
    let handles = (0..10_000)
        .map(|i| registry.register(identity(&i.to_string()), policy()))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(registry.snapshot().summary.tracked_backends, 10_000);
    drop(handles);
    assert_eq!(registry.snapshot().summary.tracked_backends, 0);
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let registry = &registry;
                scope.spawn(move || -> Result<(), HealthError> {
                    for _ in 0..1000 {
                        let handle = registry.register(identity("shared"), policy())?;
                        report(&handle, HealthOutcome::Success);
                        let same = registry.register(identity("shared"), policy())?;
                        assert!(same.snapshot().last_observation.is_some());
                    }
                    Ok(())
                })
            })
            .collect();
        for worker in workers {
            assert!(matches!(worker.join(), Ok(Ok(()))));
        }
    });
    assert_eq!(registry.snapshot().summary.tracked_backends, 0);
    Ok(())
}
