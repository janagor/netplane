//! Review regressions for administrative halts, stage deadlines and baseline policy.
mod rollout_support;
use netplane_control::rollout::*;
use rollout_support::*;
use std::{sync::atomic::Ordering, time::Duration};

#[test]
fn reject_equal_observation_duration_and_timeout() {
    let mut rollout = spec(&[100]);
    rollout.stages[0].timeout = rollout.stages[0].min_observation;
    assert_eq!(rollout.validate(), Err(ValidationError::Timing(0)));
}

async fn timeout_survives_update(membership: u64) -> TestResult {
    let (updates, reconciliation, _) = fixture().await?;
    let metrics = Metrics::new(0);
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        metrics.clone(),
        spec(&[5, 100]),
        Duration::from_secs(1),
    )?;
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Blocked).await;
    ticks(22).await;
    assert_eq!(
        handle.status().blocked_reason,
        Some(EvidenceReason::Timeout)
    );
    let epoch = handle.status().epoch;
    updates.submit(Config {
        weight: 5,
        unrelated: 99,
        membership,
    })?;
    converged(&updates).await;
    metrics.count.store(100, Ordering::SeqCst);
    ticks(5).await;
    assert_eq!(
        handle.status().blocked_reason,
        Some(EvidenceReason::Timeout)
    );
    assert_eq!(handle.status().epoch, epoch);
    handle.resume()?;
    state(&handle, RolloutState::Completed).await;
    task.await?;
    updates.shutdown();
    reconciliation.await?;
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn timeout_survives_unrelated_configuration_update() -> TestResult {
    timeout_survives_update(1).await
}
#[tokio::test(start_paused = true)]
async fn timeout_survives_membership_update() -> TestResult {
    timeout_survives_update(2).await
}

#[tokio::test(start_paused = true)]
async fn configuration_conflict_requires_explicit_resume() -> TestResult {
    let (updates, reconciliation, _) = fixture().await?;
    let metrics = Metrics::new(0);
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        metrics.clone(),
        spec(&[5, 100]),
        Duration::from_secs(1),
    )?;
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Blocked).await;
    updates.submit(Config {
        weight: 30,
        unrelated: 7,
        membership: 1,
    })?;
    converged(&updates).await;
    ticks(2).await;
    assert_eq!(
        handle.status().blocked_reason,
        Some(EvidenceReason::ConfigurationConflict)
    );
    updates.submit(Config {
        weight: 5,
        unrelated: 8,
        membership: 1,
    })?;
    converged(&updates).await;
    metrics.count.store(100, Ordering::SeqCst);
    ticks(5).await;
    assert_eq!(
        handle.status().blocked_reason,
        Some(EvidenceReason::ConfigurationConflict)
    );
    handle.resume()?;
    state(&handle, RolloutState::Completed).await;
    task.await?;
    updates.shutdown();
    reconciliation.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn initial_weight_below_baseline_is_rejected() -> TestResult {
    let (updates, reconciliation, _) = fixture().await?;
    updates.submit(Config {
        weight: 50,
        unrelated: 7,
        membership: 1,
    })?;
    converged(&updates).await;
    assert!(matches!(
        RolloutDriver::start(
            updates.clone(),
            Metrics::new(100),
            spec(&[5, 100]),
            Duration::from_secs(1)
        ),
        Err(RolloutError::Invalid(ValidationError::Weight(0)))
    ));
    updates.shutdown();
    reconciliation.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn repeated_eligibility_changes_do_not_extend_stage_deadline() -> TestResult {
    let (updates, reconciliation, _) = fixture().await?;
    let metrics = Metrics::new(0);
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        metrics.clone(),
        spec(&[5, 100]),
        Duration::from_secs(1),
    )?;
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Blocked).await;
    for revision in 1..=12 {
        metrics.eligibility.store(revision, Ordering::SeqCst);
        ticks(2).await;
    }
    assert_eq!(
        handle.status().blocked_reason,
        Some(EvidenceReason::Timeout)
    );
    handle.cancel()?;
    task.await?;
    updates.shutdown();
    reconciliation.await?;
    Ok(())
}

struct UnstableEligibility {
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl ObservationProvider for UnstableEligibility {
    async fn observe(
        &self,
        request: ObservationRequest,
    ) -> Result<RolloutObservation, ObservationUnavailable> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        // Fail a tight-loop implementation instead of hanging the test executor.
        assert!(
            call < 64,
            "eligibility restarts bypassed the polling interval"
        );
        let groups = request
            .groups
            .iter()
            .map(|group| GroupObservation {
                group: group.clone(),
                completed_requests: 0,
                failed_requests: 0,
                p95_latency: None,
                eligible_backends: 1,
                eligibility_revision: call as u64,
            })
            .collect();
        Ok(RolloutObservation {
            request,
            complete: true,
            eligibility_changed: true,
            groups,
        })
    }
}

#[tokio::test(start_paused = true)]
async fn continuous_eligibility_changes_respect_polling_interval() -> TestResult {
    let (updates, reconciliation, _) = fixture().await?;
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        UnstableEligibility {
            calls: calls.clone(),
        },
        spec(&[5, 100]),
        Duration::from_secs(1),
    )?;
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Observing).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(500)).await;
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // Reconciliation wakes the wait but must not authorize an early provider call.
    updates.submit(Config {
        weight: 5,
        unrelated: 99,
        membership: 2,
    })?;
    converged(&updates).await;
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    ticks(21).await;
    assert_eq!(
        handle.status().blocked_reason,
        Some(EvidenceReason::Timeout)
    );
    assert!(calls.load(Ordering::SeqCst) <= 20);
    handle.cancel()?;
    task.await?;
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        UnstableEligibility {
            calls: calls.clone(),
        },
        spec(&[5, 100]),
        Duration::from_secs(1),
    )?;
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Observing).await;
    let before_cancel = tokio::time::Instant::now();
    handle.cancel()?;
    tokio::time::timeout(Duration::from_millis(10), task).await??;
    assert!(before_cancel.elapsed() < Duration::from_secs(1));
    updates.shutdown();
    reconciliation.await?;
    Ok(())
}
