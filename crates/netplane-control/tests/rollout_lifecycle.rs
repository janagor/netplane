//! Progressive application, lifecycle, evidence and ownership regression tests.
mod rollout_support;
use netplane_control::rollout::*;
use rollout_support::*;
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test(start_paused = true)]
async fn progression_observes_final_stage_and_preserves_pinned_requests() -> TestResult {
    let (updates, reconcile, controller) = fixture().await?;
    let pinned = controller.handle().execute(());
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        Metrics::new(100),
        spec(&[5, 25, 100]),
        Duration::from_secs(1),
    )?;
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Blocked).await;
    assert_eq!(handle.status().applied_canary_weight, Some(5));
    ticks(2).await;
    assert!(!handle.status().state.is_terminal());
    state(&handle, RolloutState::Completed).await;
    task.await?;
    assert_eq!(handle.status().last_successful_stage, Some(2));
    assert_eq!(controller.handle().execute(()).await?.weight, 100);
    assert_eq!(pinned.await?.weight, 0);
    assert_eq!(controller.handle().execute(()).await?.unrelated, 7);
    updates.shutdown();
    reconcile.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn insufficient_evidence_pause_resume_and_cancel_keep_weights() -> TestResult {
    let (updates, reconcile, controller) = fixture().await?;
    let metrics = Metrics::new(0);
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        metrics.clone(),
        spec(&[5, 100]),
        Duration::from_secs(1),
    )?;
    assert!(matches!(
        RolloutDriver::start(
            updates.clone(),
            metrics.clone(),
            spec(&[100]),
            Duration::from_secs(1)
        ),
        Err(RolloutError::Busy)
    ));
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Blocked).await;
    let epoch = handle.status().epoch;
    handle.pause()?;
    state(&handle, RolloutState::Paused).await;
    metrics.count.store(100, Ordering::SeqCst);
    ticks(5).await;
    assert_eq!(controller.handle().execute(()).await?.weight, 5);
    handle.resume()?;
    state(&handle, RolloutState::Blocked).await;
    assert!(handle.status().epoch > epoch);
    handle.cancel()?;
    task.await?;
    assert_eq!(handle.status().state, RolloutState::Cancelled);
    assert_eq!(controller.handle().execute(()).await?.weight, 5);
    updates.shutdown();
    reconcile.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn definite_failure_retains_applied_stage() -> TestResult {
    let (updates, reconcile, controller) = fixture().await?;
    let metrics = Metrics::new(100);
    metrics.failed.store(2, Ordering::SeqCst);
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        metrics,
        spec(&[25, 100]),
        Duration::from_secs(1),
    )?;
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Failed).await;
    task.await?;
    assert_eq!(controller.handle().execute(()).await?.weight, 25);
    assert_eq!(handle.status().failed_stage, Some(0));
    assert!(handle.resume().is_err());
    updates.shutdown();
    reconcile.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn administrator_update_blocks_and_unrelated_update_is_preserved() -> TestResult {
    let (updates, reconcile, controller) = fixture().await?;
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
        weight: 5,
        unrelated: 99,
        membership: 2,
    })?;
    converged(&updates).await;
    ticks(3).await;
    assert_eq!(controller.handle().execute(()).await?.unrelated, 99);
    updates.submit(Config {
        weight: 30,
        unrelated: 99,
        membership: 2,
    })?;
    converged(&updates).await;
    metrics.count.store(100, Ordering::SeqCst);
    ticks(5).await;
    assert_eq!(
        handle.status().blocked_reason,
        Some(EvidenceReason::ConfigurationConflict)
    );
    assert_eq!(controller.handle().execute(()).await?.weight, 30);
    handle.cancel()?;
    task.await?;
    updates.shutdown();
    reconcile.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn timeout_requires_resume_and_shutdown_terminates_paused_driver() -> TestResult {
    let (updates, reconcile, _) = fixture().await?;
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        Metrics::new(0),
        spec(&[5, 100]),
        Duration::from_secs(1),
    )?;
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Blocked).await;
    ticks(25).await;
    assert_eq!(
        handle.status().blocked_reason,
        Some(EvidenceReason::Timeout)
    );
    handle.pause()?;
    state(&handle, RolloutState::Paused).await;
    updates.shutdown();
    reconcile.await?;
    task.await?;
    assert_eq!(handle.status().state, RolloutState::Cancelled);
    Ok(())
}

struct PendingMetrics(std::sync::Arc<std::sync::atomic::AtomicUsize>);
struct ActiveObservation(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl Drop for ActiveObservation {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ObservationProvider for PendingMetrics {
    async fn observe(
        &self,
        _: ObservationRequest,
    ) -> Result<RolloutObservation, ObservationUnavailable> {
        let previous = self.0.fetch_add(1, Ordering::SeqCst);
        assert_eq!(previous, 0, "only one provider future may be outstanding");
        let _active = ActiveObservation(self.0.clone());
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn observation_timeout_pause_cancel_and_drop_release_resources() -> TestResult {
    let (updates, reconcile, _) = fixture().await?;
    let active = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        PendingMetrics(active.clone()),
        spec(&[5, 100]),
        Duration::from_secs(1),
    )?;
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Blocked).await;
    assert_eq!(
        handle.status().blocked_reason,
        Some(EvidenceReason::Unavailable)
    );
    handle.pause()?;
    state(&handle, RolloutState::Paused).await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
    handle.resume()?;
    ticks(3).await;
    handle.cancel()?;
    task.await?;
    assert_eq!(active.load(Ordering::SeqCst), 0);
    let (unused, handle) = RolloutDriver::start(
        updates.clone(),
        Metrics::new(0),
        spec(&[100]),
        Duration::from_secs(1),
    )?;
    drop(unused);
    assert_eq!(handle.status().state, RolloutState::Cancelled);
    updates.shutdown();
    reconcile.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn eligibility_change_restarts_epoch_without_rebuilding_same_policy() -> TestResult {
    let (updates, reconcile, controller) = fixture().await?;
    let metrics = Metrics::new(0);
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        metrics.clone(),
        spec(&[5, 100]),
        Duration::from_secs(1),
    )?;
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Blocked).await;
    let before = handle.status();
    let builds = controller.inspect().counters.replacements_total;
    metrics.eligibility.store(1, Ordering::SeqCst);
    ticks(3).await;
    assert!(handle.status().epoch > before.epoch);
    assert_eq!(
        handle.status().active_stage_publication,
        before.active_stage_publication
    );
    assert_eq!(controller.inspect().counters.replacements_total, builds);
    handle.cancel()?;
    task.await?;
    updates.shutdown();
    reconcile.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn external_publication_cannot_be_replaced_by_rollout() -> TestResult {
    use netplane_control::GraphPreparer;
    let (updates, reconcile, controller) = fixture().await?;
    let metrics = Metrics::new(0);
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        metrics.clone(),
        spec(&[5, 100]),
        Duration::from_secs(1),
    )?;
    let task = tokio::spawn(driver.run());
    state(&handle, RolloutState::Blocked).await;
    let external = Preparer
        .prepare(Config {
            weight: 70,
            unrelated: 42,
            membership: 1,
        })
        .await?;
    controller.publish_if_current(controller.inspect().token.ok_or("no token")?, external)?;
    metrics.count.store(100, Ordering::SeqCst);
    ticks(5).await;
    assert_eq!(
        handle.status().blocked_reason,
        Some(EvidenceReason::ConfigurationConflict)
    );
    assert_eq!(controller.handle().execute(()).await?.weight, 70);
    handle.cancel()?;
    task.await?;
    updates.shutdown();
    reconcile.await?;
    Ok(())
}

struct GatedPreparer {
    entered: std::sync::Arc<tokio::sync::Semaphore>,
    gate: std::sync::Arc<tokio::sync::Semaphore>,
    fail: bool,
}
impl netplane_control::GraphPreparer<Config, (), Config> for GatedPreparer {
    type Error = netplane::ConfigError;
    fn validate(&self, _: &Config) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn prepare(
        &self,
        desired: Config,
    ) -> Result<netplane::GraphVersion<(), Config>, Self::Error> {
        if desired.weight == 5 {
            self.entered.add_permits(1);
            self.gate
                .acquire()
                .await
                .map_err(|_| netplane::ConfigError::ZeroConcurrency)?
                .forget();
            if self.fail {
                return Err(netplane::ConfigError::ZeroConcurrency);
            }
        }
        Preparer.prepare(desired).await
    }
}

#[tokio::test(start_paused = true)]
async fn pending_intents_cannot_overwrite_cancel_admin_external_or_failed_candidates() -> TestResult
{
    use netplane_control::{GraphPreparer, Reconciler, ReconciliationDriver, UpdatePolicy};
    for case in ["cancel", "admin", "external", "failure", "resume"] {
        let config = Config {
            weight: 0,
            unrelated: 7,
            membership: 1,
        };
        let runtime = netplane::Netplane::new(
            Preparer.prepare(config.clone()).await?,
            netplane::RuntimeLimits { max_outstanding: 8 },
        )?;
        let controller = std::sync::Arc::new(netplane::ReconfigurationController::new(
            runtime,
            netplane::ControllerOptions::default(),
        ));
        let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let entered = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let reconciler = Reconciler::new(
            controller.clone(),
            GatedPreparer {
                gate: gate.clone(),
                entered: entered.clone(),
                fail: case == "failure",
            },
        );
        let (worker, updates) = ReconciliationDriver::new(
            reconciler,
            UpdatePolicy {
                debounce: Duration::ZERO,
                ..UpdatePolicy::default()
            },
        )?;
        let mut outcomes = worker.status();
        let reconcile = tokio::spawn(worker.run(|_| false));
        updates.submit(config)?;
        converged(&updates).await;
        let (driver, handle) = RolloutDriver::start(
            updates.clone(),
            Metrics::new(100),
            spec(&[5, 100]),
            Duration::from_secs(1),
        )?;
        let rollout = tokio::spawn(driver.run());
        entered.acquire().await?.forget();
        assert_eq!(handle.status().state, RolloutState::Applying);
        assert_eq!(handle.status().active_stage_publication, None);
        if case == "resume" {
            handle.pause()?;
            state(&handle, RolloutState::Paused).await;
            handle.resume()?;
            tokio::task::yield_now().await;
        } else if case == "cancel" {
            handle.cancel()?;
        } else if case == "admin" {
            updates.submit(Config {
                weight: 70,
                unrelated: 42,
                membership: 1,
            })?;
        } else if case == "external" {
            let candidate = Preparer
                .prepare(Config {
                    weight: 70,
                    unrelated: 42,
                    membership: 1,
                })
                .await?;
            controller
                .publish_if_current(controller.inspect().token.ok_or("no token")?, candidate)?;
        }
        gate.add_permits(1);
        loop {
            let settled = matches!(&**outcomes.borrow_and_update(), netplane_control::DriverStatus::Applied(sequence, _) if *sequence > 1)
                || matches!(
                    &**outcomes.borrow(),
                    netplane_control::DriverStatus::Invalidated(_)
                        | netplane_control::DriverStatus::Failed { .. }
                );
            if settled {
                break;
            }
            outcomes.changed().await?;
        }
        ticks(5).await;
        let (weight, terminal) = expected_intervention(case);
        assert_eq!(controller.handle().execute(()).await?.weight, weight);
        if !["cancel", "failure", "resume"].contains(&case) {
            handle.cancel()?;
        }
        rollout.await?;
        assert_eq!(handle.status().state, terminal);
        updates.shutdown();
        reconcile.await?;
    }
    Ok(())
}

fn expected_intervention(case: &str) -> (u8, RolloutState) {
    match case {
        "admin" | "external" => (70, RolloutState::Cancelled),
        "resume" => (100, RolloutState::Completed),
        "failure" => (0, RolloutState::Failed),
        _ => (0, RolloutState::Cancelled),
    }
}
