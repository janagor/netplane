//! Snapshot consistency, cancellation, and structured tracing contracts.
use netplane::{
    ControllerOptions, GraphDraft, GraphVersion, Netplane, ReconfigurationController, RuntimeLimits,
};
use netplane_control::{
    ApplyOutcome, GraphPreparer, Reconciler, ReconciliationDriver,
    ReconciliationFailure as Failure, ReconciliationPhase as Phase, UpdatePolicy,
};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
fn graph(value: u32) -> Result<GraphVersion<(), u32>, netplane::ConfigError> {
    GraphDraft::new(tower::service_fn(move |()| async move {
        Ok::<_, Infallible>(value)
    }))
    .validate()
}
struct Preparer(Option<Arc<Semaphore>>);
impl GraphPreparer<u32, (), u32> for Preparer {
    type Error = &'static str;
    fn validate(&self, desired: &u32) -> Result<(), Self::Error> {
        if *desired == 0 {
            Err("private application data")
        } else {
            Ok(())
        }
    }
    async fn prepare(&self, desired: u32) -> Result<GraphVersion<(), u32>, Self::Error> {
        if let Some(gate) = &self.0 {
            gate.acquire().await.map_err(|_| "closed")?.forget();
        }
        graph(desired).map_err(|_| "graph rejected")
    }
}
fn controller()
-> Result<Arc<ReconfigurationController<(), u32>>, Box<dyn std::error::Error + Send + Sync>> {
    Ok(Arc::new(ReconfigurationController::new(
        Netplane::new(graph(1)?, RuntimeLimits { max_outstanding: 4 })?,
        ControllerOptions::default(),
    )))
}
fn policy() -> UpdatePolicy {
    UpdatePolicy {
        debounce: Duration::ZERO,
        ..UpdatePolicy::default()
    }
}
#[tokio::test]
async fn snapshots_deduplication_failures_and_unread_subscription() -> TestResult {
    let controller = controller()?;
    let reconciler = Reconciler::new(Arc::clone(&controller), Preparer(None));
    let subscription = reconciler.subscribe_status();
    let initial = reconciler.status();
    assert_eq!(initial.desired_revision, None);
    assert_eq!(initial.applied_revision, None);
    assert_eq!(initial.phase, Phase::Idle);
    assert!(!initial.is_converged());
    reconciler.apply(2).await.map_err(|_| "apply failed")?;
    let published = reconciler.status().active_publication;
    assert!(reconciler.status().is_converged());
    for revision in 2..=100 {
        assert!(matches!(
            reconciler.apply(2).await,
            Ok(ApplyOutcome::Unchanged(_))
        ));
        assert_eq!(reconciler.status().applied_revision, Some(revision));
    }
    assert_eq!(reconciler.status().active_publication, published);
    assert!(reconciler.apply(0).await.is_err());
    let failed = reconciler.status();
    assert_eq!(failed.phase, Phase::Blocked);
    assert_eq!(failed.desired_revision, Some(101));
    assert_eq!(failed.applied_revision, Some(100));
    assert_eq!(failed.last_failure, Some(Failure::Validation));
    assert_eq!(failed.active_publication, published);
    assert_eq!(*subscription.borrow(), failed);
    assert_eq!(controller.handle().execute(()).await?, 2);
    let (driver, updates) = ReconciliationDriver::new(reconciler, policy())?;
    assert_eq!(updates.submit(3)?, 102);
    assert_eq!(updates.status().desired_revision, Some(102));
    drop(driver);
    assert_eq!(updates.status().phase, Phase::Stopped);
    Ok(())
}

#[tokio::test]
async fn dropping_explicit_apply_reports_cancellation_and_can_recover() -> TestResult {
    let gate = Arc::new(Semaphore::new(0));
    let reconciler = Reconciler::new(controller()?, Preparer(Some(Arc::clone(&gate))));
    let mut pending = Box::pin(reconciler.apply(2));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert_eq!(reconciler.status().phase, Phase::Preparing);
    let before_busy = reconciler.status();
    assert!(matches!(
        reconciler.apply(3).await,
        Err(netplane_control::ApplyError::Busy)
    ));
    assert_eq!(reconciler.status(), before_busy);
    drop(pending);
    assert_eq!(reconciler.status().phase, Phase::Blocked);
    assert_eq!(reconciler.status().last_failure, Some(Failure::Cancelled));
    gate.add_permits(1);
    reconciler.apply(3).await.map_err(|_| "apply failed")?;
    assert!(reconciler.status().is_converged());
    assert_eq!(reconciler.status().desired_revision, Some(2));
    Ok(())
}

#[tokio::test]
async fn unused_driver_and_unpolled_run_report_stopped_on_drop() -> TestResult {
    for poll_run in [false, true] {
        let (driver, updates) =
            ReconciliationDriver::new(Reconciler::new(controller()?, Preparer(None)), policy())?;
        let outcomes = driver.status();
        updates.submit(2)?;
        if poll_run {
            drop(driver.run(|_| false));
        } else {
            drop(driver);
        }
        assert_eq!(updates.status().phase, Phase::Stopped);
        assert!(matches!(
            outcomes.borrow().as_ref(),
            netplane_control::DriverStatus::Stopped
        ));
        assert_eq!(updates.status().applied_revision, None);
        assert!(updates.submit(3).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn runtime_shutdown_records_publication_failure_before_stopping() -> TestResult {
    let control = controller()?;
    let (driver, updates) = ReconciliationDriver::new(
        Reconciler::new(Arc::clone(&control), Preparer(None)),
        policy(),
    )?;
    control.shutdown(Duration::from_secs(1)).await?;
    updates.submit(2)?;
    driver.run(|_| false).await;
    let status = updates.status();
    assert_eq!(status.phase, Phase::Stopped);
    assert_eq!(status.last_failure, Some(Failure::Publication));
    assert_eq!(status.desired_revision, Some(1));
    assert_eq!(status.applied_revision, None);
    assert!(updates.submit(3).is_err());
    Ok(())
}

#[tokio::test]
async fn stopped_reconciler_stays_terminal_when_moved_into_driver() -> TestResult {
    for run_worker in [false, true] {
        let control = controller()?;
        let reconciler = Reconciler::new(Arc::clone(&control), Preparer(None));
        control.shutdown(Duration::from_secs(1)).await?;
        assert!(reconciler.apply(2).await.is_err());
        let stopped = reconciler.status();
        assert_eq!(stopped.phase, Phase::Stopped);
        let (driver, updates) = ReconciliationDriver::new(reconciler, policy())?;
        assert_eq!(
            updates.submit(3),
            Err(netplane_control::UpdateError::Stopped)
        );
        assert_eq!(updates.status(), stopped);
        assert!(matches!(
            driver.status().borrow().as_ref(),
            netplane_control::DriverStatus::Stopped
        ));
        if run_worker {
            driver.run(|_| false).await;
        } else {
            drop(driver);
        }
        assert_eq!(updates.status().phase, Phase::Stopped);
        assert_eq!(updates.status().desired_revision, stopped.desired_revision);
        assert_eq!(
            updates.submit(4),
            Err(netplane_control::UpdateError::Stopped)
        );
    }
    Ok(())
}
