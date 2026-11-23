//! Deterministic worker scheduling and publication tests.
use netplane::{
    ControllerOptions, GraphDraft, GraphVersion, Netplane, ReconfigurationController, RuntimeLimits,
};
use netplane_control::{
    DriverStatus, GraphPreparer, Reconciler, ReconciliationDriver, UpdateError, UpdatePolicy,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, watch};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
#[derive(Debug, thiserror::Error)]
#[error("preparation rejected")]
struct Rejected;
fn graph(value: usize) -> Result<GraphVersion<(), usize>, netplane::ConfigError> {
    GraphDraft::new(tower::service_fn(move |()| async move {
        Ok::<_, std::convert::Infallible>(value)
    }))
    .validate()
}
struct Preparer {
    entered: Arc<Semaphore>,
    gate: Arc<Semaphore>,
    failures: Arc<AtomicUsize>,
    released: Arc<Semaphore>,
}
struct CandidateOwner(Arc<Semaphore>);
impl Drop for CandidateOwner {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}
impl GraphPreparer<usize, (), usize> for Preparer {
    type Error = Rejected;
    fn validate(&self, desired: &usize) -> Result<(), Rejected> {
        if *desired == 0 { Err(Rejected) } else { Ok(()) }
    }
    async fn prepare(&self, desired: usize) -> Result<GraphVersion<(), usize>, Rejected> {
        let owner = CandidateOwner(Arc::clone(&self.released));
        let candidate = GraphDraft::new(tower::service_fn(move |()| {
            let _ = &owner;
            async move { Ok::<_, std::convert::Infallible>(desired) }
        }))
        .validate()
        .map_err(|_| Rejected)?;
        self.entered.add_permits(1);
        self.gate.acquire().await.map_err(|_| Rejected)?.forget();
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Rejected);
        }
        Ok(candidate)
    }
}
struct Fixture {
    controller: Arc<ReconfigurationController<(), usize>>,
    driver: ReconciliationDriver<usize, Preparer, (), usize>,
    sender: netplane_control::UpdateSender<usize>,
    entered: Arc<Semaphore>,
    gate: Arc<Semaphore>,
    failures: Arc<AtomicUsize>,
    released: Arc<Semaphore>,
}
fn fixture(policy: UpdatePolicy) -> Result<Fixture, Box<dyn std::error::Error + Send + Sync>> {
    let controller = Arc::new(ReconfigurationController::new(
        Netplane::new(graph(1)?, RuntimeLimits { max_outstanding: 8 })?,
        ControllerOptions::default(),
    ));
    let entered = Arc::new(Semaphore::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let failures = Arc::new(AtomicUsize::new(0));
    let released = Arc::new(Semaphore::new(0));
    let reconciler = Reconciler::new(
        Arc::clone(&controller),
        Preparer {
            entered: Arc::clone(&entered),
            gate: Arc::clone(&gate),
            failures: Arc::clone(&failures),
            released: Arc::clone(&released),
        },
    );
    let (driver, sender) = ReconciliationDriver::new(reconciler, policy)?;
    Ok(Fixture {
        controller,
        driver,
        sender,
        entered,
        gate,
        failures,
        released,
    })
}
fn policy() -> UpdatePolicy {
    UpdatePolicy {
        debounce: Duration::ZERO,
        ..UpdatePolicy::default()
    }
}
async fn wait_status(
    status: &mut watch::Receiver<Arc<DriverStatus<Rejected>>>,
    predicate: impl Fn(&DriverStatus<Rejected>) -> bool,
) -> TestResult {
    loop {
        if predicate(&status.borrow_and_update()) {
            return Ok(());
        }
        status.changed().await?;
    }
}

#[tokio::test(start_paused = true)]
async fn burst_discards_obsolete_and_retains_old_request() -> TestResult {
    let f = fixture(policy())?;
    let mut status = f.driver.status();
    f.sender.submit(2)?;
    let worker = tokio::spawn(f.driver.run(|_| true));
    f.entered.acquire().await?.forget();
    let before = f.controller.inspect();
    let old = f.controller.handle().execute(());
    for value in 3..=1000 {
        f.sender.submit(value)?;
    }
    f.gate.add_permits(1);
    f.entered.acquire().await?.forget();
    f.released.acquire().await?.forget();
    assert_eq!(f.controller.inspect(), before);
    assert_eq!(f.entered.available_permits(), 0);
    f.gate.add_permits(1);
    wait_status(&mut status, |s| matches!(s, DriverStatus::Applied(999, _))).await?;
    assert_eq!(old.await?, 1);
    assert_eq!(f.controller.handle().execute(()).await?, 1000);
    f.sender.shutdown();
    worker.await?;
    assert_eq!(f.sender.submit(4), Err(UpdateError::Stopped));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn transient_recovery_and_interruptible_retry() -> TestResult {
    let f = fixture(policy())?;
    f.failures.store(2, Ordering::SeqCst);
    f.gate.add_permits(10);
    let mut status = f.driver.status();
    f.sender.submit(2)?;
    let worker = tokio::spawn(f.driver.run(|_| true));
    wait_status(&mut status, |s| {
        matches!(
            s,
            DriverStatus::Failed {
                retry_in: Some(_),
                ..
            }
        )
    })
    .await?;
    assert_eq!(f.controller.handle().execute(()).await?, 1);
    f.sender.submit(3)?;
    wait_status(&mut status, |s| {
        matches!(
            s,
            DriverStatus::Failed {
                sequence: 2,
                retry_in: Some(_),
                ..
            }
        )
    })
    .await?;
    wait_status(&mut status, |s| matches!(s, DriverStatus::Applied(2, _))).await?;
    assert_eq!(f.controller.handle().execute(()).await?, 3);
    f.sender.shutdown();
    worker.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn invalid_and_exhausted_updates_wait_for_resubmission() -> TestResult {
    let f = fixture(UpdatePolicy {
        max_retry_attempts: Some(0),
        ..policy()
    })?;
    let mut status = f.driver.status();
    f.sender.submit(0)?;
    let worker = tokio::spawn(f.driver.run(|_| true));
    wait_status(&mut status, |s| {
        matches!(
            s,
            DriverStatus::Failed {
                sequence: 1,
                retry_in: None,
                ..
            }
        )
    })
    .await?;
    tokio::time::advance(Duration::from_secs(100)).await;
    assert_eq!(f.entered.available_permits(), 0);
    f.failures.store(1, Ordering::SeqCst);
    f.gate.add_permits(2);
    f.sender.submit(2)?;
    wait_status(&mut status, |s| {
        matches!(
            s,
            DriverStatus::Failed {
                sequence: 2,
                retry_in: None,
                ..
            }
        )
    })
    .await?;
    assert_eq!(f.controller.handle().execute(()).await?, 1);
    f.sender.submit(2)?;
    wait_status(&mut status, |s| matches!(s, DriverStatus::Applied(3, _))).await?;
    f.sender.shutdown();
    worker.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stale_publication_rebuilds_without_overwriting() -> TestResult {
    let f = fixture(policy())?;
    let mut status = f.driver.status();
    f.sender.submit(2)?;
    let worker = tokio::spawn(f.driver.run(|_| true));
    f.entered.acquire().await?.forget();
    f.controller
        .publish_if_current(f.controller.current_token().ok_or("no token")?, graph(7)?)?;
    f.gate.add_permits(1);
    f.entered.acquire().await?.forget();
    assert_eq!(f.controller.handle().execute(()).await?, 7);
    f.gate.add_permits(1);
    wait_status(&mut status, |s| matches!(s, DriverStatus::Applied(1, _))).await?;
    assert_eq!(f.controller.handle().execute(()).await?, 2);
    f.sender.shutdown();
    worker.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn shutdown_during_preparation_discards_candidate() -> TestResult {
    let f = fixture(policy())?;
    let mut status = f.driver.status();
    f.sender.submit(2)?;
    let worker = tokio::spawn(f.driver.run(|_| true));
    f.entered.acquire().await?.forget();
    f.sender.shutdown();
    assert!(!worker.is_finished());
    assert_eq!(f.released.available_permits(), 0);
    f.gate.add_permits(1);
    worker.await?;
    f.released.acquire().await?.forget();
    wait_status(&mut status, |s| matches!(s, DriverStatus::Stopped)).await?;
    assert_eq!(f.controller.handle().execute(()).await?, 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_retry() -> TestResult {
    let f = fixture(policy())?;
    f.failures.store(10, Ordering::SeqCst);
    f.gate.add_permits(10);
    let mut status = f.driver.status();
    f.sender.submit(2)?;
    let worker = tokio::spawn(f.driver.run(|_| true));
    wait_status(&mut status, |s| {
        matches!(
            s,
            DriverStatus::Failed {
                retry_in: Some(_),
                ..
            }
        )
    })
    .await?;
    f.sender.shutdown();
    worker.await?;
    tokio::time::advance(Duration::from_secs(100)).await;
    assert_eq!(f.entered.available_permits(), 1);
    assert_eq!(f.controller.handle().execute(()).await?, 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn continuous_updates_cannot_extend_coalescing_window() -> TestResult {
    let f = fixture(UpdatePolicy::default())?;
    f.sender.submit(2)?;
    let worker = tokio::spawn(f.driver.run(|_| true));
    tokio::task::yield_now().await;
    let started = tokio::time::Instant::now();
    for value in 3..=21 {
        tokio::time::advance(Duration::from_millis(50)).await;
        f.sender.submit(value)?;
        tokio::task::yield_now().await;
        assert_eq!(f.entered.available_permits(), 0);
    }
    tokio::time::advance(Duration::from_millis(49)).await;
    f.sender.submit(22)?;
    tokio::task::yield_now().await;
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_millis(999)
    );
    assert_eq!(f.entered.available_permits(), 0);
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_secs(1)
    );
    assert_eq!(f.entered.available_permits(), 1);
    for value in 23..=30 {
        tokio::time::advance(Duration::from_millis(50)).await;
        f.sender.submit(value)?;
        tokio::task::yield_now().await;
        assert_eq!(f.entered.available_permits(), 1);
    }
    f.sender.shutdown();
    f.gate.add_permits(1);
    worker.await?;
    assert_eq!(f.controller.handle().execute(()).await?, 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn producer_drop_stops_worker_and_invalid_policy_is_rejected() -> TestResult {
    assert!(
        fixture(UpdatePolicy {
            retry_initial: Duration::ZERO,
            ..policy()
        })
        .is_err()
    );
    assert!(
        fixture(UpdatePolicy {
            retry_max: Duration::from_nanos(1),
            ..policy()
        })
        .is_err()
    );
    assert!(
        fixture(UpdatePolicy {
            max_coalesce_window: Duration::ZERO,
            ..policy()
        })
        .is_err()
    );
    let f = fixture(policy())?;
    let worker = tokio::spawn(f.driver.run(|_| true));
    drop(f.sender);
    worker.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn retry_delays_grow_and_do_not_prepare_early() -> TestResult {
    let f = fixture(policy())?;
    f.failures.store(2, Ordering::SeqCst);
    f.gate.add_permits(3);
    let mut status = f.driver.status();
    f.sender.submit(2)?;
    let worker = tokio::spawn(f.driver.run(|_| true));
    for attempt in 0..2 {
        f.entered.acquire().await?.forget();
        wait_status(&mut status, |s| {
            matches!(
                s,
                DriverStatus::Failed {
                    retry_in: Some(_),
                    ..
                }
            )
        })
        .await?;
        let delay = match status.borrow_and_update().as_ref() {
            DriverStatus::Failed {
                retry_in: Some(delay),
                ..
            } => *delay,
            _ => return Err("expected retry deadline".into()),
        };
        let base = Duration::from_millis(100 * (1 << attempt));
        assert!(delay >= base.mul_f64(0.8));
        assert!(delay <= base.mul_f64(1.2));
        tokio::time::advance(delay / 2).await;
        tokio::task::yield_now().await;
        assert_eq!(f.entered.available_permits(), 0);
        tokio::time::advance(delay + Duration::from_millis(1)).await;
    }
    wait_status(&mut status, |s| matches!(s, DriverStatus::Applied(1, _))).await?;
    f.sender.shutdown();
    worker.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dropping_worker_releases_preparation_without_detached_tasks() -> TestResult {
    let f = fixture(policy())?;
    f.sender.submit(2)?;
    let worker = tokio::spawn(f.driver.run(|_| true));
    f.entered.acquire().await?.forget();
    worker.abort();
    assert!(worker.await.is_err());
    f.released.acquire().await?.forget();
    assert_eq!(f.sender.submit(3), Err(UpdateError::Stopped));
    assert_eq!(f.controller.handle().execute(()).await?, 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn finite_retry_budget_counts_retries_after_initial_attempt() -> TestResult {
    let f = fixture(UpdatePolicy {
        max_retry_attempts: Some(2),
        ..policy()
    })?;
    f.failures.store(10, Ordering::SeqCst);
    f.gate.add_permits(10);
    let mut status = f.driver.status();
    f.sender.submit(2)?;
    let worker = tokio::spawn(f.driver.run(|_| true));
    wait_status(&mut status, |s| {
        matches!(
            s,
            DriverStatus::Failed {
                sequence: 1,
                retry_in: None,
                ..
            }
        )
    })
    .await?;
    assert_eq!(f.entered.available_permits(), 3);
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::task::yield_now().await;
    assert_eq!(f.entered.available_permits(), 3);
    assert_eq!(f.controller.handle().execute(()).await?, 1);
    f.sender.submit(2)?;
    wait_status(&mut status, |s| {
        matches!(
            s,
            DriverStatus::Failed {
                sequence: 2,
                retry_in: None,
                ..
            }
        )
    })
    .await?;
    assert_eq!(f.entered.available_permits(), 6);
    f.sender.shutdown();
    worker.await?;
    Ok(())
}
