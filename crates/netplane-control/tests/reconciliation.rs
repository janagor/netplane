//! Desired-state publication invariants without sockets.
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use netplane::{
    ControllerError, ControllerOptions, GraphDraft, GraphVersion, Netplane,
    ReconfigurationController, RuntimeLimits,
};
use netplane_control::{ApplyError, ApplyOutcome, GraphPreparer, Reconciler};
use tokio::sync::{Notify, Semaphore};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
#[derive(Default)]
struct Ownership {
    count: AtomicUsize,
    released: Notify,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Config(u32);

#[derive(Debug, thiserror::Error)]
#[error("rejected configuration")]
struct Rejected;

fn graph(value: u32) -> Result<GraphVersion<(), u32>, netplane::ConfigError> {
    GraphDraft::new(tower::service_fn(move |()| async move {
        Ok::<_, std::convert::Infallible>(value)
    }))
    .validate()
}

struct Preparer {
    calls: Arc<AtomicUsize>,
    gate: Option<Arc<Semaphore>>,
    candidate_owners: Arc<Ownership>,
}

struct CandidateOwner(Arc<Ownership>);
impl Drop for CandidateOwner {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::SeqCst);
        self.0.released.notify_one();
    }
}

impl GraphPreparer<Config, (), u32> for Preparer {
    type Error = Rejected;
    fn validate(&self, desired: &Config) -> Result<(), Rejected> {
        if desired.0 == 0 {
            Err(Rejected)
        } else {
            Ok(())
        }
    }
    async fn prepare(&self, desired: Config) -> Result<GraphVersion<(), u32>, Rejected> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if desired.0 == 99 {
            return Err(Rejected);
        }
        self.candidate_owners.count.fetch_add(1, Ordering::SeqCst);
        let owner = CandidateOwner(Arc::clone(&self.candidate_owners));
        let candidate = GraphDraft::new(tower::service_fn(move |()| {
            let _ = &owner;
            async move { Ok::<_, std::convert::Infallible>(desired.0) }
        }))
        .validate()
        .map_err(|_| Rejected)?;
        if let Some(gate) = &self.gate {
            gate.acquire().await.map_err(|_| Rejected)?.forget();
        }
        Ok(candidate)
    }
}

type Control = ReconfigurationController<(), u32>;
type Reconcile = Reconciler<Config, Preparer, (), u32>;

fn setup(
    gated: bool,
) -> Result<
    (
        Arc<Control>,
        Reconcile,
        Arc<AtomicUsize>,
        Arc<Semaphore>,
        Arc<Ownership>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let controller = Arc::new(Control::new(
        Netplane::new(graph(1)?, RuntimeLimits { max_outstanding: 8 })?,
        ControllerOptions {
            max_retained_versions: 0,
        },
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let owners = Arc::new(Ownership::default());
    let reconciler = Reconciler::new(
        Arc::clone(&controller),
        Preparer {
            calls: Arc::clone(&calls),
            gate: gated.then(|| Arc::clone(&gate)),
            candidate_owners: Arc::clone(&owners),
        },
    );
    Ok((controller, reconciler, calls, gate, owners))
}

#[tokio::test]
async fn applies_initial_desired_state() -> TestResult {
    let (controller, reconciler, calls, _, _) = setup(false)?;
    assert!(reconciler.last_applied().is_none());
    let ApplyOutcome::Applied(outcome) = reconciler.apply(Config(2)).await? else {
        return Err("expected publication".into());
    };
    assert_eq!(controller.handle().execute(()).await?, 2);
    assert_eq!(
        reconciler.last_applied().ok_or("missing record")?.published,
        outcome.published
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn identical_state_is_noop() -> TestResult {
    let (controller, reconciler, calls, _, _) = setup(false)?;
    reconciler.apply(Config(2)).await?;
    let before = controller.inspect();
    assert_eq!(
        reconciler.apply(Config(2)).await?,
        ApplyOutcome::Unchanged(before.active.as_ref().ok_or("missing active")?.id)
    );
    assert_eq!(controller.inspect(), before);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn changed_state_publishes() -> TestResult {
    let (controller, reconciler, _, _, _) = setup(false)?;
    reconciler.apply(Config(2)).await?;
    let before = controller.inspect().active.ok_or("missing active")?.id;
    reconciler.apply(Config(3)).await?;
    assert_ne!(
        controller.inspect().active.ok_or("missing active")?.id,
        before
    );
    assert_eq!(controller.handle().execute(()).await?, 3);
    Ok(())
}

#[tokio::test]
async fn invalid_state_preserves_active() -> TestResult {
    let (controller, reconciler, calls, _, _) = setup(false)?;
    reconciler.apply(Config(2)).await?;
    let before = controller.inspect();
    assert!(matches!(
        reconciler.apply(Config(0)).await,
        Err(ApplyError::Validation(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(controller.inspect(), before);
    assert_eq!(
        reconciler.last_applied().ok_or("missing record")?.desired,
        Config(2)
    );
    assert_eq!(controller.handle().execute(()).await?, 2);
    Ok(())
}

#[tokio::test]
async fn preparation_failure_preserves_active() -> TestResult {
    let (controller, reconciler, _, _, _) = setup(false)?;
    reconciler.apply(Config(2)).await?;
    let before = controller.inspect();
    assert!(matches!(
        reconciler.apply(Config(99)).await,
        Err(ApplyError::Preparation(_))
    ));
    assert_eq!(controller.inspect(), before);
    assert_eq!(
        reconciler.last_applied().ok_or("missing record")?.desired,
        Config(2)
    );
    assert_eq!(controller.handle().execute(()).await?, 2);
    Ok(())
}

#[tokio::test]
async fn stale_token_rejected() -> TestResult {
    let (controller, reconciler, calls, gate, _) = setup(true)?;
    let pending = reconciler.apply(Config(2));
    tokio::pin!(pending);
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    controller.publish_if_current(
        controller.current_token().ok_or("missing token")?,
        graph(7)?,
    )?;
    gate.add_permits(1);
    assert!(matches!(
        pending.await,
        Err(ApplyError::Publication(ControllerError::Stale { .. }))
    ));
    assert!(reconciler.last_applied().is_none());
    assert_eq!(controller.handle().execute(()).await?, 7);
    Ok(())
}

#[tokio::test]
async fn external_publication_invalidates_noop() -> TestResult {
    let (controller, reconciler, calls, _, _) = setup(false)?;
    reconciler.apply(Config(2)).await?;
    controller.publish_if_current(
        controller.current_token().ok_or("missing token")?,
        graph(7)?,
    )?;
    assert!(matches!(
        reconciler.apply(Config(2)).await?,
        ApplyOutcome::Applied(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(controller.handle().execute(()).await?, 2);
    Ok(())
}

#[tokio::test]
async fn concurrent_apply_is_single_flight() -> TestResult {
    let (_, reconciler, calls, gate, _) = setup(true)?;
    let pending = reconciler.apply(Config(2));
    tokio::pin!(pending);
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert!(matches!(
        reconciler.apply(Config(3)).await,
        Err(ApplyError::Busy)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    gate.add_permits(1);
    pending.await?;
    assert_eq!(
        reconciler.last_applied().ok_or("missing record")?.desired,
        Config(2)
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_preparation_preserves_active() -> TestResult {
    let (controller, reconciler, calls, gate, owners) = setup(true)?;
    let before = controller.inspect();
    let mut pending = Box::pin(reconciler.apply(Config(2)));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    // Candidate is fully constructed; preparation has not returned it for publication.
    assert_eq!(owners.count.load(Ordering::SeqCst), 1);
    drop(pending);
    assert_eq!(controller.inspect(), before);
    assert!(reconciler.last_applied().is_none());
    // The bounded worker observes endpoint cancellation on the next scheduler turn.
    owners.released.notified().await;
    assert_eq!(owners.count.load(Ordering::SeqCst), 0);
    gate.add_permits(1);
    reconciler.apply(Config(3)).await?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn inflight_request_retains_version() -> TestResult {
    let (controller, reconciler, _, _, _) = setup(false)?;
    reconciler.apply(Config(2)).await?;
    let handle = controller.handle();
    let accepted = handle.execute(());
    reconciler.apply(Config(3)).await?;
    assert_eq!(accepted.await?, 2);
    assert_eq!(handle.execute(()).await?, 3);
    Ok(())
}

#[tokio::test]
async fn shutdown_rejects_reconciliation() -> TestResult {
    let (controller, reconciler, calls, _, _) = setup(false)?;
    let drain = controller.shutdown(Duration::from_secs(1));
    assert!(matches!(
        reconciler.apply(Config(2)).await,
        Err(ApplyError::Publication(ControllerError::ShuttingDown))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drain.await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_during_preparation_rejects_publication() -> TestResult {
    let (controller, reconciler, _, gate, _) = setup(true)?;
    let pending = reconciler.apply(Config(2));
    tokio::pin!(pending);
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    let drain = controller.shutdown(Duration::from_secs(1));
    gate.add_permits(1);
    assert!(matches!(
        pending.await,
        Err(ApplyError::Publication(ControllerError::ShuttingDown))
    ));
    assert!(reconciler.last_applied().is_none());
    drain.await?;
    Ok(())
}

#[tokio::test]
async fn dispatched_request_completes_after_replacement() -> TestResult {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let service_entered = Arc::clone(&entered);
    let service_release = Arc::clone(&release);
    let initial = GraphDraft::new(tower::service_fn(move |()| {
        let entered = Arc::clone(&service_entered);
        let release = Arc::clone(&service_release);
        async move {
            entered.notify_one();
            release.acquire().await?.forget();
            Ok::<_, tokio::sync::AcquireError>(1)
        }
    }))
    .validate()?;
    let controller = Arc::new(Control::new(
        Netplane::new(initial, RuntimeLimits { max_outstanding: 8 })?,
        ControllerOptions {
            max_retained_versions: 0,
        },
    ));
    let reconciler = Reconciler::new(
        Arc::clone(&controller),
        Preparer {
            calls: Arc::new(AtomicUsize::new(0)),
            gate: None,
            candidate_owners: Arc::new(Ownership::default()),
        },
    );
    let handle = controller.handle();
    let request = tokio::spawn(async move { handle.execute(()).await });
    entered.notified().await;
    reconciler.apply(Config(2)).await?;
    assert_eq!(controller.handle().execute(()).await?, 2);
    release.add_permits(1);
    assert_eq!(request.await??, 1);
    Ok(())
}

#[tokio::test]
async fn rollback_generation_invalidates_noop() -> TestResult {
    let controller = Arc::new(Control::new(
        Netplane::new(graph(1)?, RuntimeLimits { max_outstanding: 8 })?,
        ControllerOptions::default(),
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let reconciler = Reconciler::new(
        Arc::clone(&controller),
        Preparer {
            calls: Arc::clone(&calls),
            gate: None,
            candidate_owners: Arc::new(Ownership::default()),
        },
    );
    reconciler.apply(Config(2)).await?;
    let recorded = reconciler.last_applied().ok_or("missing record")?;
    controller.publish_if_current(
        controller.current_token().ok_or("missing token")?,
        graph(3)?,
    )?;
    let rollback = controller.rollback_if_current(
        controller.current_token().ok_or("missing token")?,
        recorded.published,
    )?;
    assert_ne!(rollback.published, recorded.published);
    assert_eq!(controller.handle().execute(()).await?, 2);
    assert!(matches!(
        reconciler.apply(Config(2)).await?,
        ApplyOutcome::Applied(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn publication_and_record_complete_in_same_poll() -> TestResult {
    let (controller, reconciler, _, gate, _) = setup(true)?;
    let mut pending = Box::pin(reconciler.apply(Config(2)));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    gate.add_permits(1);
    let completed =
        std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx))).await;
    let std::task::Poll::Ready(Ok(ApplyOutcome::Applied(outcome))) = completed else {
        return Err("publication must finish with its record in one poll".into());
    };
    drop(pending);
    assert_eq!(
        controller.inspect().active.ok_or("missing active")?.id,
        outcome.published
    );
    let record = reconciler.last_applied().ok_or("missing record")?;
    assert_eq!(record.published, outcome.published);
    assert_eq!(record.desired, Config(2));
    Ok(())
}

#[tokio::test]
async fn competing_reconcilers_reject_losing_publication() -> TestResult {
    let (controller, first, first_calls, first_gate, _) = setup(true)?;
    let second_calls = Arc::new(AtomicUsize::new(0));
    let second_gate = Arc::new(Semaphore::new(0));
    let second = Reconciler::new(
        Arc::clone(&controller),
        Preparer {
            calls: Arc::clone(&second_calls),
            gate: Some(Arc::clone(&second_gate)),
            candidate_owners: Arc::new(Ownership::default()),
        },
    );
    let initial = controller.inspect().active.ok_or("missing active")?.id;
    let mut first_apply = Box::pin(first.apply(Config(2)));
    let mut second_apply = Box::pin(second.apply(Config(3)));
    for pending in [&mut first_apply, &mut second_apply] {
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx)))
                .await
                .is_pending()
        );
    }
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        controller.inspect().active.ok_or("missing active")?.id,
        initial
    );
    first_gate.add_permits(1);
    let ApplyOutcome::Applied(winner) = first_apply.await? else {
        return Err("expected first reconciler to publish".into());
    };
    second_gate.add_permits(1);
    assert!(matches!(second_apply.await,
        Err(ApplyError::Publication(ControllerError::Stale { expected, actual }))
            if expected == initial && actual == winner.published));
    assert!(second.last_applied().is_none());
    assert_eq!(
        first.last_applied().ok_or("missing record")?.published,
        winner.published
    );
    assert_eq!(
        controller.inspect().active.ok_or("missing active")?.id,
        winner.published
    );
    assert_eq!(controller.handle().execute(()).await?, 2);
    Ok(())
}
