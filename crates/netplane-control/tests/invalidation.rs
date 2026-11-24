//! Invalidation freezes publication without stopping the reconciliation worker.
use netplane::{
    ControllerOptions, GraphDraft, GraphVersion, Netplane, ReconfigurationController, RuntimeLimits,
};
use netplane_control::{
    DriverStatus, GraphPreparer, Reconciler, ReconciliationDriver, UpdatePolicy,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Semaphore, watch};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct Preparer {
    entered: Arc<Semaphore>,
    gate: Arc<Semaphore>,
    released: Arc<Semaphore>,
}
struct Owner(Arc<Semaphore>);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}
fn graph(value: usize) -> Result<GraphVersion<(), usize>> {
    Ok(GraphDraft::new(tower::service_fn(move |()| async move {
        Ok::<_, std::convert::Infallible>(value)
    }))
    .validate()?)
}
impl GraphPreparer<usize, (), usize> for Preparer {
    type Error = netplane::ConfigError;
    fn validate(&self, _: &usize) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
    async fn prepare(
        &self,
        desired: usize,
    ) -> std::result::Result<GraphVersion<(), usize>, Self::Error> {
        let owner = Owner(self.released.clone());
        let candidate = GraphDraft::new(tower::service_fn(move |()| {
            let _ = &owner;
            async move { Ok::<_, std::convert::Infallible>(desired) }
        }))
        .validate()?;
        self.entered.add_permits(1);
        if let Ok(permit) = self.gate.acquire().await {
            permit.forget();
        }
        Ok(candidate)
    }
}
async fn invalidated(
    status: &mut watch::Receiver<Arc<DriverStatus<netplane::ConfigError>>>,
    sequence: u64,
) -> Result {
    loop {
        if matches!(status.borrow_and_update().as_ref(), DriverStatus::Invalidated(current) if *current == sequence)
        {
            return Ok(());
        }
        status.changed().await?;
    }
}
#[tokio::test]
async fn invalidate_in_progress_candidate_releases_resources_and_recovery_applies_latest() -> Result
{
    tokio::time::timeout(Duration::from_secs(5), async {
        let controller = Arc::new(ReconfigurationController::new(
            Netplane::new(graph(1)?, RuntimeLimits { max_outstanding: 8 })?,
            ControllerOptions {
                max_retained_versions: 0,
            },
        ));
        let entered = Arc::new(Semaphore::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let released = Arc::new(Semaphore::new(0));
        let (driver, updates) = ReconciliationDriver::new(
            Reconciler::new(
                controller.clone(),
                Preparer {
                    entered: entered.clone(),
                    gate: gate.clone(),
                    released: released.clone(),
                },
            ),
            UpdatePolicy {
                debounce: Duration::ZERO,
                ..UpdatePolicy::default()
            },
        )?;
        let mut status = driver.status();
        let worker = tokio::spawn(driver.run(|_| false));
        assert_eq!(updates.submit(2)?, 1);
        entered.acquire().await?.forget();
        let before = controller.inspect().active.ok_or("active")?.id;
        let sequence = updates.invalidate_pending()?;
        assert_eq!(sequence, 2);
        assert!(!updates.status().is_converged());
        gate.add_permits(1);
        invalidated(&mut status, sequence).await?;
        released.acquire().await?.forget();
        assert_eq!(controller.inspect().active.ok_or("active")?.id, before);
        assert_eq!(controller.handle().execute(()).await?, 1);
        assert_eq!(updates.submit(3)?, 3);
        entered.acquire().await?.forget();
        gate.add_permits(1);
        loop {
            if matches!(
                status.borrow_and_update().as_ref(),
                DriverStatus::Applied(3, _)
            ) {
                break;
            }
            status.changed().await?;
        }
        assert_eq!(controller.handle().execute(()).await?, 3);
        let sequence = updates.invalidate_pending()?;
        invalidated(&mut status, sequence).await?;
        assert_eq!(controller.handle().execute(()).await?, 3);
        updates.shutdown();
        worker.await?;
        assert!(updates.invalidate_pending().is_err());
        controller.shutdown(Duration::from_secs(1)).await?;
        Ok(())
    })
    .await?
}

struct InvalidateOnPublication {
    updates: netplane_control::UpdateSender<usize>,
    entered: Arc<Semaphore>,
}
struct PublicationEvent(bool);
impl tracing::field::Visit for PublicationEvent {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "event" && format!("{value:?}") == "\"reconfiguration.published\"" {
            self.0 = true;
        }
    }
}
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for InvalidateOnPublication {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut publication = PublicationEvent(false);
        event.record(&mut publication);
        if publication.0 {
            let updates = self.updates.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            let invalidation = std::thread::spawn(move || {
                let _ = tx.send(updates.invalidate_pending());
            });
            // This must finish before the publication callback returns. A retained
            // mailbox guard would block it, even though the atomic swap is complete.
            assert!(matches!(rx.recv_timeout(Duration::from_secs(1)), Ok(Ok(_))));
            assert!(invalidation.join().is_ok());
            self.entered.add_permits(1);
        }
    }
}
#[tokio::test]
async fn publication_releases_freshness_guard_before_subscriber_callbacks() -> Result {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::layer::SubscriberExt;
    tokio::time::timeout(Duration::from_secs(5), async {
        let controller = Arc::new(ReconfigurationController::new(
            Netplane::new(graph(1)?, RuntimeLimits { max_outstanding: 8 })?,
            ControllerOptions::default(),
        ));
        let (driver, updates) = ReconciliationDriver::new(
            Reconciler::new(
                controller.clone(),
                Preparer {
                    entered: Arc::new(Semaphore::new(0)),
                    gate: Arc::new(Semaphore::new(1)),
                    released: Arc::new(Semaphore::new(0)),
                },
            ),
            UpdatePolicy {
                debounce: Duration::ZERO,
                ..UpdatePolicy::default()
            },
        )?;
        let mut status = driver.status();
        let entered = Arc::new(Semaphore::new(0));
        let subscriber = tracing_subscriber::registry().with(InvalidateOnPublication {
            updates: updates.clone(),
            entered: entered.clone(),
        });
        let worker = tokio::spawn(driver.run(|_| false).with_subscriber(subscriber));
        updates.submit(2)?;
        entered.acquire().await?.forget();
        invalidated(&mut status, 2).await?;
        assert_eq!(controller.handle().execute(()).await?, 2); // swap preceded invalidation
        assert!(!updates.status().is_converged());
        updates.shutdown();
        worker.await?;
        controller.shutdown(Duration::from_secs(1)).await?;
        Ok(())
    })
    .await?
}

struct ContendedPreparer(Arc<tokio::sync::Barrier>);
impl GraphPreparer<usize, (), usize> for ContendedPreparer {
    type Error = netplane::ConfigError;
    fn validate(&self, _: &usize) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
    async fn prepare(
        &self,
        desired: usize,
    ) -> std::result::Result<GraphVersion<(), usize>, Self::Error> {
        let candidate = GraphDraft::new(tower::service_fn(move |()| async move {
            Ok::<_, std::convert::Infallible>(desired)
        }))
        .validate()?;
        self.0.wait().await;
        Ok(candidate)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalidation_and_publication_contend_without_late_commits() -> Result {
    tokio::time::timeout(Duration::from_secs(10), async {
        let controller = Arc::new(ReconfigurationController::new(
            Netplane::new(graph(0)?, RuntimeLimits { max_outstanding: 8 })?,
            ControllerOptions {
                max_retained_versions: 0,
            },
        ));
        let boundary = Arc::new(tokio::sync::Barrier::new(2));
        let (driver, updates) = ReconciliationDriver::new(
            Reconciler::new(controller.clone(), ContendedPreparer(boundary.clone())),
            UpdatePolicy {
                debounce: Duration::ZERO,
                ..UpdatePolicy::default()
            },
        )?;
        let mut status = driver.status();
        let worker = tokio::spawn(driver.run(|_| false));
        for desired in 1..=256 {
            let before = controller.inspect().active.ok_or("active")?.id;
            updates.submit(desired)?;
            let (producer, runtime, rendezvous) =
                (updates.clone(), controller.clone(), boundary.clone());
            let invalidator = tokio::spawn(async move {
                rendezvous.wait().await;
                // Vary which contender gets more executor time after rendezvous.
                if desired % 2 == 0 {
                    tokio::task::yield_now().await;
                }
                let sequence = producer.invalidate_pending()?;
                let at_return = runtime.inspect().active.ok_or("active")?.id;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((sequence, at_return))
            });
            let (sequence, at_return) = invalidator.await??;
            invalidated(&mut status, sequence).await?;
            assert_eq!(
                controller.inspect().active.ok_or("active")?.id,
                at_return,
                "candidate committed after invalidation returned in round {desired}"
            );
            let response = controller.handle().execute(()).await?;
            if at_return == before {
                assert!(response < desired);
            } else {
                assert_eq!(response, desired);
            }
        }
        updates.shutdown();
        worker.await?;
        controller.shutdown(Duration::from_secs(1)).await?;
        Ok(())
    })
    .await?
}
