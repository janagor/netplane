use super::{
    Result, service, slice,
    support::{Harness, bounded},
};
use crate::{DesiredState, IntegrationStatus, KubernetesIntegration, PortSelector, ServiceBinding};
use kube::runtime::watcher::{Error, Event};
use netplane::{
    ControllerOptions, GraphDraft, GraphVersion, Netplane, ReconfigurationController, RuntimeLimits,
};
use netplane_control::{
    DriverStatus, GraphPreparer, Reconciler, ReconciliationDriver, UpdatePolicy,
};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;

pub(super) struct CountPreparer {
    pub(super) attempts: Arc<AtomicUsize>,
    pub(super) gate: Arc<Semaphore>,
    pub(super) started: Arc<Semaphore>,
}
impl GraphPreparer<DesiredState, (), usize> for CountPreparer {
    type Error = netplane::ConfigError;
    fn validate(&self, _: &DesiredState) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
    async fn prepare(
        &self,
        desired: DesiredState,
    ) -> std::result::Result<GraphVersion<(), usize>, Self::Error> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.started.add_permits(1);
        if let Ok(permit) = self.gate.acquire().await {
            permit.forget();
        }
        graph(desired.backends().len())
    }
}
pub(super) fn graph(
    count: usize,
) -> std::result::Result<GraphVersion<(), usize>, netplane::ConfigError> {
    GraphDraft::new(tower::service_fn(move |()| async move {
        Ok::<_, std::convert::Infallible>(count)
    }))
    .validate()
}
pub(super) fn binding() -> ServiceBinding {
    ServiceBinding {
        namespace: "default".into(),
        service_name: "backend".into(),
        port: PortSelector::Name("http".into()),
        backend_capacity: NonZeroUsize::MIN,
    }
}
pub(super) async fn applied(
    status: &mut watch::Receiver<Arc<DriverStatus<netplane::ConfigError>>>,
    sequence: u64,
) -> Result {
    loop {
        if matches!(status.borrow_and_update().as_ref(), DriverStatus::Applied(current, _) if *current == sequence)
        {
            return Ok(());
        }
        status.changed().await?;
    }
}
pub(super) async fn submitted(
    status: &mut watch::Receiver<IntegrationStatus>,
    revision: u64,
) -> std::result::Result<u64, Box<dyn std::error::Error>> {
    loop {
        if let IntegrationStatus::Submitted {
            revision: current,
            sequence,
            ..
        } = *status.borrow_and_update()
        {
            if current == revision {
                return Ok(sequence);
            }
        }
        status.changed().await?;
    }
}
#[tokio::test]
async fn discovery_bridge_coalesces_reconsiders_conflicts_and_stops() -> Result {
    bounded(async {
        let harness = Harness::initialized().await?;
        let controller = Arc::new(ReconfigurationController::new(
            Netplane::new(graph(0)?, RuntimeLimits { max_outstanding: 8 })?,
            ControllerOptions {
                max_retained_versions: 0,
            },
        ));
        let attempts = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(Semaphore::new(0));
        let preparer = CountPreparer {
            attempts: attempts.clone(),
            gate: gate.clone(),
            started: started.clone(),
        };
        let (driver, updates) = ReconciliationDriver::new(
            Reconciler::new(controller.clone(), preparer),
            UpdatePolicy {
                debounce: Duration::ZERO,
                ..UpdatePolicy::default()
            },
        )?;
        let mut status = driver.status();
        let worker = tokio::spawn(driver.run(|_| false));
        let integration = KubernetesIntegration::new(&harness.handle, binding(), updates.clone());
        let mut translated = integration.status();
        let stop = CancellationToken::new();
        let bridge = tokio::spawn(integration.run(stop.clone()));
        submitted(&mut translated, 1).await?;
        started.acquire().await?.forget();
        // A burst arrives while the first candidate is preparing.
        for count in 2..=4 {
            let mut changed = slice()?;
            changed.endpoints[0].addresses =
                (1..=count).map(|index| format!("10.0.0.{index}")).collect();
            harness.slices.send(Ok(Event::Apply(changed))).await?;
        }
        let latest = harness.handle.snapshot().ok_or("snapshot")?.revision;
        let sequence = submitted(&mut translated, latest).await?;
        gate.add_permits(1); // stale first candidate is discarded
        started.acquire().await?.forget();
        // Force an optimistic publication conflict during latest-state preparation.
        let inspected = controller.inspect();
        controller.publish_if_current(inspected.token.ok_or("token")?, graph(99)?)?;
        gate.add_permits(1);
        started.acquire().await?.forget(); // existing driver retries the conflict
        gate.add_permits(1);
        applied(&mut status, sequence).await?;
        assert_eq!(
            controller
                .handle()
                .execute(())
                .await
                .map_err(|error| error.to_string())?,
            4
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        // A metadata-only update does not even create a new snapshot.
        let previous_revision = harness.handle.snapshot().ok_or("snapshot")?.revision;
        let mut same_service = service()?;
        same_service.metadata.resource_version = Some("metadata-only".into());
        harness
            .services
            .send(Ok(Event::Apply(same_service)))
            .await?;
        assert_eq!(
            harness.handle.snapshot().ok_or("snapshot")?.revision,
            previous_revision
        );
        // Authoritative Service deletion becomes empty, not a retained route.
        harness.services.send(Ok(Event::Delete(service()?))).await?;
        let revision = harness.handle.snapshot().ok_or("snapshot")?.revision;
        let sequence = submitted(&mut translated, revision).await?;
        gate.add_permits(1);
        applied(&mut status, sequence).await?;
        assert_eq!(
            controller
                .handle()
                .execute(())
                .await
                .map_err(|error| error.to_string())?,
            0
        );
        stop.cancel();
        bridge.await??;
        worker.await?;
        assert_eq!(*translated.borrow(), IntegrationStatus::Stopped);
        assert!(
            updates
                .submit(crate::translate(
                    harness.handle.snapshot().ok_or("snapshot")?.as_ref(),
                    &binding()
                )?)
                .is_err()
        );
        controller.shutdown(Duration::from_secs(1)).await?;
        harness.stop().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn discovery_recovery_and_translation_errors_retain_active_graph() -> Result {
    bounded(async {
        let harness = Harness::initialized().await?;
        let controller = Arc::new(ReconfigurationController::new(
            Netplane::new(graph(0)?, RuntimeLimits { max_outstanding: 8 })?,
            ControllerOptions::default(),
        ));
        let (driver, updates) = ReconciliationDriver::new(
            Reconciler::new(
                controller.clone(),
                CountPreparer {
                    attempts: Arc::new(AtomicUsize::new(0)),
                    gate: Arc::new(Semaphore::new(100)),
                    started: Arc::new(Semaphore::new(0)),
                },
            ),
            UpdatePolicy {
                debounce: Duration::ZERO,
                ..UpdatePolicy::default()
            },
        )?;
        let mut driver_status = driver.status();
        let worker = tokio::spawn(driver.run(|_| false));
        let integration = KubernetesIntegration::new(&harness.handle, binding(), updates);
        let mut status = integration.status();
        let stop = CancellationToken::new();
        let bridge = tokio::spawn(integration.run(stop.clone()));
        let sequence = submitted(&mut status, 1).await?;
        applied(&mut driver_status, sequence).await?;
        disconnect(&harness).await?;
        waiting(&mut status).await?;
        let publication = controller.inspect().active.ok_or("active")?.id;
        harness.services.send(Ok(Event::Apply(service()?))).await?;
        let sequence = submitted(&mut status, 1).await?;
        applied(&mut driver_status, sequence).await?;
        assert_eq!(controller.inspect().active.ok_or("active")?.id, publication);
        disconnect(&harness).await?;
        // Even if N7 emits a changed complete cache while degraded, N8 holds it.
        let mut changed = slice()?;
        changed.endpoints.clear();
        harness.slices.send(Ok(Event::Apply(changed))).await?;
        waiting(&mut status).await?;
        assert_eq!(
            controller
                .handle()
                .execute(())
                .await
                .map_err(|error| error.to_string())?,
            1
        );
        // A complete Service update restores health but rejects the overridden policy.
        let mut invalid = service()?;
        invalid
            .spec
            .as_mut()
            .ok_or("spec")?
            .publish_not_ready_addresses = Some(true);
        harness.services.send(Ok(Event::Apply(invalid))).await?;
        loop {
            if matches!(
                *status.borrow_and_update(),
                IntegrationStatus::Rejected { .. }
            ) {
                break;
            }
            status.changed().await?;
        }
        assert_eq!(
            controller
                .handle()
                .execute(())
                .await
                .map_err(|error| error.to_string())?,
            1
        );
        harness.services.send(Ok(Event::Apply(service()?))).await?;
        let revision = harness.handle.snapshot().ok_or("snapshot")?.revision;
        let sequence = submitted(&mut status, revision).await?;
        applied(&mut driver_status, sequence).await?;
        assert_eq!(
            controller
                .handle()
                .execute(())
                .await
                .map_err(|error| error.to_string())?,
            0
        );
        stop.cancel();
        bridge.await??;
        worker.await?;
        controller.shutdown(Duration::from_secs(1)).await?;
        harness.stop().await?;
        Ok(())
    })
    .await
}

pub(super) async fn disconnect(harness: &Harness) -> Result {
    harness
        .services
        .send(Err(Error::WatchFailed(kube::Error::Api(
            kube::core::Status::failure("offline", "Disconnected")
                .with_code(503)
                .boxed(),
        ))))
        .await
}
pub(super) async fn waiting(status: &mut watch::Receiver<IntegrationStatus>) -> Result {
    loop {
        if *status.borrow_and_update() == IntegrationStatus::Waiting {
            return Ok(());
        }
        status.changed().await?;
    }
}
