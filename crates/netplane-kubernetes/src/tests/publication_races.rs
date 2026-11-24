use super::{
    Result,
    integration::{CountPreparer, applied, binding, disconnect, graph, submitted, waiting},
    service, slice,
    support::{Harness, bounded},
};
use crate::{IntegrationStatus, KubernetesIntegration};
use kube::runtime::watcher::Event;
use netplane::{ControllerOptions, Netplane, ReconfigurationController, RuntimeLimits};
use netplane_control::{DriverStatus, Reconciler, ReconciliationDriver, UpdatePolicy};
use std::{
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_observation_supersedes_preparing_candidate() -> Result {
    bounded(exercise(true)).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unhealthy_observation_supersedes_preparing_candidate() -> Result {
    bounded(exercise(false)).await
}

async fn exercise(rejected: bool) -> Result {
    let harness = Harness::initialized().await?;
    let controller = Arc::new(ReconfigurationController::new(
        Netplane::new(graph(10)?, RuntimeLimits { max_outstanding: 8 })?,
        ControllerOptions {
            max_retained_versions: 0,
        },
    ));
    let before = controller.inspect().active.ok_or("active")?.id;
    let gate = Arc::new(Semaphore::new(0));
    let started = Arc::new(Semaphore::new(0));
    let (driver, updates) = ReconciliationDriver::new(
        Reconciler::new(
            controller.clone(),
            CountPreparer {
                attempts: Arc::new(AtomicUsize::new(0)),
                gate: gate.clone(),
                started: started.clone(),
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
    submitted(&mut status, 1).await?;
    started.acquire().await?.forget();
    if rejected {
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
    } else {
        disconnect(&harness).await?;
        waiting(&mut status).await?;
    }
    gate.add_permits(1);
    loop {
        if matches!(
            driver_status.borrow_and_update().as_ref(),
            DriverStatus::Invalidated(_)
        ) {
            break;
        }
        driver_status.changed().await?;
    }
    assert_eq!(controller.inspect().active.ok_or("active")?.id, before);
    assert_eq!(
        controller
            .handle()
            .execute(())
            .await
            .map_err(|error| error.to_string())?,
        10
    );
    // Recovery must use the most recent snapshot, including changes while suspended.
    let mut latest = slice()?;
    latest.endpoints[0].addresses = vec!["10.0.0.2".into(), "10.0.0.3".into()];
    harness.slices.send(Ok(Event::Apply(latest))).await?;
    harness.services.send(Ok(Event::Apply(service()?))).await?;
    let revision = harness.handle.snapshot().ok_or("snapshot")?.revision;
    let sequence = submitted(&mut status, revision).await?;
    started.acquire().await?.forget();
    gate.add_permits(1);
    applied(&mut driver_status, sequence).await?;
    assert_eq!(
        controller
            .handle()
            .execute(())
            .await
            .map_err(|error| error.to_string())?,
        2
    );
    stop.cancel();
    bridge.await??;
    worker.await?;
    controller.shutdown(Duration::from_secs(1)).await?;
    harness.stop().await
}

#[tokio::test]
async fn coalesced_unhealthy_observation_at_same_revision_does_not_invalidate_candidate() -> Result
{
    bounded(async {
        let harness = Harness::initialized().await?;
        let controller = Arc::new(ReconfigurationController::new(
            Netplane::new(graph(10)?, RuntimeLimits { max_outstanding: 8 })?,
            ControllerOptions { max_retained_versions: 0 },
        ));
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(Semaphore::new(0));
        let (driver, updates) = ReconciliationDriver::new(Reconciler::new(controller.clone(), CountPreparer {
            attempts: Arc::new(AtomicUsize::new(0)), gate: gate.clone(), started: started.clone(),
        }), UpdatePolicy { debounce: Duration::ZERO, ..UpdatePolicy::default() })?;
        let mut driver_status = driver.status();
        let worker = tokio::spawn(driver.run(|_| false));
        let integration = KubernetesIntegration::new(&harness.handle, binding(), updates.clone());
        let status = integration.status();
        let stop = CancellationToken::new();
        let mut bridge = Box::pin(integration.run(stop.clone()));
        assert!(futures_util::poll!(bridge.as_mut()).is_pending());
        started.acquire().await?.forget();
        let sequence = updates.status().desired_revision.ok_or("desired revision")?;
        let revision = harness.handle.snapshot().ok_or("snapshot")?.revision;
        // Keep the bridge unpolled until both observations have been produced.
        disconnect(&harness).await?;
        harness.services.send(Ok(Event::Apply(service()?))).await?;
        let observations = harness.handle.subscribe_observations();
        let recovered = observations.borrow().clone();
        assert_eq!(recovered.status.services, crate::WatchStatus::Watching);
        assert_eq!(recovered.status.endpoint_slices, crate::WatchStatus::Watching);
        assert_eq!(recovered.snapshot.as_ref().map(|snapshot| snapshot.revision), Some(revision));
        assert!(futures_util::poll!(bridge.as_mut()).is_pending());
        assert_eq!(updates.status().desired_revision, Some(sequence));
        assert!(matches!(*status.borrow(), IntegrationStatus::Submitted { sequence: current, .. } if current == sequence));
        gate.add_permits(1);
        applied(&mut driver_status, sequence).await?;
        assert_eq!(controller.handle().execute(()).await.map_err(|error| error.to_string())?, 1);
        stop.cancel(); bridge.await?; worker.await?;
        controller.shutdown(Duration::from_secs(1)).await?;
        harness.stop().await
    }).await
}
