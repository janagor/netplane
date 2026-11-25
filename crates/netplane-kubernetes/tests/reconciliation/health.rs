//! Shared active-check ownership across prepared and retained executable graphs.

use std::{
    future::{Ready, ready},
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use crate::support::{Result, Server, desired, desired_for_uid};
use netplane::health::{
    ActiveHealthConfig, BackendIdentity, HealthMode, HealthOutcome, HealthPolicy, HealthProbe,
    HealthRegistry, HealthScheduler, ProbeError,
};
use netplane_control::GraphPreparer;
use netplane_kubernetes::{HttpGraphPreparer, PreparationError, UpstreamLimits};

struct ReadyProbe;
impl HealthProbe for ReadyProbe {
    type Future = Ready<HealthOutcome>;
    fn identity(&self) -> netplane::health::ProbeIdentity {
        netplane::health::ProbeIdentity {
            configuration_key: "ready-v1".into(),
        }
    }
    fn check(&self) -> Self::Future {
        ready(HealthOutcome::Success)
    }
}

#[tokio::test]
async fn probe_configuration_changes_are_rejected_during_candidate_preparation() -> Result {
    use netplane_http::health::HttpOutcomeClassifier;
    let registry = HealthRegistry::default();
    let scheduler = HealthScheduler::new(ActiveHealthConfig {
        interval: Duration::from_secs(1),
        timeout: Duration::from_secs(1),
        max_concurrent_checks: NonZeroUsize::MIN,
        max_registered_backends: NonZeroUsize::MIN,
    })?;
    let handle = scheduler.handle();
    let policy = HealthPolicy {
        failure_threshold: NonZeroU32::MIN,
        recovery_threshold: NonZeroU32::MIN,
        recovery_delay: Duration::ZERO,
    };
    let preparer = HttpGraphPreparer::new(UpstreamLimits::default(), |_| b"key")
        .with_active_health(registry.clone(), policy.clone(), handle.clone(), "/health");
    let topology = desired(&["127.0.0.1:12345".parse()?])?;
    let original = preparer.prepare(topology.clone()).await?;
    let compatible = preparer.prepare(topology.clone()).await?;
    assert_eq!(handle.status().registered_backends, 1);
    let path_change =
        preparer
            .clone()
            .with_active_health(registry, policy, handle.clone(), "/ready");
    assert!(matches!(
        path_change.prepare(topology.clone()).await,
        Err(PreparationError::Probe(
            ProbeError::IncompatibleProbeConfiguration
        ))
    ));
    let classifier_change =
        preparer.with_health_classifier(HttpOutcomeClassifier::default().with_status(
            http::StatusCode::SERVICE_UNAVAILABLE,
            HealthOutcome::Ignored,
        ));
    assert!(matches!(
        classifier_change.prepare(topology).await,
        Err(PreparationError::Probe(
            ProbeError::IncompatibleProbeConfiguration
        ))
    ));
    assert_eq!(handle.status().registered_backends, 1);
    drop((compatible, original));
    handle.shutdown();
    Ok(())
}

#[tokio::test]
async fn retained_graphs_share_probes_and_failed_candidates_preserve_registration() -> Result {
    tokio::time::timeout(Duration::from_secs(5), async {
        let server = Server::start("healthy", false).await?;
        let registry = HealthRegistry::default();
        let scheduler = HealthScheduler::new(ActiveHealthConfig {
            interval: Duration::from_millis(1),
            timeout: Duration::from_secs(1),
            max_concurrent_checks: NonZeroUsize::MIN,
            max_registered_backends: NonZeroUsize::MIN,
        })?;
        let scheduler_handle = scheduler.handle();
        let policy = HealthPolicy {
            failure_threshold: NonZeroU32::MIN,
            recovery_threshold: NonZeroU32::MIN,
            recovery_delay: Duration::ZERO,
        };
        let preparer = HttpGraphPreparer::new(UpstreamLimits::default(), |_| b"key")
            .with_active_health(
                registry.clone(),
                policy.clone(),
                scheduler_handle.clone(),
                "/health",
            );
        let original = preparer.prepare(desired(&[server.address])?).await?;
        assert_eq!(scheduler_handle.status().registered_backends, 1);
        let scheduler_task = tokio::spawn(scheduler.run());
        server.entered.acquire().await?.forget();
        let candidate = preparer.prepare(desired(&[server.address])?).await?;
        assert_eq!(scheduler_handle.status().registered_backends, 1);
        drop(candidate);
        server.entered.acquire().await?.forget();
        // Replacement needs an independent slot. Failed preparation leaves the
        // original registration and its already executable graph untouched.
        assert!(matches!(
            preparer
                .prepare(desired_for_uid(&[server.address], "replacement")?)
                .await,
            Err(PreparationError::Probe(ProbeError::AtCapacity))
        ));
        server.entered.acquire().await?.forget();
        let empty = preparer.prepare(desired(&[])?).await?;
        assert_eq!(scheduler_handle.status().registered_backends, 1);
        // Current membership may be empty while a retained old graph owns checks.
        server.entered.acquire().await?.forget();
        drop((original, empty));
        let other = registry.register_with_mode(
            BackendIdentity {
                service: "other".into(),
                instance: "other".into(),
                incarnation: "other".into(),
            },
            policy,
            HealthMode::Active,
        )?;
        let registration = loop {
            match scheduler_handle.register(other.clone(), ReadyProbe) {
                Ok(registration) => break registration,
                Err(ProbeError::AtCapacity) => tokio::task::yield_now().await,
                Err(error) => return Err(error.into()),
            }
        };
        // Acquiring the sole slot proves the last graph registration was reclaimed.
        drop(registration);
        scheduler_handle.shutdown();
        scheduler_task.await?;
        assert_eq!(scheduler_handle.status().running_checks, 0);
        assert_eq!(registry.snapshot().summary.active_probes, 0);
        server.stop().await?;
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn reconciliation_waits_for_history_capacity_then_converges() -> Result {
    use netplane::{ControllerOptions, Netplane, ReconfigurationController, RuntimeLimits};
    use netplane_control::{ApplyError, Reconciler};
    use std::sync::Arc;

    tokio::time::timeout(Duration::from_secs(5), async {
        let server = Server::start("healthy", false).await?;
        let registry = HealthRegistry::default();
        let scheduler = HealthScheduler::new(ActiveHealthConfig {
            interval: Duration::from_millis(1),
            timeout: Duration::from_secs(1),
            max_concurrent_checks: NonZeroUsize::MIN,
            max_registered_backends: NonZeroUsize::MIN,
        })?;
        let scheduler_handle = scheduler.handle();
        let preparer = HttpGraphPreparer::new(UpstreamLimits::default(), |_| b"key")
            .with_active_health(
                registry,
                HealthPolicy {
                    failure_threshold: NonZeroU32::MIN,
                    recovery_threshold: NonZeroU32::MIN,
                    recovery_delay: Duration::ZERO,
                },
                scheduler_handle.clone(),
                "/health",
            );
        let runtime = Netplane::new(
            preparer.prepare(desired(&[])?).await?,
            RuntimeLimits { max_outstanding: 4 },
        )?;
        let controller = Arc::new(ReconfigurationController::new(
            runtime,
            ControllerOptions {
                max_retained_versions: 1,
            },
        ));
        let reconciler = Reconciler::new(controller.clone(), preparer.clone());
        let task = tokio::spawn(scheduler.run());
        reconciler.apply(desired(&[server.address])?).await?;
        let replacement = desired_for_uid(&[server.address], "replacement")?;
        let publication = controller.inspect().active.ok_or("active")?.id;
        // Repeating an update cannot release resources owned by the active graph.
        for _ in 0..2 {
            assert!(matches!(
                reconciler.apply(replacement.clone()).await,
                Err(ApplyError::Preparation(PreparationError::Probe(
                    ProbeError::AtCapacity
                )))
            ));
            assert_eq!(controller.inspect().active.ok_or("active")?.id, publication);
        }
        reconciler.apply(desired(&[])?).await?;
        // Publishing empty membership still retains the previous graph in history.
        assert_eq!(scheduler_handle.status().registered_backends, 1);
        assert!(matches!(
            reconciler.apply(replacement.clone()).await,
            Err(ApplyError::Preparation(PreparationError::Probe(
                ProbeError::AtCapacity
            )))
        ));
        controller.publish_if_current(
            controller.current_token().ok_or("token")?,
            preparer.prepare(desired(&[])?).await?,
        )?;
        while scheduler_handle.status().registered_backends != 0 {
            tokio::task::yield_now().await;
        }
        // Eviction released the old registration; the same desired update now succeeds.
        reconciler.apply(replacement).await?;
        assert_eq!(scheduler_handle.status().registered_backends, 1);
        controller.shutdown(Duration::from_secs(1)).await?;
        scheduler_handle.shutdown();
        task.await?;
        assert_eq!(scheduler_handle.status().registered_backends, 0);
        assert_eq!(scheduler_handle.status().running_checks, 0);
        server.stop().await?;
        Ok(())
    })
    .await?
}
