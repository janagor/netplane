use super::{Result, config, service, slice};
use crate::{
    DesiredState, DiscoverySnapshot, HttpGraphPreparer, PortSelector, ServiceBinding,
    UpstreamLimits, state::ResourceCache, topology, translate,
};
use bytes::Bytes;
use http::{Request, StatusCode};
use kube::runtime::watcher::Event;
use netplane::{
    Netplane, RuntimeLimits,
    health::{HealthOutcome, HealthPolicy, HealthRegistry, HealthState},
};
use netplane_control::GraphPreparer;
use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

fn desired(
    uid: &str,
    ready: bool,
) -> std::result::Result<DesiredState, Box<dyn std::error::Error>> {
    let mut services = ResourceCache::default();
    services.apply(Event::Apply(service()?));
    let mut endpoint_slice = slice()?;
    endpoint_slice.endpoints[0].target_ref = Some(serde_json::from_value(
        serde_json::json!({"kind":"Pod", "uid":uid}),
    )?);
    endpoint_slice.endpoints[0].conditions =
        Some(serde_json::from_value(serde_json::json!({"ready":ready}))?);
    let mut slices = ResourceCache::default();
    slices.apply(Event::Apply(endpoint_slice));
    Ok(translate(
        &DiscoverySnapshot {
            revision: 1,
            services: topology::build(&services, &slices, &config()),
        },
        &ServiceBinding {
            namespace: "default".into(),
            service_name: "backend".into(),
            port: PortSelector::Number(80),
            backend_capacity: NonZeroUsize::MIN,
        },
    )?)
}

fn policy() -> HealthPolicy {
    HealthPolicy {
        failure_threshold: NonZeroU32::MIN,
        recovery_threshold: NonZeroU32::MIN,
        recovery_delay: Duration::from_secs(60),
    }
}

#[tokio::test]
async fn unchanged_identity_survives_preparation_and_replacement_is_independent() -> Result {
    let registry = HealthRegistry::default();
    let preparer = HttpGraphPreparer::new(UpstreamLimits::default(), |_| b"key")
        .with_health(registry.clone(), policy());
    let original = desired("pod-old", true)?;
    let identity = original.backends()[0].id.health_identity();
    let health = registry.register(identity.clone(), policy())?;
    health.report(health.begin_observation(), HealthOutcome::Failure);
    let old_graph = preparer.prepare(original.clone()).await?;
    let replacement_graph = preparer.prepare(original).await?;
    assert_eq!(
        registry
            .backend_snapshot(&identity)
            .ok_or("snapshot")?
            .state,
        HealthState::Unhealthy
    );
    let next = desired("pod-new", true)?;
    let next_identity = next.backends()[0].id.health_identity();
    assert_ne!(identity, next_identity);
    let next_graph = preparer.prepare(next).await?;
    assert_eq!(
        registry
            .backend_snapshot(&next_identity)
            .ok_or("new snapshot")?
            .state,
        HealthState::Healthy
    );
    assert_eq!(registry.snapshot().summary.tracked_backends, 2);
    drop((old_graph, replacement_graph, next_graph));
    Ok(())
}

#[tokio::test]
async fn no_healthy_endpoints_maps_to_503_without_publishing_or_overriding_readiness() -> Result {
    let registry = HealthRegistry::default();
    let preparer = HttpGraphPreparer::new(UpstreamLimits::default(), |_| b"key")
        .with_health(registry.clone(), policy());
    let topology = desired("pod", true)?;
    let health = registry.register(topology.backends()[0].id.health_identity(), policy())?;
    health.report(health.begin_observation(), HealthOutcome::Failure);
    let graph = preparer.prepare(topology).await?;
    let mut runtime = Netplane::new(graph, RuntimeLimits { max_outstanding: 4 })?;
    let publication = runtime.active_version_id();
    let error = runtime
        .handle()
        .execute(Request::new(Bytes::new()))
        .await
        .err()
        .ok_or("expected unavailable")?;
    assert_eq!(
        crate::map_http_error(&error).status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(runtime.active_version_id(), publication);
    let unready = desired("pod", false)?;
    assert!(unready.backends().is_empty());
    runtime.publish(preparer.prepare(unready).await?)?;
    let error = runtime
        .handle()
        .execute(Request::new(Bytes::new()))
        .await
        .err()
        .ok_or("expected unavailable")?;
    assert_eq!(
        crate::map_http_error(&error).status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
