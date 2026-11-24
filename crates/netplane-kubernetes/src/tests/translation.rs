use super::{Result, config, service, slice};
use crate::{
    DiscoverySnapshot, PortSelector, ServiceBinding, TranslationError, state::ResourceCache,
    topology, translate,
};
use kube::runtime::watcher::Event;
use std::num::NonZeroUsize;

#[test]
fn slices_resolve_per_pod_ports_and_preserve_target_lifetimes() -> Result {
    let mut services = ResourceCache::default();
    services.apply(Event::Apply(service()?));
    let mut first = slice()?;
    first.endpoints[0].target_ref = Some(serde_json::from_value(
        serde_json::json!({"kind":"Pod","uid":"pod-a"}),
    )?);
    let mut second = first.clone();
    second.metadata.name = Some("another-slice".into());
    second.ports.as_mut().ok_or("ports")?[0].port = Some(8081);
    second.endpoints[0].target_ref.as_mut().ok_or("target")?.uid = Some("pod-b".into());
    let mut slices = ResourceCache::default();
    slices.apply(Event::Apply(first.clone()));
    slices.apply(Event::Apply(second.clone()));
    let binding = ServiceBinding {
        namespace: "default".into(),
        service_name: "backend".into(),
        port: PortSelector::Number(80),
        backend_capacity: NonZeroUsize::MIN,
    };
    let snapshot = DiscoverySnapshot {
        revision: 1,
        services: topology::build(&services, &slices, &config()),
    };
    let expected = translate(&snapshot, &binding)?;
    assert_eq!(expected.backends().len(), 2);
    assert_eq!(expected.backends()[0].id.address.port(), 8080);
    assert_eq!(expected.backends()[1].id.address.port(), 8081);
    assert_eq!(
        expected.backends()[0].id.target_uid.as_deref(),
        Some("pod-a")
    );
    // Slice recreation/redistribution never enters endpoint identity.
    slices.apply(Event::Delete(first.clone()));
    first.metadata.name = Some("replacement".into());
    first.metadata.uid = Some("new-slice-uid".into());
    slices.apply(Event::Apply(first.clone()));
    assert_eq!(
        expected,
        translate(
            &DiscoverySnapshot {
                revision: 9,
                services: topology::build(&services, &slices, &config())
            },
            &binding
        )?
    );
    first.endpoints[0].target_ref.as_mut().ok_or("target")?.uid = Some("pod-replacement".into());
    slices.apply(Event::Apply(first));
    assert_ne!(
        expected,
        translate(
            &DiscoverySnapshot {
                revision: 10,
                services: topology::build(&services, &slices, &config())
            },
            &binding
        )?
    );
    Ok(())
}

#[test]
fn conflicts_and_invalid_endpoints_are_safely_excluded() -> Result {
    let mut services = ResourceCache::default();
    services.apply(Event::Apply(service()?));
    let mut first = slice()?;
    first.endpoints[0].target_ref =
        Some(serde_json::from_value(serde_json::json!({"uid":"pod-a"}))?);
    let mut conflict = first.clone();
    conflict.metadata.name = Some("conflict".into());
    conflict.endpoints[0]
        .target_ref
        .as_mut()
        .ok_or("target")?
        .uid = Some("pod-b".into());
    let mut slices = ResourceCache::default();
    slices.apply(Event::Apply(first.clone()));
    slices.apply(Event::Apply(conflict.clone()));
    let topology = topology::build(&services, &slices, &config());
    assert!(!topology.values().next().ok_or("service")?.endpoints[0].is_ready());
    let binding = ServiceBinding {
        namespace: "default".into(),
        service_name: "backend".into(),
        port: PortSelector::Name("http".into()),
        backend_capacity: NonZeroUsize::MIN,
    };
    assert!(
        translate(
            &DiscoverySnapshot {
                revision: 1,
                services: topology
            },
            &binding
        )?
        .backends()
        .is_empty()
    );
    slices.apply(Event::Delete(conflict));
    first.endpoints[0].addresses = vec!["malformed".into()];
    slices.apply(Event::Apply(first.clone()));
    assert!(
        topology::build(&services, &slices, &config())
            .values()
            .next()
            .ok_or("service")?
            .endpoints
            .is_empty()
    );
    first.endpoints[0].addresses = vec!["10.0.0.1".into()];
    first.ports.as_mut().ok_or("ports")?[0].port = None;
    slices.apply(Event::Apply(first));
    assert!(
        topology::build(&services, &slices, &config())
            .values()
            .next()
            .ok_or("service")?
            .endpoints
            .is_empty()
    );
    let mut unsupported = service()?;
    unsupported
        .spec
        .as_mut()
        .ok_or("spec")?
        .ports
        .as_mut()
        .ok_or("ports")?[0]
        .protocol = Some("UDP".into());
    services.apply(Event::Apply(unsupported));
    assert_eq!(
        translate(
            &DiscoverySnapshot {
                revision: 2,
                services: topology::build(&services, &slices, &config())
            },
            &binding
        ),
        Err(TranslationError::UnsupportedProtocol)
    );
    Ok(())
}

#[test]
fn missing_target_uids_merge_with_known_identity_and_external_names_are_excluded() -> Result {
    let mut services = ResourceCache::default();
    services.apply(Event::Apply(service()?));
    let first = slice()?;
    let mut known = first.clone();
    known.metadata.name = Some("known".into());
    known.endpoints[0].target_ref = Some(serde_json::from_value(
        serde_json::json!({"kind":"Pod","uid":"pod-a"}),
    )?);
    let mut slices = ResourceCache::default();
    slices.apply(Event::Apply(first));
    slices.apply(Event::Apply(known));
    let normalized = topology::build(&services, &slices, &config());
    let endpoint = &normalized.values().next().ok_or("service")?.endpoints[0];
    assert!(endpoint.is_ready());
    assert_eq!(endpoint.target_uid.as_deref(), Some("pod-a"));
    let mut external = service()?;
    external.spec.as_mut().ok_or("spec")?.type_ = Some("ExternalName".into());
    external.spec.as_mut().ok_or("spec")?.external_name = Some("external.invalid".into());
    services.apply(Event::Apply(external));
    assert!(topology::build(&services, &slices, &config()).is_empty());
    Ok(())
}
