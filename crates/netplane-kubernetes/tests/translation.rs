//! Typed Kubernetes topology translation semantics.
use netplane_kubernetes::*;
use std::{collections::BTreeMap, num::NonZeroUsize};

type Result = std::result::Result<(), Box<dyn std::error::Error>>;
fn binding() -> ServiceBinding {
    ServiceBinding {
        namespace: "default".into(),
        service_name: "backend".into(),
        port: PortSelector::Name("http".into()),
        backend_capacity: NonZeroUsize::MIN,
    }
}
fn snapshot() -> DiscoverySnapshot {
    let key = ServiceKey {
        namespace: "default".into(),
        name: "backend".into(),
    };
    DiscoverySnapshot {
        revision: 1,
        services: BTreeMap::from([(
            key.clone(),
            DiscoveredService {
                key,
                uid: "service-1".into(),
                ports: vec![ServicePort {
                    name: Some("http".into()),
                    port: 80,
                    protocol: TransportProtocol::Tcp,
                    app_protocol: None,
                }],
                publish_not_ready_addresses: false,
                endpoints: vec![],
            },
        )]),
    }
}
fn endpoint(
    address: &str,
    port: u16,
) -> std::result::Result<DiscoveredEndpoint, std::net::AddrParseError> {
    Ok(DiscoveredEndpoint {
        address: address.parse()?,
        port,
        port_name: Some("http".into()),
        protocol: TransportProtocol::Tcp,
        conditions: EndpointConditions::default(),
        target_uid: Some("pod-1".into()),
    })
}
#[test]
fn stable_topology_and_endpoint_identities() -> Result {
    let mut snapshot = snapshot();
    let service = snapshot.services.values_mut().next().ok_or("service")?;
    service.endpoints = vec![
        endpoint("10.0.0.2", 8081)?,
        endpoint("10.0.0.1", 8080)?,
        endpoint("10.0.0.1", 8080)?,
    ];
    let desired = translate(&snapshot, &binding())?;
    assert_eq!(desired.backends().len(), 2);
    snapshot.revision += 1;
    snapshot
        .services
        .values_mut()
        .next()
        .ok_or("service")?
        .endpoints
        .reverse();
    assert_eq!(desired, translate(&snapshot, &binding())?);
    let mut numeric = binding();
    numeric.port = PortSelector::Number(80);
    assert_eq!(desired, translate(&snapshot, &numeric)?);
    snapshot.services.values_mut().next().ok_or("service")?.uid = "service-2".into();
    assert_ne!(desired, translate(&snapshot, &binding())?);
    snapshot.services.values_mut().next().ok_or("service")?.uid = "service-1".into();
    for endpoint in &mut snapshot
        .services
        .values_mut()
        .next()
        .ok_or("service")?
        .endpoints
    {
        endpoint.target_uid = Some("pod-2".into());
    }
    assert_ne!(desired, translate(&snapshot, &binding())?);
    Ok(())
}
#[test]
fn readiness_ipv6_and_authoritative_empty() -> Result {
    let mut snapshot = snapshot();
    let service = snapshot.services.values_mut().next().ok_or("service")?;
    let mut unready = endpoint("10.0.0.1", 8080)?;
    unready.conditions.ready = Some(false);
    let mut terminating = endpoint("10.0.0.2", 8080)?;
    terminating.conditions.terminating = Some(true);
    terminating.conditions.serving = Some(true);
    service.endpoints = vec![unready, terminating, endpoint("::1", 8082)?];
    let desired = translate(&snapshot, &binding())?;
    assert_eq!(desired.backends().len(), 1);
    assert_eq!(desired.backends()[0].id.address.to_string(), "[::1]:8082");
    snapshot.services.clear();
    assert!(translate(&snapshot, &binding())?.backends().is_empty());
    Ok(())
}
#[test]
fn unsupported_service_policy_and_port_are_rejected() -> Result {
    let mut snapshot = snapshot();
    snapshot
        .services
        .values_mut()
        .next()
        .ok_or("service")?
        .publish_not_ready_addresses = true;
    assert!(matches!(
        translate(&snapshot, &binding()),
        Err(TranslationError::PublishNotReadyAddresses)
    ));
    snapshot
        .services
        .values_mut()
        .next()
        .ok_or("service")?
        .publish_not_ready_addresses = false;
    let mut binding = binding();
    binding.port = PortSelector::Number(81);
    assert!(matches!(
        translate(&snapshot, &binding),
        Err(TranslationError::PortNotFound)
    ));
    Ok(())
}
