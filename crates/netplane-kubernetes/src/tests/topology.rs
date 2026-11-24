use super::*;
use crate::{state::ResourceCache, topology::build};
use kube::runtime::watcher::Event;

#[test]
fn normalized_destinations_and_conditions() -> Result {
    let mut services = ResourceCache::default();
    services.apply(Event::Apply(service()?));
    let mut slices = ResourceCache::default();
    let first = slice()?;
    slices.apply(Event::Apply(first.clone()));
    let mut duplicate = first;
    duplicate.metadata.name = Some("slice-2".into());
    duplicate.endpoints[0].conditions = Some(serde_json::from_value(
        json!({"ready":false,"terminating":true}),
    )?);
    slices.apply(Event::Apply(duplicate));
    let topology = build(&services, &slices, &config());
    let discovered = topology.values().next().ok_or("missing service")?;
    assert_eq!(discovered.ports[0].port, 80);
    assert_eq!(discovered.endpoints.len(), 1);
    assert_eq!(discovered.endpoints[0].port, 8080);
    assert!(!discovered.endpoints[0].is_ready());
    Ok(())
}
#[test]
fn filters_ownership_and_invalid_entries() -> Result {
    let mut services = ResourceCache::default();
    let mut svc = service()?;
    services.apply(Event::Apply(svc.clone()));
    let mut slices = ResourceCache::default();
    let mut endpoints = slice()?;
    endpoints.endpoints[0].addresses = vec!["bad".into(), "::1".into(), "10.0.0.1".into()];
    slices.apply(Event::Apply(endpoints.clone()));
    assert_eq!(
        build(&services, &slices, &config())
            .values()
            .next()
            .ok_or("missing")?
            .endpoints
            .len(),
        1
    );
    svc.metadata.uid = Some("uid-2".into());
    services.apply(Event::Apply(svc.clone()));
    assert!(
        build(&services, &slices, &config())
            .values()
            .next()
            .ok_or("missing")?
            .endpoints
            .is_empty()
    );
    let mut filtered = config();
    filtered.service_names.insert("other".into());
    assert!(build(&services, &slices, &filtered).is_empty());
    filtered = config();
    filtered.namespace = "other".into();
    assert!(build(&services, &slices, &filtered).is_empty());
    svc.spec.as_mut().ok_or("spec")?.type_ = Some("ExternalName".into());
    services.apply(Event::Apply(svc));
    assert!(build(&services, &slices, &config()).is_empty());
    Ok(())
}
#[test]
fn ports_ipv6_and_unspecified_readiness() -> Result {
    let mut svc = service()?;
    svc.spec.as_mut().ok_or("spec")?.ports = Some(serde_json::from_value(json!([
        {"name":"http","port":80}, {"name":"dns","port":53,"protocol":"UDP"}, {"name":"sctp","port":90,"protocol":"SCTP"}
    ]))?);
    let mut services = ResourceCache::default();
    services.apply(Event::Apply(svc));
    let mut endpoints = slice()?;
    endpoints.address_type = "IPv6".into();
    endpoints.ports = Some(serde_json::from_value(json!([
        {"name":"http","port":8080}, {"name":"dns","port":5353,"protocol":"UDP"}, {"name":"sctp","port":9090,"protocol":"SCTP"},
        {"name":"unknown","port":9999}, {"name":"http"}, {"name":"http","port":0}, {"name":"http","port":70000}, {"name":"http","port":8000,"protocol":"BAD"}
    ]))?);
    endpoints.endpoints[0].addresses = vec!["::1".into()];
    endpoints.endpoints[0].conditions = None;
    let mut slices = ResourceCache::default();
    slices.apply(Event::Apply(endpoints.clone()));
    let expected = build(&services, &slices, &config());
    let discovered = expected.values().next().ok_or("missing")?;
    assert_eq!(discovered.endpoints.len(), 3);
    assert!(
        discovered
            .endpoints
            .iter()
            .all(crate::DiscoveredEndpoint::is_ready)
    );
    endpoints.ports.as_mut().ok_or("ports")?.reverse();
    slices.apply(Event::Apply(endpoints));
    assert_eq!(expected, build(&services, &slices, &config()));
    Ok(())
}
#[test]
fn staging_replaces_atomically_and_deletes_match_uid() -> Result {
    let mut cache = ResourceCache::default();
    cache.apply(Event::Init);
    cache.apply(Event::InitApply(service()?));
    assert!(!cache.synchronized());
    assert!(cache.committed.is_empty());
    cache.apply(Event::InitDone);
    assert!(cache.synchronized());
    cache.apply(Event::Init);
    assert_eq!(cache.committed.len(), 1);
    cache.apply(Event::InitApply(service()?));
    cache.apply(Event::Init); // restarted relist discards partial staging
    cache.apply(Event::InitDone);
    assert!(cache.committed.is_empty());
    let mut recreated = service()?;
    recreated.metadata.uid = Some("new".into());
    cache.apply(Event::Apply(recreated.clone()));
    cache.apply(Event::Delete(service()?));
    assert_eq!(cache.committed.len(), 1);
    cache.apply(Event::Delete(recreated));
    assert!(cache.committed.is_empty());
    Ok(())
}

#[test]
fn slice_aggregation_and_order_ignore_resource_metadata() -> Result {
    let mut services = ResourceCache::default();
    services.apply(Event::Apply(service()?));
    let mut slices = ResourceCache::default();
    let mut first = slice()?;
    first.endpoints[0].addresses = vec!["10.0.0.2".into(), "10.0.0.1".into()];
    let mut second = first.clone();
    second.metadata.name = Some("slice-2".into());
    second.endpoints[0].addresses = vec!["10.0.0.3".into(), "10.0.0.2".into()];
    slices.apply(Event::Apply(second.clone()));
    slices.apply(Event::Apply(first.clone()));
    let expected = build(&services, &slices, &config());
    assert_eq!(
        expected.values().next().ok_or("service")?.endpoints.len(),
        3
    );
    first.endpoints[0].addresses.reverse();
    first.metadata.resource_version = Some("another-version".into());
    first.metadata.annotations = Some(std::collections::BTreeMap::from([(
        "irrelevant".into(),
        "value".into(),
    )]));
    slices.apply(Event::Apply(first));
    assert_eq!(expected, build(&services, &slices, &config()));
    slices.apply(Event::Delete(second));
    assert_eq!(
        build(&services, &slices, &config())
            .values()
            .next()
            .ok_or("service")?
            .endpoints
            .len(),
        2
    );
    Ok(())
}

#[test]
fn missing_owner_is_supported_and_policy_hint_preserves_conditions() -> Result {
    let mut svc = service()?;
    svc.spec.as_mut().ok_or("spec")?.publish_not_ready_addresses = Some(true);
    let mut services = ResourceCache::default();
    services.apply(Event::Apply(svc));
    let mut endpoints = slice()?;
    endpoints.metadata.owner_references = None;
    endpoints.endpoints[0].conditions = Some(serde_json::from_value(
        json!({"ready":true,"serving":true,"terminating":true}),
    )?);
    let mut slices = ResourceCache::default();
    slices.apply(Event::Apply(endpoints));
    let topology = build(&services, &slices, &config());
    let service = topology.values().next().ok_or("service")?;
    assert!(service.publish_not_ready_addresses);
    assert_eq!(service.endpoints[0].conditions.serving, Some(true));
    assert!(!service.endpoints[0].is_ready());
    Ok(())
}

#[test]
fn unnamed_ports_and_headless_destinations() -> Result {
    let mut svc = service()?;
    let spec = svc.spec.as_mut().ok_or("spec")?;
    spec.cluster_ip = Some("None".into());
    spec.ports.as_mut().ok_or("ports")?[0].name = None;
    let mut endpoint_slice = slice()?;
    endpoint_slice.ports.as_mut().ok_or("ports")?[0].name = None;
    let mut services = ResourceCache::default();
    let mut slices = ResourceCache::default();
    services.apply(Event::Apply(svc));
    slices.apply(Event::Apply(endpoint_slice));
    let topology = build(&services, &slices, &config());
    let service = topology.values().next().ok_or("service")?;
    assert_eq!(service.ports[0].name, None);
    assert_eq!(service.endpoints.len(), 1);
    assert_eq!(service.endpoints[0].port_name, None);
    assert_eq!(service.endpoints[0].port, 8080);
    Ok(())
}

#[test]
fn duplicate_conditions_are_independent_of_slice_insertion_order() -> Result {
    let mut services = ResourceCache::default();
    services.apply(Event::Apply(service()?));
    let first = slice()?;
    let mut second = slice()?;
    second.metadata.name = Some("second".into());
    second.endpoints[0].conditions = Some(serde_json::from_value(
        json!({"ready":false,"serving":false,"terminating":true}),
    )?);
    let mut forward = ResourceCache::default();
    let mut reverse = ResourceCache::default();
    forward.apply(Event::Apply(first.clone()));
    forward.apply(Event::Apply(second.clone()));
    reverse.apply(Event::Apply(second.clone()));
    reverse.apply(Event::Apply(first.clone()));
    let expected = build(&services, &forward, &config());
    assert_eq!(expected, build(&services, &reverse, &config()));
    // Swapping names reverses the BTreeMap traversal order as well.
    let first_name = first.metadata.name.clone();
    let mut first = first;
    first.metadata.name.clone_from(&second.metadata.name);
    second.metadata.name = first_name;
    reverse.apply(Event::Apply(first));
    reverse.apply(Event::Apply(second));
    assert_eq!(expected, build(&services, &reverse, &config()));
    let conditions = &expected.values().next().ok_or("service")?.endpoints[0].conditions;
    assert_eq!(conditions.ready, Some(false));
    assert_eq!(conditions.serving, Some(false));
    assert_eq!(conditions.terminating, Some(true));
    Ok(())
}

#[test]
fn cache_reports_duplicate_and_unmatched_delete_as_unchanged() -> Result {
    let mut cache = ResourceCache::default();
    assert!(!cache.apply(Event::Init));
    assert!(!cache.apply(Event::InitApply(service()?)));
    assert!(cache.apply(Event::InitDone));
    assert!(!cache.apply(Event::Apply(service()?)));
    let mut stale = service()?;
    stale.metadata.uid = Some("stale".into());
    assert!(!cache.apply(Event::Delete(stale)));
    assert!(cache.apply(Event::Delete(service()?)));
    assert!(!cache.apply(Event::Delete(service()?)));
    Ok(())
}

#[test]
fn port_name_matching_normalizes_missing_and_empty_names() -> Result {
    for (service_name, slice_name, matches) in [
        (None, None, true),
        (None, Some(""), true),
        (Some(""), None, true),
        (Some(""), Some(""), true),
        (Some("http"), Some("http"), true),
        (Some("http"), Some("https"), false),
    ] {
        let mut svc = service()?;
        svc.spec
            .as_mut()
            .ok_or("spec")?
            .ports
            .as_mut()
            .ok_or("ports")?[0]
            .name = service_name.map(str::to_owned);
        let mut endpoint_slice = slice()?;
        endpoint_slice.ports.as_mut().ok_or("ports")?[0].name = slice_name.map(str::to_owned);
        let mut services = ResourceCache::default();
        let mut slices = ResourceCache::default();
        services.apply(Event::Apply(svc));
        slices.apply(Event::Apply(endpoint_slice));
        let topology = build(&services, &slices, &config());
        let discovered = topology.values().next().ok_or("service")?;
        let expected_name = service_name
            .filter(|name| !name.is_empty())
            .map(str::to_owned);
        assert_eq!(discovered.ports[0].name, expected_name);
        assert_eq!(
            discovered.endpoints.len(),
            usize::from(matches),
            "Service {service_name:?}, slice {slice_name:?}"
        );
        if matches {
            assert_eq!(discovered.endpoints[0].port_name, expected_name);
        }
    }
    Ok(())
}

#[test]
fn equivalent_unnamed_destinations_deduplicate_and_merge_conditions() -> Result {
    let mut svc = service()?;
    svc.spec
        .as_mut()
        .ok_or("spec")?
        .ports
        .as_mut()
        .ok_or("ports")?[0]
        .name = None;
    let mut first = slice()?;
    first.ports.as_mut().ok_or("ports")?[0].name = None;
    let mut second = first.clone();
    second.metadata.name = Some("second".into());
    second.ports.as_mut().ok_or("ports")?[0].name = Some(String::new());
    second.endpoints[0].conditions = Some(serde_json::from_value(json!({"ready":false}))?);
    let mut services = ResourceCache::default();
    let mut slices = ResourceCache::default();
    services.apply(Event::Apply(svc));
    slices.apply(Event::Apply(first));
    slices.apply(Event::Apply(second));
    let topology = build(&services, &slices, &config());
    let endpoints = &topology.values().next().ok_or("service")?.endpoints;
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].port_name, None);
    assert_eq!(endpoints[0].conditions.ready, Some(false));
    Ok(())
}
