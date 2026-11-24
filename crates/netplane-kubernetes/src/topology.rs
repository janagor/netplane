use crate::{
    DiscoveredEndpoint, DiscoveredService, DiscoveryConfig, EndpointConditions, ServiceKey,
    ServicePort, TransportProtocol, state::ResourceCache,
};
use k8s_openapi::api::{core::v1::Service, discovery::v1::EndpointSlice};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
};

fn normalize_port_name(name: Option<&str>) -> Option<String> {
    name.filter(|name| !name.is_empty()).map(str::to_owned)
}

fn protocol(value: Option<&str>) -> Option<TransportProtocol> {
    match value.unwrap_or("TCP") {
        "TCP" => Some(TransportProtocol::Tcp),
        "UDP" => Some(TransportProtocol::Udp),
        "SCTP" => Some(TransportProtocol::Sctp),
        _ => {
            tracing::warn!("unsupported_endpoint: unknown protocol");
            None
        }
    }
}
fn port(value: Option<i32>) -> Option<u16> {
    let port = value.and_then(|p| u16::try_from(p).ok()).filter(|p| *p > 0);
    if port.is_none() {
        tracing::warn!("unsupported_endpoint: missing or invalid port");
    }
    port
}
fn merge(a: Option<bool>, b: Option<bool>, adverse: bool) -> Option<bool> {
    if a == Some(adverse) || b == Some(adverse) {
        Some(adverse)
    } else {
        a.or(b)
    }
}

fn service_topology(
    services: &ResourceCache<Service>,
    config: &DiscoveryConfig,
) -> BTreeMap<ServiceKey, DiscoveredService> {
    let mut result = BTreeMap::new();
    for ((namespace, name), service) in &services.committed {
        if namespace != &config.namespace
            || (!config.service_names.is_empty() && !config.service_names.contains(name))
        {
            continue;
        }
        let Some(spec) = &service.spec else {
            continue;
        };
        if spec.type_.as_deref() == Some("ExternalName") {
            tracing::warn!(service = name, "unsupported Service: ExternalName");
            continue;
        }
        let mut ports: Vec<_> = spec
            .ports
            .iter()
            .flatten()
            .filter_map(|p| {
                Some(ServicePort {
                    name: normalize_port_name(p.name.as_deref()),
                    port: port(Some(p.port))?,
                    protocol: protocol(p.protocol.as_deref())?,
                    app_protocol: p.app_protocol.clone(),
                })
            })
            .collect();
        ports.sort();
        ports.dedup();
        let key = ServiceKey {
            namespace: namespace.clone(),
            name: name.clone(),
        };
        result.insert(
            key.clone(),
            DiscoveredService {
                key,
                uid: service.metadata.uid.clone().unwrap_or_default(),
                ports,
                endpoints: Vec::new(),
                publish_not_ready_addresses: spec.publish_not_ready_addresses.unwrap_or(false),
            },
        );
    }
    result
}

pub fn build(
    services: &ResourceCache<Service>,
    slices: &ResourceCache<EndpointSlice>,
    config: &DiscoveryConfig,
) -> BTreeMap<ServiceKey, DiscoveredService> {
    let mut result = service_topology(services, config);
    let port_index: BTreeMap<_, BTreeSet<_>> = result
        .iter()
        .map(|(key, service)| {
            (
                key.clone(),
                service
                    .ports
                    .iter()
                    .map(|p| (p.name.clone(), p.protocol))
                    .collect(),
            )
        })
        .collect();
    // Accumulate destinations by Service without scanning unrelated resources.
    let mut destinations =
        BTreeMap::<_, BTreeMap<_, (EndpointConditions, BTreeSet<String>)>>::new();
    for ((namespace, _), slice) in &slices.committed {
        let Some(name) = slice
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"))
        else {
            continue;
        };
        let key = ServiceKey {
            namespace: namespace.clone(),
            name: name.clone(),
        };
        let Some(service) = result.get(&key) else {
            continue;
        };
        if slice
            .metadata
            .owner_references
            .iter()
            .flatten()
            .any(|o| o.kind == "Service" && (o.uid != service.uid || o.name != *name))
        {
            continue;
        }
        if !matches!(slice.address_type.as_str(), "IPv4" | "IPv6") {
            tracing::warn!("unsupported_endpoint: address type");
            continue;
        }
        for p in slice.ports.iter().flatten() {
            let (Some(number), Some(transport)) = (port(p.port), protocol(p.protocol.as_deref()))
            else {
                continue;
            };
            let port_name = normalize_port_name(p.name.as_deref());
            if !port_index
                .get(&key)
                .is_some_and(|ports| ports.contains(&(port_name.clone(), transport)))
            {
                continue;
            }
            for endpoint in &slice.endpoints {
                let conditions = endpoint_conditions(endpoint);
                let target_uid = endpoint
                    .target_ref
                    .as_ref()
                    .and_then(|target| target.uid.clone());
                for address in &endpoint.addresses {
                    let Ok(address) = address.parse::<IpAddr>() else {
                        tracing::warn!("unsupported_endpoint: invalid IP");
                        continue;
                    };
                    if address.is_ipv4() != (slice.address_type == "IPv4") {
                        tracing::warn!("unsupported_endpoint: IP family mismatch");
                        continue;
                    }
                    let entries = destinations.entry(key.clone()).or_default();
                    entries
                        .entry((address, number, port_name.clone(), transport))
                        .and_modify(|(old, uids)| {
                            uids.extend(target_uid.clone());
                            old.ready = merge(old.ready, conditions.ready, false);
                            old.serving = merge(old.serving, conditions.serving, false);
                            old.terminating = merge(old.terminating, conditions.terminating, true);
                        })
                        .or_insert_with(|| {
                            (conditions.clone(), target_uid.iter().cloned().collect())
                        });
                }
            }
        }
    }
    for (key, entries) in destinations {
        if let Some(service) = result.get_mut(&key) {
            service.endpoints = entries
                .into_iter()
                .map(|(destination, observation)| discovered_endpoint(destination, observation))
                .collect();
        }
    }
    result
}

fn endpoint_conditions(endpoint: &k8s_openapi::api::discovery::v1::Endpoint) -> EndpointConditions {
    endpoint
        .conditions
        .as_ref()
        .map_or_else(EndpointConditions::default, |c| EndpointConditions {
            ready: c.ready,
            serving: c.serving,
            terminating: c.terminating,
        })
}

fn discovered_endpoint(
    (address, port, port_name, protocol): (IpAddr, u16, Option<String>, TransportProtocol),
    (mut conditions, uids): (EndpointConditions, BTreeSet<String>),
) -> DiscoveredEndpoint {
    if uids.len() > 1 {
        // A destination with conflicting Pod identities is unsafe to route.
        conditions.ready = Some(false);
        tracing::warn!("unsupported_endpoint: conflicting target UIDs");
    }
    DiscoveredEndpoint {
        target_uid: uids.into_iter().next(),
        address,
        port,
        port_name,
        protocol,
        conditions,
    }
}
