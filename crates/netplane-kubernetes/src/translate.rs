use std::{collections::BTreeMap, net::SocketAddr, num::NonZeroUsize};

use crate::{
    DiscoveryConfig, DiscoverySnapshot, PortSelector, ServiceBinding, ServiceKey, TransportProtocol,
};

/// Service lifetime identity, independent of resource versions and slice names.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceId {
    /// Namespaced Service reference.
    pub key: ServiceKey,
    /// Kubernetes UID when known; absent for an authoritatively deleted Service.
    pub uid: Option<String>,
}

/// Stable destination identity. A known Pod UID change creates a new identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EndpointId {
    /// Owning Service lifetime.
    pub service: ServiceId,
    /// Direct Pod address and `EndpointSlice` target port.
    pub address: SocketAddr,
    /// Kubernetes target object UID, when supplied.
    pub target_uid: Option<String>,
}

impl EndpointId {
    /// Instance health identity independent of slices, routing weights and versions.
    ///
    /// Service and Pod UIDs distinguish known replacements at a reused address.
    /// Without a target UID, replacement at the same address cannot be detected
    /// until discovery supplies a different identity.
    #[must_use]
    pub fn health_identity(&self) -> netplane::health::BackendIdentity {
        netplane::health::BackendIdentity {
            service: format!("{}/{}", self.service.key.namespace, self.service.key.name),
            instance: self.address.to_string(),
            incarnation: format!(
                "{}:{}",
                self.service.uid.as_deref().unwrap_or("unknown-service"),
                self.target_uid.as_deref().unwrap_or("unknown-target")
            ),
        }
    }
}

/// Configuration for a bounded backend, without live execution resources.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveredBackend {
    /// Stable identity across slice recreation and redistribution.
    pub id: EndpointId,
    /// Positive concurrency and queue capacity for this backend.
    pub capacity: NonZeroUsize,
}

/// Normalized application-owned desired state for `netplane-control` semantic equality.
///
/// An empty set is valid and publishes an unavailable destination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesiredState {
    service: ServiceId,
    backends: Vec<DiscoveredBackend>,
}
impl DesiredState {
    /// Service lifetime represented by this configuration.
    #[must_use]
    pub const fn service(&self) -> &ServiceId {
        &self.service
    }
    /// Sorted, unique direct destinations. No discovery revision participates in equality.
    #[must_use]
    pub fn backends(&self) -> &[DiscoveredBackend] {
        &self.backends
    }
}

/// An invalid binding or unsupported Service policy. Active graphs remain usable.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TranslationError {
    /// Namespace, Service name, or selector is invalid.
    #[error("invalid Service binding")]
    InvalidBinding,
    /// No virtual Service port matches the configured selector.
    #[error("configured Service port was not found")]
    PortNotFound,
    /// More than one Service port matches the selector.
    #[error("configured Service port is ambiguous")]
    AmbiguousPort,
    /// Only TCP forwarding is supported.
    #[error("selected Service port is not TCP")]
    UnsupportedProtocol,
    /// Kubernetes readiness is overridden by this Service policy.
    #[error("publishNotReadyAddresses is unsupported")]
    PublishNotReadyAddresses,
    /// Duplicate destinations disagree about target identity.
    #[error("destination has conflicting target identities")]
    ConflictingIdentity,
}

/// Translate a complete N7 snapshot into normalized desired state.
///
/// N7 resolves actual ports, validates IP families, merges duplicate readiness,
/// and filters stale Service owner references before this step. Invalid addresses
/// and missing slice ports are excluded there with tracing diagnostics.
///
/// # Errors
/// Returns a typed error for invalid bindings, unsupported readiness policy or
/// protocols, unresolved Service ports, or conflicting target identities.
/// A missing Service and an empty eligible set are successful unavailable states.
pub fn translate(
    snapshot: &DiscoverySnapshot,
    binding: &ServiceBinding,
) -> Result<DesiredState, TranslationError> {
    DiscoveryConfig {
        namespace: binding.namespace.clone(),
        service_names: [binding.service_name.clone()].into(),
    }
    .validate()
    .map_err(|_| TranslationError::InvalidBinding)?;
    if matches!(&binding.port, PortSelector::Name(name) if name.is_empty())
        || matches!(binding.port, PortSelector::Number(0))
    {
        return Err(TranslationError::InvalidBinding);
    }
    let key = ServiceKey {
        namespace: binding.namespace.clone(),
        name: binding.service_name.clone(),
    };
    let Some(service) = snapshot.services.get(&key) else {
        return Ok(DesiredState {
            service: ServiceId { key, uid: None },
            backends: vec![],
        });
    };
    if service.publish_not_ready_addresses {
        return Err(TranslationError::PublishNotReadyAddresses);
    }
    let mut ports = service.ports.iter().filter(|port| match &binding.port {
        PortSelector::Name(name) => port.name.as_ref() == Some(name),
        PortSelector::Number(number) => port.port == *number,
    });
    let selected = ports.next().ok_or(TranslationError::PortNotFound)?;
    if ports.next().is_some() {
        return Err(TranslationError::AmbiguousPort);
    }
    if selected.protocol != TransportProtocol::Tcp {
        return Err(TranslationError::UnsupportedProtocol);
    }
    let service_id = ServiceId {
        key,
        uid: (!service.uid.is_empty()).then(|| service.uid.clone()),
    };
    // Merge before eligibility so a duplicate cannot override a negative readiness signal.
    let mut destinations = BTreeMap::new();
    for endpoint in &service.endpoints {
        if endpoint.protocol != TransportProtocol::Tcp
            || endpoint.port_name != selected.name
            || endpoint.port == 0
        {
            continue;
        }
        let address = SocketAddr::new(endpoint.address, endpoint.port);
        let (uid, ready) = destinations
            .entry(address)
            .or_insert_with(|| (endpoint.target_uid.clone(), true));
        if *uid != endpoint.target_uid {
            return Err(TranslationError::ConflictingIdentity);
        }
        *ready &= endpoint.is_ready();
    }
    let backends = destinations
        .into_iter()
        .filter(|(_, (_, ready))| *ready)
        .map(|(address, (target_uid, _))| DiscoveredBackend {
            id: EndpointId {
                service: service_id.clone(),
                address,
                target_uid,
            },
            capacity: binding.backend_capacity,
        })
        .collect();
    Ok(DesiredState {
        service: service_id,
        backends,
    })
}
