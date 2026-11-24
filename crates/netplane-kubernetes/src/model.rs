use std::{collections::BTreeMap, net::IpAddr};

/// Namespaced Service identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceKey {
    /// Kubernetes namespace.
    pub namespace: String,
    /// Service name.
    pub name: String,
}
/// Transport used by a service or endpoint port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransportProtocol {
    /// TCP.
    Tcp,
    /// UDP.
    Udp,
    /// SCTP.
    Sctp,
}
/// A virtual Service port; destination ports come from `EndpointSlices`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServicePort {
    /// Optional port name; absent and empty Kubernetes names normalize to `None`.
    pub name: Option<String>,
    /// Virtual Service port.
    pub port: u16,
    /// Transport protocol.
    pub protocol: TransportProtocol,
    /// Application protocol hint.
    pub app_protocol: Option<String>,
}
/// Original endpoint health signals, conservatively merged for duplicates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EndpointConditions {
    /// Unspecified readiness is eligible by default.
    pub ready: Option<bool>,
    /// Serving signal independent of termination.
    pub serving: Option<bool>,
    /// Termination makes a destination ineligible by default.
    pub terminating: Option<bool>,
}
/// Concrete IP destination, including ineligible endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredEndpoint {
    /// IPv4 or IPv6 address.
    pub address: IpAddr,
    /// Actual destination port.
    pub port: u16,
    /// Service port name; absent and empty Kubernetes names normalize to `None`.
    pub port_name: Option<String>,
    /// Transport protocol.
    pub protocol: TransportProtocol,
    /// Preserved Kubernetes health signals.
    pub conditions: EndpointConditions,
}
impl DiscoveredEndpoint {
    /// Default eligibility: ready or unspecified, and not terminating.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.conditions.ready.unwrap_or(true) && !self.conditions.terminating.unwrap_or(false)
    }
}
/// Normalized Service, retained even when it has no destinations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredService {
    /// Namespaced identity.
    pub key: ServiceKey,
    /// Kubernetes UID, distinguishing recreation.
    pub uid: String,
    /// Sorted virtual ports.
    pub ports: Vec<ServicePort>,
    /// Sorted, deduplicated concrete destinations.
    pub endpoints: Vec<DiscoveredEndpoint>,
    /// Service policy hint; does not override default eligibility.
    pub publish_not_ready_addresses: bool,
}
/// Immutable complete discovery result. Revisions are local, not resource versions
/// or runtime publication generations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoverySnapshot {
    /// Local monotonically increasing revision, starting at one.
    pub revision: u64,
    /// Services in deterministic order.
    pub services: BTreeMap<ServiceKey, DiscoveredService>,
}
/// State of an individual resource watcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchStatus {
    /// Awaiting a complete listing.
    Initializing,
    /// Complete listing followed by watch updates.
    Watching,
    /// Retrying an error or building a replacement listing.
    Recovering,
    /// Explicit cancellation or runner drop.
    Stopped,
    /// Unexpected terminal stream failure.
    Failed,
}
/// Independently observed watcher states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryStatus {
    /// Service watcher state.
    pub services: WatchStatus,
    /// `EndpointSlice` watcher state.
    pub endpoint_slices: WatchStatus,
    /// Most recently recorded local revision. This may briefly lag the snapshot;
    /// the status and snapshot channels are not an atomic observation.
    pub latest_revision: Option<u64>,
}
