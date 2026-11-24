use std::num::NonZeroUsize;

/// Explicit opt-in for one logical Kubernetes routing destination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceBinding {
    /// Namespace containing the Service.
    pub namespace: String,
    /// Service name; discovery does not expose other Services through this binding.
    pub service_name: String,
    /// Virtual Service port to resolve to `EndpointSlice` target ports.
    pub port: PortSelector,
    /// Per-endpoint concurrent exchanges and bounded queue capacity.
    pub backend_capacity: NonZeroUsize,
}

/// Select a virtual Service port, never a Pod target port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PortSelector {
    /// Service port name (matched to `EndpointSlice` port names).
    Name(String),
    /// Virtual numeric Service port (resolved to its name before matching slices).
    Number(u16),
}

/// Equal-weight immutable endpoint selection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EndpointSelection {
    /// FNV-1a hash of the caller's stable routing key modulo endpoint count.
    #[default]
    UniformHash,
}
