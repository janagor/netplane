use std::collections::BTreeSet;

/// Namespace and optional allowlist. An empty allowlist selects every Service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryConfig {
    /// Exactly one namespace; cluster-wide discovery is unsupported.
    pub namespace: String,
    /// Service names to retain, or empty for all names.
    pub service_names: BTreeSet<String>,
}

impl DiscoveryConfig {
    pub(crate) fn validate(&self) -> Result<(), DiscoveryError> {
        if !dns_label(&self.namespace) || self.service_names.iter().any(|s| !dns_label(s)) {
            return Err(DiscoveryError::InvalidScope);
        }
        Ok(())
    }
}

fn dns_label(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        && s.as_bytes()[0].is_ascii_alphanumeric()
        && s.as_bytes()[s.len() - 1].is_ascii_alphanumeric()
}

/// Local configuration and terminal discovery failures. Watch errors are retried.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    /// Namespace or service name is not a Kubernetes DNS label.
    #[error("namespace and service names must be nonempty DNS labels")]
    InvalidScope,
    /// A watcher unexpectedly stopped; the last snapshot remains available.
    #[error("{0} watch stream terminated")]
    StreamEnded(&'static str),
    /// The local monotonically increasing revision cannot advance.
    #[error("discovery revision exhausted")]
    RevisionExhausted,
}
