use super::{RoutingError, WeightedRouting};
use crate::health::HealthHandle;

/// Immutable weighted membership with live per-instance eligibility handles.
///
/// Bind a handle for every configured label, including zero-weight targets.
/// Clones and retained graphs keep their health records alive. Health updates
/// do not publish graphs, change weights, retry, or move selected requests.
#[derive(Clone, Debug)]
pub struct HealthAwareRouting {
    policy: WeightedRouting,
    handles: Box<[HealthHandle]>,
}

/// Health handles do not match the weighted policy's labels.
#[derive(Debug, thiserror::Error)]
#[error("health routing requires exactly one handle per configured target")]
pub struct HealthRoutingConfigError;

impl HealthAwareRouting {
    /// Bind graph-local labels to health records before constructing workers.
    ///
    /// # Errors
    /// Returns an error for missing, duplicate, or unknown labels.
    pub fn try_new(
        policy: WeightedRouting,
        handles: impl IntoIterator<Item = (String, HealthHandle)>,
    ) -> Result<Self, HealthRoutingConfigError> {
        let mut by_name = std::collections::BTreeMap::new();
        for (name, handle) in handles {
            if by_name.insert(name, handle).is_some() {
                return Err(HealthRoutingConfigError);
            }
        }
        let ordered = policy
            .targets()
            .map(|name| by_name.remove(name).ok_or(HealthRoutingConfigError))
            .collect::<Result<Box<[_]>, _>>()?;
        if !by_name.is_empty() {
            return Err(HealthRoutingConfigError);
        }
        Ok(Self {
            policy,
            handles: ordered,
        })
    }

    /// Select from current eligibility within this immutable membership.
    ///
    /// # Errors
    /// Returns [`RoutingError::NoEligibleBackends`] if no positive weight is eligible.
    pub fn select(&self, key: &[u8]) -> Result<&str, RoutingError> {
        // select_eligible visits positive targets once, in configuration order.
        let mut handles = self
            .policy
            .targets
            .iter()
            .zip(&self.handles)
            .filter(|(target, _)| target.weight > 0)
            .map(|(_, handle)| handle);
        self.policy.select_eligible(key, |_| {
            handles.next().is_some_and(HealthHandle::is_eligible)
        })
    }

    /// Iterate graph labels, including zero-weight targets, in configured order.
    pub fn targets(&self) -> impl Iterator<Item = &str> {
        self.policy.targets()
    }
}
