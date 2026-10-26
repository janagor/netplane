//! Capacity and validation error helpers for graph declarations.

use super::GraphBuildError;
use crate::{Capacity, ConfigError};

pub(super) const fn validate_capacity(capacity: Capacity) -> Result<(), ConfigError> {
    if capacity.max_in_flight == 0 {
        return Err(ConfigError::ZeroConcurrency);
    }
    if capacity.buffer_capacity == 0 {
        return Err(ConfigError::ZeroBuffer);
    }
    Ok(())
}
impl GraphBuildError {
    pub(super) const fn kind(&self) -> &'static str {
        match self {
            Self::MissingEntry => "missing_entry",
            Self::DuplicateNode(_) => "duplicate_node",
            Self::UnknownEntry(_) => "unknown_entry",
            Self::UnknownTarget(_) => "unknown_target",
            Self::DuplicateRoute(_) => "duplicate_route",
            Self::DuplicateFallback(_) => "duplicate_fallback",
            Self::MissingFallback => "missing_fallback",
            Self::UnsupportedTopology(_) => "unsupported_topology",
            Self::Capacity(error) => crate::config_error_kind(error),
            Self::ConflictingOuterCapacity => "conflicting_outer_capacity",
        }
    }
}
