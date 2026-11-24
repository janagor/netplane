//! Kubernetes discovery and explicit endpoint-to-runtime reconciliation.
//! Listings are complete per collection, but not transactional across collections.
#![forbid(unsafe_code)]

mod config;
mod discovery;
mod model;
mod state;
mod topology;

pub use config::{DiscoveryConfig, DiscoveryError};
pub use discovery::{Discovery, DiscoveryHandle};
pub use model::*;

#[cfg(test)]
mod tests;

mod binding;
mod reconcile;
mod translate;
mod upstream;
pub use binding::{EndpointSelection, PortSelector, ServiceBinding};
pub use reconcile::{IntegrationError, IntegrationStatus, KubernetesIntegration};
pub use translate::{
    DesiredState, DiscoveredBackend, EndpointId, ServiceId, TranslationError, translate,
};
pub use upstream::{
    HttpGraphPreparer, PreparationError, Unavailable, UpstreamLimits, map_http_error,
};
