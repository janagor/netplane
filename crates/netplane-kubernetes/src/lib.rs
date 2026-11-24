//! Kubernetes discovery without runtime publication or transport policy.
//! Listings are complete per collection, but not transactional across collections.
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
