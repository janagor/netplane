//! Explicit typed desired-state reconciliation over Netplane's conditional publisher.
//!
//! Applications normalize configurations before submission and implement
//! [`GraphPreparer`] for policy validation and asynchronous service construction.
//! There is no background task, retry policy, or configuration schema.

#![forbid(unsafe_code)]

mod desired;
mod error;
mod prepare;
mod reconciler;

pub use desired::{ApplyOutcome, LastApplied};
pub use error::ApplyError;
pub use prepare::GraphPreparer;
pub use reconciler::Reconciler;
