//! Explicit typed desired-state reconciliation over Netplane's conditional publisher.
//!
//! Applications normalize configurations before submission and implement
//! [`GraphPreparer`] for policy validation and asynchronous service construction.
//! An optional caller-owned driver coalesces updates and retries classified failures.

#![forbid(unsafe_code)]

mod desired;
mod driver;
mod error;
mod policy;
mod prepare;
mod reconciler;
mod updates;

pub use desired::{ApplyOutcome, LastApplied};
pub use driver::{DriverStatus, ReconciliationDriver};
pub use error::ApplyError;
pub use policy::{InvalidUpdatePolicy, UpdatePolicy};
pub use prepare::GraphPreparer;
pub use reconciler::Reconciler;
pub use updates::{DesiredUpdate, UpdateError, UpdateSender};
