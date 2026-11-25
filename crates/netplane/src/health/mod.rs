//! Optional backend health, independent of graph membership and publication.
//!
//! Keep a registry across publications and retain handles in routing policies and
//! backend middleware. Reuse an identity only for the same physical incarnation.
//! Cooldowns use Tokio's monotonic clock and advance on reads or observations.

mod layer;
mod registry;
mod scheduler;
mod state;

pub use layer::{HealthLayer, HealthService, ObservationFuture, OutcomeClassifier};
pub use registry::{HealthHandle, HealthRegistry, ObservationToken};
pub use scheduler::{
    ActiveHealthConfig, HealthProbe, HealthScheduler, HealthSchedulerHandle, ProbeError,
    ProbeIdentity, ProbeRegistration, SchedulerStatus,
};
pub use state::{
    BackendHealthSnapshot, BackendIdentity, HealthError, HealthMode, HealthOutcome, HealthPolicy,
    HealthSnapshot, HealthState, HealthSummary, ObservationSource,
};
