//! Guarded progressive traffic policy over the existing reconciliation mailbox.
//!
//! The caller owns both worker futures and shuts them down with the runtime.
//! No rollout task, metrics engine, retry loop or request-path lock is installed.
//! Failure, pause and cancellation retain the last committed traffic allocation.
mod audit;
mod decision;
mod driver;
mod evaluation;
mod handle;
mod machine;
mod observation;
mod spec;
mod worker;

pub use driver::{GroupPolicy, RolloutDriver, TrafficConfiguration};
pub use evaluation::{ConditionViolation, EvidenceReason, StageEvaluation, evaluate};
pub use handle::{RolloutError, RolloutHandle};
pub use machine::{RolloutAction, RolloutEvent, RolloutMachine, RolloutState, RolloutStatus};
pub use observation::{
    GroupObservation, ObservationProvider, ObservationRequest, ObservationUnavailable,
    RolloutObservation,
};
pub use spec::{BackendGroupId, Condition, RolloutSpec, Stage, ValidationError};
