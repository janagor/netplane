use netplane::ControllerError;
use thiserror::Error;

/// Failed reconciliation; the call did not replace the active graph.
#[derive(Debug, Error)]
pub enum ApplyError<E> {
    /// A driver observed a newer update or stop request before publication.
    #[error("desired state was superseded")]
    Superseded,
    /// No further local revision numbers are available.
    #[error("reconciliation revision exhausted")]
    RevisionExhausted,
    /// Another call on this reconciler holds the preparation permit.
    #[error("reconciliation is already in progress")]
    Busy,
    /// Lightweight application validation rejected the desired state.
    #[error("desired-state validation failed: {0}")]
    Validation(#[source] E),
    /// Asynchronous graph construction failed.
    #[error("graph preparation failed: {0}")]
    Preparation(#[source] E),
    /// Conditional publication failed, including stale tokens and shutdown.
    #[error("conditional publication failed: {0}")]
    Publication(#[source] ControllerError),
}
