use netplane::VersionId;

/// Local accepted-update sequence, independent of publication generations.
pub type Revision = u64;

/// Current reconciliation activity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconciliationPhase {
    /// No work remains for the last accepted update.
    Idle,
    /// An accepted update awaits preparation.
    Pending,
    /// Validation or candidate construction is in progress.
    Preparing,
    /// A bounded backoff precedes the next attempt.
    RetryScheduled,
    /// A permanent or exhausted failure requires another submission.
    Blocked,
    /// The driver has exited, including forced cancellation.
    Stopped,
}

/// Bounded failure category; never contains application error text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconciliationFailure {
    /// Application policy rejected the configuration.
    Validation,
    /// Candidate construction failed.
    Preparation,
    /// Conditional publication conflicted with another publisher.
    Conflict,
    /// Another explicit application owns the preparation permit.
    Busy,
    /// The controller rejected publication.
    Publication,
    /// No further local revision numbers are available.
    RevisionExhausted,
    /// The application future was dropped before completion.
    Cancelled,
}

/// Internally consistent observation of one reconciler, separate from runtime metrics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconciliationStatus {
    /// Latest locally accepted update, including semantically equal resubmissions.
    pub desired_revision: Option<Revision>,
    /// Last update successfully published or deduplicated.
    pub applied_revision: Option<Revision>,
    /// Last observed publication; external writers can subsequently replace it.
    pub active_publication: Option<VersionId>,
    /// Current activity of the latest desired revision.
    pub phase: ReconciliationPhase,
    /// Retries scheduled after the initial attempt, saturating at `u32::MAX`.
    pub retry_attempt: u32,
    /// Most recent failure, cleared on a new desired update or success.
    pub last_failure: Option<ReconciliationFailure>,
    /// Logical transition sequence, scoped to this reconciler and saturating at `u64::MAX`.
    pub event_sequence: u64,
}

impl ReconciliationStatus {
    /// Whether the last desired revision was applied and no reconciliation remains.
    ///
    /// Assumes exclusive publication through this reconciler. External publications
    /// are not monitored, and this says nothing about backend health.
    #[must_use]
    pub fn is_converged(&self) -> bool {
        self.desired_revision.is_some()
            && self.desired_revision == self.applied_revision
            && self.phase == ReconciliationPhase::Idle
    }
}
