use netplane::{ReconfigurationOutcome, VersionId};

/// Historical record of the most recent successful application.
///
/// An external publication can supersede this record. Its generation identifies
/// a runtime publication, not a configuration revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LastApplied<D> {
    /// Application configuration, compared using its semantic equality.
    pub desired: D,
    /// Generation created by successful reconciliation.
    pub published: VersionId,
}

/// Successful result of one explicit reconciliation call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// A newly prepared graph replaced the inspected publication.
    Applied(ReconfigurationOutcome),
    /// Successful no-op at the observed publication generation.
    ///
    /// Equal desired state named the active publication at inspection. Another
    /// publisher may replace it before this result reaches the caller; this
    /// outcome does not guarantee current convergence.
    Unchanged(VersionId),
}
