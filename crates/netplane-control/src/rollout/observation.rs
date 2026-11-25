use super::BackendGroupId;
use netplane::VersionId;
use std::{future::Future, time::Duration};
use tokio::time::Instant;

/// Attribution and full cumulative window requested from an observation source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservationRequest {
    /// Monotonically increasing window identity, also changed on resume.
    pub epoch: u64,
    /// Execution publication selected at request admission, never completion time.
    pub publication: VersionId,
    /// Inclusive beginning, after matching publication confirmation.
    pub since: Instant,
    /// Required cumulative window end.
    pub until: Instant,
    /// Exactly the stable and canary groups being evaluated.
    pub groups: [BackendGroupId; 2],
}

/// Complete cumulative group samples, with N10 eligibility identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupObservation {
    /// Logical group selected inside the request's pinned execution version.
    pub group: BackendGroupId,
    /// Completed responses and service errors; excludes admission rejection and cancellation.
    pub completed_requests: u64,
    /// Subset of completions classified as failures by the application's documented policy.
    pub failed_requests: u64,
    /// Accepted-to-terminal p95 over all completions; absent if incomplete.
    pub p95_latency: Option<Duration>,
    /// Current eligible members, sampled from N10 rather than duplicated health tracking.
    pub eligible_backends: usize,
    /// Changes whenever membership or eligibility materially changes, even if counts agree.
    pub eligibility_revision: u64,
}

/// Provider evidence. `request` must match exactly and `complete` covers the whole window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RolloutObservation {
    /// Exact attribution and window covered by these counts.
    pub request: ObservationRequest,
    /// False when samples were lost, delayed or only partially available.
    pub complete: bool,
    /// Membership or N10 eligibility changed anywhere inside this window, including
    /// before the first poll. A true value forces a fresh observation epoch.
    pub eligibility_changed: bool,
    /// Exactly one entry per requested group, with no duplicates.
    pub groups: Vec<GroupObservation>,
}

/// Application-supplied source; the driver owns at most one dropped-on-timeout future.
///
/// Providers must attribute requests by accepted execution publication and selected
/// group. Cancellations and admission rejections are excluded from both counts;
/// failures include service errors and application-defined unsuccessful responses.
/// Latency includes queueing. Document response failure classification at integration.
/// No independently spawned operation may outlive future drop without caller cleanup.
pub trait ObservationProvider {
    /// Fetch a complete cumulative window or report unavailable evidence.
    fn observe(
        &self,
        request: ObservationRequest,
    ) -> impl Future<Output = Result<RolloutObservation, ObservationUnavailable>> + Send;
}

/// Bounded source failure; unavailability can never authorize advancement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("rollout observations unavailable")]
pub struct ObservationUnavailable;
