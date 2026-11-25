use super::{Condition, ObservationRequest, RolloutObservation, Stage};

/// Reason a stage cannot advance, without retaining raw sample history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvidenceReason {
    /// Provider did not respond with usable evidence.
    Unavailable,
    /// Wrong publication, epoch, group population, incomplete window or inconsistent counts.
    Attribution,
    /// The minimum post-publication observation duration has not elapsed.
    Duration,
    /// A configured sample floor has not been reached.
    Samples,
    /// Complete latency samples are unavailable.
    Latency,
    /// Membership or N10 eligibility changed; a fresh epoch is required.
    EligibilityChanged,
    /// A compatible desired revision is being reconciled; evidence must restart.
    ConfigurationPending,
    /// An administrator changed the owned weights, or desired state is not converged.
    ConfigurationConflict,
    /// The finite observation deadline expired; explicit resume is required.
    Timeout,
    /// Reconciliation rejected this stage.
    Reconciliation,
}

/// Index of the definite failed condition in the immutable stage specification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConditionViolation {
    /// Zero-based condition index.
    pub condition: usize,
}

/// AND-combined decision. Definite violations take precedence over missing evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageEvaluation {
    /// Every configured condition passed on sufficient evidence.
    Passed,
    /// A condition failed after its own sample floor was met.
    Failed(ConditionViolation),
    /// No authorization to advance.
    Insufficient(EvidenceReason),
}

/// Evaluate attributed evidence without timers, I/O or state mutation.
#[must_use]
pub fn evaluate(
    stage: &Stage,
    request: &ObservationRequest,
    observation: &RolloutObservation,
) -> StageEvaluation {
    use StageEvaluation::{Failed, Insufficient, Passed};
    if observation.request != *request
        || !observation.complete
        || observation.groups.len() != 2
        || request.groups.iter().any(|name| {
            observation
                .groups
                .iter()
                .filter(|g| &g.group == name)
                .count()
                != 1
        })
        || observation
            .groups
            .iter()
            .any(|g| g.failed_requests > g.completed_requests)
    {
        return Insufficient(EvidenceReason::Attribution);
    }
    if observation.eligibility_changed {
        return Insufficient(EvidenceReason::EligibilityChanged);
    }
    let Some(elapsed) = request.until.checked_duration_since(request.since) else {
        return Insufficient(EvidenceReason::Attribution);
    };
    let mut missing = (elapsed < stage.min_observation).then_some(EvidenceReason::Duration);
    for (index, condition) in stage.conditions.iter().enumerate() {
        let Some(group) = observation
            .groups
            .iter()
            .find(|g| g.group == condition.group())
        else {
            return Insufficient(EvidenceReason::Attribution);
        };
        let failed = match condition {
            Condition::Eligible { minimum, .. } => group.eligible_backends < *minimum,
            Condition::Requests { minimum, .. } => {
                if group.completed_requests < *minimum {
                    missing = Some(EvidenceReason::Samples);
                }
                false
            }
            Condition::ErrorRate {
                maximum_basis_points,
                minimum_requests,
                ..
            } => {
                if group.completed_requests < *minimum_requests || group.completed_requests == 0 {
                    missing = Some(EvidenceReason::Samples);
                    false
                } else {
                    u128::from(group.failed_requests) * 10_000
                        > u128::from(group.completed_requests) * u128::from(*maximum_basis_points)
                }
            }
            Condition::P95Latency {
                maximum,
                minimum_requests,
                ..
            } => {
                if group.completed_requests < *minimum_requests || group.completed_requests == 0 {
                    missing = Some(EvidenceReason::Samples);
                    false
                } else if let Some(latency) = group.p95_latency {
                    latency > *maximum
                } else {
                    missing = Some(EvidenceReason::Latency);
                    false
                }
            }
        };
        if failed {
            return Failed(ConditionViolation { condition: index });
        }
    }
    missing.map_or(Passed, Insufficient)
}

impl EvidenceReason {
    pub(super) const fn requires_resume(self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::ConfigurationConflict | Self::Reconciliation
        )
    }
}
