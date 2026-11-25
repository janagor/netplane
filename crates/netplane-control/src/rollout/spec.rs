use std::time::Duration;

/// Logical group name resolved by the application's desired configuration.
pub type BackendGroupId = String;

/// Immutable input to one progressive rollout. Weights allocate key buckets,
/// rather than guaranteeing a measured fraction of requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RolloutSpec {
    /// Group receiving the complementary weight.
    pub stable: BackendGroupId,
    /// Group receiving the increasing weight.
    pub canary: BackendGroupId,
    /// Ordered stages, including the final fully observed 100% stage.
    pub stages: Vec<Stage>,
}

/// Explicit timing and AND-combined success conditions for a publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stage {
    /// Integer percentage, validated before any update is submitted.
    pub canary_weight: u8,
    /// Minimum complete observation window after publication confirmation.
    pub min_observation: Duration,
    /// Overall stage deadline from first confirmation; strictly exceeds `min_observation`.
    pub timeout: Duration,
    /// Nonempty list; statistical conditions carry their own positive sample floor.
    pub conditions: Vec<Condition>,
}

/// One success requirement. Statistical checks never run below their sample floor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Condition {
    /// Require completed, attributed requests from this group.
    Requests {
        /// Stable or canary group.
        group: BackendGroupId,
        /// Positive completed request floor.
        minimum: u64,
    },
    /// Compare failures / completed using exact integer arithmetic.
    ErrorRate {
        /// Stable or canary group.
        group: BackendGroupId,
        /// Inclusive maximum in 1/10,000 units, in 0..=10,000.
        maximum_basis_points: u16,
        /// Positive sample floor before success or failure is interpreted.
        minimum_requests: u64,
    },
    /// Require a complete latency sample population and an inclusive p95 bound.
    P95Latency {
        /// Stable or canary group.
        group: BackendGroupId,
        /// Inclusive accepted-to-terminal latency bound.
        maximum: Duration,
        /// Positive sample floor before success or failure is interpreted.
        minimum_requests: u64,
    },
    /// Sample authoritative N10 eligibility; never infer health from zero errors.
    Eligible {
        /// Stable or canary group.
        group: BackendGroupId,
        /// Required eligible endpoint count.
        minimum: usize,
    },
}
impl Condition {
    pub(super) fn group(&self) -> &str {
        match self {
            Self::Requests { group, .. }
            | Self::ErrorRate { group, .. }
            | Self::P95Latency { group, .. }
            | Self::Eligible { group, .. } => group,
        }
    }
}

/// Specification rejection, reported before publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    /// Empty stages or final stage is not 100% canary.
    #[error("rollout needs stages ending at 100% canary")]
    Stages,
    /// Empty, identical, or unresolved logical groups.
    #[error("stable and canary must be distinct resolvable groups")]
    Groups,
    /// Invalid or decreasing percentage at this zero-based stage.
    #[error("invalid or decreasing traffic weight at stage {0}")]
    Weight(usize),
    /// Missing conditions, invalid thresholds, missing samples or zero-traffic metrics.
    #[error("invalid success condition at stage {0}")]
    Condition(usize),
    /// Zero, overflowing, or inconsistent stage timing.
    #[error("invalid observation deadline at stage {0}")]
    Timing(usize),
}
impl RolloutSpec {
    /// Validate every stage without preparing graphs.
    /// # Errors
    /// Returns the first structured specification violation. Group resolution is
    /// checked separately against desired state by the driver before starting.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.stable.is_empty() || self.canary.is_empty() || self.stable == self.canary {
            return Err(ValidationError::Groups);
        }
        if self.stages.is_empty() || self.stages.last().is_none_or(|s| s.canary_weight != 100) {
            return Err(ValidationError::Stages);
        }
        let mut previous = 0;
        for (index, stage) in self.stages.iter().enumerate() {
            if stage.canary_weight > 100 || stage.canary_weight < previous {
                return Err(ValidationError::Weight(index));
            }
            previous = stage.canary_weight;
            if stage.timeout.is_zero()
                || stage.timeout <= stage.min_observation
                || std::time::Instant::now()
                    .checked_add(stage.timeout)
                    .is_none()
            {
                return Err(ValidationError::Timing(index));
            }
            if stage.conditions.is_empty() {
                return Err(ValidationError::Condition(index));
            }
            if stage.canary_weight > 0
                && !stage.conditions.iter().any(|condition| {
                    condition.group() == self.canary
                        && !matches!(condition, Condition::Eligible { .. })
                })
            {
                return Err(ValidationError::Condition(index));
            }
            for condition in &stage.conditions {
                let group = condition.group();
                let traffic = if group == self.canary {
                    stage.canary_weight
                } else if group == self.stable {
                    100 - stage.canary_weight
                } else {
                    return Err(ValidationError::Groups);
                };
                let valid = match condition {
                    Condition::Requests { minimum, .. } => traffic > 0 && *minimum > 0,
                    Condition::ErrorRate {
                        maximum_basis_points,
                        minimum_requests,
                        ..
                    } => traffic > 0 && *maximum_basis_points <= 10_000 && *minimum_requests > 0,
                    Condition::P95Latency {
                        minimum_requests, ..
                    } => traffic > 0 && *minimum_requests > 0,
                    Condition::Eligible { .. } => true,
                };
                if !valid {
                    return Err(ValidationError::Condition(index));
                }
            }
        }
        Ok(())
    }
}
