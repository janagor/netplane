use super::{EvidenceReason, StageEvaluation};
use netplane::VersionId;

/// Rollout lifecycle. Terminal states never process asynchronous results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RolloutState {
    /// Validated; no stage has been requested.
    Pending,
    /// Waiting for matching reconciliation confirmation.
    Applying,
    /// Collecting evidence for a confirmed publication.
    Observing,
    /// Evidence or configuration prevents advancement.
    Blocked,
    /// Administrator suspended progression.
    Paused,
    /// The final 100% stage passed.
    Completed,
    /// A definite violation or unrecoverable application error stopped progression.
    Failed,
    /// Cancelled or driver dropped; applied weights remain active.
    Cancelled,
}
impl RolloutState {
    /// Whether no future stage can be applied.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Bounded read-only snapshot, including metadata for a future rollback policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RolloutStatus {
    /// Identifier scoped to the reconciler; independent of desired revisions.
    pub id: u64,
    /// Current lifecycle state.
    pub state: RolloutState,
    /// Zero-based active stage index.
    pub current_stage: usize,
    /// Number of validated stages.
    pub total_stages: usize,
    /// Requested weight, separate from publication confirmation.
    pub desired_canary_weight: u8,
    /// Most recent confirmed weight; cancellation never restores a prior weight.
    pub applied_canary_weight: Option<u8>,
    /// Pre-rollout publication; existing history eviction still applies.
    pub baseline_publication: VersionId,
    /// Pre-rollout desired revision.
    pub baseline_revision: u64,
    /// Publication whose requests may satisfy current conditions.
    pub active_stage_publication: Option<VersionId>,
    /// Increasing attribution epoch; pause/resume invalidates earlier samples.
    pub epoch: u64,
    /// Most recent condition decision.
    pub last_evaluation: Option<StageEvaluation>,
    /// Explanation for blocked progression.
    pub blocked_reason: Option<EvidenceReason>,
    /// Most recent successful stage, retained at failure for N12.
    pub last_successful_stage: Option<usize>,
    /// Stage that definitively failed.
    pub failed_stage: Option<usize>,
}

/// Explicit inputs to the deterministic machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RolloutEvent {
    /// Begin the first stage.
    Start,
    /// Confirm publication for this application epoch.
    Published {
        /// Application epoch supplied by the stage action.
        epoch: u64,
        /// Confirmed publication.
        publication: VersionId,
    },
    /// Attributed condition decision.
    Evaluated {
        /// Observation epoch.
        epoch: u64,
        /// Publication covered by evidence.
        publication: VersionId,
        /// Pure evaluation output.
        evaluation: StageEvaluation,
    },
    /// Prevent automatic progression, awaiting evidence or explicit resume.
    Block(EvidenceReason),
    /// Stop before another stage can be requested.
    Pause,
    /// Revalidate and restart the current stage's observation.
    Resume,
    /// Restart evidence after configuration changes, reverting only an unsubmitted
    /// next-stage intent to the last applied weight (never published traffic).
    Revalidate,
    /// Terminal cancellation; no restoration action is emitted.
    Cancel,
    /// Unrecoverable preparation or validation failure.
    Fail,
}

/// Work requested by a transition; never executes I/O itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RolloutAction {
    /// Submit current stage through guarded reconciliation.
    ApplyStage,
    /// Begin a new cumulative observation window.
    Observe,
    /// No asynchronous action is authorized.
    None,
}

/// Deterministic transition logic; stale results and terminal reactivation are ignored.
pub struct RolloutMachine {
    pub(super) status: RolloutStatus,
    weights: Vec<u8>,
}
impl RolloutMachine {
    /// Validate a specification and create a pending machine without runtime work.
    /// # Errors
    /// Returns a structured specification violation before any action is possible.
    pub fn from_spec(
        spec: &super::RolloutSpec,
        publication: VersionId,
        revision: u64,
    ) -> Result<Self, super::ValidationError> {
        spec.validate()?;
        Ok(Self::new(
            spec.stages
                .iter()
                .map(|stage| stage.canary_weight)
                .collect(),
            publication,
            revision,
        ))
    }

    pub(super) fn new(weights: Vec<u8>, publication: VersionId, revision: u64) -> Self {
        Self {
            status: RolloutStatus {
                id: revision.saturating_add(1),
                state: RolloutState::Pending,
                current_stage: 0,
                total_stages: weights.len(),
                desired_canary_weight: weights[0],
                applied_canary_weight: None,
                baseline_publication: publication,
                baseline_revision: revision,
                active_stage_publication: None,
                epoch: 0,
                last_evaluation: None,
                blocked_reason: None,
                last_successful_stage: None,
                failed_stage: None,
            },
            weights,
        }
    }

    /// Read the machine's current snapshot without retaining history.
    #[must_use]
    pub const fn status(&self) -> &RolloutStatus {
        &self.status
    }

    /// Process one explicit event. Publication and evaluation must match the
    /// current epoch; duplicate confirmations cannot restart a window.
    pub fn transition(&mut self, event: RolloutEvent) -> RolloutAction {
        use RolloutAction::{ApplyStage, None, Observe};
        use RolloutEvent::{
            Block, Cancel, Evaluated, Fail, Pause, Published, Resume, Revalidate, Start,
        };
        use RolloutState::{Applying, Blocked, Cancelled, Failed, Observing, Paused, Pending};
        let s = &mut self.status;
        if s.state.is_terminal() {
            return None;
        }
        match event {
            Start if s.state == Pending => {
                s.epoch += 1;
                s.state = Applying;
                ApplyStage
            }
            Published { epoch, publication } if s.state == Applying && s.epoch == epoch => {
                s.active_stage_publication = Some(publication);
                s.applied_canary_weight = Some(s.desired_canary_weight);
                s.state = Observing;
                s.blocked_reason = Option::None;
                Observe
            }
            Evaluated {
                epoch,
                publication,
                evaluation,
            } if matches!(s.state, Observing | Blocked)
                && epoch == s.epoch
                && s.active_stage_publication == Some(publication)
                && !matches!(
                    s.blocked_reason,
                    Some(
                        EvidenceReason::Timeout
                            | EvidenceReason::ConfigurationConflict
                            | EvidenceReason::Reconciliation
                    )
                ) =>
            {
                evaluate_stage(s, &self.weights, evaluation)
            }
            Block(reason) => {
                s.state = Blocked;
                s.blocked_reason = Some(reason);
                None
            }
            Pause => {
                s.state = Paused;
                None
            }
            Resume if matches!(s.state, Paused | Blocked) => {
                s.epoch = s.epoch.saturating_add(1);
                s.last_evaluation = Option::None;
                s.blocked_reason = Option::None;
                s.state = Applying;
                ApplyStage
            }
            Revalidate => {
                if s.state == Applying
                    && s.active_stage_publication.is_some()
                    && s.applied_canary_weight != Some(s.desired_canary_weight)
                {
                    if let Some(previous) = s.last_successful_stage {
                        s.current_stage = previous;
                        s.desired_canary_weight = self.weights[previous];
                    }
                }
                s.epoch = s.epoch.saturating_add(1);
                s.last_evaluation = Option::None;
                s.blocked_reason = Option::None;
                s.state = Applying;
                ApplyStage
            }
            Cancel => {
                s.state = Cancelled;
                None
            }
            Fail => {
                s.state = Failed;
                s.failed_stage = Some(s.current_stage);
                None
            }
            _ => None,
        }
    }
}

fn evaluate_stage(
    s: &mut RolloutStatus,
    weights: &[u8],
    evaluation: StageEvaluation,
) -> RolloutAction {
    use RolloutAction::{ApplyStage, None};
    use RolloutState::{Applying, Blocked, Completed, Failed};
    s.last_evaluation = Some(evaluation);
    match evaluation {
        StageEvaluation::Passed => {
            s.last_successful_stage = Some(s.current_stage);
            s.blocked_reason = Option::None;
            if s.current_stage + 1 == s.total_stages {
                s.state = Completed;
                None
            } else {
                s.current_stage += 1;
                s.epoch = s.epoch.saturating_add(1);
                s.desired_canary_weight = weights[s.current_stage];
                s.state = Applying;
                ApplyStage
            }
        }
        StageEvaluation::Failed(_) => {
            s.state = Failed;
            s.blocked_reason = Option::None;
            s.failed_stage = Some(s.current_stage);
            None
        }
        StageEvaluation::Insufficient(reason) => {
            s.state = Blocked;
            s.blocked_reason = Some(reason);
            None
        }
    }
}
