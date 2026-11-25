use super::{
    EvidenceReason, ObservationProvider, RolloutState, RolloutStatus, StageEvaluation,
    driver::{RolloutDriver, TrafficConfiguration},
};
use netplane::VersionId;

// Event context belongs to the transition, independently of the latest status.
struct AuditTransition {
    event: &'static str,
    rollout: u64,
    stage: usize,
    publication: Option<VersionId>,
    epoch: u64,
    baseline_publication: VersionId,
    baseline_revision: u64,
    desired_weight: u8,
    applied_weight: Option<u8>,
    state: RolloutState,
    reason: Option<EvidenceReason>,
    decision: Option<StageEvaluation>,
}
impl AuditTransition {
    fn capture(event: &'static str, status: &RolloutStatus) -> Self {
        let decision = if matches!(
            event,
            "rollout.stage.blocked" | "rollout.configuration_conflict"
        ) {
            status.blocked_reason.map(StageEvaluation::Insufficient)
        } else {
            status.last_evaluation
        };
        Self {
            event,
            rollout: status.id,
            stage: status.current_stage,
            publication: status.active_stage_publication,
            epoch: status.epoch,
            baseline_publication: status.baseline_publication,
            baseline_revision: status.baseline_revision,
            desired_weight: status.desired_canary_weight,
            applied_weight: status.applied_canary_weight,
            state: status.state,
            reason: status.blocked_reason,
            decision,
        }
    }
    fn emit(self, runtime_id: u64, reconciler_id: u64) {
        tracing::info!(target: "netplane_control", event = self.event, runtime_id, reconciler_id,
            rollout_id = self.rollout, stage = self.stage, publication_id = self.publication.map(VersionId::get),
            epoch = self.epoch, baseline_publication_id = self.baseline_publication.get(), baseline_revision = self.baseline_revision,
            desired_canary_weight = self.desired_weight, applied_canary_weight = self.applied_weight, state = ?self.state, blocked_reason = ?self.reason, evaluation = ?self.decision);
    }
}
impl<D: TrafficConfiguration, O: ObservationProvider> RolloutDriver<D, O> {
    pub(super) fn emit(&self, event: &'static str) {
        self.emit_for(event, self.machine.status());
    }

    pub(super) fn emit_for(&self, event: &'static str, transition: &RolloutStatus) {
        let event = AuditTransition::capture(event, transition);
        self.stop.status.send_replace(self.machine.status().clone());
        event.emit(self.updates.1.runtime_id, self.updates.1.reconciler_id);
    }

    pub(super) fn emit_decision(&self, transition: &RolloutStatus, evaluation: StageEvaluation) {
        self.emit_for(
            match evaluation {
                StageEvaluation::Passed => "rollout.stage.passed",
                StageEvaluation::Failed(_) => "rollout.stage.failed",
                StageEvaluation::Insufficient(_) => "rollout.stage.blocked",
            },
            transition,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rollout::{Condition, RolloutEvent, RolloutMachine, RolloutSpec, Stage};
    use std::time::Duration;

    #[tokio::test]
    async fn audit_payload_preserves_the_transition_decision_and_reason()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Obtain the opaque publication ID without changing its public construction API.
        let graph = netplane::GraphDraft::new(tower::service_fn(|()| async {
            Ok::<_, std::convert::Infallible>(())
        }))
        .validate()?;
        let controller = netplane::ReconfigurationController::new(
            netplane::Netplane::new(graph, netplane::RuntimeLimits { max_outstanding: 4 })?,
            netplane::ControllerOptions::default(),
        );
        let publication = controller.inspect().active.ok_or("no publication")?.id;
        let spec = RolloutSpec {
            stable: "stable".into(),
            canary: "canary".into(),
            stages: vec![Stage {
                canary_weight: 100,
                min_observation: Duration::from_secs(1),
                timeout: Duration::from_secs(2),
                conditions: vec![Condition::Requests {
                    group: "canary".into(),
                    minimum: 1,
                }],
            }],
        };
        let mut machine = RolloutMachine::from_spec(&spec, publication, 1)?;
        machine.transition(RolloutEvent::Start);
        machine.transition(RolloutEvent::Published {
            epoch: 1,
            publication,
        });
        machine.transition(RolloutEvent::Block(EvidenceReason::EligibilityChanged));
        let blocked = AuditTransition::capture("rollout.stage.blocked", machine.status());
        machine.transition(RolloutEvent::Resume);
        assert_eq!(blocked.state, RolloutState::Blocked);
        assert_eq!(blocked.reason, Some(EvidenceReason::EligibilityChanged));
        assert_eq!(
            blocked.decision,
            Some(StageEvaluation::Insufficient(
                EvidenceReason::EligibilityChanged
            ))
        );
        assert_eq!(blocked.publication, Some(publication));
        assert_eq!(blocked.epoch, 1);
        machine.transition(RolloutEvent::Published {
            epoch: 2,
            publication,
        });
        let mut evaluated = machine.status().clone();
        evaluated.last_evaluation = Some(StageEvaluation::Passed);
        evaluated.blocked_reason = None;
        machine.transition(RolloutEvent::Evaluated {
            epoch: 2,
            publication,
            evaluation: StageEvaluation::Passed,
        });
        evaluated.state = machine.status().state;
        let passed = AuditTransition::capture("rollout.stage.passed", &evaluated);
        assert_eq!(passed.decision, Some(StageEvaluation::Passed));
        assert_eq!(passed.reason, None);
        assert_eq!(passed.stage, 0);
        assert_eq!(passed.state, RolloutState::Completed);
        Ok(())
    }
}
