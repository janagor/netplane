use super::{
    EvidenceReason, ObservationProvider, ObservationRequest, RolloutAction, RolloutEvent,
    RolloutState, StageEvaluation,
    driver::{RolloutDriver, TrafficConfiguration, policies},
    evaluate,
    handle::Mode,
};
use crate::ReconciliationPhase;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout_at};

impl<D: TrafficConfiguration, O: ObservationProvider> RolloutDriver<D, O> {
    async fn wait(&mut self, delay: Duration) {
        let mut reconciliation = self.updates.subscribe_status();
        tokio::select! {
            _ = self.control.changed() => {},
            _ = reconciliation.changed() => {},
            () = sleep(delay) => {},
        }
    }

    async fn apply_stage(&mut self, window: &mut Window) -> RolloutAction {
        let pending = self.control.borrow().pending == Some(self.sequence)
            && matches!(
                self.updates.status().phase,
                ReconciliationPhase::Pending
                    | ReconciliationPhase::Preparing
                    | ReconciliationPhase::RetryScheduled
            );
        if !pending {
            let Ok(snapshot) = self.current() else {
                self.reject_stage_submission();
                return RolloutAction::None;
            };
            if snapshot.revision != self.sequence {
                self.machine.transition(RolloutEvent::Revalidate);
                self.sequence = snapshot.revision;
                self.expected = snapshot.groups;
            }
            if self.submit(snapshot).is_err() {
                // A stale snapshot has already invalidated the next-stage intent.
                return RolloutAction::None;
            }
            self.emit("rollout.stage.requested");
        }
        let result = tokio::select! {
            biased;
            _ = self.control.changed() => { return RolloutAction::None; }
            result = Self::applied(&self.updates, self.sequence) => result,
        };
        match result {
            Ok(publication) => {
                self.stop.control.send_if_modified(|control| {
                    control.pending = None;
                    false
                });
                let action = self.machine.transition(RolloutEvent::Published {
                    epoch: self.machine.status().epoch,
                    publication,
                });
                let now = Instant::now();
                if window.stage != self.machine.status().current_stage
                    || window.stage_started_at.is_none()
                {
                    window.stage = self.machine.status().current_stage;
                    window.stage_started_at = Some(now);
                }
                window.observation_started_at = now;
                window.eligibility = None;
                self.emit("rollout.stage.applied");
                self.emit("rollout.stage.observing");
                action
            }
            Err(reason) => {
                let status = self.updates.status();
                if reason == EvidenceReason::Reconciliation
                    && status.phase == ReconciliationPhase::Blocked
                    && status.last_failure != Some(crate::ReconciliationFailure::Conflict)
                {
                    self.machine.transition(RolloutEvent::Fail);
                    self.emit("rollout.stage.failed");
                } else {
                    self.block(reason);
                }
                RolloutAction::None
            }
        }
    }

    pub(super) fn reject_stage_submission(&mut self) {
        // Invalidate authorization before adopting any newer configuration. This
        // rewinds an unsubmitted next stage without reducing published traffic.
        self.machine.transition(RolloutEvent::Revalidate);
        if self.machine.status().active_stage_publication.is_none() {
            self.block(EvidenceReason::ConfigurationConflict);
        } else {
            self.block_configuration();
        }
    }

    fn refresh_configuration(&mut self, window: &mut Window) -> bool {
        if self
            .machine
            .status()
            .blocked_reason
            .is_some_and(EvidenceReason::requires_resume)
        {
            return false;
        }
        if window.stage_started_at.is_some_and(|started| {
            Instant::now() >= started + self.spec.stages[window.stage].timeout
        }) {
            self.block(EvidenceReason::Timeout);
            return false;
        }
        let Ok(snapshot) = self.current() else {
            self.block_configuration();
            return false;
        };
        if snapshot.revision != self.sequence {
            self.sequence = snapshot.revision;
            self.expected = snapshot.groups;
            self.restart_observation(window, snapshot.publication);
        }
        true
    }

    fn restart_observation(&mut self, window: &mut Window, publication: netplane::VersionId) {
        // A fresh evidence window does not extend the stage's original deadline.
        self.machine.transition(RolloutEvent::Revalidate);
        self.machine.transition(RolloutEvent::Published {
            epoch: self.machine.status().epoch,
            publication,
        });
        window.observation_started_at = Instant::now();
        window.eligibility = None;
        self.emit("rollout.stage.observing");
    }

    pub(super) fn block_configuration(&mut self) {
        let update = self.updates.0.borrow().update.clone();
        let compatible = update.as_ref().is_some_and(|update| {
            update.sequence != self.sequence
                && policies(&update.desired, &self.spec).is_some_and(|policy| {
                    policy[0].weight == self.expected[0].weight
                        && policy[1].weight == self.expected[1].weight
                })
        });
        self.block(if compatible {
            EvidenceReason::ConfigurationPending
        } else {
            EvidenceReason::ConfigurationConflict
        });
    }

    async fn observe(&mut self, window: &mut Window) -> RolloutAction {
        let stage = &self.spec.stages[self.machine.status().current_stage];
        let deadline = window
            .stage_started_at
            .unwrap_or(window.observation_started_at)
            + stage.timeout;
        if Instant::now() >= deadline {
            self.block(EvidenceReason::Timeout);
            return RolloutAction::None;
        }
        if Instant::now() < window.next_observation_at {
            self.wait(
                window
                    .next_observation_at
                    .min(deadline)
                    .saturating_duration_since(Instant::now()),
            )
            .await;
            return RolloutAction::None;
        }
        window.next_observation_at = Instant::now() + self.interval;
        let Some(publication) = self.machine.status().active_stage_publication else {
            self.block(EvidenceReason::Reconciliation);
            return RolloutAction::None;
        };
        let identity = super::decision::EvaluationIdentity {
            desired_revision: self.sequence,
            publication,
            observation_epoch: self.machine.status().epoch,
            control_generation: self.control.borrow().generation,
            deadline,
        };
        let request = ObservationRequest {
            epoch: self.machine.status().epoch,
            publication,
            since: window.observation_started_at,
            until: Instant::now(),
            groups: [self.spec.stable.clone(), self.spec.canary.clone()],
        };
        let mut reconciliation = self.updates.subscribe_status();
        let observed = tokio::select! {
            biased;
            _ = self.control.changed() => { return RolloutAction::None; }
            _ = reconciliation.changed() => { return RolloutAction::None; }
            observed = timeout_at(deadline.min(Instant::now() + self.interval), self.provider.observe(request.clone())) => observed,
        };
        if Instant::now() >= deadline {
            self.block(EvidenceReason::Timeout);
            return RolloutAction::None;
        }
        // Revalidate after I/O so external updates cannot authorize advancement.
        if self.current().is_err() || self.updates.0.borrow().sequence != self.sequence {
            self.block_configuration();
            return RolloutAction::None;
        }
        let evaluation = match observed {
            Ok(Ok(observation)) => {
                let decision = evaluate(stage, &request, &observation);
                let mut revisions: Vec<_> = observation
                    .groups
                    .iter()
                    .map(|g| (g.group.clone(), g.eligibility_revision))
                    .collect();
                revisions.sort_unstable();
                if !matches!(
                    decision,
                    StageEvaluation::Insufficient(EvidenceReason::Attribution)
                ) {
                    if window
                        .eligibility
                        .as_ref()
                        .is_some_and(|previous| previous != &revisions)
                        || observation.eligibility_changed
                    {
                        self.block(EvidenceReason::EligibilityChanged);
                        self.restart_observation(window, publication);
                        self.wait(
                            self.interval
                                .min(deadline.saturating_duration_since(Instant::now())),
                        )
                        .await;
                        return RolloutAction::Observe;
                    }
                    window.eligibility = Some(revisions);
                }
                decision
            }
            _ => StageEvaluation::Insufficient(EvidenceReason::Unavailable),
        };
        let action = self.accept_evaluation(identity, evaluation);
        if action == RolloutAction::ApplyStage || self.machine.status().state.is_terminal() {
            return action;
        }
        self.wait(
            self.interval
                .min(deadline.saturating_duration_since(Instant::now())),
        )
        .await;
        action
    }

    /// Drive stages until completion, failure, cancellation, reconciliation shutdown
    /// or driver drop. Keep and await this future for cleanup.
    /// Pause and cancel interrupt provider calls and publication confirmation waits.
    pub async fn run(mut self) {
        let mut action = self.machine.transition(RolloutEvent::Start);
        self.emit("rollout.started");
        let mut window = Window {
            stage_started_at: None,
            observation_started_at: Instant::now(),
            next_observation_at: Instant::now(),
            stage: 0,
            eligibility: None,
        };
        let mut generation = 0;
        loop {
            if self.machine.status().state.is_terminal() {
                break;
            }
            if self.updates.status().phase == ReconciliationPhase::Stopped {
                break;
            }
            let (mode, next_generation) = {
                let control = self.control.borrow_and_update();
                (control.mode, control.generation)
            };
            let changed = generation != next_generation;
            generation = next_generation;
            if changed && mode == Mode::Running {
                self.machine.transition(RolloutEvent::Pause);
            }
            if mode == Mode::Cancelled || self.control.has_changed().is_err() {
                break;
            }
            if mode == Mode::Paused {
                if self.machine.status().state != RolloutState::Paused {
                    self.machine.transition(RolloutEvent::Pause);
                    self.emit("rollout.paused");
                }
                self.wait(self.interval).await;
                continue;
            }
            if self.machine.status().state == RolloutState::Paused {
                action = self.machine.transition(RolloutEvent::Resume);
                window.stage_started_at = None;
                self.emit("rollout.resumed");
            }
            if action == RolloutAction::ApplyStage {
                action = self.apply_stage(&mut window).await;
                continue;
            }
            // Explicit resume retries blocked configuration or timed-out stages.
            let controls_changed = self.control.has_changed().unwrap_or(false);
            if controls_changed {
                continue;
            }
            if !self.refresh_configuration(&mut window) {
                self.wait(self.interval).await;
                continue;
            }
            action = self.observe(&mut window).await;
        }
        let committed = {
            let mailbox = self.updates.0.borrow();
            (mailbox.sequence == self.sequence)
                .then(|| {
                    mailbox
                        .intent
                        .as_ref()
                        .and_then(|intent| intent.committed.get().copied())
                })
                .flatten()
        };
        let reconciled = self.updates.status();
        if let Some(publication) = committed {
            self.machine.status.applied_canary_weight = Some(self.expected[1].weight);
            self.machine.status.active_stage_publication = Some(publication);
        } else if reconciled.applied_revision == Some(self.sequence) {
            self.machine.status.applied_canary_weight = Some(self.expected[1].weight);
            self.machine.status.active_stage_publication = reconciled.active_publication;
        }
        if !self.machine.status().state.is_terminal() {
            self.machine.transition(RolloutEvent::Cancel);
        }
        match self.machine.status().state {
            RolloutState::Completed => self.emit("rollout.completed"),
            RolloutState::Failed => {}
            _ => self.emit("rollout.cancelled"),
        }
    }
}

pub(super) struct Window {
    stage_started_at: Option<Instant>,
    observation_started_at: Instant,
    next_observation_at: Instant,
    stage: usize,
    eligibility: Option<Vec<(String, u64)>>,
}
