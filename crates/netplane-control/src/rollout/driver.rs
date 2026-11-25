use super::{
    EvidenceReason, ObservationProvider, RolloutError, RolloutEvent, RolloutHandle, RolloutMachine,
    RolloutSpec, RolloutState, ValidationError,
    handle::{Control, Mode, Stop},
};
use crate::{ReconciliationPhase, UpdateSender};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, watch},
    time::Instant,
};

/// Application-resolved immutable group policy; membership identity changes when
/// endpoint incarnations or group composition change, even at an unchanged count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupPolicy {
    /// Integer routing percentage.
    pub weight: u8,
    /// Application-defined semantic membership revision.
    pub membership_revision: u64,
}

/// Bridge from application-owned typed desired state to its existing weighted policy.
///
/// Implementations must preserve all unrelated configuration in `set_weights`.
/// Logical groups may resolve to multiple endpoints. N10 eligibility remains live
/// observation data rather than being copied into desired state by this trait.
pub trait TrafficConfiguration: Clone + Eq + Send + Sync {
    /// Resolve a logical group, returning `None` if removed or unsupported.
    fn group_policy(&self, group: &str) -> Option<GroupPolicy>;
    /// Change only these two group weights. Called only after both groups resolve.
    fn set_weights(&mut self, stable: &str, canary: &str, canary_weight: u8);
}

/// Caller-owned rollout worker. Construction spawns nothing. Run alongside the
/// existing reconciliation driver and cancel both alongside runtime shutdown.
///
/// One permit per reconciler covers pending, paused and blocked rollouts. Dropping
/// this worker cancels observations, withdraws owned pending intent and releases it.
/// No automatic retry of reconciliation failures or automatic rollback occurs.
pub struct RolloutDriver<D, O> {
    pub(super) updates: UpdateSender<D>,
    pub(super) provider: O,
    pub(super) spec: RolloutSpec,
    pub(super) interval: Duration,
    pub(super) machine: RolloutMachine,
    pub(super) control: watch::Receiver<Control>,
    pub(super) stop: Stop<D>,
    pub(super) _permit: OwnedSemaphorePermit,
    pub(super) sequence: u64,
    pub(super) expected: [GroupPolicy; 2],
}
pub(super) struct ValidatedConfiguration<D> {
    pub revision: u64,
    pub publication: netplane::VersionId,
    pub desired: D,
    pub groups: [GroupPolicy; 2],
}

impl<D: TrafficConfiguration, O: ObservationProvider> RolloutDriver<D, O> {
    /// Validate and acquire exclusive rollout ownership over an already converged
    /// reconciler. Returns an unpolled worker and its lifecycle handle.
    /// # Errors
    /// Rejects invalid specifications, unresolved groups, nonconverged desired
    /// state, concurrent rollouts, and zero or overflowing polling intervals.
    pub fn start(
        updates: UpdateSender<D>,
        provider: O,
        spec: RolloutSpec,
        interval: Duration,
    ) -> Result<(Self, RolloutHandle<D>), RolloutError> {
        spec.validate()?;
        if interval.is_zero() || Instant::now().checked_add(interval).is_none() {
            return Err(RolloutError::Interval);
        }
        let permit = Arc::clone(&updates.1.rollout)
            .try_acquire_owned()
            .map_err(|_| RolloutError::Busy)?;
        let (sequence, desired, publication) = {
            let mailbox = updates.0.borrow();
            let status = updates.status();
            let update = mailbox.update.clone().ok_or(RolloutError::Conflict)?;
            let stopped = mailbox.stopped;
            drop(mailbox);
            if stopped || !status.is_converged() || status.applied_revision != Some(update.sequence)
            {
                return Err(RolloutError::Conflict);
            }
            (
                update.sequence,
                update.desired.clone(),
                status.active_publication.ok_or(RolloutError::Conflict)?,
            )
        };
        let expected = policies(&desired, &spec).ok_or(ValidationError::Groups)?;
        if u16::from(expected[0].weight) + u16::from(expected[1].weight) != 100 {
            return Err(RolloutError::Conflict);
        }
        if spec.stages[0].canary_weight < expected[1].weight {
            return Err(ValidationError::Weight(0).into());
        }
        let mut machine = RolloutMachine::new(
            spec.stages
                .iter()
                .map(|stage| stage.canary_weight)
                .collect(),
            publication,
            sequence,
        );
        machine.status.id = updates
            .1
            .next_rollout
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |id| id.checked_add(1),
            )
            .map_err(|_| RolloutError::IdentifierExhausted)?;
        let (status, receiver) = watch::channel(machine.status().clone());
        let (control, controls) = watch::channel(Control {
            mode: Mode::Running,
            generation: 0,
            pending: None,
        });
        let handle = RolloutHandle {
            updates: updates.clone(),
            control: control.clone(),
            status: receiver,
        };
        Ok((
            Self {
                stop: Stop {
                    updates: updates.clone(),
                    control,
                    status,
                },
                updates,
                provider,
                spec,
                interval,
                machine,
                control: controls,
                _permit: permit,
                sequence,
                expected,
            },
            handle,
        ))
    }

    pub(super) fn block(&mut self, reason: EvidenceReason) {
        if self.machine.status().blocked_reason != Some(reason)
            || self.machine.status().state != RolloutState::Blocked
        {
            self.machine.transition(RolloutEvent::Block(reason));
            self.emit(if reason == EvidenceReason::ConfigurationConflict {
                "rollout.configuration_conflict"
            } else {
                "rollout.stage.blocked"
            });
        }
    }

    pub(super) fn submit(
        &mut self,
        snapshot: ValidatedConfiguration<D>,
    ) -> Result<(), RolloutError> {
        let mut desired = snapshot.desired;
        desired.set_weights(
            &self.spec.stable,
            &self.spec.canary,
            self.machine.status().desired_canary_weight,
        );
        let Some(expected) = policies(&desired, &self.spec) else {
            self.reject_stage_submission();
            return Err(RolloutError::Conflict);
        };
        if expected[1].weight != self.machine.status().desired_canary_weight
            || u16::from(expected[0].weight) + u16::from(expected[1].weight) != 100
            || expected[0].membership_revision != snapshot.groups[0].membership_revision
            || expected[1].membership_revision != snapshot.groups[1].membership_revision
        {
            self.reject_stage_submission();
            return Err(RolloutError::Conflict);
        }
        let mut submitted = None;
        self.stop.control.send_if_modified(|control| {
            if control.mode == Mode::Running {
                submitted = self
                    .updates
                    .submit_guarded(desired, snapshot.revision, snapshot.publication)
                    .ok();
                control.pending = submitted.as_ref().map(|receipt| receipt.sequence);
            }
            false
        });
        let Some(receipt) = submitted else {
            self.reject_stage_submission();
            return Err(RolloutError::Conflict);
        };
        self.sequence = receipt.emit();
        self.expected = expected;
        Ok(())
    }

    pub(super) fn current(&self) -> Result<ValidatedConfiguration<D>, RolloutError> {
        let mailbox = self.updates.0.borrow();
        let update = mailbox.update.clone().ok_or(RolloutError::Conflict)?;
        let stopped = mailbox.stopped;
        let status = self.updates.status();
        drop(mailbox);
        if self
            .updates
            .1
            .inspect_publication
            .as_ref()
            .is_some_and(|inspect| inspect() != status.active_publication)
        {
            return Err(RolloutError::Conflict);
        }
        if stopped || !status.is_converged() || status.applied_revision != Some(update.sequence) {
            return Err(RolloutError::Conflict);
        }
        let policy = policies(&update.desired, &self.spec).ok_or(RolloutError::Conflict)?;
        if policy[0].weight != self.expected[0].weight
            || policy[1].weight != self.expected[1].weight
        {
            return Err(RolloutError::Conflict);
        }
        Ok(ValidatedConfiguration {
            revision: update.sequence,
            publication: status.active_publication.ok_or(RolloutError::Conflict)?,
            desired: update.desired.clone(),
            groups: policy,
        })
    }

    pub(super) async fn applied(
        updates: &UpdateSender<D>,
        sequence: u64,
    ) -> Result<netplane::VersionId, EvidenceReason> {
        let mut status = updates.subscribe_status();
        loop {
            let snapshot = status.borrow_and_update().clone();
            if updates
                .1
                .inspect_publication
                .as_ref()
                .is_some_and(|inspect| inspect() != snapshot.active_publication)
                && snapshot.phase != ReconciliationPhase::Preparing
            {
                return Err(EvidenceReason::ConfigurationConflict);
            }
            if snapshot.desired_revision != Some(sequence) {
                return Err(EvidenceReason::ConfigurationConflict);
            }
            if snapshot.applied_revision == Some(sequence) && snapshot.is_converged() {
                return snapshot
                    .active_publication
                    .ok_or(EvidenceReason::Reconciliation);
            }
            if matches!(
                snapshot.phase,
                ReconciliationPhase::Blocked | ReconciliationPhase::Stopped
            ) {
                return Err(EvidenceReason::Reconciliation);
            }
            if status.changed().await.is_err() {
                return Err(EvidenceReason::Reconciliation);
            }
        }
    }
}

pub(super) fn policies<D: TrafficConfiguration>(
    desired: &D,
    spec: &RolloutSpec,
) -> Option<[GroupPolicy; 2]> {
    Some([
        desired.group_policy(&spec.stable)?,
        desired.group_policy(&spec.canary)?,
    ])
}
