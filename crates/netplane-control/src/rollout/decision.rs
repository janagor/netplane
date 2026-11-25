use super::{
    EvidenceReason, ObservationProvider, RolloutAction, RolloutEvent, StageEvaluation,
    driver::{RolloutDriver, TrafficConfiguration},
    handle::Mode,
};
use netplane::VersionId;
use tokio::time::Instant;

/// All identities are sampled before retrieving evidence, never reconstructed afterwards.
#[derive(Clone, Copy)]
pub(super) struct EvaluationIdentity {
    pub desired_revision: u64,
    pub publication: VersionId,
    pub observation_epoch: u64,
    pub control_generation: u64,
    pub deadline: Instant,
}
impl<D: TrafficConfiguration, O: ObservationProvider> RolloutDriver<D, O> {
    pub(super) fn accept_evaluation(
        &mut self,
        identity: EvaluationIdentity,
        evaluation: StageEvaluation,
    ) -> RolloutAction {
        if self
            .machine
            .status()
            .blocked_reason
            .is_some_and(EvidenceReason::requires_resume)
        {
            return RolloutAction::None;
        }
        let accepted = {
            // Order matches lifecycle submission and publication: control -> mailbox
            // -> controller. No user callbacks or tracing run while these are held.
            let control = self.control.borrow();
            if control.mode != Mode::Running
                || control.generation != identity.control_generation
                || self.machine.status().epoch != identity.observation_epoch
                || self.machine.status().active_stage_publication != Some(identity.publication)
            {
                return RolloutAction::None;
            }
            let mailbox = self.updates.0.borrow();
            let status = self.updates.status();
            let current = !mailbox.stopped
                && mailbox.update.is_some()
                && mailbox.sequence == identity.desired_revision
                && status.is_converged()
                && status.applied_revision == Some(identity.desired_revision)
                && status.active_publication == Some(identity.publication);
            let mut accepted = None;
            if current {
                if let Some(guard) = &self.updates.1.accept_publication {
                    guard(identity.publication, &mut || {
                        let evaluation = if Instant::now() >= identity.deadline {
                            StageEvaluation::Insufficient(EvidenceReason::Timeout)
                        } else {
                            evaluation
                        };
                        let before = self.machine.status().clone();
                        let mut transition = before.clone();
                        transition.last_evaluation = Some(evaluation);
                        transition.blocked_reason = match evaluation {
                            StageEvaluation::Insufficient(reason) => Some(reason),
                            _ => None,
                        };
                        let action = self.machine.transition(RolloutEvent::Evaluated {
                            epoch: identity.observation_epoch,
                            publication: identity.publication,
                            evaluation,
                        });
                        transition.state = self.machine.status().state;
                        let changed = *self.machine.status() != before;
                        if changed {
                            self.stop.status.send_replace(self.machine.status().clone());
                        }
                        accepted = Some((transition, action, evaluation, changed));
                    });
                }
            }
            drop(mailbox);
            drop(control);
            accepted
        };
        let Some((transition, action, evaluation, changed)) = accepted else {
            self.block_configuration();
            return RolloutAction::None;
        };
        if changed {
            self.emit_decision(&transition, evaluation);
        }
        action
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rollout::{
        Condition, ConditionViolation, GroupPolicy, ObservationRequest, ObservationUnavailable,
        RolloutObservation, RolloutSpec, RolloutState, Stage,
    };
    use crate::{GraphPreparer, Reconciler, ReconciliationDriver, UpdatePolicy, UpdateSender};
    use netplane::{
        ControllerOptions, GraphDraft, GraphVersion, Netplane, ReconfigurationController,
        RuntimeLimits,
    };
    use std::{sync::Arc, time::Duration};
    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    #[derive(Clone, PartialEq, Eq)]
    struct Config {
        weight: u8,
        setting: u8,
        membership: u64,
    }
    impl TrafficConfiguration for Config {
        fn group_policy(&self, group: &str) -> Option<GroupPolicy> {
            Some(GroupPolicy {
                weight: if group == "canary" {
                    self.weight
                } else {
                    100 - self.weight
                },
                membership_revision: self.membership,
            })
        }
        fn set_weights(&mut self, _: &str, _: &str, weight: u8) {
            self.weight = weight;
        }
    }
    struct Preparer;
    impl GraphPreparer<Config, (), ()> for Preparer {
        type Error = netplane::ConfigError;
        fn validate(&self, _: &Config) -> Result<(), Self::Error> {
            Ok(())
        }
        async fn prepare(&self, _: Config) -> Result<GraphVersion<(), ()>, Self::Error> {
            GraphDraft::new(tower::service_fn(|()| async {
                Ok::<_, std::convert::Infallible>(())
            }))
            .validate()
        }
    }
    struct Unavailable;
    impl ObservationProvider for Unavailable {
        async fn observe(
            &self,
            _: ObservationRequest,
        ) -> Result<RolloutObservation, ObservationUnavailable> {
            Err(ObservationUnavailable)
        }
    }
    struct Fixture {
        driver: RolloutDriver<Config, Unavailable>,
        updates: UpdateSender<Config>,
        worker: tokio::task::JoinHandle<()>,
        controller: Arc<ReconfigurationController<(), ()>>,
    }
    impl Fixture {
        fn identity(&self) -> EvaluationIdentity {
            EvaluationIdentity {
                desired_revision: self.driver.sequence,
                publication: self.driver.machine.status().baseline_publication,
                observation_epoch: self.driver.machine.status().epoch,
                control_generation: 0,
                deadline: Instant::now() + Duration::from_secs(10),
            }
        }
        async fn finish(self) -> TestResult {
            self.updates.shutdown();
            self.worker.await?;
            Ok(())
        }
    }
    async fn fixture() -> Result<Fixture, Box<dyn std::error::Error + Send + Sync>> {
        fixture_with_weights(&[100]).await
    }
    async fn fixture_with_weights(
        weights: &[u8],
    ) -> Result<Fixture, Box<dyn std::error::Error + Send + Sync>> {
        let config = Config {
            weight: weights[0],
            setting: 0,
            membership: 1,
        };
        let controller = Arc::new(ReconfigurationController::new(
            Netplane::new(
                Preparer.prepare(config.clone()).await?,
                RuntimeLimits { max_outstanding: 4 },
            )?,
            ControllerOptions::default(),
        ));
        let (worker, updates) = ReconciliationDriver::new(
            Reconciler::new(controller.clone(), Preparer),
            UpdatePolicy {
                debounce: Duration::ZERO,
                ..UpdatePolicy::default()
            },
        )?;
        let worker = tokio::spawn(worker.run(|_| false));
        let mut reconciliation = updates.subscribe_status();
        updates.submit(config)?;
        while !reconciliation.borrow_and_update().is_converged() {
            reconciliation.changed().await?;
        }
        let spec = RolloutSpec {
            stable: "stable".into(),
            canary: "canary".into(),
            stages: weights
                .iter()
                .map(|weight| Stage {
                    canary_weight: *weight,
                    min_observation: Duration::from_secs(1),
                    timeout: Duration::from_secs(10),
                    conditions: vec![Condition::Requests {
                        group: "canary".into(),
                        minimum: 1,
                    }],
                })
                .collect(),
        };
        let (mut driver, _) =
            RolloutDriver::start(updates.clone(), Unavailable, spec, Duration::from_secs(1))?;
        driver.machine.transition(RolloutEvent::Start);
        driver.machine.transition(RolloutEvent::Published {
            epoch: driver.machine.status().epoch,
            publication: driver.machine.status().baseline_publication,
        });
        Ok(Fixture {
            driver,
            updates,
            worker,
            controller,
        })
    }

    async fn desired_update_race(evaluation: StageEvaluation) -> TestResult {
        let mut fixture = fixture().await?;
        let identity = fixture.identity();
        fixture.driver.current()?; // The old post-observation ownership check passed.
        let updates = fixture.updates.clone();
        let boundary = Arc::new(tokio::sync::Barrier::new(2));
        let publisher_boundary = boundary.clone();
        let publisher = tokio::spawn(async move {
            publisher_boundary.wait().await;
            updates.submit(Config {
                weight: 70,
                setting: 1,
                membership: 1,
            })
        });
        boundary.wait().await;
        publisher.await??; // Submission wins the decision's linearization race.
        fixture.driver.accept_evaluation(identity, evaluation);
        assert!(!fixture.driver.machine.status().state.is_terminal());
        assert_eq!(
            fixture.driver.machine.status().blocked_reason,
            Some(EvidenceReason::ConfigurationConflict)
        );
        fixture.finish().await
    }
    #[tokio::test]
    async fn external_update_racing_final_stage_success() -> TestResult {
        desired_update_race(StageEvaluation::Passed).await
    }
    #[tokio::test]
    async fn external_update_racing_condition_failure() -> TestResult {
        desired_update_race(StageEvaluation::Failed(ConditionViolation { condition: 0 })).await
    }
    #[tokio::test]
    async fn external_publication_racing_evaluation_commit() -> TestResult {
        let mut fixture = fixture().await?;
        let identity = fixture.identity();
        fixture.driver.current()?;
        let controller = fixture.controller.clone();
        tokio::spawn(async move {
            let token = controller.current_token().ok_or("no token")?;
            controller.publish_if_current(
                token,
                Preparer
                    .prepare(Config {
                        weight: 70,
                        setting: 1,
                        membership: 1,
                    })
                    .await?,
            )?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await??;
        fixture
            .driver
            .accept_evaluation(identity, StageEvaluation::Passed);
        assert_eq!(
            fixture.driver.machine.status().blocked_reason,
            Some(EvidenceReason::ConfigurationConflict)
        );
        assert_ne!(
            fixture.driver.machine.status().state,
            RolloutState::Completed
        );
        fixture.finish().await
    }
    #[tokio::test(start_paused = true)]
    async fn accept_observation_before_deadline() -> TestResult {
        let mut fixture = fixture().await?;
        let identity = fixture.identity();
        fixture
            .driver
            .accept_evaluation(identity, StageEvaluation::Passed);
        assert_eq!(
            fixture.driver.machine.status().state,
            RolloutState::Completed
        );
        fixture.finish().await
    }
    #[tokio::test(start_paused = true)]
    async fn reject_observation_after_deadline() -> TestResult {
        let mut fixture = fixture().await?;
        let identity = fixture.identity();
        tokio::time::advance(Duration::from_secs(11)).await;
        fixture
            .driver
            .accept_evaluation(identity, StageEvaluation::Passed);
        assert_eq!(
            fixture.driver.machine.status().blocked_reason,
            Some(EvidenceReason::Timeout)
        );
        assert_ne!(
            fixture.driver.machine.status().state,
            RolloutState::Completed
        );
        fixture.finish().await
    }
    async fn stage_submission_race(membership: bool, publication: bool) -> TestResult {
        let mut fixture = fixture_with_weights(&[5, 25, 100]).await?;
        let passed = fixture.identity();
        fixture
            .driver
            .accept_evaluation(passed, StageEvaluation::Passed);
        assert_eq!(fixture.driver.machine.status().current_stage, 1);
        let validated = fixture.driver.current()?;
        let replacement = Config {
            weight: 5,
            setting: 1,
            membership: if membership { 2 } else { 1 },
        };
        if publication {
            let token = fixture.controller.current_token().ok_or("no token")?;
            fixture
                .controller
                .publish_if_current(token, Preparer.prepare(replacement).await?)?;
        } else {
            let mut status = fixture.updates.subscribe_status();
            fixture.updates.submit(replacement)?;
            while !status.borrow_and_update().is_converged() {
                status.changed().await?;
            }
        }
        let revision = fixture.updates.status().desired_revision;
        assert!(
            fixture.driver.submit(validated).is_err(),
            "authorization must not transfer to the replacement configuration"
        );
        assert_eq!(fixture.updates.status().desired_revision, revision);
        assert_eq!(fixture.driver.machine.status().current_stage, 0);
        assert_eq!(fixture.driver.machine.status().desired_canary_weight, 5);
        assert_eq!(fixture.driver.machine.status().last_evaluation, None);
        fixture
            .driver
            .accept_evaluation(passed, StageEvaluation::Passed);
        assert_eq!(
            fixture.driver.machine.status().current_stage,
            0,
            "previous stage must be observed again"
        );
        fixture.finish().await
    }
    #[tokio::test]
    async fn compatible_update_between_revalidation_and_submission() -> TestResult {
        stage_submission_race(false, false).await
    }
    #[tokio::test]
    async fn membership_update_between_revalidation_and_submission() -> TestResult {
        stage_submission_race(true, false).await
    }
    #[tokio::test]
    async fn external_publication_between_validation_and_submission() -> TestResult {
        stage_submission_race(false, true).await
    }
}
