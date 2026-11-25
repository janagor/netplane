//! Pure attribution, statistical boundary and state transition regression tests.
use netplane::{GraphDraft, Netplane, RuntimeLimits};
use netplane_control::rollout::*;
use std::time::Duration;
use tokio::time::Instant;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
fn graph() -> Result<netplane::GraphVersion<(), ()>, netplane::ConfigError> {
    GraphDraft::new(tower::service_fn(|()| async {
        Ok::<_, std::convert::Infallible>(())
    }))
    .validate()
}
fn stage() -> Stage {
    Stage {
        canary_weight: 100,
        min_observation: Duration::from_secs(2),
        timeout: Duration::from_secs(10),
        conditions: vec![
            Condition::ErrorRate {
                group: "canary".into(),
                maximum_basis_points: 100,
                minimum_requests: 100,
            },
            Condition::P95Latency {
                group: "canary".into(),
                maximum: Duration::from_millis(200),
                minimum_requests: 100,
            },
        ],
    }
}
fn evidence(publication: netplane::VersionId) -> RolloutObservation {
    let since = Instant::now();
    RolloutObservation {
        request: ObservationRequest {
            publication,
            epoch: 1,
            since,
            until: since + Duration::from_secs(2),
            groups: ["stable".into(), "canary".into()],
        },
        complete: true,
        eligibility_changed: false,
        groups: ["stable", "canary"]
            .into_iter()
            .map(|group| GroupObservation {
                group: group.into(),
                completed_requests: 100,
                failed_requests: 1,
                p95_latency: Some(Duration::from_millis(200)),
                eligible_backends: 1,
                eligibility_revision: 0,
            })
            .collect(),
    }
}

#[tokio::test]
async fn statistical_boundaries_and_missing_evidence() -> TestResult {
    let runtime = Netplane::new(graph()?, RuntimeLimits { max_outstanding: 2 })?;
    let controller =
        netplane::ReconfigurationController::new(runtime, netplane::ControllerOptions::default());
    let publication = controller.inspect().active.ok_or("no publication")?.id;
    let original = evidence(publication);
    let request = original.request.clone();
    assert_eq!(
        evaluate(&stage(), &request, &original),
        StageEvaluation::Passed
    );
    for (completed, failed, latency, expected) in [
        (100, 0, Some(199), StageEvaluation::Passed),
        (
            100,
            2,
            Some(200),
            StageEvaluation::Failed(ConditionViolation { condition: 0 }),
        ),
        (
            100,
            0,
            Some(201),
            StageEvaluation::Failed(ConditionViolation { condition: 1 }),
        ),
        (
            0,
            0,
            Some(999),
            StageEvaluation::Insufficient(EvidenceReason::Samples),
        ),
        (
            99,
            99,
            Some(999),
            StageEvaluation::Insufficient(EvidenceReason::Samples),
        ),
        (
            100,
            0,
            None,
            StageEvaluation::Insufficient(EvidenceReason::Latency),
        ),
        (
            100,
            101,
            None,
            StageEvaluation::Insufficient(EvidenceReason::Attribution),
        ),
        (
            100,
            2,
            None,
            StageEvaluation::Failed(ConditionViolation { condition: 0 }),
        ),
    ] {
        let mut observation = original.clone();
        observation.groups[1].completed_requests = completed;
        observation.groups[1].failed_requests = failed;
        observation.groups[1].p95_latency = latency.map(Duration::from_millis);
        assert_eq!(evaluate(&stage(), &request, &observation), expected);
    }
    let mut different = original.clone();
    different.request.epoch += 1;
    assert_eq!(
        evaluate(&stage(), &request, &different),
        StageEvaluation::Insufficient(EvidenceReason::Attribution)
    );
    different = original.clone();
    different.groups[1].group = "stable".into();
    assert_eq!(
        evaluate(&stage(), &request, &different),
        StageEvaluation::Insufficient(EvidenceReason::Attribution)
    );
    different = original.clone();
    different.complete = false;
    assert_eq!(
        evaluate(&stage(), &request, &different),
        StageEvaluation::Insufficient(EvidenceReason::Attribution)
    );
    let token = controller.inspect().token.ok_or("no token")?;
    different = original;
    different.request.publication = controller.publish_if_current(token, graph()?)?.published;
    assert_eq!(
        evaluate(&stage(), &request, &different),
        StageEvaluation::Insufficient(EvidenceReason::Attribution)
    );
    Ok(())
}

#[tokio::test]
async fn machine_rejects_stale_duplicate_timed_out_and_terminal_events() -> TestResult {
    let runtime = Netplane::new(graph()?, RuntimeLimits { max_outstanding: 2 })?;
    let controller =
        netplane::ReconfigurationController::new(runtime, netplane::ControllerOptions::default());
    let publication = controller.inspect().active.ok_or("no publication")?.id;
    let spec = RolloutSpec {
        stable: "stable".into(),
        canary: "canary".into(),
        stages: vec![
            Stage {
                canary_weight: 5,
                ..stage()
            },
            stage(),
        ],
    };
    let mut machine = RolloutMachine::from_spec(&spec, publication, 1)?;
    assert_eq!(machine.status().state, RolloutState::Pending);
    assert_eq!(
        machine.transition(RolloutEvent::Start),
        RolloutAction::ApplyStage
    );
    let passed = RolloutEvent::Evaluated {
        epoch: 1,
        publication,
        evaluation: StageEvaluation::Passed,
    };
    assert_eq!(machine.transition(passed), RolloutAction::None);
    let published = RolloutEvent::Published {
        epoch: 1,
        publication,
    };
    assert_eq!(machine.transition(published), RolloutAction::Observe);
    assert_eq!(machine.transition(published), RolloutAction::None);
    assert_eq!(machine.transition(passed), RolloutAction::ApplyStage);
    assert_eq!(machine.status().current_stage, 1);
    assert_eq!(machine.transition(passed), RolloutAction::None);
    assert_eq!(machine.status().state, RolloutState::Applying);
    machine.transition(RolloutEvent::Published {
        epoch: 2,
        publication,
    });
    machine.transition(RolloutEvent::Block(EvidenceReason::Timeout));
    machine.transition(RolloutEvent::Evaluated {
        epoch: 2,
        publication,
        evaluation: StageEvaluation::Passed,
    });
    assert_eq!(machine.status().state, RolloutState::Blocked);
    machine.transition(RolloutEvent::Resume);
    machine.transition(RolloutEvent::Published {
        epoch: 3,
        publication,
    });
    machine.transition(RolloutEvent::Evaluated {
        epoch: 3,
        publication,
        evaluation: StageEvaluation::Passed,
    });
    assert_eq!(machine.status().state, RolloutState::Completed);
    machine.transition(RolloutEvent::Cancel);
    machine.transition(RolloutEvent::Start);
    assert_eq!(machine.status().state, RolloutState::Completed);
    Ok(())
}
