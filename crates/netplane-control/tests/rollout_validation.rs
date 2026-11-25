//! Whole-specification validation before any publication.
use netplane_control::rollout::{Condition, RolloutSpec, Stage, ValidationError};
use std::time::Duration;

fn stage(weight: u8) -> Stage {
    let mut conditions = vec![Condition::Eligible {
        group: "canary".into(),
        minimum: 1,
    }];
    if weight > 0 {
        conditions.push(Condition::Requests {
            group: "canary".into(),
            minimum: 1,
        });
    }
    Stage {
        canary_weight: weight,
        min_observation: Duration::from_secs(1),
        timeout: Duration::from_secs(10),
        conditions,
    }
}

#[test]
fn validates_entire_progression_before_start() {
    let mut spec = RolloutSpec {
        stable: "stable".into(),
        canary: "canary".into(),
        stages: vec![],
    };
    assert_eq!(spec.validate(), Err(ValidationError::Stages));
    spec.stages = vec![stage(0), stage(5), stage(100)];
    assert_eq!(spec.validate(), Ok(()));
    spec.stages.insert(2, stage(1));
    assert_eq!(spec.validate(), Err(ValidationError::Weight(2)));
    spec.stages = vec![stage(101), stage(100)];
    assert_eq!(spec.validate(), Err(ValidationError::Weight(0)));
    spec.stages = vec![stage(100)];
    spec.stages[0].conditions.push(Condition::ErrorRate {
        group: "canary".into(),
        maximum_basis_points: 100,
        minimum_requests: 0,
    });
    assert_eq!(spec.validate(), Err(ValidationError::Condition(0)));
}

#[test]
fn rejects_invalid_groups_timings_conditions_and_zero_traffic_samples() {
    let valid = RolloutSpec {
        stable: "stable".into(),
        canary: "canary".into(),
        stages: vec![stage(100)],
    };
    let mut changed = valid.clone();
    changed.canary = changed.stable.clone();
    assert_eq!(changed.validate(), Err(ValidationError::Groups));
    changed = valid.clone();
    changed.stages[0].canary_weight = 50;
    assert_eq!(changed.validate(), Err(ValidationError::Stages));
    changed = valid.clone();
    changed.stages[0].conditions.clear();
    assert_eq!(changed.validate(), Err(ValidationError::Condition(0)));
    for timeout in [Duration::ZERO, Duration::from_millis(1), Duration::MAX] {
        changed = valid.clone();
        changed.stages[0].timeout = timeout;
        assert_eq!(changed.validate(), Err(ValidationError::Timing(0)));
    }
    for condition in [
        Condition::Requests {
            group: "canary".into(),
            minimum: 0,
        },
        Condition::Requests {
            group: "stable".into(),
            minimum: 10,
        },
        Condition::ErrorRate {
            group: "canary".into(),
            maximum_basis_points: 10_001,
            minimum_requests: 100,
        },
        Condition::P95Latency {
            group: "canary".into(),
            maximum: Duration::from_millis(100),
            minimum_requests: 0,
        },
    ] {
        changed = valid.clone();
        changed.stages[0].conditions = vec![condition];
        assert_eq!(changed.validate(), Err(ValidationError::Condition(0)));
    }
    changed = valid.clone();
    changed.stages[0].conditions.push(Condition::Eligible {
        group: "missing".into(),
        minimum: 1,
    });
    assert_eq!(changed.validate(), Err(ValidationError::Groups));
    changed = valid;
    changed.stages.insert(0, stage(0));
    changed.stages[0].conditions.push(Condition::Requests {
        group: "canary".into(),
        minimum: 1,
    });
    assert_eq!(changed.validate(), Err(ValidationError::Condition(0)));
}
