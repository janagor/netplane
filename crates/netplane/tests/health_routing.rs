//! Eligibility filtering retains deterministic weighted selection.
use netplane::{RoutingError, WeightedRouting, routing::UniformRouting};
use tower::BoxError;

#[test]
fn eligible_weights_redistribute_and_restore() -> Result<(), BoxError> {
    let policy = WeightedRouting::try_new([("a", 60), ("b", 30), ("c", 10), ("zero", 0)])?;
    let mut counts = [0_i32; 2];
    for index in 0_u64..100_000 {
        let key = index.to_le_bytes();
        assert_eq!(policy.select(&key), policy.select_eligible(&key, |_| true)?);
        let selected = policy.select_eligible(&key, |name| name != "b")?;
        assert_eq!(selected, policy.select_eligible(&key, |name| name != "b")?);
        counts[usize::from(selected == "c")] += 1;
        assert_eq!(policy.select_eligible(&key, |name| name == "b")?, "b");
    }
    assert!((counts[0] - 85_714).abs() < 1500, "{counts:?}");
    assert_eq!(
        policy.select_eligible(b"key", |_| false),
        Err(RoutingError::NoEligibleBackends)
    );
    assert_eq!(
        policy.select_eligible(b"key", |name| name == "zero"),
        Err(RoutingError::NoEligibleBackends)
    );
    Ok(())
}

#[test]
fn uniform_eligibility_uses_only_remaining_targets() -> Result<(), BoxError> {
    let policy = UniformRouting::try_new(["a".into(), "b".into(), "c".into()])?;
    for index in 0_u64..1000 {
        let key = index.to_le_bytes();
        assert_eq!(policy.select(&key), policy.select_eligible(&key, |_| true)?);
        assert_eq!(policy.select_eligible(&key, |name| name == "b")?, "b");
    }
    assert_eq!(
        policy.select_eligible(b"key", |_| false),
        Err(RoutingError::NoEligibleBackends)
    );
    Ok(())
}

#[test]
fn eligibility_is_sampled_once_and_zero_weights_are_not_observed() -> Result<(), BoxError> {
    let policy = WeightedRouting::try_new([("a", 60), ("zero", 0), ("b", 40)])?;
    let mut seen = Vec::new();
    let _selected = policy.select_eligible(b"key", |name| {
        seen.push(name.to_owned());
        true
    })?;
    assert_eq!(seen, ["a", "b"]);
    Ok(())
}

#[test]
fn handle_binding_rejects_missing_duplicate_and_unknown_labels() -> Result<(), BoxError> {
    use netplane::{
        HealthAwareRouting,
        health::{BackendIdentity, HealthPolicy, HealthRegistry},
    };
    use std::{num::NonZeroU32, time::Duration};
    let registry = HealthRegistry::default();
    let handle = registry.register(
        BackendIdentity {
            service: "test".into(),
            instance: "a".into(),
            incarnation: "one".into(),
        },
        HealthPolicy {
            failure_threshold: NonZeroU32::MIN,
            recovery_threshold: NonZeroU32::MIN,
            recovery_delay: Duration::ZERO,
        },
    )?;
    let policy = WeightedRouting::try_new([("a", 100)])?;
    assert!(HealthAwareRouting::try_new(policy.clone(), []).is_err());
    assert!(
        HealthAwareRouting::try_new(
            policy.clone(),
            [("a".into(), handle.clone()), ("a".into(), handle.clone())]
        )
        .is_err()
    );
    assert!(
        HealthAwareRouting::try_new(
            policy.clone(),
            [
                ("a".into(), handle.clone()),
                ("extra".into(), handle.clone())
            ]
        )
        .is_err()
    );
    let bound = HealthAwareRouting::try_new(policy, [("a".into(), handle)])?;
    assert_eq!(bound.targets().collect::<Vec<_>>(), ["a"]);
    assert_eq!(bound.select(b"key")?, "a");
    Ok(())
}
