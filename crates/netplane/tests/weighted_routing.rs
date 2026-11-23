//! Weighted policy validation and immutable publication behavior.
use std::{convert::Infallible, time::Duration};

use netplane::{
    ControllerOptions, GraphBuildError, GraphBuilder, GraphVersion, Netplane,
    ReconfigurationController, RuntimeLimits, WeightedRouting, WeightedRoutingError,
};
use tower::{BoxError, service_fn};

type TestResult = Result<(), BoxError>;

#[test]
fn rejects_invalid_configuration() {
    assert_eq!(
        WeightedRouting::try_new([] as [(&str, u8); 0]).err(),
        Some(WeightedRoutingError::EmptyTargets)
    );
    assert_eq!(
        WeightedRouting::try_new([("", 100)]).err(),
        Some(WeightedRoutingError::EmptyName)
    );
    assert_eq!(
        WeightedRouting::try_new([("a", 50), ("a", 50)]).err(),
        Some(WeightedRoutingError::DuplicateTarget("a".into()))
    );
    for total in [0, 99, 101, 255] {
        assert_eq!(
            WeightedRouting::try_new([("a", total)]).err(),
            Some(WeightedRoutingError::InvalidTotal {
                total: usize::from(total)
            })
        );
    }
    assert_eq!(
        WeightedRouting::try_new([("a", 255), ("b", 255)]).err(),
        Some(WeightedRoutingError::InvalidTotal { total: 510 })
    );
}

#[test]
fn deterministic_boundaries_zero_weights_and_order() -> TestResult {
    // Known gateway FNV-1a buckets: 19, 30, 41, and 8.
    let policy =
        WeightedRouting::try_new([("zero", 0), ("a", 19), ("b", 11), ("c", 70), ("end", 0)])?;
    assert_eq!(
        policy.targets().collect::<Vec<_>>(),
        ["zero", "a", "b", "c", "end"]
    );
    for (key, expected) in [
        ("customer-1", "b"),
        ("customer-2", "c"),
        ("customer-3", "c"),
        ("user-1", "a"),
    ] {
        for _ in 0..10 {
            assert_eq!(policy.select(key.as_bytes()), expected);
            assert_eq!(policy.clone().select(key.as_bytes()), expected);
        }
    }
    for weights in [[("a", 0), ("b", 100)], [("a", 100), ("b", 0)]] {
        let policy = WeightedRouting::try_new(weights)?;
        let expected = if weights[0].1 == 0 { "b" } else { "a" };
        for key in [b"".as_slice(), b"customer-1", &[0, 255]] {
            assert_eq!(policy.select(key), expected);
        }
    }
    Ok(())
}

#[test]
fn fixed_dataset_distribution_and_monotonic_expansion() -> TestResult {
    let ten = WeightedRouting::try_new([("canary", 10), ("stable", 90)])?;
    let expanded = WeightedRouting::try_new([("canary", 25), ("stable", 75)])?;
    let three = WeightedRouting::try_new([("a", 20), ("b", 30), ("c", 50)])?;
    let mut counts = [0_i32; 3];
    for index in 0..10_000 {
        let key = format!("customer-{index}");
        if ten.select(key.as_bytes()) == "canary" {
            assert_eq!(expanded.select(key.as_bytes()), "canary");
        }
        let slot = match three.select(key.as_bytes()) {
            "a" => 0,
            "b" => 1,
            "c" => 2,
            unexpected => return Err(format!("unexpected backend label: {unexpected}").into()),
        };
        counts[slot] += 1;
    }
    for (actual, expected) in counts.into_iter().zip([2000, 3000, 5000]) {
        assert!((actual - expected).abs() < 200, "{counts:?}");
    }
    Ok(())
}

fn graph(
    percent: u8,
    buffered: bool,
) -> Result<GraphVersion<Option<String>, &'static str>, BoxError> {
    let policy = WeightedRouting::try_new([("canary", percent), ("stable", 100 - percent)])?;
    let selector = policy.clone();
    let mut builder = GraphBuilder::new()
        .service(
            "canary",
            service_fn(|_| async { Ok::<_, Infallible>("canary") }),
        )
        .service(
            "stable",
            service_fn(|_| async { Ok::<_, Infallible>("stable") }),
        )
        .service(
            "fallback",
            service_fn(|_| async { Ok::<_, Infallible>("fallback") }),
        )
        .router("policy", move |key: &Option<String>| {
            key.as_ref().map_or_else(
                || "invalid".to_owned(),
                |key| selector.select(key.as_bytes()).to_owned(),
            )
        });
    for name in policy.targets() {
        builder = builder.route("policy", name, name);
    }
    if !buffered {
        builder = builder.without_outer_capacity();
    }
    Ok(builder
        .fallback("policy", "fallback")
        .entry("policy")
        .validate()?)
}

#[tokio::test]
async fn publication_retains_unpolled_requests_and_rollback_restores_policy() -> TestResult {
    for buffered in [false, true] {
        let runtime = Netplane::new(
            graph(10, buffered)?,
            RuntimeLimits {
                max_outstanding: 10,
            },
        )?;
        let control = ReconfigurationController::new(runtime, ControllerOptions::default());
        let original = control.inspect().active.ok_or("missing active")?.id;
        let handle = control.handle();
        let accepted = handle.execute(Some("customer-1".into()));
        let dropped = handle.execute(Some("customer-1".into()));
        control.publish_if_current(
            control.current_token().ok_or("missing token")?,
            graph(25, buffered)?,
        )?;
        drop(dropped);
        assert_eq!(accepted.await?, "stable");
        assert_eq!(handle.execute(Some("customer-1".into())).await?, "canary");
        assert_eq!(handle.execute(None).await?, "fallback");
        control.rollback_if_current(control.current_token().ok_or("missing token")?, original)?;
        assert_eq!(handle.execute(Some("customer-1".into())).await?, "stable");
        assert_eq!(handle.status().outstanding, 0);
        control.shutdown(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[test]
fn missing_backend_is_rejected_before_workers_start() -> TestResult {
    let policy = WeightedRouting::try_new([("missing", 100)])?;
    let mut builder = GraphBuilder::new()
        .service(
            "fallback",
            service_fn(|(): ()| async { Ok::<_, Infallible>(()) }),
        )
        .router("policy", |(): &()| "missing");
    for name in policy.targets() {
        builder = builder.route("policy", name, name);
    }
    assert!(
        matches!(builder.fallback("policy", "fallback").entry("policy").validate(), Err(GraphBuildError::UnknownTarget(name)) if name == "missing")
    );
    Ok(())
}

#[tokio::test]
async fn dispatched_request_finishes_on_original_backend_after_publication() -> TestResult {
    let started = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let policy = WeightedRouting::try_new([("canary", 100)])?;
    let backend_started = started.clone();
    let backend_release = release.clone();
    let initial = GraphBuilder::new()
        .service(
            "canary",
            service_fn(move |_: Option<String>| {
                let started = backend_started.clone();
                let release = backend_release.clone();
                async move {
                    started.notify_one();
                    release.notified().await;
                    Ok::<_, Infallible>("old-canary")
                }
            }),
        )
        .router("policy", move |key: &Option<String>| {
            policy
                .select(key.as_deref().unwrap_or_default().as_bytes())
                .to_owned()
        })
        .route("policy", "canary", "canary")
        .fallback("policy", "canary")
        .entry("policy")
        .validate()?;
    let mut runtime = Netplane::new(initial, RuntimeLimits { max_outstanding: 2 })?;
    let handle = runtime.handle();
    let accepted = tokio::spawn(handle.execute(Some("customer-1".into())));
    tokio::time::timeout(Duration::from_secs(1), started.notified()).await?;
    runtime.publish(graph(0, true)?)?;
    assert_eq!(handle.execute(Some("customer-1".into())).await?, "stable");
    release.notify_one();
    assert_eq!(accepted.await??, "old-canary");
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
