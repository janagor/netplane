//! Controller publication, rollback, and shutdown contracts.
use std::{
    convert::Infallible,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use netplane::{
    Capacity, ControllerError, ControllerOptions, GraphBuilder, GraphDraft, LifecycleState,
    Netplane, PublicationKind, ReconfigurationController, RuntimeLimits,
};
use tokio::sync::Barrier;
use tower::service_fn;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

fn graph(value: usize) -> Result<netplane::GraphVersion<usize, usize>, netplane::ConfigError> {
    GraphDraft::new(service_fn(move |_: usize| async move {
        Ok::<_, Infallible>(value)
    }))
    .validate()
}

fn backend_only_graph(
    value: usize,
) -> Result<netplane::GraphVersion<usize, usize>, netplane::GraphBuildError> {
    GraphBuilder::new()
        .service(
            "backend",
            service_fn(move |_: usize| async move { Ok::<_, Infallible>(value) }),
        )
        .router("router", |_: &usize| "backend")
        .fallback("router", "backend")
        .entry("router")
        .without_outer_capacity()
        .validate()
}

#[tokio::test]
async fn backend_only_rollback_republishes_executable_state() -> TestResult {
    let runtime = Netplane::new(
        backend_only_graph(11)?,
        RuntimeLimits { max_outstanding: 2 },
    )?;
    let control = ReconfigurationController::new(runtime, ControllerOptions::default());
    let original = control.inspect().active.ok_or("missing active")?.id;
    let selected = control.handle().try_execute(0)?;
    control.publish_if_current(
        control.current_token().ok_or("missing token")?,
        backend_only_graph(22)?,
    )?;
    assert_eq!(selected.await?, 11);
    assert_eq!(control.handle().execute(0).await?, 22);
    control.rollback_if_current(control.current_token().ok_or("missing token")?, original)?;
    assert_eq!(control.handle().execute(0).await?, 11);
    assert_eq!(
        control
            .inspect()
            .active
            .ok_or("missing active")?
            .metadata
            .capacity,
        None
    );
    control.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

fn controller(
    limit: usize,
) -> Result<ReconfigurationController<usize, usize>, netplane::ConfigError> {
    let runtime = Netplane::new(
        graph(1)?,
        RuntimeLimits {
            max_outstanding: 16,
        },
    )?;
    Ok(ReconfigurationController::new(
        runtime,
        ControllerOptions {
            max_retained_versions: limit,
        },
    ))
}

#[tokio::test]
async fn replacement_rollback_and_bounded_history() -> TestResult {
    let control = controller(2)?;
    let handle = control.handle();
    let original = control.inspect().active.ok_or("missing original")?.id;
    let selected = handle.try_execute(0)?;
    assert_eq!(handle.status().outstanding, 1);
    let first =
        control.publish_if_current(control.current_token().ok_or("missing token")?, graph(2)?)?;
    assert_eq!(first.previous, original);
    assert_eq!(selected.await?, 1);
    assert_eq!(handle.status().outstanding, 0);
    assert_eq!(handle.execute(0).await?, 2);
    let second =
        control.publish_if_current(control.current_token().ok_or("missing token")?, graph(3)?)?;
    let rollback =
        control.rollback_if_current(control.current_token().ok_or("missing token")?, original)?;
    assert!(rollback.published.get() > second.published.get());
    assert_eq!(
        rollback.kind,
        PublicationKind::Rollback { source: original }
    );
    assert_eq!(handle.execute(0).await?, 1);
    assert_eq!(handle.status().outstanding, 0);
    let snapshot = control.inspect();
    assert_eq!(
        snapshot.active.ok_or("missing active")?.id,
        rollback.published
    );
    assert_eq!(snapshot.retained.len(), 2);
    assert_eq!(snapshot.retained[0].id, first.published);
    assert_eq!(snapshot.last_outcome, Some(rollback));
    assert_eq!(
        control.rollback_if_current(control.current_token().ok_or("missing token")?, original),
        Err(ControllerError::UnknownVersion(original))
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writers_and_foreign_tokens() -> TestResult {
    let control = Arc::new(controller(0)?);
    let foreign = controller(0)?;
    let token = control.current_token().ok_or("missing token")?;
    assert_eq!(
        control.publish_if_current(
            foreign.current_token().ok_or("missing foreign token")?,
            graph(4)?
        ),
        Err(ControllerError::ForeignRuntime)
    );
    let barrier = Arc::new(Barrier::new(3));
    let mut tasks = Vec::new();
    for value in [2, 3] {
        let candidate = graph(value)?;
        let control = Arc::clone(&control);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            control.publish_if_current(token, candidate)
        }));
    }
    barrier.wait().await;
    let results = [tasks.remove(0).await?, tasks.remove(0).await?];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(ControllerError::Stale { .. })))
            .count(),
        1
    );
    assert_eq!(control.inspect().retained.len(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_and_shutdown_are_serialized() -> TestResult {
    let control = Arc::new(controller(2)?);
    let snapshot = control.inspect();
    let initial = snapshot.active.ok_or("missing active")?.id;
    let expected = snapshot.token.ok_or("missing token")?;
    let held = control.handle().try_execute(0)?;
    let candidate = graph(2)?;
    let barrier = Arc::new(Barrier::new(3));

    let publisher = Arc::clone(&control);
    let publication_barrier = Arc::clone(&barrier);
    let publication = tokio::spawn(async move {
        publication_barrier.wait().await;
        publisher.publish_if_current(expected, candidate)
    });
    let shutting_down = Arc::clone(&control);
    let shutdown_barrier = Arc::clone(&barrier);
    let shutdown = tokio::spawn(async move {
        shutdown_barrier.wait().await;
        shutting_down.shutdown(Duration::ZERO).await
    });
    barrier.wait().await;

    let result = publication.await?;
    let drain = shutdown.await?;
    assert!(drain.is_err());
    let active = control
        .inspect()
        .active
        .ok_or("missing active before drain")?
        .id;
    match result {
        Ok(outcome) => {
            assert_eq!(outcome.previous, initial);
            assert_eq!(active, outcome.published);
        }
        Err(ControllerError::ShuttingDown) => assert_eq!(active, initial),
        Err(error) => return Err(error.into()),
    }
    assert!(control.inspect().token.is_none());
    assert_eq!(
        control.publish_if_current(expected, graph(3)?),
        Err(ControllerError::ShuttingDown)
    );
    assert_eq!(
        control.rollback_if_current(expected, initial),
        Err(ControllerError::ShuttingDown)
    );
    assert_eq!(held.await?, 1);
    control.shutdown(Duration::from_secs(1)).await?;
    assert!(control.inspect().active.is_none());
    Ok(())
}

#[tokio::test]
async fn invalid_candidate_and_shutdown_without_poll() -> TestResult {
    let control = controller(2)?;
    let initial = control.inspect().active.ok_or("missing active")?.id;
    let invalid = GraphDraft::new(service_fn(|value: usize| async move {
        Ok::<_, Infallible>(value)
    }))
    .capacity(Capacity {
        max_in_flight: 0,
        buffer_capacity: 1,
    })
    .validate::<usize, usize>();
    assert!(invalid.is_err());
    assert_eq!(
        control.inspect().active.ok_or("missing active")?.id,
        initial
    );
    let token = control.current_token().ok_or("missing token")?;
    control.publish_if_current(token, graph(2)?)?;
    let held = control.handle().try_execute(0)?;
    let drain = control.shutdown(Duration::ZERO);
    let retained = control.handle();
    assert_eq!(retained.status().lifecycle, LifecycleState::Draining);
    assert_eq!(retained.status().outstanding, 1);
    assert!(control.current_token().is_none());
    assert!(control.inspect().retained.is_empty());
    assert_eq!(
        control.publish_if_current(token, graph(3)?),
        Err(ControllerError::ShuttingDown)
    );
    assert_eq!(
        control.rollback_if_current(token, initial),
        Err(ControllerError::ShuttingDown)
    );
    assert!(control.handle().try_execute(0).is_err());
    assert!(drain.await.is_err());
    drop(held);
    assert_eq!(retained.status().outstanding, 0);
    assert_eq!(retained.status().lifecycle, LifecycleState::Draining);
    control.shutdown(Duration::from_secs(1)).await?;
    assert_eq!(retained.status().lifecycle, LifecycleState::Stopped);
    assert!(control.inspect().active.is_none());
    Ok(())
}

#[tokio::test]
async fn rollback_and_eviction_preserve_selected_worker_until_future_drop() -> TestResult {
    struct DropProbe(Arc<AtomicUsize>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let probe = DropProbe(Arc::clone(&drops));
    let old = GraphDraft::new(service_fn(move |value: usize| {
        let _ = &probe;
        async move { Ok::<_, Infallible>(value) }
    }))
    .validate()?;
    let runtime = Netplane::new(old, RuntimeLimits { max_outstanding: 4 })?;
    let control = ReconfigurationController::new(
        runtime,
        ControllerOptions {
            max_retained_versions: 1,
        },
    );
    let original = control.inspect().active.ok_or("missing active")?.id;
    control.publish_if_current(control.current_token().ok_or("missing token")?, graph(2)?)?;
    let rollback =
        control.rollback_if_current(control.current_token().ok_or("missing token")?, original)?;
    let selected_publication = rollback.published;
    let handle = control.handle();
    let selected = handle.execute(0);
    assert_eq!(handle.status().outstanding, 1);
    control.publish_if_current(control.current_token().ok_or("missing token")?, graph(3)?)?;
    control.publish_if_current(control.current_token().ok_or("missing token")?, graph(4)?)?;
    let snapshot = control.inspect();
    assert_ne!(
        snapshot.active.ok_or("missing active")?.id,
        selected_publication
    );
    assert!(
        snapshot
            .retained
            .iter()
            .all(|v| v.id != selected_publication)
    );
    assert_eq!(handle.status().outstanding, 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(selected.await?, 0);
    assert_eq!(handle.status().outstanding, 0);
    tokio::time::timeout(Duration::from_secs(1), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn stale_rollback_and_zero_history_preserve_active() -> TestResult {
    let control = controller(0)?;
    let original = control.inspect();
    let expected = original.token.ok_or("missing token")?;
    let original_id = original.active.ok_or("missing active")?.id;
    let published = control.publish_if_current(expected, graph(2)?)?;
    assert_eq!(
        control.rollback_if_current(expected, original_id),
        Err(ControllerError::Stale {
            expected: original_id,
            actual: published.published
        })
    );
    assert_eq!(
        control.rollback_if_current(control.inspect().token.ok_or("missing token")?, original_id),
        Err(ControllerError::UnknownVersion(original_id))
    );
    assert_eq!(
        control.inspect().active.ok_or("missing active")?.id,
        published.published
    );
    assert_eq!(control.handle().execute(0).await?, 2);
    Ok(())
}

#[tokio::test]
async fn rollback_preserves_inflight_request_and_shared_service_state() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let original = GraphDraft::new(service_fn(move |_: usize| {
        let count = observed.fetch_add(1, Ordering::SeqCst) + 1;
        async move { Ok::<_, Infallible>(count) }
    }))
    .validate()?;
    let runtime = Netplane::new(original, RuntimeLimits { max_outstanding: 4 })?;
    let control = ReconfigurationController::new(runtime, ControllerOptions::default());
    let original_id = control.inspect().active.ok_or("missing active")?.id;
    assert_eq!(control.handle().execute(0).await?, 1);
    let held = control.handle().try_execute(0)?;
    assert_eq!(control.handle().status().outstanding, 1);
    control.publish_if_current(control.inspect().token.ok_or("missing token")?, graph(20)?)?;
    let first_rollback = control
        .rollback_if_current(control.inspect().token.ok_or("missing token")?, original_id)?;
    assert_eq!(control.handle().execute(0).await?, 2);
    assert_eq!(control.handle().status().outstanding, 1);
    control.publish_if_current(control.inspect().token.ok_or("missing token")?, graph(30)?)?;
    assert_eq!(held.await?, 3);
    assert_eq!(control.handle().status().outstanding, 0);
    assert_eq!(control.handle().execute(0).await?, 30);
    let second_rollback = control.rollback_if_current(
        control.inspect().token.ok_or("missing token")?,
        first_rollback.published,
    )?;
    assert!(second_rollback.published.get() > first_rollback.published.get());
    assert_eq!(control.handle().execute(0).await?, 4);
    Ok(())
}

#[tokio::test]
async fn invalid_graph_builder_does_not_change_active() -> TestResult {
    let control = controller(2)?;
    let before = control.inspect();
    let invalid = GraphBuilder::<usize, usize>::new()
        .service(
            "backend",
            service_fn(|value: usize| async move { Ok::<_, Infallible>(value) }),
        )
        .router("route", |_: &usize| "missing")
        .route("route", "missing", "absent")
        .fallback("route", "backend")
        .entry("route")
        .validate();
    assert!(invalid.is_err());
    let after = control.inspect();
    assert_eq!(after.active, before.active);
    assert_eq!(after.token, before.token);
    assert_eq!(control.handle().execute(0).await?, 1);
    Ok(())
}

#[tokio::test]
async fn validated_graph_builder_publishes_through_controller() -> TestResult {
    let control = controller(1)?;
    let snapshot = control.inspect();
    let expected = snapshot.token.ok_or("missing token")?;
    let candidate = GraphBuilder::new()
        .service(
            "a",
            service_fn(|_: usize| async { Ok::<_, Infallible>(10) }),
        )
        .service(
            "b",
            service_fn(|_: usize| async { Ok::<_, Infallible>(20) }),
        )
        .router("route", |value: &usize| if *value == 0 { "a" } else { "b" })
        .route("route", "a", "a")
        .route("route", "b", "b")
        .fallback("route", "b")
        .entry("route")
        .validate()?;
    control.publish_if_current(expected, candidate)?;
    assert_eq!(control.handle().execute(0).await?, 10);
    assert_eq!(control.handle().execute(1).await?, 20);
    assert_eq!(
        control
            .inspect()
            .active
            .ok_or("missing active")?
            .metadata
            .entry,
        "route"
    );
    Ok(())
}

#[tokio::test]
async fn timed_out_shutdown_keeps_request_and_remains_retryable() -> TestResult {
    let control = controller(2)?;
    let held = control.handle().try_execute(0)?;
    assert!(control.shutdown(Duration::ZERO).await.is_err());
    assert!(control.inspect().token.is_none());
    assert_eq!(held.await?, 1);
    control.shutdown(Duration::from_secs(1)).await?;
    assert!(control.inspect().active.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_inspection_tokens_match_observed_publication() -> TestResult {
    let control = Arc::new(controller(2)?);
    let publish = Arc::clone(&control);
    let writer = tokio::spawn(async move {
        for value in 2..50 {
            loop {
                let snapshot = publish.inspect();
                let token = snapshot.token.ok_or("missing token")?;
                match publish.publish_if_current(token, graph(value)?) {
                    Ok(_) => break,
                    Err(ControllerError::Stale { .. }) => tokio::task::yield_now().await,
                    Err(error) => return Err(error.into()),
                }
            }
            tokio::task::yield_now().await;
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });
    for _ in 0..50 {
        let snapshot = control.inspect();
        let observed = snapshot.active.ok_or("missing active")?.id;
        let token = snapshot.token.ok_or("missing token")?;
        match control.publish_if_current(token, graph(100)?) {
            Ok(_) => (),
            Err(ControllerError::Stale { expected, .. }) => assert_eq!(expected, observed),
            Err(error) => return Err(error.into()),
        }
        tokio::task::yield_now().await;
    }
    writer.await??;
    Ok(())
}
