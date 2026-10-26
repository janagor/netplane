//! Selector timing and deadline behavior for backend-only routing.

use super::*;

#[tokio::test(start_paused = true)]
async fn backend_only_unpolled_future_does_not_invoke_selector() -> Result<(), BoxError> {
    let selections = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&selections);
    let runtime = Netplane::new(
        GraphBuilder::new()
            .service(
                "b",
                service_fn(
                    |request: u32| async move { Ok::<_, Infallible>(format!("B:{request}")) },
                ),
            )
            .router("dispatch", move |_: &u32| {
                counted.fetch_add(1, Ordering::SeqCst);
                "b"
            })
            .route("dispatch", "b", "b")
            .fallback("dispatch", "b")
            .entry("dispatch")
            .without_outer_capacity()
            .validate()?,
        RuntimeLimits { max_outstanding: 2 },
    )?;
    let handle = runtime.handle();
    let unpolled = handle.try_execute(1)?;
    assert_eq!(selections.load(Ordering::SeqCst), 0);
    drop(unpolled);
    assert_eq!(selections.load(Ordering::SeqCst), 0);
    assert_eq!(handle.execute(2).await?, "B:2");
    assert_eq!(selections.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn backend_only_pre_expired_deadline_does_not_invoke_selector() -> Result<(), BoxError> {
    let selections = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&selections);
    let runtime = Netplane::new(
        GraphBuilder::new()
            .service(
                "b",
                service_fn(
                    |request: u32| async move { Ok::<_, Infallible>(format!("B:{request}")) },
                ),
            )
            .router("dispatch", move |_: &u32| {
                counted.fetch_add(1, Ordering::SeqCst);
                "b"
            })
            .route("dispatch", "b", "b")
            .fallback("dispatch", "b")
            .entry("dispatch")
            .without_outer_capacity()
            .validate()?,
        RuntimeLimits { max_outstanding: 1 },
    )?;
    let handle = runtime.handle();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let pending = handle.try_execute_until(1, deadline)?;
    assert_eq!(selections.load(Ordering::SeqCst), 0);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(pending.await.is_err());
    assert_eq!(selections.load(Ordering::SeqCst), 0);
    assert_eq!(handle.execute(2).await?, "B:2");
    assert_eq!(selections.load(Ordering::SeqCst), 1);
    Ok(())
}
