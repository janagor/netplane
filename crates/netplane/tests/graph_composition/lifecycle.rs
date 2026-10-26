use super::*;

#[tokio::test]
async fn old_graph_workers_are_released() -> TestResult {
    timeout(TEST_DEADLINE, async {
        let (a, _, polled, _, dropped) = gated_backend()?;
        let mut runtime = composed(a, named_backend("old-B", Arc::new(AtomicUsize::new(0)))?, 2)?;
        let handle = runtime.handle();
        let old = tokio::spawn(handle.execute(request(EVEN_KEY)));
        polled.await?;
        runtime.publish(composed_graph(
            named_backend("new-A", Arc::new(AtomicUsize::new(0)))?,
            named_backend("new-B", Arc::new(AtomicUsize::new(0)))?,
        )?)?;
        old.abort();
        assert!(old.await.is_err());
        dropped.await?;
        assert_eq!(handle.execute(request(ODD_KEY)).await?, "new-B:3");
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_drains_composed_graph() -> TestResult {
    timeout(TEST_DEADLINE, async {
        let runtime = composed(
            named_backend("A", Arc::new(AtomicUsize::new(0)))?,
            named_backend("B", Arc::new(AtomicUsize::new(0)))?,
            1,
        )?;
        let mut runtime = runtime;
        let handle = runtime.handle();
        let accepted = handle.try_execute(request(EVEN_KEY))?;
        let draining = runtime.shutdown(TEST_DEADLINE);
        assert!(matches!(
            handle.try_execute(request(ODD_KEY)),
            Err(AdmissionError::Closed)
        ));
        assert_eq!(accepted.await?, "A:2");
        draining.await?;
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}
