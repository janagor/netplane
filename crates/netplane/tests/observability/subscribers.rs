use super::*;

#[tokio::test]
async fn worker_and_publication_keep_their_respective_subscribers() -> TestResult {
    let worker_events = Arc::new(Mutex::new(Vec::new()));
    let publication_events = Arc::new(Mutex::new(Vec::new()));
    let version = {
        let subscriber = tracing_subscriber::registry().with(Events(Arc::clone(&worker_events)));
        let _scope = tracing::subscriber::set_default(subscriber);
        graph()?
    };
    {
        let subscriber =
            tracing_subscriber::registry().with(Events(Arc::clone(&publication_events)));
        let _scope = tracing::subscriber::set_default(subscriber);
        let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
        let published = publication_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(contains(&published, "version.endpoint", None));
        drop(published);
        drop(runtime);
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let terminated = {
                let observed = worker_events
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                contains(&observed, "worker.terminated", None)
            };
            if terminated {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let worker = worker_events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let publication = publication_events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(contains(&worker, "worker.terminated", None));
    assert!(!contains(&worker, "version.endpoint", None));
    assert!(!contains(&publication, "worker.terminated", None));
    Ok(())
}
