//! Deterministic high-contention admission checks.

use std::{collections::VecDeque, error::Error, sync::Arc, time::Duration};

use netplane::{AdmissionError, GraphDraft, Netplane, RuntimeLimits};
use tokio::sync::Barrier;
use tower::service_fn;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_cancellation_and_publication_race() -> TestResult {
    const PRODUCERS: usize = 8;
    const ROUNDS: usize = 100;
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut runtime = Netplane::new(
            versioned_graph(0)?,
            RuntimeLimits {
                max_outstanding: PRODUCERS,
            },
        )?;
        let handle = runtime.handle();
        let start = Arc::new(Barrier::new(PRODUCERS + 1));
        let selected = Arc::new(Barrier::new(PRODUCERS + 1));
        let work = Arc::new(Barrier::new(PRODUCERS + 1));
        let done = Arc::new(Barrier::new(PRODUCERS + 1));
        let producers: Vec<_> = (0..PRODUCERS)
            .map(|producer| {
                let handle = handle.clone();
                let start = Arc::clone(&start);
                let selected = Arc::clone(&selected);
                let work = Arc::clone(&work);
                let done = Arc::clone(&done);
                tokio::spawn(async move {
                    for round in 0..ROUNDS {
                        start.wait().await;
                        let selected_request = handle.try_execute(producer)?;
                        selected.wait().await;
                        work.wait().await;
                        if producer % 2 == 0 {
                            drop(selected_request);
                        } else {
                            assert_eq!(selected_request.await?, round);
                        }
                        done.wait().await;
                    }
                    Ok::<_, tower::BoxError>(())
                })
            })
            .collect();
        for round in 0..ROUNDS {
            start.wait().await;
            selected.wait().await;
            assert!(matches!(
                handle.try_execute(PRODUCERS),
                Err(AdmissionError::AtCapacity { limit: PRODUCERS })
            ));
            work.wait().await;
            runtime.publish(versioned_graph(round + 1)?)?;
            done.wait().await;
            assert_eq!(handle.try_execute(PRODUCERS)?.await?, round + 1);
        }
        for producer in producers {
            producer.await??;
        }
        runtime.shutdown(Duration::from_secs(1)).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

fn versioned_graph(
    version: usize,
) -> Result<netplane::GraphVersion<usize, usize>, netplane::ConfigError> {
    GraphDraft::new(service_fn(move |_: usize| async move {
        Ok::<_, std::convert::Infallible>(version)
    }))
    .validate()
}

#[tokio::test(flavor = "current_thread")]
async fn single_thread_contention_and_publication() -> TestResult {
    const LIMIT: usize = 32;
    const VERSIONS: usize = 100;
    let mut runtime = Netplane::new(
        versioned_graph(0)?,
        RuntimeLimits {
            max_outstanding: LIMIT,
        },
    )?;
    let handle = runtime.handle();
    let mut selected = VecDeque::new();
    for version in 0..VERSIONS {
        if version > 0 {
            runtime.publish(versioned_graph(version)?)?;
        }
        if selected.len() == LIMIT {
            let (expected, future) = selected.pop_front().ok_or("missing request")?;
            assert_eq!(future.await?, expected);
        }
        selected.push_back((version, handle.try_execute(version)?));
        if selected.len() == LIMIT {
            assert!(matches!(
                handle.try_execute(VERSIONS),
                Err(AdmissionError::AtCapacity { limit: LIMIT })
            ));
        }
    }
    for (expected, future) in selected {
        assert_eq!(future.await?, expected);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn thousands_of_simultaneous_attempts_share_one_budget() -> TestResult {
    const ATTEMPTS: usize = 4096;
    const LIMIT: usize = 128;
    let graph = GraphDraft::new(service_fn(|value: usize| async move {
        Ok::<_, std::convert::Infallible>(value)
    }))
    .validate()?;
    let runtime = Netplane::new(
        graph,
        RuntimeLimits {
            max_outstanding: LIMIT,
        },
    )?;
    let handle = runtime.handle();
    let barrier = std::sync::Arc::new(Barrier::new(ATTEMPTS + 1));
    let attempts: Vec<_> = (0..ATTEMPTS)
        .map(|id| {
            let handle = handle.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                handle.try_execute(id)
            })
        })
        .collect();
    barrier.wait().await;
    let mut accepted = Vec::new();
    let mut at_capacity = 0;
    for attempt in attempts {
        match attempt.await? {
            Ok(future) => accepted.push(future),
            Err(AdmissionError::AtCapacity { limit: LIMIT }) => at_capacity += 1,
            Err(error) => return Err(error.into()),
        }
    }
    assert_eq!(accepted.len(), LIMIT);
    assert_eq!(at_capacity, ATTEMPTS - LIMIT);
    accepted.truncate(LIMIT / 2);
    let refill: Vec<_> = (0..LIMIT / 2)
        .map(|id| handle.try_execute(id))
        .collect::<Result<_, _>>()?;
    assert_eq!(refill.len(), LIMIT / 2);
    assert!(matches!(
        handle.try_execute(ATTEMPTS),
        Err(AdmissionError::AtCapacity { limit: LIMIT })
    ));
    Ok(())
}
