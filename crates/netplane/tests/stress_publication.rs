//! Concurrent publication and synchronous request-version selection.
#[path = "common/drop.rs"]
mod cleanup;
mod common;

use std::{
    convert::Infallible,
    error::Error,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use cleanup::wait_for_drop;
use common::{CAPACITY, DEADLINE, Reply, graph};
use netplane::{ConfigError, GraphDraft, GraphVersion, Netplane, RuntimeLimits};
use tokio::{sync::Barrier, time::timeout};
use tower::service_fn;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn completed_future_does_not_retain_old_worker() -> TestResult {
    timeout(DEADLINE, async {
        let (first, gate) = graph(0, CAPACITY)?;
        gate.permits.add_permits(1);
        let mut runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 1 })?;
        let handle = runtime.handle();
        let mut completed = Box::pin(handle.try_execute(1)?);
        let response = std::future::poll_fn(|cx| match completed.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => Poll::Pending,
        })
        .await?;
        assert_eq!(response.version, 0);
        let (next, _) = graph(1, CAPACITY)?;
        runtime.publish(next)?;
        wait_for_drop(&gate.dropped, 1).await;
        drop(completed);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn publication_stress_retains_and_releases_selected_versions() -> TestResult {
    const VERSIONS: usize = 200;
    timeout(Duration::from_secs(10), async {
        let (first, mut gate) = graph(0, CAPACITY)?;
        let mut runtime = Netplane::new(
            first,
            RuntimeLimits {
                max_outstanding: VERSIONS,
            },
        )?;
        let handle = runtime.handle();
        let mut retained = Vec::with_capacity(VERSIONS);
        let mut gates = Vec::with_capacity(VERSIONS);
        for version in 0..VERSIONS {
            retained.push(handle.try_execute(version)?);
            gates.push(Arc::clone(&gate.dropped));
            if version + 1 < VERSIONS {
                let (next, next_gate) = graph(version + 1, CAPACITY)?;
                runtime.publish(next)?;
                gate = next_gate;
            }
        }
        assert!(
            gates
                .iter()
                .all(|drop_count| drop_count.load(Ordering::SeqCst) == 0)
        );
        drop(retained);
        for drop_count in gates.iter().take(VERSIONS - 1) {
            wait_for_drop(drop_count, 1).await;
        }
        assert_eq!(gates[VERSIONS - 1].load(Ordering::SeqCst), 0);
        runtime.shutdown(Duration::from_secs(1)).await?;
        wait_for_drop(&gates[VERSIONS - 1], 1).await;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn accepted_requests_keep_their_version_across_multiple_publications() -> TestResult {
    timeout(DEADLINE, async {
        let (first, mut a) = graph(0, CAPACITY)?;
        let mut runtime = Netplane::new(
            first,
            RuntimeLimits {
                max_outstanding: 32,
            },
        )?;
        let handle = runtime.handle();
        let retained = handle.try_execute(1)?;
        let executing = tokio::spawn(handle.execute(2));
        assert_eq!(a.entered.recv().await, Some(2));
        for version in 1..=4 {
            let (next, mut gate) = graph(version, CAPACITY)?;
            runtime.publish(next)?;
            let result = tokio::spawn(handle.execute(version + 10));
            assert_eq!(gate.entered.recv().await, Some(version + 10));
            gate.permits.add_permits(1);
            assert_eq!(result.await??.version, version);
            assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
        }
        let old = tokio::spawn(retained);
        assert_eq!(a.entered.recv().await, Some(1));
        assert_eq!(a.dropped.load(Ordering::SeqCst), 0);
        a.permits.add_permits(2);
        assert_eq!(old.await??.version, 0);
        assert_eq!(executing.await??.version, 0);
        assert_eq!(a.calls.load(Ordering::SeqCst), 2);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_producers_observe_complete_published_versions() -> TestResult {
    timeout(DEADLINE, async {
        let (first, gate) = graph(0, CAPACITY)?;
        gate.permits.add_permits(8);
        let mut runtime = Netplane::new(
            first,
            RuntimeLimits {
                max_outstanding: 256,
            },
        )?;
        let handle = runtime.handle();
        let start = std::sync::Arc::new(Barrier::new(9));
        let selected = std::sync::Arc::new(Barrier::new(9));
        let release = std::sync::Arc::new(Barrier::new(9));
        let completed = std::sync::Arc::new(Barrier::new(9));
        let producers: Vec<_> = (0..8)
            .map(|producer| {
                let handle = handle.clone();
                let start = start.clone();
                let selected = selected.clone();
                let release = release.clone();
                let completed = completed.clone();
                tokio::spawn(async move {
                    for version in 0..16 {
                        start.wait().await;
                        let id = version * 8 + producer;
                        let future = handle.try_execute(id)?;
                        selected.wait().await;
                        release.wait().await;
                        let reply = future.await?;
                        assert_eq!(reply.id, id);
                        assert_eq!(reply.version, version);
                        completed.wait().await;
                    }
                    Ok::<_, tower::BoxError>(())
                })
            })
            .collect();
        for version in 0..16 {
            start.wait().await;
            selected.wait().await;
            let (next, replacement) = graph(version + 1, CAPACITY)?;
            replacement.permits.add_permits(9);
            runtime.publish(next)?;
            assert_eq!(handle.execute(1000 + version).await?.version, version + 1);
            release.wait().await;
            completed.wait().await;
        }
        for producer in producers {
            producer.await??;
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

fn racing_graph(
    version: usize,
    calls: Arc<Vec<AtomicUsize>>,
) -> Result<GraphVersion<usize, Reply>, ConfigError> {
    GraphDraft::new(service_fn(move |id: usize| {
        calls[id].fetch_add(1, Ordering::SeqCst);
        async move { Ok::<_, Infallible>(Reply { version, id }) }
    }))
    .validate()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publication_races_with_thousands_of_version_selections() -> TestResult {
    const ROUNDS: usize = 64;
    const PRODUCERS: usize = 8;
    const REQUESTS_PER_PRODUCER: usize = 16;
    const REQUESTS_PER_ROUND: usize = PRODUCERS * REQUESTS_PER_PRODUCER;
    const TOTAL_REQUESTS: usize = ROUNDS * (REQUESTS_PER_ROUND + 1);

    timeout(Duration::from_secs(30), async {
        let calls = Arc::new(
            (0..TOTAL_REQUESTS)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>(),
        );
        let runtime = Netplane::new(
            racing_graph(0, Arc::clone(&calls))?,
            RuntimeLimits {
                max_outstanding: REQUESTS_PER_ROUND + 1,
            },
        )?;
        let handle = runtime.handle();
        let start = Arc::new(Barrier::new(PRODUCERS + 1));
        let done = Arc::new(Barrier::new(PRODUCERS + 1));
        let producers: Vec<_> = (0..PRODUCERS)
            .map(|producer| {
                let handle = handle.clone();
                let start = Arc::clone(&start);
                let done = Arc::clone(&done);
                tokio::spawn(async move {
                    for round in 0..ROUNDS {
                        start.wait().await;
                        let mut accepted = Vec::with_capacity(REQUESTS_PER_PRODUCER);
                        for offset in 0..REQUESTS_PER_PRODUCER {
                            let id = round * REQUESTS_PER_ROUND
                                + producer * REQUESTS_PER_PRODUCER
                                + offset;
                            accepted.push((id, handle.try_execute(id)?));
                        }
                        for (id, future) in accepted {
                            let reply = future.await?;
                            assert_eq!(reply.id, id);
                            assert!(
                                reply.version == round || reply.version == round + 1,
                                "round {round}, request {id} selected version {}",
                                reply.version
                            );
                        }
                        done.wait().await;
                    }
                    Ok::<_, tower::BoxError>(())
                })
            })
            .collect();
        let publisher_calls = Arc::clone(&calls);
        let publisher = tokio::spawn(async move {
            let mut runtime = runtime;
            for round in 0..ROUNDS {
                start.wait().await;
                runtime.publish(racing_graph(round + 1, Arc::clone(&publisher_calls))?)?;
                let id = ROUNDS * REQUESTS_PER_ROUND + round;
                let reply = handle.execute(id).await?;
                assert_eq!(
                    reply,
                    Reply {
                        version: round + 1,
                        id
                    }
                );
                done.wait().await;
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        });
        for producer in producers {
            producer.await??;
        }
        publisher.await??;
        for (id, call_count) in calls.iter().enumerate() {
            assert_eq!(call_count.load(Ordering::SeqCst), 1, "request {id}");
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
