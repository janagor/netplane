//! Concurrent global admission across handles and versions.
#[path = "common/drop.rs"]
mod cleanup;
mod common;

use std::error::Error;

use cleanup::wait_for_drop;
use common::{CAPACITY, DEADLINE, graph};
use netplane::{AdmissionError, Netplane, RuntimeLimits};
use tokio::{sync::Barrier, time::timeout};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admission_never_exceeds_budget_across_versions() -> TestResult {
    timeout(DEADLINE, async {
        const LIMIT: usize = 16;
        let (first, mut a) = graph(0, CAPACITY)?;
        let mut runtime = Netplane::new(
            first,
            RuntimeLimits {
                max_outstanding: LIMIT,
            },
        )?;
        let handle = runtime.handle();
        let start = std::sync::Arc::new(Barrier::new(65));
        let attempts: Vec<_> = (0..64)
            .map(|id| {
                let handle = handle.clone();
                let start = start.clone();
                tokio::spawn(async move {
                    start.wait().await;
                    handle.try_execute(id)
                })
            })
            .collect();
        start.wait().await;
        let mut accepted = Vec::new();
        let mut rejected = 0;
        for attempt in attempts {
            match attempt.await? {
                Ok(future) => accepted.push(future),
                Err(AdmissionError::AtCapacity { limit: LIMIT }) => rejected += 1,
                Err(error) => return Err(error.into()),
            }
        }
        assert_eq!(accepted.len(), LIMIT);
        assert_eq!(rejected, 64 - LIMIT);
        let (second, b) = graph(1, CAPACITY)?;
        runtime.publish(second)?;
        assert!(matches!(
            handle.try_execute(100),
            Err(AdmissionError::AtCapacity { limit: LIMIT })
        ));
        accepted.truncate(LIMIT / 2);
        let mut replacements = Vec::new();
        for id in 100..100 + LIMIT / 2 {
            replacements.push(handle.try_execute(id)?);
        }
        assert!(matches!(
            handle.try_execute(200),
            Err(AdmissionError::AtCapacity { limit: LIMIT })
        ));
        for future in accepted {
            drop(future);
        }
        for future in replacements {
            drop(future);
        }
        b.permits.add_permits(1);
        assert_eq!(handle.try_execute(201)?.await?.version, 1);
        assert_eq!(a.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(b.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        wait_for_drop(&a.dropped, 1).await;
        assert_eq!(b.dropped.load(std::sync::atomic::Ordering::SeqCst), 0);
        let _ = a.entered.try_recv();
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
