//! Synchronous shutdown closure, draining, and timeout recovery.
#[path = "common/drop.rs"]
mod cleanup;
mod common;

use std::{error::Error, time::Duration};

use cleanup::wait_for_drop;
use common::{CAPACITY, DEADLINE, graph};
use netplane::{AdmissionError, DrainError, Netplane, PublishError, RuntimeLimits};
use tokio::time::{advance, timeout};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn shutdown_closes_synchronously_and_drains_retained_requests() -> TestResult {
    timeout(DEADLINE, async {
        let (first, mut gate) = graph(0, CAPACITY)?;
        let mut runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 3 })?;
        let handle = runtime.handle();
        let unpolled = handle.try_execute(1)?;
        let active = tokio::spawn(handle.execute(2));
        assert_eq!(gate.entered.recv().await, Some(2));
        let shutdown = runtime.shutdown(Duration::from_secs(2));
        assert!(matches!(handle.try_execute(3), Err(AdmissionError::Closed)));
        let (next, _) = graph(1, CAPACITY)?;
        assert!(matches!(
            runtime.publish(next),
            Err(PublishError::ShuttingDown)
        ));
        drop(unpolled);
        gate.permits.add_permits(1);
        assert_eq!(active.await??.id, 2);
        shutdown.await?;
        assert_eq!(gate.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        wait_for_drop(&gate.dropped, 1).await;
        assert!(matches!(handle.try_execute(4), Err(AdmissionError::Closed)));
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn timed_out_drain_reports_count_and_can_be_retried() -> TestResult {
    timeout(DEADLINE, async {
        let (first, _) = graph(0, CAPACITY)?;
        let mut runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 3 })?;
        let handle = runtime.handle();
        let first = handle.try_execute(1)?;
        let second = handle.try_execute(2)?;
        assert!(matches!(
            runtime.shutdown(Duration::ZERO).await,
            Err(DrainError::TimedOut { remaining: 2 })
        ));
        assert!(matches!(handle.try_execute(3), Err(AdmissionError::Closed)));
        drop(first);
        assert!(matches!(
            runtime.shutdown(Duration::ZERO).await,
            Err(DrainError::TimedOut { remaining: 1 })
        ));
        drop(second);
        runtime.shutdown(Duration::ZERO).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn nonzero_deadline_waits_then_times_out_and_recovers() -> TestResult {
    let (first, _) = graph(0, CAPACITY)?;
    let mut runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    let pending = handle.try_execute(1)?;
    let drain = tokio::spawn(runtime.shutdown(Duration::from_secs(10)));
    assert!(matches!(handle.try_execute(2), Err(AdmissionError::Closed)));
    tokio::task::yield_now().await;
    assert!(!drain.is_finished());
    advance(Duration::from_secs(9)).await;
    tokio::task::yield_now().await;
    assert!(!drain.is_finished());
    advance(Duration::from_secs(1)).await;
    let result = timeout(Duration::from_secs(1), drain).await??;
    assert!(matches!(result, Err(DrainError::TimedOut { remaining: 1 })));
    drop(pending);
    timeout(
        Duration::from_secs(1),
        runtime.shutdown(Duration::from_secs(10)),
    )
    .await??;
    Ok(())
}
