//! Read-only admission and lifecycle snapshots.
use std::{convert::Infallible, error::Error, time::Duration};

use netplane::{
    AdmissionError, DeadlineExceeded, DrainError, GraphDraft, LifecycleState, Netplane,
    RuntimeLimits, RuntimeStatus,
};
use tokio::sync::oneshot;
use tower::service_fn;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

fn graph(value: usize) -> Result<netplane::GraphVersion<usize, usize>, netplane::ConfigError> {
    GraphDraft::new(service_fn(move |_: usize| async move {
        Ok::<_, Infallible>(value)
    }))
    .validate()
}

fn runtime(limit: usize) -> Result<Netplane<usize, usize>, netplane::ConfigError> {
    Netplane::new(
        graph(1)?,
        RuntimeLimits {
            max_outstanding: limit,
        },
    )
}

const fn status(
    outstanding: usize,
    max_outstanding: usize,
    lifecycle: LifecycleState,
) -> RuntimeStatus {
    RuntimeStatus {
        outstanding,
        max_outstanding,
        lifecycle,
    }
}

#[tokio::test]
async fn admission_across_handles_and_publications() -> TestResult {
    let mut runtime = runtime(2)?;
    let handle = runtime.handle();
    let clone = handle.clone();
    assert_eq!(handle.status(), status(0, 2, LifecycleState::Running));

    let old = handle.try_execute(0)?;
    assert_eq!(clone.status(), status(1, 2, LifecycleState::Running));
    runtime.publish(graph(2)?)?;
    assert_eq!(handle.status(), status(1, 2, LifecycleState::Running));

    let new = clone.try_execute(0)?;
    assert_eq!(handle.status(), status(2, 2, LifecycleState::Running));
    assert!(matches!(
        handle.try_execute(0),
        Err(AdmissionError::AtCapacity { limit: 2 })
    ));
    assert_eq!(handle.status().outstanding, 2);
    assert_eq!(new.await?, 2);
    assert_eq!(handle.status().outstanding, 1);
    drop(old);
    assert_eq!(clone.status().outstanding, 0);

    let drain = runtime.shutdown(Duration::from_secs(1));
    assert_eq!(handle.status(), status(0, 2, LifecycleState::Draining));
    drain.await?;
    drop(runtime);
    assert_eq!(clone.status(), status(0, 2, LifecycleState::Stopped));
    Ok(())
}

#[tokio::test]
async fn failure_deadline_and_drop_release_slots() -> TestResult {
    let failure = GraphDraft::new(service_fn(|_: usize| async {
        Err::<usize, _>(std::io::Error::other("service failure"))
    }))
    .validate()?;
    let runtime = Netplane::new(failure, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    let pending = handle.try_execute(0)?;
    assert_eq!(handle.status().outstanding, 1);
    drop(pending);
    assert_eq!(handle.status().outstanding, 0);
    assert!(handle.try_execute(0)?.await.is_err());
    assert_eq!(handle.status().outstanding, 0);

    let expired = handle.try_execute_until(0, tokio::time::Instant::now())?;
    assert_eq!(handle.status().outstanding, 1);
    let error = expired.await.err().ok_or("expected deadline failure")?;
    assert!(error.downcast_ref::<DeadlineExceeded>().is_some());
    assert_eq!(handle.status().outstanding, 0);
    Ok(())
}

#[tokio::test]
async fn cancel_after_dispatch_releases_outstanding() -> TestResult {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let mut entered_tx = Some(entered_tx);
    let mut release_rx = Some(release_rx);
    let blocked = GraphDraft::new(service_fn(move |value: usize| {
        let entered = entered_tx.take();
        let release = release_rx.take();
        async move {
            if let Some(entered) = entered {
                let _ = entered.send(());
            }
            if let Some(release) = release {
                let _ = release.await;
            }
            Ok::<_, Infallible>(value)
        }
    }))
    .validate()?;
    let runtime = Netplane::new(blocked, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    let execution = tokio::spawn(handle.try_execute(0)?);
    tokio::time::timeout(Duration::from_secs(1), entered_rx).await??;
    assert_eq!(handle.status(), status(1, 1, LifecycleState::Running));
    execution.abort();
    let error = execution.await.err().ok_or("expected task cancellation")?;
    assert!(error.is_cancelled());
    assert_eq!(handle.status(), status(0, 1, LifecycleState::Running));
    let _ = release_tx.send(());
    Ok(())
}

#[tokio::test]
async fn timeout_remains_draining_until_retry_succeeds() -> TestResult {
    let mut runtime = runtime(1)?;
    let handle = runtime.handle();
    let retained = handle.clone();
    let pending = handle.try_execute(0)?;
    let drain = runtime.shutdown(Duration::ZERO);
    assert_eq!(retained.status(), status(1, 1, LifecycleState::Draining));
    assert!(matches!(
        drain.await,
        Err(DrainError::TimedOut { remaining: 1 })
    ));
    assert_eq!(retained.status(), status(1, 1, LifecycleState::Draining));
    assert!(matches!(
        retained.try_execute(0),
        Err(AdmissionError::Closed)
    ));
    drop(pending);
    assert_eq!(retained.status(), status(0, 1, LifecycleState::Draining));
    runtime.shutdown(Duration::from_secs(1)).await?;
    drop(runtime);
    assert_eq!(retained.status(), status(0, 1, LifecycleState::Stopped));
    Ok(())
}
