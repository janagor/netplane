//! Global synchronous admission across handles and graph publications.
use netplane::{AdmissionError, ConfigError, GraphDraft, Netplane, RuntimeLimits};
use std::{
    convert::Infallible,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Barrier, oneshot},
    time::timeout,
};
use tower::service_fn;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

fn graph(calls: Arc<AtomicUsize>) -> Result<netplane::GraphVersion<usize, usize>, ConfigError> {
    GraphDraft::new(service_fn(move |value: usize| {
        calls.fetch_add(1, Ordering::SeqCst);
        async move { Ok::<_, Infallible>(value) }
    }))
    .validate()
}

#[tokio::test]
async fn zero_outstanding_limit_is_rejected() -> TestResult {
    let result = Netplane::new(
        graph(Arc::new(AtomicUsize::new(0)))?,
        RuntimeLimits { max_outstanding: 0 },
    );
    assert!(matches!(result, Err(ConfigError::ZeroOutstanding)));
    Ok(())
}

#[tokio::test]
async fn unpolled_futures_exhaust_budget_and_drop_restores_it() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let runtime = Netplane::new(graph(calls.clone())?, RuntimeLimits { max_outstanding: 2 })?;
    let handle = runtime.handle();
    let first = handle.try_execute(1)?;
    let second = handle.execute(2);
    assert!(matches!(
        handle.try_execute(3),
        Err(AdmissionError::AtCapacity { limit: 2 })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(first);
    assert_eq!(handle.try_execute(3)?.await?, 3);
    assert_eq!(handle.try_execute(4)?.await?, 4);
    drop(second);
    Ok(())
}

#[tokio::test]
async fn execute_preserves_typed_overload_error() -> TestResult {
    let runtime = Netplane::new(
        graph(Arc::new(AtomicUsize::new(0)))?,
        RuntimeLimits { max_outstanding: 1 },
    )?;
    let handle = runtime.handle();
    let pending = handle.execute(1);
    let error = handle.execute(2).await.err().ok_or("expected overload")?;
    assert!(matches!(
        error.downcast_ref::<AdmissionError>(),
        Some(AdmissionError::AtCapacity { limit: 1 })
    ));
    drop(pending);
    assert_eq!(handle.execute(3).await?, 3);
    Ok(())
}

#[tokio::test]
async fn budget_is_shared_across_handles_and_publications() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = Netplane::new(graph(calls.clone())?, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    let clone = handle.clone();
    let old = handle.try_execute(1)?;
    runtime.publish(graph(calls.clone())?)?;
    assert!(matches!(
        clone.try_execute(2),
        Err(AdmissionError::AtCapacity { limit: 1 })
    ));
    drop(old);
    assert_eq!(clone.try_execute(2)?.await?, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn executing_old_version_keeps_budget_after_publication() -> TestResult {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let mut entered_tx = Some(entered_tx);
    let mut release_rx = Some(release_rx);
    let old_graph = GraphDraft::new(service_fn(move |value: usize| {
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
    let mut runtime = Netplane::new(old_graph, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    let active = tokio::spawn(handle.try_execute(1)?);
    timeout(Duration::from_secs(5), entered_rx).await??;
    runtime.publish(graph(Arc::new(AtomicUsize::new(0)))?)?;
    assert!(matches!(
        handle.try_execute(2),
        Err(AdmissionError::AtCapacity { limit: 1 })
    ));
    let _ = release_tx.send(());
    assert_eq!(active.await??, 1);
    assert_eq!(handle.try_execute(2)?.await?, 2);
    Ok(())
}

#[tokio::test]
async fn service_failure_and_cancellation_restore_capacity() -> TestResult {
    let fail = GraphDraft::new(service_fn(|_: usize| async {
        Err::<usize, _>(std::io::Error::other("failure"))
    }))
    .validate()?;
    let runtime = Netplane::new(fail, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    assert!(handle.try_execute(1)?.await.is_err());
    assert!(handle.try_execute(2)?.await.is_err());
    let pending = handle.try_execute(3)?;
    assert!(matches!(
        handle.try_execute(4),
        Err(AdmissionError::AtCapacity { limit: 1 })
    ));
    drop(pending);
    assert!(handle.try_execute(5)?.await.is_err());
    Ok(())
}

#[tokio::test]
async fn cancelling_dispatched_request_restores_budget() -> TestResult {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (_release_tx, release_rx) = oneshot::channel::<()>();
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
    let active = tokio::spawn(handle.execute(1));
    timeout(Duration::from_secs(5), entered_rx).await??;
    assert!(matches!(
        handle.try_execute(2),
        Err(AdmissionError::AtCapacity { limit: 1 })
    ));
    active.abort();
    assert!(matches!(active.await, Err(error) if error.is_cancelled()));
    let accepted = handle.try_execute(3)?;
    drop(accepted);
    Ok(())
}

#[tokio::test]
async fn shutdown_takes_precedence_over_overload() -> TestResult {
    let mut runtime = Netplane::new(
        graph(Arc::new(AtomicUsize::new(0)))?,
        RuntimeLimits { max_outstanding: 1 },
    )?;
    let handle = runtime.handle();
    let pending = handle.try_execute(1)?;
    let drain = runtime.shutdown(std::time::Duration::from_secs(1));
    assert!(matches!(handle.try_execute(2), Err(AdmissionError::Closed)));
    drop(pending);
    drain.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admission_never_exceeds_limit() -> TestResult {
    const CALLERS: usize = 32;
    const LIMIT: usize = 3;
    let runtime = Netplane::new(
        graph(Arc::new(AtomicUsize::new(0)))?,
        RuntimeLimits {
            max_outstanding: LIMIT,
        },
    )?;
    let barrier = Arc::new(Barrier::new(CALLERS + 1));
    let mut tasks = Vec::new();
    for value in 0..CALLERS {
        let handle = runtime.handle();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            handle.try_execute(value)
        }));
    }
    barrier.wait().await;
    let mut accepted = Vec::new();
    let mut rejected = 0;
    for task in tasks {
        match task.await? {
            Ok(future) => accepted.push(future),
            Err(AdmissionError::AtCapacity { limit: LIMIT }) => rejected += 1,
            Err(error) => return Err(Box::new(error) as Box<dyn Error + Send + Sync>),
        }
    }
    assert_eq!(accepted.len(), LIMIT);
    assert_eq!(rejected, CALLERS - LIMIT);
    drop(accepted);
    Ok(())
}
