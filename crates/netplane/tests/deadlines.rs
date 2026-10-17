//! Deadline and admission ownership regression tests.
#[path = "common/drop.rs"]
mod cleanup;
mod common;

use std::{
    convert::Infallible,
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use netplane::{AdmissionError, Capacity, DeadlineExceeded, GraphDraft, Netplane, RuntimeLimits};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tower::{Service, service_fn};

use cleanup::wait_for_drop;
use common::{CAPACITY, DEADLINE, graph};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[tokio::test(start_paused = true)]
async fn deadline_before_first_poll_releases_slot_without_dispatch() -> TestResult {
    let (called, mut calls) = mpsc::unbounded_channel();
    let graph = GraphDraft::new(service_fn(move |value: usize| {
        let called = called.clone();
        async move {
            let _ = called.send(value);
            Ok::<_, Infallible>(value)
        }
    }))
    .validate()?;
    let runtime = Netplane::new(graph, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let pending = handle.try_execute_until(1, deadline)?;
    assert!(matches!(
        handle.try_execute(2),
        Err(AdmissionError::AtCapacity { limit: 1 })
    ));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(
        handle.try_execute(2),
        Err(AdmissionError::AtCapacity { limit: 1 })
    ));
    assert!(matches!(pending.await, Err(error) if error.is::<DeadlineExceeded>()));
    assert!(calls.try_recv().is_err());
    assert_eq!(handle.try_execute(3)?.await?, 3);
    assert_eq!(calls.try_recv()?, 3);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn deadline_during_execution_releases_slot_and_allows_shutdown() -> TestResult {
    let permits = std::sync::Arc::new(Semaphore::new(0));
    let (entered, mut calls) = mpsc::unbounded_channel();
    let graph = GraphDraft::new(service_fn({
        let permits = permits.clone();
        move |value: usize| {
            let permits = permits.clone();
            let entered = entered.clone();
            async move {
                let _ = entered.send(value);
                let permit = permits.acquire().await.map_err(std::io::Error::other)?;
                drop(permit);
                Ok::<_, std::io::Error>(value)
            }
        }
    }))
    .validate()?;
    let mut runtime = Netplane::new(graph, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let request = tokio::spawn(handle.execute_until(1, deadline));
    assert_eq!(calls.recv().await, Some(1));
    assert!(matches!(
        handle.try_execute(2),
        Err(AdmissionError::AtCapacity { limit: 1 })
    ));
    let drain = runtime.shutdown(Duration::from_secs(2));
    assert!(matches!(handle.try_execute(3), Err(AdmissionError::Closed)));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(request.await?, Err(error) if error.is::<DeadlineExceeded>()));
    drain.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn selected_version_is_stable_before_deadline() -> TestResult {
    let first = GraphDraft::new(service_fn(|_: usize| async {
        Ok::<_, Infallible>(1_usize)
    }))
    .validate()?;
    let mut runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 2 })?;
    let handle = runtime.handle();
    let old = handle.try_execute_until(0, tokio::time::Instant::now() + Duration::from_secs(1))?;
    let next = GraphDraft::new(service_fn(|_: usize| async {
        Ok::<_, Infallible>(2_usize)
    }))
    .validate()?;
    runtime.publish(next)?;
    assert_eq!(old.await?, 1);
    assert_eq!(handle.execute(0).await?, 2);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn deadline_retains_old_service_until_expiration() -> TestResult {
    let (first, mut gate) = graph(0, CAPACITY)?;
    let mut runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let old = handle.try_execute_until(1, deadline)?;
    let (replacement, _) = graph(1, CAPACITY)?;
    runtime.publish(replacement)?;
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 0);
    assert_eq!(gate.permits.available_permits(), 0);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 0);
    assert!(matches!(old.await, Err(error) if error.is::<DeadlineExceeded>()));
    tokio::time::timeout(DEADLINE, wait_for_drop(&gate.dropped, 1)).await?;
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    assert!(gate.entered.try_recv().is_err());
    Ok(())
}

struct ReadinessGate {
    ready: Arc<AtomicBool>,
    waiter: Arc<Mutex<Option<Waker>>>,
    polled: Option<mpsc::UnboundedSender<()>>,
    calls: mpsc::UnboundedSender<usize>,
}

struct QueueGate {
    ready: Arc<AtomicBool>,
    waiter: Arc<Mutex<Option<Waker>>>,
    blocked: Option<mpsc::UnboundedSender<()>>,
    calls: mpsc::UnboundedSender<usize>,
    permits: Arc<Semaphore>,
}

impl Service<usize> for QueueGate {
    type Response = usize;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<usize, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            *self
                .waiter
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cx.waker().clone());
            if let Some(blocked) = self.blocked.take() {
                let _ = blocked.send(());
            }
            Poll::Pending
        }
    }

    fn call(&mut self, value: usize) -> Self::Future {
        if value == 1 {
            self.ready.store(false, Ordering::SeqCst);
        }
        let _ = self.calls.send(value);
        let permits = Arc::clone(&self.permits);
        Box::pin(async move {
            let permit = permits.acquire().await.map_err(std::io::Error::other)?;
            drop(permit);
            Ok(value)
        })
    }
}

impl Service<usize> for ReadinessGate {
    type Response = usize;
    type Error = Infallible;
    type Future = std::future::Ready<Result<usize, Infallible>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            *self
                .waiter
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cx.waker().clone());
            if let Some(polled) = self.polled.take() {
                let _ = polled.send(());
            }
            Poll::Pending
        }
    }

    fn call(&mut self, value: usize) -> Self::Future {
        let _ = self.calls.send(value);
        std::future::ready(Ok(value))
    }
}

#[tokio::test(start_paused = true)]
async fn deadline_during_service_readiness_wait() -> TestResult {
    let ready = Arc::new(AtomicBool::new(false));
    let waiter = Arc::new(Mutex::new(None::<Waker>));
    let (polled_tx, mut polled_rx) = mpsc::unbounded_channel();
    let (calls_tx, mut calls_rx) = mpsc::unbounded_channel();
    let graph = GraphDraft::new(ReadinessGate {
        ready: ready.clone(),
        waiter: waiter.clone(),
        polled: Some(polled_tx),
        calls: calls_tx,
    })
    .validate()?;
    let runtime = Netplane::new(graph, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let expired = tokio::spawn(handle.execute_until(1, deadline));
    assert_eq!(polled_rx.recv().await, Some(()));
    assert!(matches!(
        handle.try_execute(2),
        Err(AdmissionError::AtCapacity { .. })
    ));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(expired.await?, Err(error) if error.is::<DeadlineExceeded>()));
    assert!(calls_rx.try_recv().is_err());
    let later = tokio::spawn(handle.execute(2));
    tokio::task::yield_now().await;
    assert!(calls_rx.try_recv().is_err());
    ready.store(true, Ordering::SeqCst);
    let wake = waiter
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(waker) = wake {
        waker.wake();
    }
    assert_eq!(later.await??, 2);
    assert_eq!(calls_rx.recv().await, Some(2));
    assert!(calls_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn deadline_during_queue_capacity_wait() -> TestResult {
    let permits = Arc::new(Semaphore::new(0));
    let (calls_tx, mut calls_rx) = mpsc::unbounded_channel();
    let ready = Arc::new(AtomicBool::new(true));
    let waiter = Arc::new(Mutex::new(None::<Waker>));
    let (blocked_tx, mut blocked_rx) = mpsc::unbounded_channel();
    let graph = GraphDraft::new(QueueGate {
        ready: Arc::clone(&ready),
        waiter: Arc::clone(&waiter),
        blocked: Some(blocked_tx),
        calls: calls_tx,
        permits: Arc::clone(&permits),
    })
    .capacity(Capacity {
        max_in_flight: 2,
        buffer_capacity: 1,
    })
    .validate()?;
    let runtime = Netplane::new(graph, RuntimeLimits { max_outstanding: 4 })?;
    let handle = runtime.handle();
    let running = tokio::spawn(handle.execute(1));
    assert_eq!(calls_rx.recv().await, Some(1));
    let (queued_polled_tx, queued_polled_rx) = oneshot::channel();
    let queued_handle = handle.clone();
    let queued = tokio::spawn(async move {
        let mut execution = Box::pin(queued_handle.execute(2));
        std::future::poll_fn(|cx| {
            assert!(execution.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let _ = queued_polled_tx.send(());
        execution.await
    });
    queued_polled_rx.await?;
    assert_eq!(blocked_rx.recv().await, Some(()));
    let (full_polled_tx, full_polled_rx) = oneshot::channel();
    let full_handle = handle.clone();
    let full = tokio::spawn(async move {
        let mut execution = Box::pin(full_handle.execute(3));
        std::future::poll_fn(|cx| {
            assert!(execution.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let _ = full_polled_tx.send(());
        execution.await
    });
    full_polled_rx.await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut expired = Box::pin(handle.execute_until(4, deadline));
    std::future::poll_fn(|cx| {
        assert!(expired.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(matches!(
        handle.try_execute(5),
        Err(AdmissionError::AtCapacity { .. })
    ));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(expired.await, Err(error) if error.is::<DeadlineExceeded>()));
    queued.abort();
    full.abort();
    assert!(matches!(queued.await, Err(error) if error.is_cancelled()));
    assert!(matches!(full.await, Err(error) if error.is_cancelled()));
    ready.store(true, Ordering::SeqCst);
    let wake = waiter
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(waker) = wake {
        waker.wake();
    }
    let later = tokio::spawn(handle.execute(5));
    permits.add_permits(2);
    assert_eq!(running.await??, 1);
    assert_eq!(later.await??, 5);
    assert_eq!(calls_rx.recv().await, Some(5));
    assert!(calls_rx.try_recv().is_err());
    Ok(())
}
