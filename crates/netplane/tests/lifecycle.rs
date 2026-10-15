//! Shutdown admission, draining, cancellation, and delayed readiness.
use netplane::{AdmissionError, Capacity, DrainError, GraphDraft, Netplane, PublishError};
use std::{
    convert::Infallible,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};
use tokio::{
    sync::{Barrier, mpsc, oneshot},
    time::{Duration, timeout},
};
use tower::{Service, service_fn};

const fn capacity() -> Capacity {
    Capacity {
        max_in_flight: 1,
        buffer_capacity: 1,
    }
}

#[tokio::test]
async fn shutdown_closes_admission_and_drains_active_request()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let mut entered_tx = Some(entered_tx);
        let mut release_rx = Some(release_rx);
        let graph = GraphDraft::new(service_fn(move |value: usize| {
            let entered_tx = entered_tx.take();
            let release_rx = release_rx.take();
            async move {
                if let Some(entered_tx) = entered_tx {
                    let _ = entered_tx.send(());
                }
                if let Some(release_rx) = release_rx {
                    let _ = release_rx.await;
                }
                Ok::<_, Infallible>(value + 1)
            }
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(graph);
        let handle = runtime.handle();
        let active = tokio::spawn(handle.execute(1));
        entered_rx.await?;
        let drain = runtime.shutdown(Duration::from_secs(5));
        let rejected = handle
            .execute(2)
            .await
            .err()
            .ok_or("request was admitted after shutdown")?;
        assert!(matches!(
            rejected.downcast_ref::<AdmissionError>(),
            Some(AdmissionError::Closed)
        ));
        let _ = release_tx.send(());
        assert_eq!(active.await??, 2);
        drain.await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn timed_out_drain_can_be_retried_after_unpolled_future_is_dropped()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let graph = GraphDraft::new(service_fn(move |value: usize| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, Infallible>(value) }
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(graph);
        let handle = runtime.handle();
        let pending = handle.execute(1);
        assert!(matches!(
            runtime.shutdown(Duration::ZERO).await,
            Err(DrainError::TimedOut { remaining: 1 })
        ));
        let rejected = handle
            .execute(2)
            .await
            .err()
            .ok_or("request was admitted after timeout")?;
        assert!(matches!(
            rejected.downcast_ref::<AdmissionError>(),
            Some(AdmissionError::Closed)
        ));
        drop(pending);
        runtime.shutdown(Duration::from_secs(5)).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn cancellation_during_execution_releases_drain_ownership()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        struct DropProbe {
            drops: Arc<AtomicUsize>,
            dropped: Option<oneshot::Sender<()>>,
        }
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
                if let Some(dropped) = self.dropped.take() {
                    let _ = dropped.send(());
                }
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let (drop_tx, drop_rx) = oneshot::channel();
        let probe = DropProbe {
            drops: drops.clone(),
            dropped: Some(drop_tx),
        };
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let mut entered_tx = Some(entered_tx);
        let mut release_rx = Some(release_rx);
        let graph = GraphDraft::new(service_fn(move |value: usize| {
            let _ = &probe;
            let entered_tx = entered_tx.take();
            let release_rx = release_rx.take();
            async move {
                if let Some(entered_tx) = entered_tx {
                    let _ = entered_tx.send(());
                }
                if let Some(release_rx) = release_rx {
                    let _ = release_rx.await;
                }
                Ok::<_, Infallible>(value)
            }
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(graph);
        let handle = runtime.handle();
        let active = tokio::spawn(handle.execute(1));
        entered_rx.await?;
        runtime.publish(
            GraphDraft::new(service_fn(|value: usize| async move {
                Ok::<_, Infallible>(value + 100)
            }))
            .capacity(capacity())
            .validate()?,
        )?;
        let drain = runtime.shutdown(Duration::from_secs(5));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        active.abort();
        assert!(active.await.is_err());
        drain.await?;
        let _ = release_tx.send(());
        drop_rx.await?;
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

struct DelayedReady {
    ready: Arc<AtomicBool>,
    waiter: Arc<Mutex<Option<Waker>>>,
    called: mpsc::UnboundedSender<usize>,
    polled: Option<oneshot::Sender<()>>,
}

impl Service<usize> for DelayedReady {
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

    fn call(&mut self, request: usize) -> Self::Future {
        assert!(self.ready.load(Ordering::SeqCst), "call before readiness");
        let _ = self.called.send(request);
        std::future::ready(Ok(request + 10))
    }
}

#[tokio::test]
async fn delayed_readiness_survives_publication_and_shutdown()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let ready = Arc::new(AtomicBool::new(false));
        let waiter = Arc::new(Mutex::new(None::<Waker>));
        let (called_tx, mut called_rx) = mpsc::unbounded_channel();
        let (polled_tx, polled_rx) = oneshot::channel();
        let graph_a = GraphDraft::new(DelayedReady {
            ready: ready.clone(),
            waiter: waiter.clone(),
            called: called_tx,
            polled: Some(polled_tx),
        })
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(graph_a);
        let handle = runtime.handle();
        let old = tokio::spawn(handle.execute(1));
        polled_rx.await?;
        runtime.publish(
            GraphDraft::new(service_fn(|value: usize| async move {
                Ok::<_, Infallible>(value + 100)
            }))
            .capacity(capacity())
            .validate()?,
        )?;
        assert_eq!(handle.execute(2).await?, 102);
        let drain = runtime.shutdown(Duration::from_secs(5));
        ready.store(true, Ordering::SeqCst);
        let wake = waiter
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(waker) = wake {
            waker.wake();
        }
        assert_eq!(old.await??, 11);
        assert_eq!(called_rx.recv().await, Some(1));
        drain.await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn service_failure_releases_drain_ownership() -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let graph = GraphDraft::new(service_fn(|_: usize| async move {
            Err::<usize, std::io::Error>(std::io::Error::other("backend failed"))
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(graph);
        let handle = runtime.handle();
        assert!(handle.execute(1).await.is_err());
        runtime.shutdown(Duration::ZERO).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_rejects_publication_during_and_after_drain()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let graph = GraphDraft::new(service_fn(|value: usize| async move {
            Ok::<_, Infallible>(value)
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(graph);
        let handle = runtime.handle();
        let pending = handle.execute(1);
        let drain = runtime.shutdown(Duration::from_secs(5));
        let replacement = GraphDraft::new(service_fn(|value: usize| async move {
            Ok::<_, Infallible>(value + 1)
        }))
        .capacity(capacity())
        .validate()?;
        assert!(matches!(
            runtime.publish(replacement),
            Err(PublishError::ShuttingDown)
        ));
        drop(pending);
        drain.await?;
        let replacement = GraphDraft::new(service_fn(|value: usize| async move {
            Ok::<_, Infallible>(value + 2)
        }))
        .capacity(capacity())
        .validate()?;
        assert!(matches!(
            runtime.publish(replacement),
            Err(PublishError::ShuttingDown)
        ));
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_releases_active_graph_with_retained_handle()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        struct DropProbe(Option<oneshot::Sender<()>>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                if let Some(dropped) = self.0.take() {
                    let _ = dropped.send(());
                }
            }
        }
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let probe = DropProbe(Some(dropped_tx));
        let graph = GraphDraft::new(service_fn(move |value: usize| {
            let _ = &probe;
            async move { Ok::<_, Infallible>(value) }
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(graph);
        let handle = runtime.handle();
        runtime.shutdown(Duration::from_secs(5)).await?;
        dropped_rx.await?;
        let rejected = handle
            .execute(1)
            .await
            .err()
            .ok_or("closed handle admitted work")?;
        assert!(matches!(
            rejected.downcast_ref::<AdmissionError>(),
            Some(AdmissionError::Closed)
        ));
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_without_poll_closes_admission_and_retry_is_idempotent()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let graph = GraphDraft::new(service_fn(|value: usize| async move {
            Ok::<_, Infallible>(value)
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(graph);
        let handle = runtime.handle();
        let drain = runtime.shutdown(Duration::from_secs(5));
        let rejected = handle
            .execute(1)
            .await
            .err()
            .ok_or("admission remained open")?;
        assert!(matches!(
            rejected.downcast_ref::<AdmissionError>(),
            Some(AdmissionError::Closed)
        ));
        drop(drain);
        runtime.shutdown(Duration::from_secs(5)).await?;
        runtime.shutdown(Duration::from_secs(5)).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_concurrent_with_execute() -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let graph = GraphDraft::new(service_fn(|value: usize| async move {
            Ok::<_, Infallible>(value)
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(graph);
        let handle = runtime.handle();
        let accepted = handle.execute(1);
        let barrier = Arc::new(Barrier::new(17));
        let mut workers = Vec::with_capacity(16);
        for _ in 0..16 {
            let handle = handle.clone();
            let barrier = barrier.clone();
            workers.push(tokio::spawn(async move {
                barrier.wait().await;
                handle.execute(1).await
            }));
        }
        barrier.wait().await;
        let drain = runtime.shutdown(Duration::ZERO);
        for worker in workers {
            match worker.await? {
                Ok(value) => assert_eq!(value, 1),
                Err(error) => assert!(matches!(
                    error.downcast_ref::<AdmissionError>(),
                    Some(AdmissionError::Closed)
                )),
            }
        }
        assert_eq!(accepted.await?, 1);
        let _ = drain.await;
        runtime.shutdown(Duration::from_secs(5)).await?;
        let rejected = handle
            .execute(2)
            .await
            .err()
            .ok_or("request admitted after closure")?;
        assert!(matches!(
            rejected.downcast_ref::<AdmissionError>(),
            Some(AdmissionError::Closed)
        ));
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
