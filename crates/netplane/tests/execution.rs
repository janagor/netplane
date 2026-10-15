//! Execution behavior for a single bounded endpoint.
use netplane::{Capacity, ConfigError, Endpoint};
use std::{
    convert::Infallible,
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Duration, timeout},
};
use tower::Service;

struct Controlled {
    ready: bool,
    entered: mpsc::UnboundedSender<(usize, oneshot::Sender<()>)>,
    calls: Arc<AtomicUsize>,
}

impl Service<usize> for Controlled {
    type Response = usize;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<usize, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.ready = true;
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: usize) -> Self::Future {
        assert!(std::mem::take(&mut self.ready), "call without readiness");
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (release, wait) = oneshot::channel();
        let _ = self.entered.send((request, release));
        Box::pin(async move {
            let _ = wait.await;
            Ok(request)
        })
    }
}

#[tokio::test]
async fn readiness_and_concurrency_are_preserved() -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let (entered, mut calls) = mpsc::unbounded_channel();
        let count = Arc::new(AtomicUsize::new(0));
        let endpoint = Endpoint::new(
            Controlled {
                ready: false,
                entered,
                calls: count.clone(),
            },
            Capacity {
                max_in_flight: 2,
                buffer_capacity: 2,
            },
        )?;
        let first = tokio::spawn(endpoint.execute(1));
        let second = tokio::spawn(endpoint.execute(2));
        let third = tokio::spawn(endpoint.execute(3));
        let (_, release_first) = calls.recv().await.ok_or("first did not enter")?;
        let (_, release_second) = calls.recv().await.ok_or("second did not enter")?;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert!(release_first.send(()).is_ok());
        assert!(first.await?.is_ok());
        let (_, release_third) = calls.recv().await.ok_or("third did not enter")?;
        assert!(release_second.send(()).is_ok());
        assert!(release_third.send(()).is_ok());
        assert!(second.await?.is_ok());
        assert!(third.await?.is_ok());
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

fn controlled() -> Controlled {
    let (entered, _) = mpsc::unbounded_channel();
    Controlled {
        ready: false,
        entered,
        calls: Arc::new(AtomicUsize::new(0)),
    }
}

#[tokio::test]
async fn rejects_zero_capacity() {
    assert!(matches!(
        Endpoint::new(
            controlled(),
            Capacity {
                max_in_flight: 0,
                buffer_capacity: 1
            }
        ),
        Err(ConfigError::ZeroConcurrency)
    ));
}

#[tokio::test]
async fn rejects_zero_buffer() {
    assert!(matches!(
        Endpoint::new(
            controlled(),
            Capacity {
                max_in_flight: 1,
                buffer_capacity: 0
            }
        ),
        Err(ConfigError::ZeroBuffer)
    ));
}

#[tokio::test]
async fn cloned_handles_share_service() -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let (entered, mut calls) = mpsc::unbounded_channel();
        let count = Arc::new(AtomicUsize::new(0));
        let endpoint = Endpoint::new(
            Controlled {
                ready: false,
                entered,
                calls: count.clone(),
            },
            Capacity {
                max_in_flight: 2,
                buffer_capacity: 2,
            },
        )?;
        let clone = endpoint.clone();
        let first = tokio::spawn(endpoint.execute(1));
        let second = tokio::spawn(clone.execute(2));
        let (_, release_first) = calls.recv().await.ok_or("first did not enter")?;
        let (_, release_second) = calls.recv().await.ok_or("second did not enter")?;
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert!(release_first.send(()).is_ok());
        assert!(release_second.send(()).is_ok());
        assert_eq!(first.await?? + second.await??, 3);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

struct ProbeRequest {
    id: usize,
    dropped: Arc<AtomicBool>,
}

impl Drop for ProbeRequest {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

struct ProbeService {
    entered: mpsc::UnboundedSender<(usize, oneshot::Sender<()>)>,
}

impl Service<ProbeRequest> for ProbeService {
    type Response = usize;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<usize, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ProbeRequest) -> Self::Future {
        let (release, wait) = oneshot::channel();
        let id = request.id;
        let _ = self.entered.send((id, release));
        Box::pin(async move {
            let _request = request;
            let _ = wait.await;
            Ok(id)
        })
    }
}

fn probe(id: usize) -> (ProbeRequest, Arc<AtomicBool>) {
    let dropped = Arc::new(AtomicBool::new(false));
    (
        ProbeRequest {
            id,
            dropped: dropped.clone(),
        },
        dropped,
    )
}

async fn poll_once_pending<F: Future>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn bounded_queue_applies_backpressure() -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let (entered, mut calls) = mpsc::unbounded_channel();
        let endpoint = Endpoint::new(
            ProbeService { entered },
            Capacity {
                max_in_flight: 1,
                buffer_capacity: 1,
            },
        )?;
        let (request, _) = probe(1);
        let mut first = Box::pin(endpoint.execute(request));
        poll_once_pending(first.as_mut()).await;
        let (_, release_first) = calls.recv().await.ok_or("first did not enter")?;
        let (request, _) = probe(2);
        let mut queued = Box::pin(endpoint.execute(request));
        poll_once_pending(queued.as_mut()).await;
        let (request, dropped) = probe(3);
        let mut excess = Box::pin(endpoint.execute(request));
        poll_once_pending(excess.as_mut()).await;
        drop(excess);
        assert!(
            dropped.load(Ordering::SeqCst),
            "request waiting for queue admission must remain caller-owned"
        );
        assert!(release_first.send(()).is_ok());
        assert_eq!(first.await?, 1);
        let (_, release_second) = calls.recv().await.ok_or("queued request did not enter")?;
        assert!(release_second.send(()).is_ok());
        assert_eq!(queued.await?, 2);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn cancel_queued_request() -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let (entered, mut calls) = mpsc::unbounded_channel();
        let endpoint = Endpoint::new(
            ProbeService { entered },
            Capacity {
                max_in_flight: 1,
                buffer_capacity: 1,
            },
        )?;
        let (request, _) = probe(1);
        let mut first = Box::pin(endpoint.execute(request));
        poll_once_pending(first.as_mut()).await;
        let (_, release_first) = calls.recv().await.ok_or("first did not enter")?;
        let (request, dropped) = probe(2);
        let mut queued = Box::pin(endpoint.execute(request));
        poll_once_pending(queued.as_mut()).await;
        drop(queued);
        assert!(release_first.send(()).is_ok());
        assert_eq!(first.await?, 1);
        let (request, _) = probe(3);
        let next = tokio::spawn(endpoint.execute(request));
        let (id, release_next) = calls.recv().await.ok_or("next request did not enter")?;
        assert_eq!(id, 3, "cancelled queued request reached backend");
        assert!(dropped.load(Ordering::SeqCst));
        assert!(release_next.send(()).is_ok());
        assert_eq!(next.await??, 3);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[derive(Debug)]
struct BackendFailure;

impl std::fmt::Display for BackendFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("backend failed")
    }
}
impl Error for BackendFailure {}

struct FailingService {
    fail_ready: bool,
}

impl Service<usize> for FailingService {
    type Response = usize;
    type Error = BackendFailure;
    type Future = std::future::Ready<Result<usize, BackendFailure>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.fail_ready {
            Poll::Ready(Err(BackendFailure))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, _: usize) -> Self::Future {
        std::future::ready(Err(BackendFailure))
    }
}

#[tokio::test]
async fn service_call_error_propagates() -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let endpoint = Endpoint::new(
            FailingService { fail_ready: false },
            Capacity {
                max_in_flight: 1,
                buffer_capacity: 1,
            },
        )?;
        let error = endpoint
            .execute(1)
            .await
            .err()
            .ok_or("expected service error")?;
        assert!(error.downcast_ref::<BackendFailure>().is_some());
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn readiness_error_propagates() -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let endpoint = Endpoint::new(
            FailingService { fail_ready: true },
            Capacity {
                max_in_flight: 1,
                buffer_capacity: 1,
            },
        )?;
        let first = endpoint
            .execute(1)
            .await
            .err()
            .ok_or("expected readiness error")?;
        assert!(first.to_string().contains("backend failed"));
        let second = endpoint
            .execute(2)
            .await
            .err()
            .ok_or("worker should remain closed")?;
        assert!(second.to_string().contains("backend failed"));
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
