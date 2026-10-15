//! Resource cleanup while Tower's buffer worker waits for readiness.
use netplane::{Capacity, Endpoint, GraphDraft, Netplane, RuntimeLimits};
use std::{
    convert::Infallible,
    error::Error,
    task::{Context, Poll},
};
use tokio::{
    sync::oneshot,
    time::{Duration, timeout},
};
use tower::{Service, service_fn};

struct DropProbe(Option<oneshot::Sender<()>>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        if let Some(dropped) = self.0.take() {
            let _ = dropped.send(());
        }
    }
}

struct NeverReady {
    polled: Option<oneshot::Sender<()>>,
    _probe: DropProbe,
}

impl Service<usize> for NeverReady {
    type Response = usize;
    type Error = Infallible;
    type Future = std::future::Ready<Result<usize, Infallible>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if let Some(polled) = self.polled.take() {
            let _ = polled.send(());
        }
        Poll::Pending
    }

    fn call(&mut self, request: usize) -> Self::Future {
        std::future::ready(Ok(request))
    }
}

const fn capacity() -> Capacity {
    Capacity {
        max_in_flight: 1,
        buffer_capacity: 1,
    }
}

#[tokio::test]
async fn cancel_request_during_pending_readiness_releases_old_service()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let (polled_tx, polled_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let graph_a = GraphDraft::new(NeverReady {
            polled: Some(polled_tx),
            _probe: DropProbe(Some(dropped_tx)),
        })
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(
            graph_a,
            RuntimeLimits {
                max_outstanding: 1024,
            },
        )?;
        let handle = runtime.handle();
        let request = tokio::spawn(handle.execute(1));
        polled_rx.await?;
        request.abort();
        assert!(request.await.is_err());
        runtime.publish(
            GraphDraft::new(service_fn(|value: usize| async move {
                Ok::<_, Infallible>(value + 100)
            }))
            .capacity(capacity())
            .validate()?,
        )?;
        dropped_rx.await?;
        assert_eq!(handle.execute(1).await?, 101);
        runtime.shutdown(Duration::from_secs(5)).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_releases_service_waiting_for_readiness()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let (polled_tx, polled_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let graph = GraphDraft::new(NeverReady {
            polled: Some(polled_tx),
            _probe: DropProbe(Some(dropped_tx)),
        })
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(
            graph,
            RuntimeLimits {
                max_outstanding: 1024,
            },
        )?;
        let handle = runtime.handle();
        let request = tokio::spawn(handle.execute(1));
        polled_rx.await?;
        request.abort();
        assert!(request.await.is_err());
        runtime.shutdown(Duration::from_secs(5)).await?;
        dropped_rx.await?;
        drop(handle);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn endpoint_future_keeps_worker_alive_after_endpoint_drop()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let mut entered_tx = Some(entered_tx);
        let mut release_rx = Some(release_rx);
        let endpoint = Endpoint::new(
            service_fn(move |value: usize| {
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
            }),
            capacity(),
        )?;
        let execution = endpoint.execute(1);
        drop(endpoint);
        let active = tokio::spawn(execution);
        entered_rx.await?;
        let _ = release_tx.send(());
        assert_eq!(active.await??, 2);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
