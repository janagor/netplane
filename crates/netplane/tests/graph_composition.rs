//! Internal composition prototype using existing Tower and Netplane primitives.

use std::{
    convert::Infallible,
    error::Error,
    future::{Future, Ready, ready},
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use netplane::{AdmissionError, Capacity, Endpoint, GraphDraft, Netplane, RuntimeLimits};
use tokio::{sync::oneshot, time::timeout};
use tower::{BoxError, Service, ServiceBuilder, filter::FilterLayer, service_fn};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

const BACKEND_CAPACITY: Capacity = Capacity {
    max_in_flight: 1,
    buffer_capacity: 1,
};
const OUTER_CAPACITY: Capacity = Capacity {
    max_in_flight: 2,
    buffer_capacity: 2,
};
const TEST_DEADLINE: Duration = Duration::from_secs(5);
const READINESS_TRANSITIONS: usize = 64;
const EVEN_KEY: usize = 2;
const ODD_KEY: usize = 3;

#[derive(Clone, Copy)]
struct Request {
    key: usize,
    authorized: bool,
}

const fn request(key: usize) -> Request {
    Request {
        key,
        authorized: true,
    }
}

const fn routes_to_a(request: &Request) -> bool {
    request.key.is_multiple_of(2)
}

fn named_backend(
    name: &'static str,
    calls: Arc<AtomicUsize>,
) -> Result<Endpoint<Request, String>, BoxError> {
    Ok(Endpoint::new(
        service_fn(move |request: Request| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, Infallible>(format!("{name}:{}", request.key)) }
        }),
        BACKEND_CAPACITY,
    )?)
}

fn composed(
    a: Endpoint<Request, String>,
    b: Endpoint<Request, String>,
    limit: usize,
) -> Result<Netplane<Request, String>, BoxError> {
    let graph = composed_graph(a, b)?;
    Ok(Netplane::new(
        graph,
        RuntimeLimits {
            max_outstanding: limit,
        },
    )?)
}

struct ReadinessGate {
    state: Arc<Mutex<GateState>>,
    polled: Option<oneshot::Sender<()>>,
    calls: Arc<AtomicUsize>,
    dropped: Option<oneshot::Sender<()>>,
}

struct GateState {
    ready: bool,
    waker: Option<Waker>,
}

impl ReadinessGate {
    fn open(state: &Arc<Mutex<GateState>>) {
        let waker = match state.lock() {
            Ok(mut state) => {
                state.ready = true;
                state.waker.take()
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.ready = true;
                state.waker.take()
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Drop for ReadinessGate {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

impl Service<Request> for ReadinessGate {
    type Response = String;
    type Error = Infallible;
    type Future = Ready<Result<String, Infallible>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.ready {
            return Poll::Ready(Ok(()));
        }
        state.waker = Some(cx.waker().clone());
        drop(state);
        if let Some(polled) = self.polled.take() {
            let _ = polled.send(());
        }
        Poll::Pending
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let is_ready = match self.state.lock() {
            Ok(state) => state.ready,
            Err(poisoned) => poisoned.into_inner().ready,
        };
        assert!(is_ready, "call preceded readiness");
        self.calls.fetch_add(1, Ordering::SeqCst);
        ready(Ok(format!("A:{}", request.key)))
    }
}

fn gated_backend() -> Result<
    (
        Endpoint<Request, String>,
        Arc<Mutex<GateState>>,
        oneshot::Receiver<()>,
        Arc<AtomicUsize>,
        oneshot::Receiver<()>,
    ),
    BoxError,
> {
    let state = Arc::new(Mutex::new(GateState {
        ready: false,
        waker: None,
    }));
    let calls = Arc::new(AtomicUsize::new(0));
    let (polled_tx, polled_rx) = oneshot::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let endpoint = Endpoint::new(
        ReadinessGate {
            state: state.clone(),
            polled: Some(polled_tx),
            calls: calls.clone(),
            dropped: Some(dropped_tx),
        },
        BACKEND_CAPACITY,
    )?;
    Ok((endpoint, state, polled_rx, calls, dropped_rx))
}

#[tokio::test]
async fn routes_to_named_backends() -> TestResult {
    let a_calls = Arc::new(AtomicUsize::new(0));
    let b_calls = Arc::new(AtomicUsize::new(0));
    let runtime = composed(
        named_backend("A", a_calls.clone())?,
        named_backend("B", b_calls.clone())?,
        2,
    )?;
    let handle = runtime.handle();
    assert_eq!(handle.execute(request(EVEN_KEY)).await?, "A:2");
    assert_eq!(handle.execute(request(ODD_KEY)).await?, "B:3");
    assert_eq!(a_calls.load(Ordering::SeqCst), 1);
    assert_eq!(b_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn middleware_precedes_routing() -> TestResult {
    let a_calls = Arc::new(AtomicUsize::new(0));
    let b_calls = Arc::new(AtomicUsize::new(0));
    let runtime = composed(
        named_backend("A", a_calls.clone())?,
        named_backend("B", b_calls.clone())?,
        2,
    )?;
    let result = runtime
        .handle()
        .execute(Request {
            key: EVEN_KEY,
            authorized: false,
        })
        .await;
    assert!(result.is_err());
    assert_eq!(a_calls.load(Ordering::SeqCst), 0);
    assert_eq!(b_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn delayed_backend_does_not_block_other_branch() -> TestResult {
    timeout(TEST_DEADLINE, async {
        let (a, state, polled, calls, _) = gated_backend()?;
        let runtime = composed(a, named_backend("B", Arc::new(AtomicUsize::new(0)))?, 2)?;
        let handle = runtime.handle();
        let blocked = tokio::spawn(handle.execute(request(EVEN_KEY)));
        polled.await?;
        assert_eq!(handle.execute(request(ODD_KEY)).await?, "B:3");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        ReadinessGate::open(&state);
        assert_eq!(blocked.await??, "A:2");
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn router_respects_backend_readiness() -> TestResult {
    timeout(TEST_DEADLINE, async {
        let (a, state, polled, calls, _) = gated_backend()?;
        let runtime = composed(a, named_backend("B", Arc::new(AtomicUsize::new(0)))?, 2)?;
        let blocked = tokio::spawn(runtime.handle().execute(request(EVEN_KEY)));
        polled.await?;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        ReadinessGate::open(&state);
        assert_eq!(blocked.await??, "A:2");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn global_admission_remains_bounded() -> TestResult {
    let runtime = composed(
        named_backend("A", Arc::new(AtomicUsize::new(0)))?,
        named_backend("B", Arc::new(AtomicUsize::new(0)))?,
        2,
    )?;
    let handle = runtime.handle();
    let first = handle.try_execute(request(EVEN_KEY))?;
    let second = handle.try_execute(request(ODD_KEY))?;
    assert!(matches!(
        handle.try_execute(request(EVEN_KEY)),
        Err(AdmissionError::AtCapacity { limit: 2 })
    ));
    drop(first);
    assert_eq!(handle.try_execute(request(EVEN_KEY))?.await?, "A:2");
    drop(second);
    Ok(())
}

#[tokio::test]
async fn backend_dispatch_stays_within_concurrency_limit() -> TestResult {
    timeout(TEST_DEADLINE, async {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut entered = Some(entered_tx);
        let a = Endpoint::new(
            service_fn({
                let release = release.clone();
                let calls = calls.clone();
                move |request: Request| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let entered = entered.take();
                    let release = release.clone();
                    async move {
                        if let Some(entered) = entered {
                            let _ = entered.send(());
                        }
                        let pending_release = release.lock().await.take();
                        if let Some(release) = pending_release {
                            let _ = release.await;
                        }
                        Ok::<_, Infallible>(format!("A:{}", request.key))
                    }
                }
            }),
            BACKEND_CAPACITY,
        )?;
        let first = tokio::spawn(a.execute(request(EVEN_KEY)));
        entered_rx.await?;
        let mut second = Box::pin(a.execute(request(EVEN_KEY)));
        let mut third = Box::pin(a.execute(request(EVEN_KEY)));
        std::future::poll_fn(|cx| -> Poll<()> {
            assert!(second.as_mut().poll(cx).is_pending());
            assert!(third.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(second);
        drop(third);
        let _ = release_tx.send(());
        assert_eq!(first.await??, "A:2");
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_open_never_loses_wakeup() -> TestResult {
    for _ in 0..READINESS_TRANSITIONS {
        timeout(TEST_DEADLINE, async {
            let (a, state, polled, calls, _) = gated_backend()?;
            let runtime = composed(a, named_backend("B", Arc::new(AtomicUsize::new(0)))?, 1)?;
            let active = tokio::spawn(runtime.handle().execute(request(EVEN_KEY)));
            polled.await?;
            ReadinessGate::open(&state);
            assert_eq!(active.await??, "A:2");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            Ok::<_, BoxError>(())
        })
        .await??;
    }
    Ok(())
}

#[tokio::test]
async fn cancel_during_backend_execution() -> TestResult {
    timeout(TEST_DEADLINE, async {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (_release_tx, release_rx) = oneshot::channel::<()>();
        let mut entered = Some(entered_tx);
        let mut release = Some(release_rx);
        let a = Endpoint::new(
            service_fn(move |request: Request| {
                let entered = entered.take();
                let release = release.take();
                async move {
                    if let Some(entered) = entered {
                        let _ = entered.send(());
                    }
                    if let Some(release) = release {
                        let _ = release.await;
                    }
                    Ok::<_, Infallible>(format!("A:{}", request.key))
                }
            }),
            BACKEND_CAPACITY,
        )?;
        let runtime = composed(a, named_backend("B", Arc::new(AtomicUsize::new(0)))?, 1)?;
        let handle = runtime.handle();
        let active = tokio::spawn(handle.execute(request(EVEN_KEY)));
        entered_rx.await?;
        assert!(matches!(
            handle.try_execute(request(ODD_KEY)),
            Err(AdmissionError::AtCapacity { limit: 1 })
        ));
        active.abort();
        assert!(active.await.is_err());
        assert_eq!(handle.try_execute(request(ODD_KEY))?.await?, "B:3");
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn cancel_while_waiting_for_readiness() -> TestResult {
    timeout(TEST_DEADLINE, async {
        let (a, _, polled, calls, dropped) = gated_backend()?;
        let mut runtime = composed(a, named_backend("B", Arc::new(AtomicUsize::new(0)))?, 1)?;
        let handle = runtime.handle();
        let blocked = tokio::spawn(handle.execute(request(EVEN_KEY)));
        polled.await?;
        blocked.abort();
        assert!(blocked.await.is_err());
        assert_eq!(handle.try_execute(request(ODD_KEY))?.await?, "B:3");
        runtime.publish(
            GraphDraft::new(service_fn(|request: Request| async move {
                Ok::<_, Infallible>(format!("new:{}", request.key))
            }))
            .validate()?,
        )?;
        dropped.await?;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn backend_error_isolated() -> TestResult {
    let failed = Endpoint::new(
        service_fn(|_: Request| async { Err::<String, _>(io::Error::other("backend A failed")) }),
        BACKEND_CAPACITY,
    )?;
    let runtime = composed(
        failed,
        named_backend("B", Arc::new(AtomicUsize::new(0)))?,
        2,
    )?;
    assert!(runtime.handle().execute(request(EVEN_KEY)).await.is_err());
    assert_eq!(runtime.handle().execute(request(ODD_KEY)).await?, "B:3");
    Ok(())
}

#[tokio::test]
async fn publication_preserves_old_graph() -> TestResult {
    timeout(TEST_DEADLINE, async {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let mut entered = Some(entered_tx);
        let mut release = Some(release_rx);
        let old_a = Endpoint::new(
            service_fn(move |request: Request| {
                let entered = entered.take();
                let release = release.take();
                async move {
                    if let Some(entered) = entered {
                        let _ = entered.send(());
                    }
                    if let Some(release) = release {
                        let _ = release.await;
                    }
                    Ok::<_, Infallible>(format!("old-A:{}", request.key))
                }
            }),
            BACKEND_CAPACITY,
        )?;
        let mut runtime = composed(
            old_a,
            named_backend("old-B", Arc::new(AtomicUsize::new(0)))?,
            2,
        )?;
        let handle = runtime.handle();
        let old = tokio::spawn(handle.execute(request(EVEN_KEY)));
        entered_rx.await?;
        let new_a = named_backend("new-A", Arc::new(AtomicUsize::new(0)))?;
        let new_b = named_backend("new-B", Arc::new(AtomicUsize::new(0)))?;
        runtime.publish(composed_graph(new_a, new_b)?)?;
        assert_eq!(handle.execute(request(ODD_KEY)).await?, "new-B:3");
        let _ = release_tx.send(());
        assert_eq!(old.await??, "old-A:2");
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

fn composed_graph(
    a: Endpoint<Request, String>,
    b: Endpoint<Request, String>,
) -> Result<netplane::GraphVersion<Request, String>, BoxError> {
    let router = service_fn(move |request: Request| {
        let selected = if routes_to_a(&request) {
            a.clone()
        } else {
            b.clone()
        };
        async move { selected.execute(request).await }
    });
    let authorized = ServiceBuilder::new()
        .layer(FilterLayer::new(
            |request: Request| -> Result<Request, io::Error> {
                request
                    .authorized
                    .then_some(request)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "unauthorized"))
            },
        ))
        .service(router);
    Ok(GraphDraft::new(authorized)
        .capacity(OUTER_CAPACITY)
        .validate()?)
}

#[tokio::test]
async fn old_graph_workers_are_released() -> TestResult {
    timeout(TEST_DEADLINE, async {
        let (a, _, polled, _, dropped) = gated_backend()?;
        let mut runtime = composed(a, named_backend("old-B", Arc::new(AtomicUsize::new(0)))?, 2)?;
        let handle = runtime.handle();
        let old = tokio::spawn(handle.execute(request(EVEN_KEY)));
        polled.await?;
        runtime.publish(composed_graph(
            named_backend("new-A", Arc::new(AtomicUsize::new(0)))?,
            named_backend("new-B", Arc::new(AtomicUsize::new(0)))?,
        )?)?;
        old.abort();
        assert!(old.await.is_err());
        dropped.await?;
        assert_eq!(handle.execute(request(ODD_KEY)).await?, "new-B:3");
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_drains_composed_graph() -> TestResult {
    timeout(TEST_DEADLINE, async {
        let runtime = composed(
            named_backend("A", Arc::new(AtomicUsize::new(0)))?,
            named_backend("B", Arc::new(AtomicUsize::new(0)))?,
            1,
        )?;
        let mut runtime = runtime;
        let handle = runtime.handle();
        let accepted = handle.try_execute(request(EVEN_KEY))?;
        let draining = runtime.shutdown(TEST_DEADLINE);
        assert!(matches!(
            handle.try_execute(request(ODD_KEY)),
            Err(AdmissionError::Closed)
        ));
        assert_eq!(accepted.await?, "A:2");
        draining.await?;
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}
