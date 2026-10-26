//! Lifecycle guarantees of graphs compiled through the public builder.

use std::{
    convert::Infallible,
    future::{Future, Ready, ready},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use netplane::{AdmissionError, Capacity, GraphBuildError, GraphBuilder, Netplane, RuntimeLimits};
use tokio::{
    sync::oneshot,
    time::{Duration, timeout},
};
use tower::{BoxError, Service, service_fn};

const DEADLINE: Duration = Duration::from_secs(5);

struct GateState {
    open: AtomicBool,
    waker: Mutex<Option<Waker>>,
    calls: AtomicUsize,
    drops: AtomicUsize,
}

impl GateState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            open: AtomicBool::new(false),
            waker: Mutex::new(None),
            calls: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
        })
    }

    fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
        let waker = self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct GatedBackend {
    state: Arc<GateState>,
    polled: Option<oneshot::Sender<()>>,
}

impl Drop for GatedBackend {
    fn drop(&mut self) {
        self.state.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl Service<u32> for GatedBackend {
    type Response = String;
    type Error = Infallible;
    type Future = Ready<Result<String, Infallible>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.state.open.load(Ordering::SeqCst) {
            return Poll::Ready(Ok(()));
        }
        *self
            .state
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cx.waker().clone());
        if let Some(polled) = self.polled.take() {
            let _ = polled.send(());
        }
        Poll::Pending
    }

    fn call(&mut self, request: u32) -> Self::Future {
        self.state.calls.fetch_add(1, Ordering::SeqCst);
        ready(Ok(format!("A:{request}")))
    }
}

fn graph(
    state: Arc<GateState>,
    polled: oneshot::Sender<()>,
    outer: usize,
) -> Result<netplane::GraphVersion<u32, String>, GraphBuildError> {
    graph_with_policy(state, polled, Some(outer))
}

fn graph_with_policy(
    state: Arc<GateState>,
    polled: oneshot::Sender<()>,
    outer: Option<usize>,
) -> Result<netplane::GraphVersion<u32, String>, GraphBuildError> {
    let builder = GraphBuilder::new()
        .service(
            "a",
            GatedBackend {
                state,
                polled: Some(polled),
            },
        )
        .service(
            "b",
            service_fn(|request: u32| async move { Ok::<_, Infallible>(format!("B:{request}")) }),
        )
        .router("dispatch", |request: &u32| {
            if request.is_multiple_of(2) { "a" } else { "b" }
        })
        .route("dispatch", "a", "a")
        .route("dispatch", "b", "b")
        .fallback("dispatch", "b")
        .entry("dispatch")
        .backend_capacity(Capacity {
            max_in_flight: 1,
            buffer_capacity: 1,
        });
    match outer {
        Some(limit) => builder
            .capacity(Capacity {
                max_in_flight: limit,
                buffer_capacity: 2,
            })
            .validate(),
        None => builder.without_outer_capacity().validate(),
    }
}

#[tokio::test]
async fn backend_only_progresses_independently_and_retains_unpolled_version() -> Result<(), BoxError>
{
    timeout(DEADLINE, async {
        let state = GateState::new();
        let (polled, observed) = oneshot::channel();
        let mut runtime = Netplane::new(
            graph_with_policy(state.clone(), polled, None)?,
            RuntimeLimits { max_outstanding: 2 },
        )?;
        let handle = runtime.handle();
        let waiting = tokio::spawn(handle.execute(2));
        observed.await?;
        let retained = handle.try_execute(3)?;
        assert!(matches!(
            handle.try_execute(5),
            Err(AdmissionError::AtCapacity { .. })
        ));
        assert_eq!(retained.await?, "B:3");
        runtime.publish(
            GraphBuilder::new()
                .service(
                    "new",
                    service_fn(|request: u32| async move {
                        Ok::<_, Infallible>(format!("N:{request}"))
                    }),
                )
                .router("dispatch", |_: &u32| "new")
                .fallback("dispatch", "new")
                .entry("dispatch")
                .without_outer_capacity()
                .validate()?,
        )?;
        assert_eq!(handle.execute(5).await?, "N:5");
        state.open();
        assert_eq!(waiting.await??, "A:2");
        runtime.shutdown(Duration::from_secs(1)).await?;
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn backend_only_deadline_and_cancel_release_global_slots() -> Result<(), BoxError> {
    timeout(DEADLINE, async {
        let state = GateState::new();
        let (polled, observed) = oneshot::channel();
        let runtime = Netplane::new(
            graph_with_policy(state, polled, None)?,
            RuntimeLimits { max_outstanding: 1 },
        )?;
        let handle = runtime.handle();
        let pending =
            handle.try_execute_until(2, tokio::time::Instant::now() + Duration::from_millis(20))?;
        let task = tokio::spawn(pending);
        observed.await?;
        assert!(task.await?.is_err());
        assert_eq!(handle.status().outstanding, 0);
        let cancelled = handle.try_execute(2)?;
        drop(cancelled);
        assert_eq!(handle.status().outstanding, 0);
        assert_eq!(handle.execute(3).await?, "B:3");
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn unready_backend_does_not_block_ready_backend_with_outer_slot() -> Result<(), BoxError> {
    timeout(DEADLINE, async {
        let state = GateState::new();
        let (polled, observed) = oneshot::channel();
        let runtime = Netplane::new(
            graph(state.clone(), polled, 2)?,
            RuntimeLimits { max_outstanding: 3 },
        )?;
        let waiting = tokio::spawn(runtime.handle().execute(2));
        observed.await?;
        assert_eq!(state.calls.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.handle().execute(3).await?, "B:3");
        state.open();
        assert_eq!(waiting.await??, "A:2");
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn occupied_outer_slot_blocks_ready_backend() -> Result<(), BoxError> {
    timeout(DEADLINE, async {
        let state = GateState::new();
        let (polled, observed) = oneshot::channel();
        let runtime = Netplane::new(
            graph(state.clone(), polled, 1)?,
            RuntimeLimits { max_outstanding: 2 },
        )?;
        let waiting = tokio::spawn(runtime.handle().execute(2));
        observed.await?;
        let mut ready_branch = Box::pin(runtime.handle().execute(3));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            Pin::as_mut(&mut ready_branch).poll(&mut context),
            Poll::Pending
        ));
        state.open();
        assert_eq!(waiting.await??, "A:2");
        assert_eq!(ready_branch.await?, "B:3");
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn publication_retains_selected_and_dispatched_requests() -> Result<(), BoxError> {
    timeout(DEADLINE, async {
        let state = GateState::new();
        let (polled, observed) = oneshot::channel();
        let mut runtime = Netplane::new(
            graph(state.clone(), polled, 2)?,
            RuntimeLimits { max_outstanding: 3 },
        )?;
        let handle = runtime.handle();
        let selected = handle.execute(2);
        let dispatched = tokio::spawn(handle.execute(4));
        observed.await?;
        runtime.publish(
            GraphBuilder::new()
                .service(
                    "new",
                    service_fn(|request: u32| async move {
                        Ok::<_, Infallible>(format!("new:{request}"))
                    }),
                )
                .router("dispatch", |_| "new")
                .route("dispatch", "new", "new")
                .fallback("dispatch", "new")
                .entry("dispatch")
                .validate()?,
        )?;
        assert_eq!(handle.execute(6).await?, "new:6");
        state.open();
        assert_eq!(dispatched.await??, "A:4");
        assert_eq!(selected.await?, "A:2");
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn cancellation_and_publication_release_admission_and_old_worker() -> Result<(), BoxError> {
    timeout(DEADLINE, async {
        let state = GateState::new();
        let (polled, observed) = oneshot::channel();
        let mut runtime = Netplane::new(
            graph(state.clone(), polled, 1)?,
            RuntimeLimits { max_outstanding: 2 },
        )?;
        let handle = runtime.handle();
        let waiting = tokio::spawn(handle.execute(2));
        observed.await?;
        let mut queued = Box::pin(handle.try_execute(4)?);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(queued.as_mut().poll(&mut context), Poll::Pending));
        assert!(matches!(
            handle.try_execute(3),
            Err(AdmissionError::AtCapacity { limit: 2 })
        ));
        drop(queued);
        let retained = handle.try_execute(3)?;
        runtime.publish(
            GraphBuilder::new()
                .service(
                    "new",
                    service_fn(|request: u32| async move {
                        Ok::<_, Infallible>(format!("new:{request}"))
                    }),
                )
                .router("dispatch", |_| "new")
                .fallback("dispatch", "new")
                .entry("dispatch")
                .validate()?,
        )?;
        assert!(matches!(
            handle.try_execute(3),
            Err(AdmissionError::AtCapacity { limit: 2 })
        ));
        waiting.abort();
        assert!(waiting.await.is_err());
        assert_eq!(handle.execute(3).await?, "new:3");
        drop(retained);
        while state.drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(state.calls.load(Ordering::SeqCst), 0);
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn invalid_graph_never_starts_backend_worker() -> Result<(), BoxError> {
    let state = GateState::new();
    let (polled, observed) = oneshot::channel();
    let invalid = GraphBuilder::<u32, String>::new()
        .service(
            "a",
            GatedBackend {
                state: state.clone(),
                polled: Some(polled),
            },
        )
        .router("dispatch", |_| "missing")
        .route("dispatch", "missing", "missing")
        .fallback("dispatch", "a")
        .entry("dispatch")
        .validate();
    assert!(matches!(invalid, Err(GraphBuildError::UnknownTarget(_))));
    assert!(observed.await.is_err());
    assert_eq!(state.calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

struct BlockingBackend {
    entered: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
    drops: Arc<AtomicUsize>,
}

impl Drop for BlockingBackend {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl Service<u32> for BlockingBackend {
    type Response = String;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<String, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: u32) -> Self::Future {
        let entered = self.entered.take();
        let release = self.release.take();
        Box::pin(async move {
            if let Some(entered) = entered {
                let _ = entered.send(());
            }
            if let Some(release) = release {
                let _ = release.await;
            }
            Ok(format!("old:{request}"))
        })
    }
}

#[tokio::test]
async fn cancelling_dispatched_call_releases_admission_and_old_worker() -> Result<(), BoxError> {
    timeout(DEADLINE, async {
        let drops = Arc::new(AtomicUsize::new(0));
        let (entered, observed) = oneshot::channel();
        let (_release, release) = oneshot::channel();
        let old = GraphBuilder::new()
            .service(
                "old",
                BlockingBackend {
                    entered: Some(entered),
                    release: Some(release),
                    drops: drops.clone(),
                },
            )
            .router("dispatch", |_| "old")
            .fallback("dispatch", "old")
            .entry("dispatch")
            .validate()?;
        let mut runtime = Netplane::new(old, RuntimeLimits { max_outstanding: 1 })?;
        let handle = runtime.handle();
        let executing = tokio::spawn(handle.execute(2));
        observed.await?;
        assert!(matches!(
            handle.try_execute(3),
            Err(AdmissionError::AtCapacity { limit: 1 })
        ));
        runtime.publish(
            GraphBuilder::new()
                .service(
                    "new",
                    service_fn(|request: u32| async move {
                        Ok::<_, Infallible>(format!("new:{request}"))
                    }),
                )
                .router("dispatch", |_| "new")
                .fallback("dispatch", "new")
                .entry("dispatch")
                .validate()?,
        )?;
        executing.abort();
        assert!(executing.await.is_err());
        assert_eq!(handle.execute(3).await?, "new:3");
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_publication_wakeups_across_threads() -> Result<(), BoxError> {
    timeout(DEADLINE, async {
        for _ in 0..32 {
            let state = GateState::new();
            let (polled, observed) = oneshot::channel();
            let mut runtime = Netplane::new(
                graph(state.clone(), polled, 2)?,
                RuntimeLimits { max_outstanding: 2 },
            )?;
            let handle = runtime.handle();
            let old = tokio::spawn(handle.execute(2));
            observed.await?;
            runtime.publish(
                GraphBuilder::new()
                    .service(
                        "new",
                        service_fn(|request: u32| async move {
                            Ok::<_, Infallible>(format!("new:{request}"))
                        }),
                    )
                    .router("dispatch", |_| "new")
                    .fallback("dispatch", "new")
                    .entry("dispatch")
                    .validate()?,
            )?;
            let new = tokio::spawn(handle.execute(3));
            state.open();
            assert_eq!(old.await??, "A:2");
            assert_eq!(new.await??, "new:3");
        }
        Ok::<_, BoxError>(())
    })
    .await??;
    Ok(())
}

#[path = "graph_builder_lifecycle/backend_only.rs"]
mod backend_only;
