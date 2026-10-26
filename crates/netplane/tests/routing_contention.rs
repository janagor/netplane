//! Public router behavior when one backend withholds readiness.
use std::{
    convert::Infallible,
    error::Error,
    future::{Ready, ready},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use netplane::{Capacity, GraphBuilder, Netplane, RuntimeLimits};
use tokio::{sync::oneshot, time::timeout};
use tower::{Service, service_fn};
#[path = "common/drop.rs"]
mod drop_helper;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;
const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Default)]
struct State {
    open: bool,
    waker: Option<Waker>,
}

struct GatedBackend {
    state: Arc<Mutex<State>>,
    polled: Option<oneshot::Sender<()>>,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Drop for GatedBackend {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl Service<usize> for GatedBackend {
    type Response = &'static str;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.open {
            return Poll::Ready(Ok(()));
        }
        state.waker = Some(cx.waker().clone());
        drop(state);
        if let Some(polled) = self.polled.take() {
            let _ = polled.send(());
        }
        Poll::Pending
    }

    fn call(&mut self, _: usize) -> Self::Future {
        assert!(
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .open
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        ready(Ok("A"))
    }
}

fn open(state: &Arc<Mutex<State>>) {
    let waker = {
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.open = true;
        state.waker.take()
    };
    if let Some(waker) = waker {
        waker.wake();
    }
}

fn runtime(
    outer_slots: usize,
) -> Result<
    (
        Netplane<usize, &'static str>,
        Arc<Mutex<State>>,
        oneshot::Receiver<()>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ),
    Box<dyn Error + Send + Sync>,
> {
    let state = Arc::new(Mutex::new(State::default()));
    let calls_a = Arc::new(AtomicUsize::new(0));
    let drops_a = Arc::new(AtomicUsize::new(0));
    let calls_b = Arc::new(AtomicUsize::new(0));
    let (polled, observed) = oneshot::channel();
    let b = Arc::clone(&calls_b);
    let graph = GraphBuilder::new()
        .service_with_capacity(
            "a",
            GatedBackend {
                state: Arc::clone(&state),
                polled: Some(polled),
                calls: Arc::clone(&calls_a),
                drops: Arc::clone(&drops_a),
            },
            Capacity {
                max_in_flight: 1,
                buffer_capacity: 1,
            },
        )
        .service_with_capacity(
            "b",
            service_fn(move |_: usize| {
                b.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, Infallible>("B") }
            }),
            Capacity {
                max_in_flight: 8,
                buffer_capacity: 8,
            },
        )
        .router(
            "router",
            |request: &usize| if *request == 0 { "a" } else { "b" },
        )
        .route("router", "a", "a")
        .route("router", "b", "b")
        .fallback("router", "b")
        .entry("router")
        .capacity(Capacity {
            max_in_flight: outer_slots,
            buffer_capacity: 2,
        })
        .backend_capacity(Capacity {
            max_in_flight: 1,
            buffer_capacity: 1,
        })
        .validate()?;
    Ok((
        Netplane::new(graph, RuntimeLimits { max_outstanding: 2 })?,
        state,
        observed,
        calls_a,
        calls_b,
        drops_a,
    ))
}

#[tokio::test]
async fn pending_backend_does_not_block_ready_branch_with_outer_capacity() -> TestResult {
    timeout(WATCHDOG, async {
        let (mut runtime, _state, polled, calls_a, calls_b, drops) = runtime(2)?;
        let handle = runtime.handle();
        let a = tokio::spawn(handle.try_execute(0)?);
        polled.await?;
        assert_eq!(handle.clone().try_execute(1)?.await?, "B");
        assert_eq!(calls_a.load(Ordering::SeqCst), 0);
        assert_eq!(calls_b.load(Ordering::SeqCst), 1);
        a.abort();
        assert!(a.await.is_err());
        runtime.shutdown(WATCHDOG).await?;
        drop_helper::wait_for_drop(&drops, 1).await;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn saturated_outer_capacity_blocks_other_backend() -> TestResult {
    timeout(WATCHDOG, async {
        let (mut runtime, state, polled, calls_a, calls_b, drops) = runtime(1)?;
        let handle = runtime.handle();
        let a = tokio::spawn(handle.try_execute(0)?);
        polled.await?;
        let mut b = Box::pin(handle.try_execute(1)?);
        assert!(timeout(Duration::from_millis(200), &mut b).await.is_err());
        assert_eq!(calls_b.load(Ordering::SeqCst), 0);
        open(&state);
        assert_eq!(a.await??, "A");
        assert_eq!(b.await?, "B");
        assert_eq!(calls_a.load(Ordering::SeqCst), 1);
        assert_eq!(calls_b.load(Ordering::SeqCst), 1);
        runtime.shutdown(WATCHDOG).await?;
        drop_helper::wait_for_drop(&drops, 1).await;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
