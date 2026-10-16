//! Run with `cargo run -p netplane --example live_reconfiguration`.

use std::{
    convert::Infallible,
    error::Error,
    future::{Ready, ready},
    io,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::sync::oneshot;
use tower::{BoxError, Service, ServiceBuilder, filter::FilterLayer, service_fn};

use netplane::{Capacity, Endpoint, GraphDraft, GraphVersion, Netplane, RuntimeLimits};

#[derive(Clone, Copy)]
/// Request data used by both published compositions.
pub struct Request {
    /// Backend routing key.
    pub key: u32,
    /// Whether the version B simulated authorization filter permits the request.
    pub authorized: bool,
}

/// Observed outputs from one version transition.
pub struct Results {
    /// Response from the request selected on version A.
    pub old_request: String,
    /// Response routed to Backend A on version B.
    pub routed_a: String,
    /// Response routed to Backend B on version B.
    pub routed_b: String,
    /// Whether version B rejected an unauthorized request.
    pub rejected: bool,
}

const CAPACITY: Capacity = Capacity {
    max_in_flight: 2,
    buffer_capacity: 4,
};
const BACKEND_CAPACITY: Capacity = Capacity {
    max_in_flight: 1,
    buffer_capacity: 1,
};

const fn routes_to_a(request: Request) -> bool {
    request.key.is_multiple_of(2)
}

struct ReadinessGate {
    state: Arc<Mutex<GateState>>,
    polled: Option<oneshot::Sender<()>>,
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
        ready(Ok(format!("A:{}", request.key)))
    }
}

fn routed_graph() -> Result<
    (
        GraphVersion<Request, String>,
        Arc<Mutex<GateState>>,
        oneshot::Receiver<()>,
    ),
    BoxError,
> {
    let state = Arc::new(Mutex::new(GateState {
        ready: false,
        waker: None,
    }));
    let (polled_tx, polled_rx) = oneshot::channel();
    let backend_a = Endpoint::new(
        ReadinessGate {
            state: state.clone(),
            polled: Some(polled_tx),
        },
        BACKEND_CAPACITY,
    )?;
    let backend_b = Endpoint::new(
        service_fn(
            |request: Request| async move { Ok::<_, Infallible>(format!("B:{}", request.key)) },
        ),
        BACKEND_CAPACITY,
    )?;
    let router = service_fn(move |request: Request| {
        let selected = if routes_to_a(request) {
            backend_a.clone()
        } else {
            backend_b.clone()
        };
        async move { selected.execute(request).await }
    });
    let authorized_router = ServiceBuilder::new()
        .layer(FilterLayer::new(|request: Request| {
            request
                .authorized
                .then_some(request)
                .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "unauthorized"))
        }))
        .service(router);
    let graph = GraphDraft::new(authorized_router)
        .capacity(CAPACITY)
        .validate()?;
    Ok((graph, state, polled_rx))
}

/// Execute one transition while an old-version request is in progress.
///
/// # Errors
/// Returns an error if setup, publication, execution, or synchronization fails.
pub async fn run_demo() -> Result<Results, Box<dyn Error + Send + Sync>> {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let mut entered_tx = Some(entered_tx);
    let mut release_rx = Some(release_rx);

    // Version A has one direct backend. Its first request pauses after dispatch.
    let direct_a = service_fn(move |request: Request| {
        let entered = entered_tx.take();
        let release = release_rx.take();
        async move {
            if let Some(entered) = entered {
                let _ = entered.send(());
            }
            if let Some(release) = release {
                let _ = release.await;
            }
            Ok::<_, Infallible>(format!("A:{}", request.key))
        }
    });
    let graph_a = GraphDraft::new(direct_a).capacity(CAPACITY).validate()?;
    let mut runtime = Netplane::new(
        graph_a,
        RuntimeLimits {
            max_outstanding: 1024,
        },
    )?;
    let handle = runtime.handle();
    let old_request = tokio::spawn(handle.execute(Request {
        key: 1,
        authorized: false,
    }));
    entered_rx.await?;

    // Version B checks simulated authorization, then routes even keys to A and odd keys to B.
    let (graph_b, state, polled_rx) = routed_graph()?;
    runtime.publish(graph_b)?;

    // A waits for readiness while B proceeds through its independent worker.
    let routed_a = tokio::spawn(handle.execute(Request {
        key: 2,
        authorized: true,
    }));
    tokio::time::timeout(Duration::from_secs(5), polled_rx).await??;
    let routed_b = handle
        .execute(Request {
            key: 3,
            authorized: true,
        })
        .await?;
    ReadinessGate::open(&state);
    let routed_a = routed_a.await??;
    let rejected = match handle
        .execute(Request {
            key: 4,
            authorized: false,
        })
        .await
    {
        Err(error) => error
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.kind() == io::ErrorKind::PermissionDenied),
        Ok(_) => false,
    };
    release_tx
        .send(())
        .map_err(|()| io::Error::other("old request finished before release"))?;
    let old_request = old_request.await??;
    runtime.shutdown(Duration::from_secs(5)).await?;

    Ok(Results {
        old_request,
        routed_a,
        routed_b,
        rejected,
    })
}

#[cfg(not(test))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let results = run_demo().await?;
    println!("old request: {}", results.old_request);
    println!("new route A: {}", results.routed_a);
    println!("new route B: {}", results.routed_b);
    println!("unauthorized request rejected: {}", results.rejected);
    Ok(())
}
