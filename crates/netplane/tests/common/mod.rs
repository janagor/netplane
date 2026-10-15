use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use netplane::{Capacity, ConfigError, GraphDraft, GraphVersion};
use tokio::sync::{Semaphore, mpsc};
use tower::Service;

pub const DEADLINE: Duration = Duration::from_secs(5);
pub const CAPACITY: Capacity = Capacity {
    max_in_flight: 32,
    buffer_capacity: 32,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reply {
    pub version: usize,
    pub id: usize,
}

pub struct Gate {
    pub permits: Arc<Semaphore>,
    pub entered: mpsc::UnboundedReceiver<usize>,
    pub calls: Arc<AtomicUsize>,
    pub dropped: Arc<AtomicUsize>,
}

struct GateService {
    version: usize,
    permits: Arc<Semaphore>,
    entered: mpsc::UnboundedSender<usize>,
    calls: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl Drop for GateService {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Service<usize> for GateService {
    type Response = Reply;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Reply, io::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, id: usize) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _ = self.entered.send(id);
        let permits = Arc::clone(&self.permits);
        let version = self.version;
        Box::pin(async move {
            permits
                .acquire_owned()
                .await
                .map_err(io::Error::other)?
                .forget();
            Ok(Reply { version, id })
        })
    }
}

pub fn graph(
    version: usize,
    capacity: Capacity,
) -> Result<(GraphVersion<usize, Reply>, Gate), ConfigError> {
    let permits = Arc::new(Semaphore::new(0));
    let (entered, receiver) = mpsc::unbounded_channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let service = GateService {
        version,
        permits: Arc::clone(&permits),
        entered,
        calls: Arc::clone(&calls),
        dropped: Arc::clone(&dropped),
    };
    let graph = GraphDraft::new(service).capacity(capacity).validate()?;
    Ok((
        graph,
        Gate {
            permits,
            entered: receiver,
            calls,
            dropped,
        },
    ))
}
