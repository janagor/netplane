//! Fixture counters around the public graph preparer.
use bytes::Bytes;
use http::{Request, Response};
use netplane::GraphVersion;
use netplane_control::GraphPreparer;
use netplane_kubernetes::{DesiredState, HttpGraphPreparer, PreparationError};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
#[derive(Clone)]
pub struct Counters {
    pub active: Arc<AtomicUsize>,
    pub maximum: Arc<AtomicUsize>,
    pub sizes: Arc<Mutex<Vec<usize>>>,
    pub paused: Arc<tokio::sync::watch::Sender<bool>>,
}
impl Default for Counters {
    fn default() -> Self {
        Self {
            active: Arc::default(),
            maximum: Arc::default(),
            sizes: Arc::default(),
            paused: Arc::new(tokio::sync::watch::channel(false).0),
        }
    }
}
struct Preparing(Counters);
impl Drop for Preparing {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
#[derive(Clone)]
pub struct Preparer {
    pub inner: HttpGraphPreparer,
    pub counters: Counters,
}
impl GraphPreparer<DesiredState, Request<Bytes>, Response<Bytes>> for Preparer {
    type Error = PreparationError;
    fn validate(&self, desired: &DesiredState) -> Result<(), Self::Error> {
        self.inner.validate(desired)
    }
    async fn prepare(
        &self,
        desired: DesiredState,
    ) -> Result<GraphVersion<Request<Bytes>, Response<Bytes>>, Self::Error> {
        let active = self.counters.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.counters.maximum.fetch_max(active, Ordering::SeqCst);
        let _guard = Preparing(self.counters.clone());
        {
            let mut sizes = self
                .counters
                .sizes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if sizes.len() < 256 {
                sizes.push(desired.backends().len());
            }
        }
        let mut gate = self.counters.paused.subscribe();
        while *gate.borrow_and_update() {
            if gate.changed().await.is_err() {
                break;
            }
        }
        self.inner.prepare(desired).await
    }
}
