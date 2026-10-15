//! Version retention and buffer-worker cleanup under pending work.
#[path = "common/drop.rs"]
mod cleanup;
mod common;

use std::{
    convert::Infallible,
    error::Error,
    future::{Future, pending},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use cleanup::wait_for_drop;
use common::{CAPACITY, DEADLINE, graph};
use netplane::{Endpoint, Netplane, RuntimeLimits};
use tokio::time::timeout;
use tower::Service;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn old_versions_survive_unpolled_requests_and_release_in_any_order() -> TestResult {
    timeout(DEADLINE, async {
        let (first, a) = graph(0, CAPACITY)?;
        let mut runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 4 })?;
        let handle = runtime.handle();
        let old = handle.try_execute(0)?;
        let (second, b) = graph(1, CAPACITY)?;
        runtime.publish(second)?;
        let middle = handle.try_execute(1)?;
        let (third, c) = graph(2, CAPACITY)?;
        assert_eq!(a.permits.available_permits(), 0);
        assert!(!a.entered.is_closed());
        runtime.publish(third)?;
        drop(handle);
        assert_eq!(a.dropped.load(Ordering::SeqCst), 0);
        assert_eq!(b.dropped.load(Ordering::SeqCst), 0);
        drop(middle);
        wait_for_drop(&b.dropped, 1).await;
        assert_eq!(a.dropped.load(Ordering::SeqCst), 0);
        drop(old);
        wait_for_drop(&a.dropped, 1).await;
        drop(runtime);
        wait_for_drop(&c.dropped, 1).await;
        assert_eq!(a.calls.load(Ordering::SeqCst), 0);
        assert_eq!(b.calls.load(Ordering::SeqCst), 0);
        assert_eq!(c.calls.load(Ordering::SeqCst), 0);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

struct NeverReady {
    dropped: Arc<AtomicUsize>,
    polled: Arc<AtomicUsize>,
}

impl Drop for NeverReady {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Service<usize> for NeverReady {
    type Response = common::Reply;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<common::Reply, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.polled.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }

    fn call(&mut self, _: usize) -> Self::Future {
        Box::pin(pending())
    }
}

#[tokio::test]
async fn last_owner_aborts_worker_pending_in_readiness() -> TestResult {
    timeout(DEADLINE, async {
        let dropped = Arc::new(AtomicUsize::new(0));
        let polled = Arc::new(AtomicUsize::new(0));
        let endpoint = Endpoint::new(
            NeverReady {
                dropped: Arc::clone(&dropped),
                polled: Arc::clone(&polled),
            },
            CAPACITY,
        )?;
        let future = endpoint.execute(1);
        let task = tokio::spawn(future);
        while polled.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        drop(endpoint);
        task.abort();
        let _ = task.await;
        wait_for_drop(&dropped, 1).await;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn cancellation_during_readiness_releases_global_admission() -> TestResult {
    timeout(DEADLINE, async {
        let dropped = Arc::new(AtomicUsize::new(0));
        let polled = Arc::new(AtomicUsize::new(0));
        let version = netplane::GraphDraft::new(NeverReady {
            dropped: Arc::clone(&dropped),
            polled: Arc::clone(&polled),
        })
        .validate()?;
        let mut runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
        let handle = runtime.handle();
        let pending = tokio::spawn(handle.execute(1));
        while polled.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        pending.abort();
        let _ = pending.await;
        let (replacement, mut gate) = graph(1, CAPACITY)?;
        runtime.publish(replacement)?;
        let next = tokio::spawn(handle.execute(2));
        assert_eq!(gate.entered.recv().await, Some(2));
        gate.permits.add_permits(1);
        assert_eq!(next.await??.id, 2);
        wait_for_drop(&dropped, 1).await;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
