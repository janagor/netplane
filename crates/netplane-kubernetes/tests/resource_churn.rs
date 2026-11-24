//! Measured cleanup of version-local workers and HTTP pool connections.
#[path = "support/runtime.rs"]
mod support;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::{Result, desired, desired_for_uid, request, runtime};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
};
use tracing_subscriber::layer::SubscriberExt;

struct Workers {
    terminated: Arc<Semaphore>,
    count: Arc<AtomicUsize>,
}
struct TerminationEvent(bool);
impl tracing::field::Visit for TerminationEvent {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "event" && format!("{value:?}") == "\"worker.terminated\"" {
            self.0 = true;
        }
    }
}
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Workers {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "netplane" {
            return;
        }
        let mut termination = TerminationEvent(false);
        event.record(&mut termination);
        if termination.0 {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.terminated.add_permits(1);
        }
    }
}
#[tokio::test]
async fn churn_returns_worker_and_connection_counts_to_baseline() -> Result {
    const CYCLES: usize = 24;
    let terminated = Arc::new(Semaphore::new(0));
    let count = Arc::new(AtomicUsize::new(0));
    let subscriber = tracing_subscriber::registry().with(Workers {
        terminated: terminated.clone(),
        count: count.clone(),
    });
    tracing::subscriber::set_global_default(subscriber)?;
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let live = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(Semaphore::new(0));
        let (active, finished) = (live.clone(), closed.clone());
        let server = tokio::spawn(async move {
            for _ in 0..CYCLES {
                let (mut socket, _) = listener.accept().await?;
                active.fetch_add(1, Ordering::SeqCst);
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") {
                    socket.read_exact(&mut byte).await?;
                    request.push(byte[0]);
                }
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nA")
                    .await?;
                assert_eq!(socket.read(&mut byte).await?, 0);
                active.fetch_sub(1, Ordering::SeqCst);
                finished.add_permits(1);
            }
            Ok::<_, std::io::Error>(())
        });
        let (controller, reconciler) = runtime().await?;
        for cycle in 0..CYCLES {
            reconciler
                .apply(desired_for_uid(&[address], &format!("pod-{cycle}"))?)
                .await?;
            controller.handle().execute(request()).await?;
            assert_eq!(live.load(Ordering::SeqCst), 1);
            reconciler.apply(desired(&[])?).await?;
            tokio::time::timeout(Duration::from_secs(2), closed.acquire())
                .await
                .map_err(|_| "pool did not close its socket")??
                .forget(); // version-local pool actually closed its socket
            tokio::time::timeout(Duration::from_secs(2), terminated.acquire_many(2))
                .await
                .map_err(|_| {
                    format!(
                        "worker termination count {}, expected {}",
                        count.load(Ordering::SeqCst),
                        (cycle + 1) * 2
                    )
                })??
                .forget(); // old empty root + replaced backend
            assert_eq!(live.load(Ordering::SeqCst), 0);
            assert_eq!(count.load(Ordering::SeqCst), (cycle + 1) * 2);
            // 2 workers created per cycle + initial root, leaving one empty root.
        }
        controller.shutdown(Duration::from_secs(1)).await?;
        tokio::time::timeout(Duration::from_secs(2), terminated.acquire())
            .await
            .map_err(|_| "final worker did not terminate")??
            .forget();
        assert_eq!(count.load(Ordering::SeqCst), CYCLES * 2 + 1);
        assert_eq!(live.load(Ordering::SeqCst), 0);
        server.await??;
        Ok(())
    })
    .await?
}
