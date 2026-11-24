mod runtime;
pub use runtime::*;
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, sync::Semaphore, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub struct Server {
    pub address: SocketAddr,
    pub entered: Arc<Semaphore>,
    pub release: Arc<Semaphore>,
    stop: CancellationToken,
    task: Option<JoinHandle<std::io::Result<()>>>,
}
impl Server {
    pub async fn start(name: &'static str, blocked: bool) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let entered = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let (seen, gate) = (Arc::clone(&entered), Arc::clone(&release));
        let app = axum::Router::new().fallback(move || {
            let (seen, gate) = (Arc::clone(&seen), Arc::clone(&gate));
            async move {
                seen.add_permits(1);
                if blocked {
                    if let Ok(permit) = gate.acquire().await {
                        permit.forget();
                    }
                }
                name
            }
        });
        let stop = CancellationToken::new();
        let task = tokio::spawn(
            axum::serve(listener, app)
                .with_graceful_shutdown(stop.clone().cancelled_owned())
                .into_future(),
        );
        Ok(Self {
            address,
            entered,
            release,
            stop,
            task: Some(task),
        })
    }
    pub async fn stop(mut self) -> Result {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.await??;
        }
        Ok(())
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
use std::future::IntoFuture;
