use kube::{Client, Config};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tower::BoxError;

#[derive(Clone)]
pub struct Fault {
    state: Arc<watch::Sender<bool>>,
    active: Arc<AtomicUsize>,
}
impl Fault {
    pub fn set(&self, disconnected: bool) {
        self.state.send_replace(disconnected);
    }
    pub fn disconnected(&self) -> bool {
        *self.state.borrow()
    }
    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
    fn new() -> Self {
        Self {
            state: Arc::new(watch::channel(false).0),
            active: Arc::default(),
        }
    }
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
fn admitted(state: &mut watch::Receiver<bool>, count: usize) -> bool {
    !*state.borrow_and_update() && count < 8
}
async fn tunnel(
    mut downstream: TcpStream,
    mut state: watch::Receiver<bool>,
    host: String,
    port: u16,
) {
    tokio::select! {
        biased;
        _ = state.changed() => {},
        () = async {
            if let Ok(mut upstream) = TcpStream::connect((host.as_str(), port)).await {
                let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
            }
        } => {},
    }
}
pub async fn client(
    stop: CancellationToken,
    tasks: &mut JoinSet<Result<(), BoxError>>,
) -> Result<(Client, Fault), BoxError> {
    let mut config = Config::incluster()?;
    let host = config.cluster_url.host().ok_or("API host")?.to_owned();
    let port = config.cluster_url.port_u16().unwrap_or(443);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    config.tls_server_name = Some(host.clone());
    config.cluster_url = format!("https://{}", listener.local_addr()?).parse()?;
    let fault = Fault::new();
    let control = fault.clone();
    tasks.spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                Some(_) = connections.join_next(), if !connections.is_empty() => {},
                accepted = listener.accept() => {
                    let (downstream, _) = accepted?;
                    let mut state = control.state.subscribe();
                    if !admitted(&mut state, connections.len()) { continue; }
                    let host = host.clone();
                    control.active.fetch_add(1, Ordering::SeqCst);
                    let active = Active(control.active.clone());
                    connections.spawn(async move {
                        let _active = active;
                        tunnel(downstream, state, host, port).await;
                    });
                }
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        Ok(())
    });
    Ok((Client::try_from(config)?, fault))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    #[tokio::test]
    async fn activation_between_subscription_and_admission_rejects() {
        let fault = Fault::new();
        let mut receiver = fault.state.subscribe();
        fault.set(true);
        assert!(!admitted(&mut receiver, 0));
    }
    #[tokio::test]
    async fn activation_after_admission_closes_accepted_socket_without_connecting()
    -> Result<(), BoxError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut client = TcpStream::connect(listener.local_addr()?).await?;
        let (downstream, _) = listener.accept().await?;
        let upstream = TcpListener::bind("127.0.0.1:0").await?;
        let fault = Fault::new();
        let mut receiver = fault.state.subscribe();
        assert!(admitted(&mut receiver, 0));
        // Force the exact admission/spawn interleaving without sleeps.
        fault.set(true);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tunnel(
                downstream,
                receiver,
                "127.0.0.1".into(),
                upstream.local_addr()?.port(),
            ),
        )
        .await?;
        assert!(
            tokio::time::timeout(std::time::Duration::ZERO, upstream.accept())
                .await
                .is_err()
        );
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), client.read(&mut byte))
                .await??,
            0
        );
        drop(client);
        Ok(())
    }
}
