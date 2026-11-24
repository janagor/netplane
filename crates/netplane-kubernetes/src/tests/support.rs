use super::{Result, config};
use crate::{Discovery, DiscoveryHandle};
use futures_util::{Stream, stream};
use kube::{
    Client, Config,
    runtime::watcher::{Error, Event},
};
use tokio::sync::{mpsc, oneshot};

pub async fn bounded(future: impl std::future::Future<Output = Result>) -> Result {
    tokio::time::timeout(std::time::Duration::from_secs(5), future).await?
}

pub fn runner() -> std::result::Result<(Discovery, DiscoveryHandle), Box<dyn std::error::Error>> {
    Ok(Discovery::new(
        Client::try_from(Config::new("http://127.0.0.1:1".parse()?))?,
        config(),
    )?)
}

type Delivery<K> = (std::result::Result<Event<K>, Error>, oneshot::Sender<()>);
pub struct Feed<K>(mpsc::UnboundedSender<Delivery<K>>);
impl<K> Feed<K> {
    pub async fn send(&self, event: std::result::Result<Event<K>, Error>) -> Result {
        let (acknowledge, processed) = oneshot::channel();
        self.0
            .send((event, acknowledge))
            .map_err(|_| "event stream closed")?;
        processed.await?;
        Ok(())
    }
}
pub fn events<K: Send + 'static>() -> (
    Feed<K>,
    impl Stream<Item = std::result::Result<Event<K>, Error>>,
) {
    let (tx, rx) = mpsc::unbounded_channel::<Delivery<K>>();
    let stream = stream::unfold(
        (rx, None::<oneshot::Sender<()>>),
        |(mut rx, previous)| async move {
            // The runner polls again only after processing and attempting publication
            // for the previous event. Merely yielding an event would acknowledge too early.
            if let Some(previous) = previous {
                let _ = previous.send(());
            }
            rx.recv()
                .await
                .map(|(event, acknowledge)| (event, (rx, Some(acknowledge))))
        },
    );
    (Feed(tx), stream)
}

pub struct Harness {
    pub services: Feed<k8s_openapi::api::core::v1::Service>,
    pub slices: Feed<k8s_openapi::api::discovery::v1::EndpointSlice>,
    pub handle: DiscoveryHandle,
    pub cancellation: tokio_util::sync::CancellationToken,
    pub task: tokio::task::JoinHandle<std::result::Result<(), crate::DiscoveryError>>,
}
impl Harness {
    pub async fn initialized() -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let (discovery, handle) = runner()?;
        let (services, service_stream) = events();
        let (slices, slice_stream) = events();
        let cancellation = tokio_util::sync::CancellationToken::new();
        let task =
            tokio::spawn(discovery.run_streams(service_stream, slice_stream, cancellation.clone()));
        services.send(Ok(Event::Init)).await?;
        services
            .send(Ok(Event::InitApply(super::service()?)))
            .await?;
        services.send(Ok(Event::InitDone)).await?;
        slices.send(Ok(Event::Init)).await?;
        slices.send(Ok(Event::InitApply(super::slice()?))).await?;
        slices.send(Ok(Event::InitDone)).await?;
        Ok(Self {
            services,
            slices,
            handle,
            cancellation,
            task,
        })
    }
    pub async fn stop(self) -> Result {
        self.cancellation.cancel();
        self.task.await??;
        Ok(())
    }
}
