use crate::{
    DiscoveryConfig, DiscoveryError, DiscoveryObservation, DiscoverySnapshot, DiscoveryStatus,
    WatchStatus, state::ResourceCache, topology,
};
use futures_util::{Stream, StreamExt};
use k8s_openapi::api::{core::v1::Service, discovery::v1::EndpointSlice};
use kube::{
    Api, Client,
    runtime::{WatchStreamExt, watcher},
};
use std::sync::Arc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Caller-owned discovery runner. Construction starts no tasks.
/// Dropping the runner stops polling and retains the last published snapshot.
pub struct Discovery {
    client: Client,
    config: DiscoveryConfig,
    snapshots: watch::Sender<Option<Arc<DiscoverySnapshot>>>,
    status: watch::Sender<DiscoveryStatus>,
    observations: watch::Sender<Arc<DiscoveryObservation>>,
}
/// Cloneable access to latest immutable topology and watcher status.
#[derive(Clone)]
pub struct DiscoveryHandle {
    snapshots: watch::Receiver<Option<Arc<DiscoverySnapshot>>>,
    status: watch::Receiver<DiscoveryStatus>,
    observations: watch::Receiver<Arc<DiscoveryObservation>>,
}
impl DiscoveryHandle {
    /// Subscribe to coherent topology and health captured after each watcher event.
    ///
    /// Prefer this channel for reconciliation. Legacy snapshot/status subscriptions
    /// remain independently sampled. Clone the observation and release its watch
    /// guard before doing work; holding a guard can block discovery publication.
    /// This is a latest-value channel, not an event log: intermediate unhealthy
    /// observations can be overwritten by recovery without changing topology revision.
    #[must_use]
    pub fn subscribe_observations(&self) -> watch::Receiver<Arc<DiscoveryObservation>> {
        self.observations.clone()
    }
    /// Latest snapshot, or `None` before both initial listings complete.
    #[must_use]
    pub fn snapshot(&self) -> Option<Arc<DiscoverySnapshot>> {
        self.snapshots.borrow().clone()
    }
    /// Subscribe to coalesced latest snapshots.
    /// Clone the contained `Arc` and drop the watch read guard before processing
    /// or awaiting. Holding a guard can block publication. Processing an owned
    /// snapshot does not block discovery or accumulate a queue.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Option<Arc<DiscoverySnapshot>>> {
        self.snapshots.clone()
    }
    /// Current independent watcher states.
    /// Status and snapshots are separate observations, not an atomic pair.
    /// Use `subscribe_observations` when pairing health with topology for reconciliation.
    #[must_use]
    pub fn status(&self) -> DiscoveryStatus {
        self.status.borrow().clone()
    }
    /// Subscribe to latest status changes, observed independently of snapshots.
    /// Drop watch read guards before awaiting or performing lengthy work.
    #[must_use]
    pub fn subscribe_status(&self) -> watch::Receiver<DiscoveryStatus> {
        self.status.clone()
    }
}
impl Discovery {
    /// Construct a namespace-scoped runner and observation handle.
    ///
    /// # Errors
    /// Returns [`DiscoveryError::InvalidScope`] for invalid namespace or names.
    pub fn new(
        client: Client,
        config: DiscoveryConfig,
    ) -> Result<(Self, DiscoveryHandle), DiscoveryError> {
        config.validate()?;
        let (snapshots, snapshot_rx) = watch::channel(None);
        let (status, status_rx) = watch::channel(DiscoveryStatus {
            services: WatchStatus::Initializing,
            endpoint_slices: WatchStatus::Initializing,
            latest_revision: None,
        });
        let (observations, observation_rx) = watch::channel(Arc::new(DiscoveryObservation {
            snapshot: None,
            status: status.borrow().clone(),
        }));
        Ok((
            Self {
                client,
                config,
                snapshots,
                status,
                observations,
            },
            DiscoveryHandle {
                snapshots: snapshot_rx,
                status: status_rx,
                observations: observation_rx,
            },
        ))
    }
    /// Poll two kube-rs watchers with backoff until cancellation.
    /// Watch failures force a complete relist so quiet collections recover health
    /// without waiting for another object update. List retries use kube-rs backoff.
    /// Watch failures retain snapshots and expose `Recovering` while retrying.
    /// Cancellation drops both streams, including outstanding backoff timers.
    /// After initialization, updates may publish from committed caches while
    /// either watcher is recovering. A relist pauses publication until both
    /// caches are synchronized again. Consumers decide whether to apply a
    /// snapshot observed alongside degraded status.
    ///
    /// # Errors
    /// Returns a typed error on unexpected stream termination or revision exhaustion.
    pub async fn run(self, cancellation: CancellationToken) -> Result<(), DiscoveryError> {
        let services = Api::<Service>::namespaced(self.client.clone(), &self.config.namespace);
        let slices = Api::<EndpointSlice>::namespaced(self.client.clone(), &self.config.namespace);
        let services =
            relisting_watch(move || watcher(services.clone(), watcher::Config::default()))
                .default_backoff();
        let slices = relisting_watch(move || watcher(slices.clone(), watcher::Config::default()))
            .default_backoff();
        self.run_streams(services, slices, cancellation).await
    }
    pub(crate) async fn run_streams<S, E>(
        self,
        services: S,
        slices: E,
        cancellation: CancellationToken,
    ) -> Result<(), DiscoveryError>
    where
        S: Stream<Item = Result<watcher::Event<Service>, watcher::Error>>,
        E: Stream<Item = Result<watcher::Event<EndpointSlice>, watcher::Error>>,
    {
        tokio::pin!(services, slices);
        let mut service_cache = ResourceCache::default();
        let mut slice_cache = ResourceCache::default();
        tracing::info!("discovery_started");
        loop {
            let (changed, processed) = tokio::select! {
                () = cancellation.cancelled() => { return Ok(()); }
                event = services.next() => {
                    let marker = processed_resource(event.as_ref(), "services");
                    (self.process(event, &mut service_cache, true)?, marker)
                }
                event = slices.next() => {
                    let marker = processed_resource(event.as_ref(), "endpoint_slices");
                    (self.process(event, &mut slice_cache, false)?, marker)
                }
            };
            if changed && service_cache.synchronized() && slice_cache.synchronized() {
                let topology = topology::build(&service_cache, &slice_cache, &self.config);
                let previous = self.snapshots.borrow().clone();
                if !previous.as_ref().is_some_and(|p| p.services == topology) {
                    let revision = previous
                        .as_ref()
                        .map_or(Some(1), |p| p.revision.checked_add(1))
                        .ok_or(DiscoveryError::RevisionExhausted)?;
                    self.snapshots
                        .send_replace(Some(Arc::new(DiscoverySnapshot {
                            revision,
                            services: topology,
                        })));
                    self.status
                        .send_modify(|s| s.latest_revision = Some(revision));
                    tracing::info!(revision, "snapshot_published");
                }
            }
            self.publish_observation();
            if let Some((collection, resource_name, resource_version)) = processed {
                tracing::debug!(target: "netplane", event = "discovery.resource_processed",
                    collection, resource_name, resource_version);
            }
        }
    }
    fn publish_observation(&self) {
        self.observations
            .send_replace(Arc::new(DiscoveryObservation {
                snapshot: self.snapshots.borrow().clone(),
                status: self.status.borrow().clone(),
            }));
    }
    fn process<K: kube::ResourceExt + PartialEq>(
        &self,
        event: Option<Result<watcher::Event<K>, watcher::Error>>,
        cache: &mut ResourceCache<K>,
        services: bool,
    ) -> Result<bool, DiscoveryError> {
        let mut changed = false;
        let name = if services {
            "services"
        } else {
            "endpoint_slices"
        };
        let state = match event {
            None => {
                self.set_status(services, WatchStatus::Failed);
                return Err(DiscoveryError::StreamEnded(name));
            }
            Some(Err(error)) => {
                // Never log full API responses or resource objects.
                tracing::warn!(
                    watcher = name,
                    error_kind = error_kind(&error),
                    "watch_error"
                );
                WatchStatus::Recovering
            }
            Some(Ok(event)) => {
                let initializing =
                    matches!(event, watcher::Event::Init | watcher::Event::InitApply(_));
                match &event {
                    watcher::Event::Init => tracing::info!(watcher = name, "watch_relisting"),
                    watcher::Event::InitDone => tracing::info!(watcher = name, "watch_initialized"),
                    _ => tracing::trace!(watcher = name, "resource_update"),
                }
                changed = cache.apply(event);
                if cache.synchronized() {
                    WatchStatus::Watching
                } else if initializing && self.snapshots.borrow().is_some() {
                    WatchStatus::Recovering
                } else {
                    WatchStatus::Initializing
                }
            }
        };
        self.set_status(services, state);
        Ok(changed)
    }
    fn set_status(&self, services: bool, state: WatchStatus) {
        let mut recovered = false;
        self.status.send_if_modified(|s| {
            let field = if services {
                &mut s.services
            } else {
                &mut s.endpoint_slices
            };
            if *field == state {
                false
            } else {
                recovered = *field == WatchStatus::Recovering && state == WatchStatus::Watching;
                *field = state;
                true
            }
        });
        if recovered {
            tracing::debug!(
                watcher = if services {
                    "services"
                } else {
                    "endpoint_slices"
                },
                "watch_recovered"
            );
        }
    }
}
impl Drop for Discovery {
    fn drop(&mut self) {
        self.status.send_modify(|s| {
            if s.services != WatchStatus::Failed {
                s.services = WatchStatus::Stopped;
            }
            if s.endpoint_slices != WatchStatus::Failed {
                s.endpoint_slices = WatchStatus::Stopped;
            }
        });
        self.publish_observation();
        tracing::info!("discovery_stopped");
    }
}
fn error_kind(error: &watcher::Error) -> &'static str {
    let api_error = match error {
        watcher::Error::InitialListFailed(e)
        | watcher::Error::WatchStartFailed(e)
        | watcher::Error::WatchFailed(e) => Some(e),
        _ => None,
    };
    let code = match (error, api_error) {
        (watcher::Error::WatchError(response), _) | (_, Some(kube::Error::Api(response))) => {
            Some(response.code)
        }
        _ => None,
    };
    if let Some(code) = code {
        return match code {
            401 => "unauthorized",
            403 => "forbidden",
            410 => "resource_version_expired",
            _ => "api",
        };
    }
    match error {
        watcher::Error::InitialListFailed(_) => "initial_list",
        watcher::Error::WatchStartFailed(_) => "watch_start",
        watcher::Error::WatchFailed(_) => "watch",
        _ => "watch_protocol",
    }
}

// kube-rs can resume a watch without emitting any event on a quiet collection.
// Relisting after watch errors provides an explicit InitDone health boundary.
fn relisting_watch<T, S, F>(factory: F) -> impl Stream<Item = Result<T, watcher::Error>>
where
    S: Stream<Item = Result<T, watcher::Error>>,
    F: Fn() -> S,
{
    futures_util::stream::unfold((factory, None), |(factory, stream)| async move {
        let mut stream = stream.unwrap_or_else(|| Box::pin(factory()));
        let event = stream.next().await?;
        // Leave initial-list retries to kube-rs. All replacement streams share
        // the same surrounding default backoff.
        let restart =
            matches!(&event, Err(error) if !matches!(error, watcher::Error::InitialListFailed(_)));
        Some((event, (factory, (!restart).then_some(stream))))
    })
}

// Emitted only after the cache, topology and coherent observation have been updated.
fn processed_resource<K: kube::ResourceExt>(
    event: Option<&Result<watcher::Event<K>, watcher::Error>>,
    collection: &'static str,
) -> Option<(&'static str, String, String)> {
    // Both watcher paths skip identity allocation when DEBUG is disabled.
    if !tracing::enabled!(target: "netplane", tracing::Level::DEBUG) {
        return None;
    }
    match event?.as_ref().ok()? {
        watcher::Event::Apply(resource)
        | watcher::Event::InitApply(resource)
        | watcher::Event::Delete(resource) => Some((
            collection,
            resource.name_any(),
            resource.resource_version()?,
        )),
        watcher::Event::Init | watcher::Event::InitDone => None,
    }
}

#[cfg(test)]
mod retry_tests;
