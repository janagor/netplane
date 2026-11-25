use crate::{DesiredState, EndpointSelection};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use netplane::health::{
    HealthHandle, HealthLayer, HealthMode, HealthPolicy, HealthRegistry, HealthSchedulerHandle,
};
use netplane::{Capacity, GraphBuilder, GraphDraft, GraphVersion, routing::UniformRouting};
use netplane_control::GraphPreparer;
use netplane_http::health::{HttpHealthProbe, HttpOutcomeClassifier};
use netplane_http::upstream::{self, HttpUpstream};
use std::{sync::Arc, time::Duration};
use tower::{BoxError, Layer, service_fn};

/// Transport and preparation bounds; global admission is configured on `Netplane`.
#[derive(Clone, Debug)]
pub struct UpstreamLimits {
    /// Immutable routing policy for eligible destinations.
    pub selection: EndpointSelection,
    /// Deadline for the entire exchange, including response collection.
    pub exchange_timeout: Duration,
    /// Maximum buffered upstream response bytes.
    pub max_response_bytes: usize,
    /// Maximum backend workers in one candidate graph. Larger updates are rejected.
    pub max_backends: usize,
}
impl Default for UpstreamLimits {
    fn default() -> Self {
        Self {
            selection: EndpointSelection::UniformHash,
            exchange_timeout: Duration::from_secs(30),
            max_response_bytes: 1024 * 1024,
            max_backends: 256,
        }
    }
}

/// Candidate construction failed; the reconciler retains the active graph.
#[derive(Debug, thiserror::Error)]
pub enum PreparationError {
    /// A configured transport bound is zero or the topology exceeds its worker bound.
    #[error("upstream limits must be positive and endpoint count must not exceed max_backends")]
    InvalidLimits,
    /// HTTP client or origin construction failed before graph publication.
    #[error("HTTP upstream construction failed")]
    Transport(#[source] BoxError),
    /// Netplane rejected the bounded router declaration.
    #[error(transparent)]
    Graph(#[from] netplane::GraphBuildError),
    /// Netplane rejected the unavailable root capacity.
    #[error(transparent)]
    Capacity(#[from] netplane::ConfigError),
    /// Immutable routing policy validation failed.
    #[error(transparent)]
    Routing(#[from] netplane::routing::UniformRoutingError),
    /// Health identity or configuration conflicts with a live registration.
    #[error(transparent)]
    Health(#[from] netplane::health::HealthError),
    /// Probe registration exceeds scheduler bounds or the scheduler has stopped.
    #[error(transparent)]
    Probe(#[from] netplane::health::ProbeError),
}

/// No eligible Kubernetes destination exists in the selected version.
#[derive(Debug, thiserror::Error)]
#[error("Kubernetes destination unavailable")]
pub struct Unavailable;

/// Prepare ordinary bounded Tower graphs with version-local HTTP connection pools.
///
/// Each preparation creates one client shared only within that version. This avoids
/// reusing a connection when a known Pod UID changes at the same address. Idle pools
/// expire after 30 seconds, retain at most eight connections per destination, and
/// disappear with their last version/request owner. No worker or queue is shared
/// across versions. Controller rollback history can intentionally retain old pools.
/// Input request bodies must be bounded by ingress before execution.
#[derive(Clone)]
pub struct HttpGraphPreparer {
    limits: UpstreamLimits,
    key: Arc<dyn for<'a> Fn(&'a Request<Bytes>) -> &'a [u8] + Send + Sync>,
    health: Option<(HealthRegistry, HealthPolicy)>,
    active_checks: Option<(HealthSchedulerHandle, String)>,
    health_classifier: HttpOutcomeClassifier,
    probe_classifier: HttpOutcomeClassifier,
}
impl HttpGraphPreparer {
    /// Select by caller-supplied stable key bytes using `EndpointSelection::UniformHash`.
    ///
    /// Missing or invalid keys are application policy; the callback must handle them.
    /// Construction spawns no tasks and opens no connections.
    #[must_use]
    pub fn new(
        limits: UpstreamLimits,
        key: impl for<'a> Fn(&'a Request<Bytes>) -> &'a [u8] + Send + Sync + 'static,
    ) -> Self {
        Self {
            limits,
            key: Arc::new(key),
            health: None,
            active_checks: None,
            health_classifier: HttpOutcomeClassifier::passive_default(),
            probe_classifier: HttpOutcomeClassifier::probe_default(),
        }
    }

    /// Observe upstream outcomes and filter local health at each route selection.
    ///
    /// Unchanged endpoint identities and compatible policies reuse live records
    /// across candidate preparation, publication and rollback. Kubernetes readiness
    /// remains a membership constraint; local recovery cannot add removed endpoints.
    #[must_use]
    pub fn with_health(mut self, registry: HealthRegistry, policy: HealthPolicy) -> Self {
        self.health = Some((registry, policy));
        self.active_checks = None;
        self
    }

    /// Probe discovered backends before admitting ordinary traffic.
    ///
    /// The caller runs and shuts down the shared scheduler. Registrations belong
    /// to executable graph workers; retained versions share registrations, and
    /// rejecting or dropping a candidate cannot cancel an active graph's checks.
    /// Invalid origin-form paths are rejected during preparation.
    #[must_use]
    pub fn with_active_health(
        mut self,
        registry: HealthRegistry,
        policy: HealthPolicy,
        scheduler: HealthSchedulerHandle,
        path: impl Into<String>,
    ) -> Self {
        self.health = Some((registry, policy));
        self.active_checks = Some((scheduler, path.into()));
        self
    }

    /// Customize HTTP status outcomes for passive observations and active checks.
    #[must_use]
    pub fn with_health_classifier(mut self, classifier: HttpOutcomeClassifier) -> Self {
        self.probe_classifier = classifier.clone();
        self.health_classifier = classifier;
        self
    }

    fn health_handles(
        &self,
        desired: &DesiredState,
    ) -> Result<Vec<Option<HealthHandle>>, netplane::health::HealthError> {
        desired
            .backends()
            .iter()
            .map(|backend| {
                self.health
                    .as_ref()
                    .map(|(registry, policy)| {
                        registry.register_with_mode(
                            backend.id.health_identity(),
                            policy.clone(),
                            if self.active_checks.is_some() {
                                HealthMode::Active
                            } else {
                                HealthMode::Passive
                            },
                        )
                    })
                    .transpose()
            })
            .collect()
    }
}
impl GraphPreparer<DesiredState, Request<Bytes>, Response<Bytes>> for HttpGraphPreparer {
    type Error = PreparationError;
    fn validate(&self, desired: &DesiredState) -> Result<(), Self::Error> {
        if self.limits.exchange_timeout.is_zero()
            || self.limits.max_response_bytes == 0
            || self.limits.max_backends == 0
            || desired.backends().len() > self.limits.max_backends
        {
            return Err(PreparationError::InvalidLimits);
        }
        Ok(())
    }
    async fn prepare(
        &self,
        desired: DesiredState,
    ) -> Result<GraphVersion<Request<Bytes>, Response<Bytes>>, Self::Error> {
        self.validate(&desired)?;
        if desired.backends().is_empty() {
            return Ok(GraphDraft::new(service_fn(|_: Request<Bytes>| async {
                Err::<Response<Bytes>, _>(Unavailable)
            }))
            .capacity(Capacity {
                max_in_flight: 1,
                buffer_capacity: 1,
            })
            .validate()?);
        }
        let names: Vec<_> = (0..desired.backends().len())
            .map(|index| format!("endpoint-{index}"))
            .collect();
        let policy = match self.limits.selection {
            EndpointSelection::UniformHash => UniformRouting::try_new(names.clone())?,
        };
        let key = Arc::clone(&self.key);
        let handles = self.health_handles(&desired)?;
        let eligibility = handles.clone();
        let health_enabled = self.health.is_some();
        let mut builder = GraphBuilder::new()
            .try_router("endpoints", move |request: &Request<Bytes>| {
                if !health_enabled {
                    return Ok(policy.select(key(request)).to_owned());
                }
                let mut handles = eligibility.iter();
                policy
                    .select_eligible(key(request), |_| {
                        handles
                            .next()
                            .and_then(Option::as_ref)
                            .is_none_or(HealthHandle::is_eligible)
                    })
                    .map(str::to_owned)
            })
            .entry("endpoints")
            .without_outer_capacity()
            .fallback("endpoints", names[0].clone());
        let client = upstream::client(self.limits.exchange_timeout)
            .map_err(|error| PreparationError::Transport(Box::new(error)))?;
        for ((backend, name), health) in desired.backends().iter().zip(names).zip(handles) {
            let upstream = HttpUpstream::new(
                client.clone(),
                &format!("http://{}", backend.id.address),
                self.limits.max_response_bytes,
            )
            .map_err(PreparationError::Transport)?;
            let registration = match (&self.active_checks, &health) {
                (Some((scheduler, path)), Some(health)) => Some(
                    scheduler.register(
                        health.clone(),
                        HttpHealthProbe::new(upstream.clone(), path)
                            .map_err(PreparationError::Transport)?
                            .with_classifier(self.probe_classifier.clone())
                            .with_transport_key(format!(
                                "netplane-http-client-v1:{:?}",
                                self.limits.exchange_timeout
                            )),
                    )?,
                ),
                _ => None,
            };
            let service = service_fn(move |request| {
                let upstream = upstream.clone();
                let registration = registration.clone();
                async move {
                    let response = upstream.execute(request).await;
                    drop(registration);
                    response
                }
            });
            let capacity = Capacity {
                max_in_flight: backend.capacity.get(),
                buffer_capacity: backend.capacity.get(),
            };
            builder = if let Some(health) = health {
                builder.service_with_capacity(
                    name.clone(),
                    HealthLayer::new(health, self.health_classifier.clone()).layer(service),
                    capacity,
                )
            } else {
                builder.service_with_capacity(name.clone(), service, capacity)
            }
            .route("endpoints", name.clone(), name);
        }
        Ok(builder.validate()?)
    }
}

/// Map unavailable versions to HTTP 503 and other failures through the HTTP adapter.
///
/// Walks Tower error sources so buffer wrappers preserve typed unavailable errors.
#[must_use]
pub fn map_http_error(error: &BoxError) -> Response<Bytes> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error.as_ref());
    while let Some(source) = current {
        if source.is::<Unavailable>() {
            let mut response = Response::new(Bytes::new());
            *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
            return response;
        }
        current = source.source();
    }
    upstream::map_error(error)
}
