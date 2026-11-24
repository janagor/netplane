use crate::{DesiredState, EndpointSelection};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use netplane::{Capacity, GraphBuilder, GraphDraft, GraphVersion, routing::UniformRouting};
use netplane_control::GraphPreparer;
use netplane_http::upstream::{self, HttpUpstream};
use std::{sync::Arc, time::Duration};
use tower::{BoxError, service_fn};

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
        }
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
        let mut builder = GraphBuilder::new()
            .router("endpoints", move |request: &Request<Bytes>| {
                policy.select(key(request)).to_owned()
            })
            .entry("endpoints")
            .without_outer_capacity()
            .fallback("endpoints", names[0].clone());
        let client = upstream::client(self.limits.exchange_timeout)
            .map_err(|error| PreparationError::Transport(Box::new(error)))?;
        for (backend, name) in desired.backends().iter().zip(names) {
            let upstream = HttpUpstream::new(
                client.clone(),
                &format!("http://{}", backend.id.address),
                self.limits.max_response_bytes,
            )
            .map_err(PreparationError::Transport)?;
            builder = builder
                .service_with_capacity(
                    name.clone(),
                    service_fn(move |request| {
                        let upstream = upstream.clone();
                        async move { upstream.execute(request).await }
                    }),
                    Capacity {
                        max_in_flight: backend.capacity.get(),
                        buffer_capacity: backend.capacity.get(),
                    },
                )
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
