//! Validated, single-router composition of named Tower backends.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    future::Future,
};

use thiserror::Error;
use tower::{BoxError, Service, ServiceExt, service_fn, util::BoxService};

use crate::{Capacity, ConfigError, Endpoint, ExecutionGraph, GraphVersion};

mod metadata;
pub use metadata::{BackendCapacityMetadata, GraphMetadata, NodeKind, NodeMetadata, RouteMetadata};

/// A graph declaration failed structural or capacity validation.
#[derive(Debug, Error)]
pub enum GraphBuildError {
    /// No entry was declared.
    #[error("graph entry is missing")]
    MissingEntry,
    /// A node name was registered more than once.
    #[error("duplicate node: {0}")]
    DuplicateNode(String),
    /// The entry does not name the router.
    #[error("unknown entry: {0}")]
    UnknownEntry(String),
    /// A route or fallback names an absent service.
    #[error("unknown target: {0}")]
    UnknownTarget(String),
    /// A router has a repeated selector label.
    #[error("duplicate route label: {0}")]
    DuplicateRoute(String),
    /// A router has more than one fallback declaration.
    #[error("duplicate fallback for router: {0}")]
    DuplicateFallback(String),
    /// A router lacks an explicit fallback.
    #[error("router has no fallback")]
    MissingFallback,
    /// The declaration exceeds the supported single-router topology.
    #[error("unsupported topology: {0}")]
    UnsupportedTopology(String),
    /// An endpoint capacity is invalid.
    #[error("invalid capacity: {0}")]
    Capacity(#[from] ConfigError),
    /// An explicit outer capacity conflicts with backend-only execution.
    #[error("outer capacity cannot be set with without_outer_capacity")]
    ConflictingOuterCapacity,
}

struct Router<Request> {
    name: String,
    selector: Box<dyn Fn(&Request) -> Cow<'static, str> + Send + Sync>,
    routes: BTreeMap<String, String>,
    fallback: Option<String>,
}

struct RegisteredService<Request, Response> {
    service: BoxService<Request, Response, BoxError>,
    capacity_override: Option<Capacity>,
}
/// Builds a double-buffered router over named, bounded Tower services by default.
///
/// Register ordinary `ServiceBuilder` compositions as services to use middleware.
/// Duplicate declarations are recorded and reported by [`Self::validate`].
pub struct GraphBuilder<Request, Response> {
    services: BTreeMap<String, RegisteredService<Request, Response>>,
    router: Option<Router<Request>>,
    entry: Option<String>,
    capacity: Capacity,
    capacity_explicit: bool,
    without_outer_capacity: bool,
    backend_capacity: Capacity,
    error: Option<GraphBuildError>,
}

impl<Request, Response> Default for GraphBuilder<Request, Response> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Request, Response> GraphBuilder<Request, Response> {
    /// Create a builder with the same outer defaults as [`crate::GraphDraft`].
    #[must_use]
    pub const fn new() -> Self {
        let capacity = Capacity {
            max_in_flight: 32,
            buffer_capacity: 64,
        };
        Self {
            services: BTreeMap::new(),
            router: None,
            entry: None,
            capacity,
            capacity_explicit: false,
            without_outer_capacity: false,
            backend_capacity: capacity,
            error: None,
        }
    }

    /// Register a named Tower service, including any already applied middleware.
    #[must_use]
    pub fn service<S>(self, name: impl Into<String>, service: S) -> Self
    where
        Request: Send + 'static,
        Response: Send + 'static,
        S: Service<Request, Response = Response> + Send + 'static,
        S::Error: Into<BoxError> + Send + Sync,
        S::Future: Future<Output = Result<Response, S::Error>> + Send + 'static,
    {
        self.register_service(name, service, None)
    }

    /// Register a named Tower service with an explicit endpoint capacity.
    ///
    /// This override takes precedence over [`Self::backend_capacity`] regardless
    /// of call order. Both capacities must be positive at validation. Backend
    /// isolation requires an available outer execution slot.
    #[must_use]
    pub fn service_with_capacity<S>(
        self,
        name: impl Into<String>,
        service: S,
        capacity: Capacity,
    ) -> Self
    where
        Request: Send + 'static,
        Response: Send + 'static,
        S: Service<Request, Response = Response> + Send + 'static,
        S::Error: Into<BoxError> + Send + Sync,
        S::Future: Future<Output = Result<Response, S::Error>> + Send + 'static,
    {
        self.register_service(name, service, Some(capacity))
    }

    fn register_service<S>(
        mut self,
        name: impl Into<String>,
        service: S,
        capacity_override: Option<Capacity>,
    ) -> Self
    where
        Request: Send + 'static,
        Response: Send + 'static,
        S: Service<Request, Response = Response> + Send + 'static,
        S::Error: Into<BoxError> + Send + Sync,
        S::Future: Future<Output = Result<Response, S::Error>> + Send + 'static,
    {
        let name = name.into();
        if self.services.contains_key(&name) || self.router.as_ref().is_some_and(|r| r.name == name)
        {
            self.error
                .get_or_insert(GraphBuildError::DuplicateNode(name));
        } else {
            self.services.insert(
                name,
                RegisteredService {
                    service: BoxService::new(service.map_err(Into::into)),
                    capacity_override,
                },
            );
        }
        self
    }

    /// Register the single request-dependent router.
    ///
    /// The selector's output is matched against declared labels. Unknown labels
    /// use the explicit fallback. Selection is synchronous at dispatch or admission.
    #[must_use]
    pub fn router<F, L>(mut self, name: impl Into<String>, selector: F) -> Self
    where
        F: Fn(&Request) -> L + Send + Sync + 'static,
        L: Into<Cow<'static, str>>,
    {
        let name = name.into();
        if let Some(existing) = self.router.as_ref() {
            let error = if existing.name == name {
                GraphBuildError::DuplicateNode(name)
            } else {
                GraphBuildError::UnsupportedTopology("multiple routers are not supported".into())
            };
            self.error.get_or_insert(error);
        } else if self.services.contains_key(&name) {
            self.error
                .get_or_insert(GraphBuildError::DuplicateNode(name));
        } else {
            self.router = Some(Router {
                name,
                selector: Box::new(move |request| selector(request).into()),
                routes: BTreeMap::new(),
                fallback: None,
            });
        }
        self
    }

    /// Declare a selector label and target service for the named router.
    ///
    /// Register the router before declaring routes. Services and the entry may
    /// be registered in any order. Validation resolves target names.
    #[must_use]
    pub fn route(
        mut self,
        router: impl AsRef<str>,
        label: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        let name = router.as_ref();
        if let Some(node) = self.router.as_mut().filter(|r| r.name == name) {
            let label = label.into();
            if node.routes.insert(label.clone(), target.into()).is_some() {
                self.error
                    .get_or_insert(GraphBuildError::DuplicateRoute(label));
            }
        } else {
            self.error
                .get_or_insert(GraphBuildError::UnsupportedTopology(format!(
                    "route source {name} is not the router"
                )));
        }
        self
    }

    /// Set the target for undeclared selector labels.
    ///
    /// Register the router before setting its fallback. A second fallback
    /// declaration is rejected during validation.
    #[must_use]
    pub fn fallback(mut self, router: impl AsRef<str>, target: impl Into<String>) -> Self {
        let name = router.as_ref();
        if let Some(node) = self.router.as_mut().filter(|r| r.name == name) {
            if node.fallback.is_some() {
                self.error
                    .get_or_insert(GraphBuildError::DuplicateFallback(name.into()));
            } else {
                node.fallback = Some(target.into());
            }
        } else {
            self.error
                .get_or_insert(GraphBuildError::UnsupportedTopology(format!(
                    "fallback source {name} is not the router"
                )));
        }
        self
    }

    /// Select the graph entry; the supported topology requires the router.
    #[must_use]
    pub fn entry(mut self, name: impl Into<String>) -> Self {
        self.entry = Some(name.into());
        self
    }

    /// Set the outer Tower endpoint capacity for buffered execution.
    ///
    /// Combining this with [`Self::without_outer_capacity`] fails validation,
    /// regardless of method call order.
    #[must_use]
    pub const fn capacity(mut self, capacity: Capacity) -> Self {
        self.capacity = capacity;
        self.capacity_explicit = true;
        self
    }

    /// Route directly to backend endpoints without an outer execution limit.
    ///
    /// Backend capacities and [`crate::RuntimeLimits::max_outstanding`] still
    /// apply. This removes aggregate per-version concurrency and buffer limits.
    /// Combining it with an explicit [`Self::capacity`] fails validation.
    #[must_use]
    pub const fn without_outer_capacity(mut self) -> Self {
        self.without_outer_capacity = true;
        self
    }

    /// Set the default capacity for backends without an explicit override.
    ///
    /// The declared default is validated even if every backend has an override.
    #[must_use]
    pub const fn backend_capacity(mut self, capacity: Capacity) -> Self {
        self.backend_capacity = capacity;
        self
    }
}

impl<Request, Response> GraphBuilder<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    /// Validate the complete declaration, then start its Tower workers.
    ///
    /// Invalid declarations start no workers. Call inside an active Tokio runtime.
    /// Buffered execution has an outer worker and one worker per backend.
    /// Backend-only execution has only backend workers. In both policies,
    /// `Handle` admission selects the graph version synchronously; route
    /// selection runs when the accepted future is first polled (or, for
    /// buffered mode, when the outer worker dequeues the request). A saturated
    /// branch can occupy outer execution slots only in buffered mode.
    ///
    /// # Errors
    /// Returns [`GraphBuildError`] for invalid declarations or capacities.
    pub fn validate(self) -> Result<GraphVersion<Request, Response>, GraphBuildError> {
        match self.validate_inner() {
            Ok(version) => {
                let node_count = version.metadata.nodes.len();
                let route_count = version.metadata.routes.len();
                tracing::debug!(target: "netplane", event = "graph.validated", node_count, route_count);
                Ok(version)
            }
            Err(error) => {
                tracing::warn!(target: "netplane", event = "graph.validation_failed", reason = error.kind());
                Err(error)
            }
        }
    }

    fn validate_inner(self) -> Result<GraphVersion<Request, Response>, GraphBuildError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        let entry = self.entry.clone().ok_or(GraphBuildError::MissingEntry)?;
        let metadata = {
            let router = self
                .router
                .as_ref()
                .ok_or_else(|| GraphBuildError::UnknownEntry(entry.clone()))?;
            if entry != router.name {
                return Err(GraphBuildError::UnknownEntry(entry));
            }
            let fallback = router
                .fallback
                .as_deref()
                .ok_or(GraphBuildError::MissingFallback)?;
            self.validate_capacities_and_targets(router, fallback)?;
            self.metadata(router, entry.clone(), fallback)
        };
        let router = self
            .router
            .ok_or_else(|| GraphBuildError::UnknownEntry(entry.clone()))?;
        let fallback = router.fallback.ok_or(GraphBuildError::MissingFallback)?;
        let reachable: BTreeSet<_> = router
            .routes
            .values()
            .chain(std::iter::once(&fallback))
            .collect();
        if let Some(name) = self.services.keys().find(|name| !reachable.contains(name)) {
            return Err(GraphBuildError::UnsupportedTopology(format!(
                "unreachable service {name}"
            )));
        }
        let endpoints: BTreeMap<_, _> = self
            .services
            .into_iter()
            .map(|(name, registered)| {
                let capacity = registered
                    .capacity_override
                    .unwrap_or(self.backend_capacity);
                Endpoint::new(registered.service, capacity).map(|endpoint| (name, endpoint))
            })
            .collect::<Result<_, _>>()?;
        let resolved_routes: BTreeMap<_, _> = router
            .routes
            .into_iter()
            .map(|(label, target)| {
                endpoints
                    .get(&target)
                    .cloned()
                    .map(|endpoint| (label, endpoint))
                    .ok_or(GraphBuildError::UnknownTarget(target))
            })
            .collect::<Result<_, _>>()?;
        let fallback_endpoint = endpoints
            .get(&fallback)
            .cloned()
            .ok_or_else(|| GraphBuildError::UnknownTarget(fallback.clone()))?;
        let mut endpoint_ids: Vec<u64> = endpoints.values().map(Endpoint::id).collect();
        let execution = if self.without_outer_capacity {
            ExecutionGraph::BackendOnly(crate::routed::RoutedGraph::new(
                router.selector,
                resolved_routes,
                fallback_endpoint,
            ))
        } else {
            let selector = router.selector;
            let root = service_fn(move |request: Request| {
                let label = selector(&request);
                let endpoint = resolved_routes
                    .get(label.as_ref())
                    .unwrap_or(&fallback_endpoint)
                    .clone();
                async move { endpoint.execute(request).await }
            });
            let endpoint = Endpoint::new(root, self.capacity)?;
            endpoint_ids.push(endpoint.id());
            ExecutionGraph::Buffered(endpoint)
        };
        Ok(GraphVersion {
            execution,
            endpoint_ids,
            metadata,
        })
    }

    fn metadata(&self, router: &Router<Request>, entry: String, fallback: &str) -> GraphMetadata {
        let mut nodes: Vec<_> = std::iter::once(NodeMetadata {
            name: router.name.clone(),
            kind: NodeKind::Router,
        })
        .chain(self.services.keys().cloned().map(|name| NodeMetadata {
            name,
            kind: NodeKind::Service,
        }))
        .collect();
        nodes.sort_by(|a, b| a.name.cmp(&b.name));
        GraphMetadata {
            entry,
            nodes,
            routes: router
                .routes
                .iter()
                .map(|(label, target)| RouteMetadata {
                    label: label.clone(),
                    target: target.clone(),
                })
                .collect(),
            fallback: Some(fallback.into()),
            capacity: (!self.without_outer_capacity).then_some(self.capacity),
            backend_capacity: Some(self.backend_capacity),
            backend_capacities: self
                .services
                .iter()
                .map(|(name, registered)| BackendCapacityMetadata {
                    name: name.clone(),
                    capacity: registered
                        .capacity_override
                        .unwrap_or(self.backend_capacity),
                })
                .collect(),
        }
    }

    fn validate_capacities_and_targets(
        &self,
        router: &Router<Request>,
        fallback: &str,
    ) -> Result<(), GraphBuildError> {
        if self.without_outer_capacity && self.capacity_explicit {
            return Err(GraphBuildError::ConflictingOuterCapacity);
        }
        if !self.without_outer_capacity {
            validate_capacity(self.capacity)?;
        }
        validate_capacity(self.backend_capacity)?;
        for registered in self.services.values() {
            if let Some(capacity) = registered.capacity_override {
                validate_capacity(capacity)?;
            }
        }
        for target in router
            .routes
            .values()
            .map(String::as_str)
            .chain(std::iter::once(fallback))
        {
            if !self.services.contains_key(target) {
                return Err(GraphBuildError::UnknownTarget(target.into()));
            }
        }
        Ok(())
    }
}

const fn validate_capacity(capacity: Capacity) -> Result<(), ConfigError> {
    if capacity.max_in_flight == 0 {
        return Err(ConfigError::ZeroConcurrency);
    }
    if capacity.buffer_capacity == 0 {
        return Err(ConfigError::ZeroBuffer);
    }
    Ok(())
}
impl GraphBuildError {
    const fn kind(&self) -> &'static str {
        match self {
            Self::MissingEntry => "missing_entry",
            Self::DuplicateNode(_) => "duplicate_node",
            Self::UnknownEntry(_) => "unknown_entry",
            Self::UnknownTarget(_) => "unknown_target",
            Self::DuplicateRoute(_) => "duplicate_route",
            Self::DuplicateFallback(_) => "duplicate_fallback",
            Self::MissingFallback => "missing_fallback",
            Self::UnsupportedTopology(_) => "unsupported_topology",
            Self::Capacity(error) => crate::config_error_kind(error),
            Self::ConflictingOuterCapacity => "conflicting_outer_capacity",
        }
    }
}
