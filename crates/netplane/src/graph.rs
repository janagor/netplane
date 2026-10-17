//! Validated, single-router composition of named Tower backends.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    future::Future,
};

use thiserror::Error;
use tower::{BoxError, Service, ServiceExt, service_fn, util::BoxService};

use crate::{Capacity, ConfigError, Endpoint, GraphVersion};

/// The supported kind of a declared node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeKind {
    /// A Tower service executed by a bounded endpoint.
    Service,
    /// A request selector with declared routes and a fallback.
    Router,
}

/// One named graph node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeMetadata {
    /// Unique node identifier.
    pub name: String,
    /// Node kind.
    pub kind: NodeKind,
}

/// One declared route from a router to a service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteMetadata {
    /// Selector label.
    pub label: String,
    /// Target service name.
    pub target: String,
}

/// Deterministic, read-only description of one graph version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphMetadata {
    /// Entry node name.
    pub entry: String,
    /// Nodes sorted by name.
    pub nodes: Vec<NodeMetadata>,
    /// Routes sorted by label.
    pub routes: Vec<RouteMetadata>,
    /// Explicit fallback target, if a router exists.
    pub fallback: Option<String>,
    /// Capacity of the outer version endpoint.
    pub capacity: Capacity,
    /// Capacity of each backend endpoint, if a router exists.
    pub backend_capacity: Option<Capacity>,
}

impl GraphMetadata {
    pub(crate) fn single_root(capacity: Capacity) -> Self {
        Self {
            entry: "root".into(),
            nodes: vec![NodeMetadata {
                name: "root".into(),
                kind: NodeKind::Service,
            }],
            routes: Vec::new(),
            fallback: None,
            capacity,
            backend_capacity: None,
        }
    }
}

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
}

struct Router<Request> {
    name: String,
    selector: Box<dyn Fn(&Request) -> Cow<'static, str> + Send + Sync>,
    routes: BTreeMap<String, String>,
    fallback: Option<String>,
}

/// Builds one router that selects among named, independently bounded Tower services.
///
/// Register ordinary `ServiceBuilder` compositions as services to use middleware.
/// Duplicate declarations are recorded and reported by [`Self::validate`].
pub struct GraphBuilder<Request, Response> {
    services: BTreeMap<String, BoxService<Request, Response, BoxError>>,
    router: Option<Router<Request>>,
    entry: Option<String>,
    capacity: Capacity,
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
    pub fn new() -> Self {
        let capacity = Capacity {
            max_in_flight: 32,
            buffer_capacity: 64,
        };
        Self {
            services: BTreeMap::new(),
            router: None,
            entry: None,
            capacity,
            backend_capacity: capacity,
            error: None,
        }
    }

    /// Register a named Tower service, including any already applied middleware.
    #[must_use]
    pub fn service<S>(mut self, name: impl Into<String>, service: S) -> Self
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
            self.services
                .insert(name, BoxService::new(service.map_err(Into::into)));
        }
        self
    }

    /// Register the single request-dependent router.
    ///
    /// The selector's output is matched against declared labels. Unknown labels
    /// use the explicit fallback. The selector runs synchronously on dispatch.
    /// Static labels are borrowed without allocation; dynamic labels may be owned.
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

    /// Set the outer version capacity.
    #[must_use]
    pub const fn capacity(mut self, capacity: Capacity) -> Self {
        self.capacity = capacity;
        self
    }

    /// Set the capacity used by each independently owned backend.
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
    /// Validate the complete declaration, then start workers and compile a version.
    ///
    /// Invalid declarations start no workers. Call inside an active Tokio runtime.
    /// The router reports readiness before selecting a backend; backend readiness
    /// and dispatch occur in the selected endpoint worker. A saturated branch can
    /// occupy outer execution slots and delay other branches.
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
        let entry = self.entry.ok_or(GraphBuildError::MissingEntry)?;
        let router = self
            .router
            .ok_or_else(|| GraphBuildError::UnknownEntry(entry.clone()))?;
        if entry != router.name {
            return Err(GraphBuildError::UnknownEntry(entry));
        }
        let fallback = router.fallback.ok_or(GraphBuildError::MissingFallback)?;
        if self.capacity.max_in_flight == 0 || self.backend_capacity.max_in_flight == 0 {
            return Err(ConfigError::ZeroConcurrency.into());
        }
        if self.capacity.buffer_capacity == 0 || self.backend_capacity.buffer_capacity == 0 {
            return Err(ConfigError::ZeroBuffer.into());
        }
        for target in router.routes.values().chain(std::iter::once(&fallback)) {
            if !self.services.contains_key(target) {
                return Err(GraphBuildError::UnknownTarget(target.clone()));
            }
        }
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
        let mut nodes: Vec<_> = std::iter::once(NodeMetadata {
            name: router.name,
            kind: NodeKind::Router,
        })
        .chain(self.services.keys().cloned().map(|name| NodeMetadata {
            name,
            kind: NodeKind::Service,
        }))
        .collect();
        nodes.sort_by(|a, b| a.name.cmp(&b.name));
        let metadata = GraphMetadata {
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
            fallback: Some(fallback.clone()),
            capacity: self.capacity,
            backend_capacity: Some(self.backend_capacity),
        };
        let endpoints: BTreeMap<_, _> = self
            .services
            .into_iter()
            .map(|(name, service)| {
                Endpoint::new(service, self.backend_capacity).map(|endpoint| (name, endpoint))
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
        Ok(GraphVersion {
            endpoint,
            endpoint_ids,
            metadata,
        })
    }
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
        }
    }
}
