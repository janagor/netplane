//! Execute a bounded, transport-independent Tower service composition.
//!
//! [`Endpoint`] executes one bounded Tower service. [`Netplane`] publishes
//! validated, immutable versions for cloneable execution handles.
//! [`GraphBuilder`] validates a single request router over named Tower services.

#![forbid(unsafe_code)]

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwapOption;
mod endpoint;
mod graph;
pub use endpoint::Endpoint;
mod lifecycle;
mod observability;
pub use graph::{
    GraphBuildError, GraphBuilder, GraphMetadata, NodeKind, NodeMetadata, RouteMetadata,
};
use lifecycle::Admission;
pub use lifecycle::{AdmissionError, DrainError};
use observability::{RequestObservation, observe};
use thiserror::Error;
use tower::{BoxError, Service};

const INITIAL_GENERATION: u64 = 1;
const NO_PREVIOUS_VERSION: u64 = 0;
const INITIAL_REQUEST_ID: u64 = 1;
const INITIAL_SHUTDOWN_ATTEMPT_ID: u64 = 1;

/// Capacity settings for one endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capacity {
    /// Maximum requests executing in the underlying service at once.
    pub max_in_flight: usize,
    /// Maximum requests queued in the underlying Tower buffer channel.
    ///
    /// This excludes in-flight requests and the request held by the buffer
    /// worker while it waits for service readiness.
    pub buffer_capacity: usize,
}

/// Global limit for accepted, unfinished requests across all handles and versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeLimits {
    /// Maximum accepted requests, including futures that have not been polled.
    pub max_outstanding: usize,
}

/// An invalid endpoint capacity setting.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The concurrent request limit must be positive.
    #[error("max_in_flight must be positive")]
    ZeroConcurrency,
    /// The buffer capacity must be positive.
    #[error("buffer_capacity must be positive")]
    ZeroBuffer,
    /// The global outstanding request limit must be positive.
    #[error("max_outstanding must be positive")]
    ZeroOutstanding,
}

const fn config_error_kind(error: &ConfigError) -> &'static str {
    match error {
        ConfigError::ZeroConcurrency => "zero_concurrency",
        ConfigError::ZeroBuffer => "zero_buffer",
        ConfigError::ZeroOutstanding => "zero_outstanding",
    }
}

/// A graph version could not be published.
#[derive(Debug, Error)]
pub enum PublishError {
    /// Shutdown has already begun.
    #[error("cannot publish after shutdown has started")]
    ShuttingDown,
    /// No unused publication generation remains.
    #[error("publication generation exhausted")]
    GenerationExhausted,
}

/// Publication generation within one Netplane runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct VersionId(u64);

impl VersionId {
    /// Return the generation number, starting at one.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

struct PublishedVersion<Request: Send, Response> {
    id: VersionId,
    runtime_id: u64,
    dispatch: tracing::Dispatch,
    graph: GraphVersion<Request, Response>,
}

impl<Request: Send, Response> Drop for PublishedVersion<Request, Response> {
    fn drop(&mut self) {
        let runtime_id = self.runtime_id;
        let version_id = self.id.get();
        tracing::dispatcher::with_default(&self.dispatch, || {
            tracing::debug!(target: "netplane", event = "version.released", runtime_id, version_id);
        });
    }
}

/// A service root and capacity settings awaiting validation.
pub struct GraphDraft<S> {
    service: S,
    capacity: Capacity,
}

impl<S> GraphDraft<S> {
    /// Create a draft around a Tower service or composed service root.
    #[must_use]
    pub const fn new(service: S) -> Self {
        Self {
            service,
            capacity: Capacity {
                max_in_flight: 32,
                buffer_capacity: 64,
            },
        }
    }

    /// Set per-version concurrency and buffer capacity.
    #[must_use]
    pub const fn capacity(mut self, capacity: Capacity) -> Self {
        self.capacity = capacity;
        self
    }

    /// Validate capacities and prepare an immutable execution version.
    ///
    /// This creates the bounded endpoint and starts its Tower buffer worker,
    /// so call it inside an active Tokio runtime. The service root is present
    /// by construction; Tower's types validate its composition.
    ///
    /// # Errors
    /// Returns [`ConfigError`] if either capacity is zero.
    pub fn validate<Request, Response>(self) -> Result<GraphVersion<Request, Response>, ConfigError>
    where
        Request: Send + 'static,
        Response: Send + 'static,
        S: Service<Request, Response = Response> + Send + 'static,
        S::Error: Into<BoxError> + Send + Sync,
        S::Future: Send + 'static,
    {
        let endpoint = match Endpoint::new(self.service, self.capacity) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                tracing::warn!(target: "netplane", event = "graph.validation_failed", reason = config_error_kind(&error));
                return Err(error);
            }
        };
        tracing::debug!(target: "netplane", event = "graph.validated", node_count = 1_u64, route_count = 0_u64);
        Ok(GraphVersion {
            endpoint_ids: vec![endpoint.id()],
            endpoint,
            metadata: GraphMetadata::single_root(self.capacity),
        })
    }
}

/// One validated, immutable Tower composition and its capacity limits.
pub struct GraphVersion<Request: Send, Response> {
    endpoint: Endpoint<Request, Response>,
    endpoint_ids: Vec<u64>,
    metadata: GraphMetadata,
}

impl<Request: Send, Response> GraphVersion<Request, Response> {
    /// Inspect the immutable declaration used to build this version.
    #[must_use]
    pub const fn metadata(&self) -> &GraphMetadata {
        &self.metadata
    }
}

/// Single publisher for a sequence of immutable graph versions.
///
/// Publication atomically changes the version selected by later `execute`
/// calls. Existing execution futures keep their selected version alive.
/// Shutdown closes admission for all handles, waits for accepted futures, and
/// releases the active graph after draining succeeds.
pub struct Netplane<Request: Send, Response> {
    active: Arc<ArcSwapOption<PublishedVersion<Request, Response>>>,
    admission: Arc<Admission>,
    runtime_id: u64,
    generation: u64,
    next_request_id: Arc<AtomicU64>,
    next_shutdown_attempt_id: u64,
}

impl<Request, Response> Netplane<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    /// Start with a validated version and a positive global admission limit.
    ///
    /// # Errors
    /// Returns [`ConfigError::ZeroOutstanding`] if the limit is zero.
    pub fn new(
        version: GraphVersion<Request, Response>,
        limits: RuntimeLimits,
    ) -> Result<Self, ConfigError> {
        if limits.max_outstanding == 0 {
            return Err(ConfigError::ZeroOutstanding);
        }
        let runtime_id = observability::next_runtime_id();
        let version_id = VersionId(INITIAL_GENERATION);
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let published = PublishedVersion {
            id: version_id,
            runtime_id,
            dispatch,
            graph: version,
        };
        tracing::info!(target: "netplane", event = "version.published", runtime_id, old_version = NO_PREVIOUS_VERSION, new_version = version_id.get());
        for &endpoint_id in &published.graph.endpoint_ids {
            tracing::debug!(target: "netplane", event = "version.endpoint", runtime_id, version_id = version_id.get(), endpoint_id);
        }
        Ok(Self {
            active: Arc::new(ArcSwapOption::from(Some(Arc::new(published)))),
            admission: Admission::new(limits.max_outstanding),
            runtime_id,
            generation: version_id.get(),
            next_request_id: Arc::new(AtomicU64::new(INITIAL_REQUEST_ID)),
            next_shutdown_attempt_id: INITIAL_SHUTDOWN_ATTEMPT_ID,
        })
    }

    /// Obtain a cloneable request execution handle.
    #[must_use]
    pub fn handle(&self) -> Handle<Request, Response> {
        Handle {
            active: Arc::clone(&self.active),
            admission: Arc::clone(&self.admission),
            runtime_id: self.runtime_id,
            next_request_id: Arc::clone(&self.next_request_id),
        }
    }

    /// Return the active publication generation, or `None` after a completed shutdown.
    #[must_use]
    pub fn active_version_id(&self) -> Option<VersionId> {
        self.active.load_full().map(|version| version.id)
    }

    /// Atomically publish a fully validated version while running.
    ///
    /// Already selected requests continue to use their previous version.
    ///
    /// # Errors
    /// Returns [`PublishError::ShuttingDown`] once shutdown has begun.
    pub fn publish(
        &mut self,
        version: GraphVersion<Request, Response>,
    ) -> Result<(), PublishError> {
        if !self.admission.can_publish() {
            tracing::debug!(target: "netplane", event = "version.publish_rejected", runtime_id = self.runtime_id, reason = "shutting_down");
            return Err(PublishError::ShuttingDown);
        }
        let Some(next) = self.generation.checked_add(1) else {
            tracing::warn!(target: "netplane", event = "version.publish_rejected", runtime_id = self.runtime_id, reason = "generation_exhausted");
            return Err(PublishError::GenerationExhausted);
        };
        let old_version = self.generation;
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let next_version = Arc::new(PublishedVersion {
            id: VersionId(next),
            runtime_id: self.runtime_id,
            dispatch,
            graph: version,
        });
        let previous = self.active.swap(Some(Arc::clone(&next_version)));
        self.generation = next;
        tracing::info!(target: "netplane", event = "version.published", runtime_id = self.runtime_id, old_version, new_version = next);
        tracing::debug!(target: "netplane", event = "version.replaced", runtime_id = self.runtime_id, old_version, new_version = next);
        for &endpoint_id in &next_version.graph.endpoint_ids {
            tracing::debug!(target: "netplane", event = "version.endpoint", runtime_id = self.runtime_id, version_id = next, endpoint_id);
        }
        drop(previous);
        Ok(())
    }

    /// Close admission immediately and drain accepted request futures.
    ///
    /// A request accepted before this call may finish normally, including one
    /// whose future has not yet been polled. Dropping such a future also counts
    /// as drained. This does not forcibly stop work retained by an underlying
    /// service after its caller drops the future. A timed-out drain can be
    /// retried; admission remains closed. Once drained, the active graph is
    /// released even if handles remain. The deadline starts when this method
    /// is called, even if its returned future is polled later.
    ///
    /// # Errors
    /// Returns [`DrainError::TimedOut`] with the outstanding future count if
    /// the deadline expires before all accepted futures finish or are dropped.
    pub fn shutdown(
        &mut self,
        grace_period: Duration,
    ) -> impl Future<Output = Result<(), DrainError>> + Send + 'static + use<Request, Response>
    {
        let mut counts = self.admission.close();
        let admission = Arc::clone(&self.admission);
        let active = Arc::clone(&self.active);
        let deadline = tokio::time::Instant::now() + grace_period;
        let started = tokio::time::Instant::now();
        let runtime_id = self.runtime_id;
        let shutdown_attempt_id = self.next_shutdown_attempt_id;
        self.next_shutdown_attempt_id += 1;
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let grace_period_ms = u64::try_from(grace_period.as_millis()).unwrap_or(u64::MAX);
        let outstanding = admission.outstanding();
        tracing::info!(target: "netplane", event = "shutdown.started", runtime_id, shutdown_attempt_id, grace_period_ms, outstanding);
        async move {
            let drain = async {
                while *counts.borrow_and_update() != 0 {
                    if counts.changed().await.is_err() {
                        break;
                    }
                }
            };
            let _ = tokio::time::timeout_at(deadline, drain).await;
            let remaining = admission.outstanding();
            let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            if remaining == 0 {
                active.store(None);
                admission.mark_stopped();
                tracing::dispatcher::with_default(&dispatch, || {
                    tracing::info!(target: "netplane", event = "shutdown.completed", runtime_id, shutdown_attempt_id, duration_ms);
                });
                Ok(())
            } else {
                tracing::dispatcher::with_default(&dispatch, || {
                    tracing::warn!(target: "netplane", event = "shutdown.timed_out", runtime_id, shutdown_attempt_id, duration_ms, remaining);
                });
                Err(DrainError::TimedOut { remaining })
            }
        }
    }
}

/// Cloneable request handle that selects a graph version at invocation time.
pub struct Handle<Request: Send, Response> {
    active: Arc<ArcSwapOption<PublishedVersion<Request, Response>>>,
    admission: Arc<Admission>,
    runtime_id: u64,
    next_request_id: Arc<AtomicU64>,
}

impl<Request: Send, Response> Clone for Handle<Request, Response> {
    fn clone(&self) -> Self {
        Self {
            active: Arc::clone(&self.active),
            admission: Arc::clone(&self.admission),
            runtime_id: self.runtime_id,
            next_request_id: Arc::clone(&self.next_request_id),
        }
    }
}

impl<Request, Response> Handle<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    /// Admit a request and select its version synchronously.
    ///
    /// The accepted future holds one global slot and its selected version until
    /// it completes or is dropped. Dropping it cancels its queue wait; a
    /// dispatched service determines whether its work can be canceled.
    ///
    /// # Errors
    /// Returns [`AdmissionError::Closed`] after shutdown begins, or
    /// [`AdmissionError::AtCapacity`] when the global budget is full.
    pub fn try_execute(
        &self,
        request: Request,
    ) -> Result<
        impl Future<Output = Result<Response, BoxError>> + Send + 'static + use<Request, Response>,
        AdmissionError,
    > {
        let guard = match self.admission.try_admit() {
            Ok(guard) => guard,
            Err(AdmissionError::Closed) => {
                tracing::debug!(target: "netplane", event = "request.rejected", runtime_id = self.runtime_id, reason = "admission_closed");
                return Err(AdmissionError::Closed);
            }
            Err(AdmissionError::AtCapacity { limit }) => {
                tracing::warn!(target: "netplane", event = "request.rejected", runtime_id = self.runtime_id, reason = "capacity_exceeded", outstanding_limit = limit);
                return Err(AdmissionError::AtCapacity { limit });
            }
        };
        let version = self.active.load_full().ok_or(AdmissionError::Closed)?;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let observation = RequestObservation::new(self.runtime_id, version.id.get(), request_id);
        let execution = version.graph.endpoint.execute(request);
        Ok(observe(execution, observation, (version, guard)))
    }

    /// Admit the request now and return a future that executes it.
    ///
    /// Admission errors are boxed into the returned future's result. The
    /// accepted future releases its slot on completion or drop.
    ///
    /// # Errors
    /// Returns a boxed [`AdmissionError`] for overload or shutdown, or a
    /// service or buffer worker error after admission.
    pub fn execute(
        &self,
        request: Request,
    ) -> impl Future<Output = Result<Response, BoxError>> + Send + 'static + use<Request, Response>
    {
        let selected = self.try_execute(request);
        async move {
            match selected {
                Ok(execution) => execution.await,
                Err(error) => Err(Box::new(error) as BoxError),
            }
        }
    }
}
