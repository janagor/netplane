//! Execute a bounded, transport-independent Tower service composition.
//!
//! [`Endpoint`] executes one bounded Tower service. [`Netplane`] publishes
//! validated, immutable versions for cloneable execution handles.
//! [`GraphBuilder`] validates a single request router over named Tower services.

#![forbid(unsafe_code)]

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use arc_swap::ArcSwapOption;
mod graph;
mod lifecycle;
pub use graph::{
    GraphBuildError, GraphBuilder, GraphMetadata, NodeKind, NodeMetadata, RouteMetadata,
};
use lifecycle::Admission;
pub use lifecycle::{AdmissionError, DrainError};
use pin_project_lite::pin_project;
use thiserror::Error;
use tower::{
    BoxError, Service, ServiceExt, buffer::Buffer, limit::ConcurrencyLimit, util::BoxService,
};

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

/// A graph version could not be published.
#[derive(Debug, Error)]
pub enum PublishError {
    /// Shutdown has already begun.
    #[error("cannot publish after shutdown has started")]
    ShuttingDown,
}

type BoxedResponse<Response> = Pin<Box<dyn Future<Output = Result<Response, BoxError>> + Send>>;

/// A cloneable handle to one bounded Tower service composition.
///
/// Construct this inside a Tokio runtime. A worker owns the service and polls
/// readiness before calling it. Dropping an execution future cancels its wait;
/// cancellation of an already dispatched call depends on the service. The
/// worker is aborted after its last endpoint and execution future owner drops.
pub struct Endpoint<Request: Send, Response> {
    buffer: Buffer<Request, BoxedResponse<Response>>,
    worker: Arc<WorkerOwner>,
}

struct WorkerOwner(tokio::task::JoinHandle<()>);

impl Drop for WorkerOwner {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pin_project! {
    struct RetainedExecution<Request, Response>
    where
        Request: Send,
        Request: 'static,
        Response: Send,
        Response: 'static,
    {
        #[pin]
        execution: tower::util::Oneshot<Buffer<Request, BoxedResponse<Response>>, Request>,
        _worker: Arc<WorkerOwner>,
    }
}

impl<Request: Send + 'static, Response: Send + 'static> Future
    for RetainedExecution<Request, Response>
{
    type Output = Result<Response, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().execution.poll(cx)
    }
}

impl<Request, Response> Endpoint<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    /// Wrap a Tower service with a concurrency limit and bounded request queue.
    ///
    /// Netplane owns the Tower buffer worker and aborts it after the final
    /// endpoint and request future release it, including during pending
    /// service readiness.
    ///
    /// # Errors
    /// Returns an error if either capacity is zero.
    pub fn new<S>(service: S, capacity: Capacity) -> Result<Self, ConfigError>
    where
        S: Service<Request, Response = Response> + Send + 'static,
        S::Error: Into<BoxError> + Send + Sync,
        S::Future: Send + 'static,
    {
        if capacity.max_in_flight == 0 {
            return Err(ConfigError::ZeroConcurrency);
        }
        if capacity.buffer_capacity == 0 {
            return Err(ConfigError::ZeroBuffer);
        }
        let boxed = BoxService::new(ConcurrencyLimit::new(
            service.map_err(Into::into),
            capacity.max_in_flight,
        ));
        let (buffer, worker) = Buffer::pair(boxed, capacity.buffer_capacity);
        Ok(Self {
            buffer,
            worker: Arc::new(WorkerOwner(tokio::spawn(worker))),
        })
    }

    /// Submit a request to this endpoint.
    ///
    /// The returned future waits for queue capacity, then for the result. A
    /// buffer worker handles readiness and dispatch on its owned service.
    ///
    /// # Errors
    /// Returns a service error or a buffer worker error.
    pub fn execute(
        &self,
        request: Request,
    ) -> impl Future<Output = Result<Response, BoxError>> + Send + 'static + use<Request, Response>
    {
        let worker = Arc::clone(&self.worker);
        let execution = self.buffer.clone().oneshot(request);
        RetainedExecution {
            execution,
            _worker: worker,
        }
    }
}

impl<Request: Send + 'static, Response: 'static> Clone for Endpoint<Request, Response> {
    fn clone(&self) -> Self {
        Self {
            buffer: self.buffer.clone(),
            worker: Arc::clone(&self.worker),
        }
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
        Ok(GraphVersion {
            endpoint: Endpoint::new(self.service, self.capacity)?,
            metadata: GraphMetadata::single_root(self.capacity),
        })
    }
}

/// One validated, immutable Tower composition and its capacity limits.
pub struct GraphVersion<Request: Send, Response> {
    endpoint: Endpoint<Request, Response>,
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
    active: Arc<ArcSwapOption<GraphVersion<Request, Response>>>,
    admission: Arc<Admission>,
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
        Ok(Self {
            active: Arc::new(ArcSwapOption::from(Some(Arc::new(version)))),
            admission: Admission::new(limits.max_outstanding),
        })
    }

    /// Obtain a cloneable request execution handle.
    #[must_use]
    pub fn handle(&self) -> Handle<Request, Response> {
        Handle {
            active: Arc::clone(&self.active),
            admission: Arc::clone(&self.admission),
        }
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
            return Err(PublishError::ShuttingDown);
        }
        self.active.store(Some(Arc::new(version)));
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
            if remaining == 0 {
                active.store(None);
                admission.mark_stopped();
                Ok(())
            } else {
                Err(DrainError::TimedOut { remaining })
            }
        }
    }
}

/// Cloneable request handle that selects a graph version at invocation time.
pub struct Handle<Request: Send, Response> {
    active: Arc<ArcSwapOption<GraphVersion<Request, Response>>>,
    admission: Arc<Admission>,
}

impl<Request: Send, Response> Clone for Handle<Request, Response> {
    fn clone(&self) -> Self {
        Self {
            active: Arc::clone(&self.active),
            admission: Arc::clone(&self.admission),
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
        let guard = self.admission.try_admit()?;
        let version = self.active.load_full().ok_or(AdmissionError::Closed)?;
        let execution = version.endpoint.execute(request);
        Ok(async move {
            let result = execution.await;
            drop(version);
            drop(guard);
            result
        })
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
