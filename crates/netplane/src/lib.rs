//! Execute a bounded, transport-independent Tower service composition.
//!
//! [`Endpoint`] executes one bounded Tower service. [`Netplane`] publishes
//! validated, immutable versions for cloneable execution handles.

#![forbid(unsafe_code)]

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use arc_swap::ArcSwapOption;
mod lifecycle;
use lifecycle::Admission;
pub use lifecycle::{AdmissionError, DrainError};
use pin_project_lite::pin_project;
use thiserror::Error;
use tower::{
    BoxError, Service, ServiceExt, buffer::Buffer, limit::ConcurrencyLimit, util::BoxService,
};

/// Capacity settings for one endpoint.
#[derive(Clone, Copy, Debug)]
pub struct Capacity {
    /// Maximum requests executing in the underlying service at once.
    pub max_in_flight: usize,
    /// Maximum requests queued in the underlying Tower buffer channel.
    ///
    /// This excludes in-flight requests and the request held by the buffer
    /// worker while it waits for service readiness.
    pub buffer_capacity: usize,
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
        })
    }
}

/// One validated, immutable Tower composition and its capacity limits.
pub struct GraphVersion<Request: Send, Response> {
    endpoint: Endpoint<Request, Response>,
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
    /// Start with a validated version.
    #[must_use]
    pub fn new(version: GraphVersion<Request, Response>) -> Self {
        Self {
            active: Arc::new(ArcSwapOption::from(Some(Arc::new(version)))),
            admission: Admission::new(),
        }
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
    /// Admit the request and select the active version now.
    ///
    /// The selected version stays alive until the future completes or is
    /// dropped. Dropping the future cancels its queue wait; cancellation after
    /// dispatch depends on the underlying service.
    ///
    /// # Errors
    /// Returns [`AdmissionError`] if shutdown has closed admission or the
    /// outstanding counter is exhausted. Otherwise returns a service or
    /// buffer worker error.
    pub fn execute(
        &self,
        request: Request,
    ) -> impl Future<Output = Result<Response, BoxError>> + Send + 'static + use<Request, Response>
    {
        let selected = self.admission.try_admit().and_then(|guard| {
            let version = self.active.load_full().ok_or(AdmissionError::Closed)?;
            let execution = version.endpoint.execute(request);
            Ok((guard, version, execution))
        });
        async move {
            match selected {
                Ok((guard, version, execution)) => {
                    let result = execution.await;
                    drop(version);
                    drop(guard);
                    result
                }
                Err(error) => Err(Box::new(error) as BoxError),
            }
        }
    }
}
