//! Bounded Tower endpoint and worker ownership.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tower::{
    BoxError, Service, ServiceExt, buffer::Buffer, limit::ConcurrencyLimit, util::BoxService,
};

use crate::{Capacity, ConfigError};

const INITIAL_ENDPOINT_ID: u64 = 1;
static NEXT_ENDPOINT_ID: AtomicU64 = AtomicU64::new(INITIAL_ENDPOINT_ID);

type BoxedResponse<Response> = Pin<Box<dyn Future<Output = Result<Response, BoxError>> + Send>>;

/// A cloneable handle to one bounded Tower service composition.
///
/// Construct this inside a Tokio runtime. A worker owns the service and polls
/// readiness before calling it. Dropping an execution future cancels its wait;
/// cancellation of an already dispatched call depends on the service. The
/// worker is aborted after its last endpoint and execution future owner drops.
pub struct Endpoint<Request: Send, Response> {
    id: u64,
    buffer: Buffer<Request, BoxedResponse<Response>>,
    worker: Arc<WorkerOwner>,
}

struct WorkerOwner(tokio::task::AbortHandle);

impl Drop for WorkerOwner {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pin_project! {
    /// Future returned by [`Endpoint::execute`].
    pub(crate) struct EndpointExecution<Request, Response>
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
    for EndpointExecution<Request, Response>
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
        let endpoint_id = NEXT_ENDPOINT_ID.fetch_add(1, Ordering::Relaxed);
        let worker = tokio::spawn(worker);
        let abort = worker.abort_handle();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        tokio::spawn(async move {
            let reason = match worker.await {
                Ok(()) => "completed",
                Err(error) if error.is_cancelled() => "aborted",
                Err(_) => "panicked",
            };
            tracing::dispatcher::with_default(&dispatch, || {
                if reason == "panicked" {
                    tracing::error!(target: "netplane", event = "worker.terminated", endpoint_id, reason);
                } else {
                    tracing::debug!(target: "netplane", event = "worker.terminated", endpoint_id, reason);
                }
            });
        });
        Ok(Self {
            id: endpoint_id,
            buffer,
            worker: Arc::new(WorkerOwner(abort)),
        })
    }

    pub(crate) const fn id(&self) -> u64 {
        self.id
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
        self.execute_owned(request)
    }

    pub(crate) fn execute_owned(&self, request: Request) -> EndpointExecution<Request, Response> {
        let worker = Arc::clone(&self.worker);
        let execution = self.buffer.clone().oneshot(request);
        EndpointExecution {
            execution,
            _worker: worker,
        }
    }
}

impl<Request: Send + 'static, Response: 'static> Clone for Endpoint<Request, Response> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            buffer: self.buffer.clone(),
            worker: Arc::clone(&self.worker),
        }
    }
}
