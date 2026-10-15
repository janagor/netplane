//! Execute a bounded, transport-independent Tower service composition.
//!
//! [`Endpoint`] executes one bounded Tower service. [`Netplane`] publishes
//! validated, immutable versions for cloneable execution handles.

#![forbid(unsafe_code)]

use std::{future::Future, pin::Pin, sync::Arc};

use arc_swap::ArcSwap;
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

/// A cloneable handle to one bounded Tower service composition.
///
/// Construct this inside a Tokio runtime. A worker owns the service and polls
/// readiness before calling it. Dropping an execution future cancels its wait;
/// cancellation of an already dispatched call depends on the service.
pub struct Endpoint<Request: Send, Response> {
    buffer: Buffer<Request, Pin<Box<dyn Future<Output = Result<Response, BoxError>> + Send>>>,
}

impl<Request, Response> Endpoint<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    /// Wrap a Tower service with a concurrency limit and bounded request queue.
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
        Ok(Self {
            buffer: Buffer::new(boxed, capacity.buffer_capacity),
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
        self.buffer.clone().oneshot(request)
    }
}

impl<Request: Send + 'static, Response: 'static> Clone for Endpoint<Request, Response> {
    fn clone(&self) -> Self {
        Self {
            buffer: self.buffer.clone(),
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
pub struct Netplane<Request: Send, Response> {
    active: Arc<ArcSwap<GraphVersion<Request, Response>>>,
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
            active: Arc::new(ArcSwap::from_pointee(version)),
        }
    }

    /// Obtain a cloneable request execution handle.
    #[must_use]
    pub fn handle(&self) -> Handle<Request, Response> {
        Handle {
            active: Arc::clone(&self.active),
        }
    }

    /// Atomically publish a fully validated version.
    ///
    /// Already selected requests continue to use their previous version.
    pub fn publish(&mut self, version: GraphVersion<Request, Response>) {
        self.active.store(Arc::new(version));
    }
}

/// Cloneable request handle that selects a graph version at invocation time.
pub struct Handle<Request: Send, Response> {
    active: Arc<ArcSwap<GraphVersion<Request, Response>>>,
}

impl<Request: Send, Response> Clone for Handle<Request, Response> {
    fn clone(&self) -> Self {
        Self {
            active: Arc::clone(&self.active),
        }
    }
}

impl<Request, Response> Handle<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    /// Select the active version now and return its request future.
    ///
    /// The selected version stays alive until the future completes or is
    /// dropped. Dropping the future cancels its queue wait; cancellation after
    /// dispatch depends on the underlying service.
    ///
    /// # Errors
    /// Returns a service error or a buffer worker error.
    pub fn execute(
        &self,
        request: Request,
    ) -> impl Future<Output = Result<Response, BoxError>> + Send + 'static {
        let version = self.active.load_full();
        let execution = version.endpoint.execute(request);
        async move {
            let result = execution.await;
            drop(version);
            result
        }
    }
}
