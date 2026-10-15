//! Execute a bounded, transport-independent Tower service composition.
//!
//! [`Endpoint`] is the first, single-version execution slice. Graph publication
//! and shutdown admission control are not yet implemented.

#![forbid(unsafe_code)]

use std::{future::Future, pin::Pin};

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
    ) -> impl Future<Output = Result<Response, BoxError>> + Send + 'static {
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
