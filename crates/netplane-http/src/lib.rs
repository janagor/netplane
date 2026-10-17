//! Bounded buffered HTTP ingress for a Netplane execution handle.
//!
//! A request selects its graph after its body is read and decoded. Dropping
//! the handler drops its execution future, although a disconnected socket need
//! not immediately drop an Axum handler. Dispatched service work may continue
//! after that drop. Slow response writers can retain encoded bodies after the
//! Netplane admission slot is released. Response size is checked after encoding,
//! so application encoders must avoid unbounded temporary allocations.

#![forbid(unsafe_code)]

use std::{fmt, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::LengthLimitError;
use netplane::{AdmissionError, Handle};
use tokio::sync::Semaphore;
use tower::BoxError;

/// Positive limits for buffered HTTP ingress and encoded response bodies.
///
/// Ingress limits apply after Axum has parsed request headers. They do not
/// bound connections that are still sending headers. The response size limit
/// rejects a body after encoding; it does not bound encoder allocations.
#[derive(Clone, Copy, Debug)]
pub struct HttpLimits {
    /// Maximum request body size in bytes.
    pub max_body_bytes: usize,
    /// Maximum requests concurrently reading or decoding a body.
    pub max_concurrent_body_reads: usize,
    /// Maximum time allowed to collect one request body.
    pub body_read_timeout: Duration,
    /// Maximum encoded response body size in bytes, checked after encoding.
    pub max_response_bytes: usize,
}

impl Default for HttpLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: 1024 * 1024,
            max_concurrent_body_reads: 64,
            body_read_timeout: Duration::from_secs(30),
            max_response_bytes: 1024 * 1024,
        }
    }
}

/// An invalid HTTP limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidLimits;

impl fmt::Display for InvalidLimits {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HTTP limits must be positive")
    }
}

impl std::error::Error for InvalidLimits {}

type Decoder<RequestType> = dyn Fn(Request<Bytes>) -> Result<RequestType, BoxError> + Send + Sync;
type Encoder<ResponseType> =
    dyn Fn(ResponseType) -> Result<Response<Bytes>, BoxError> + Send + Sync;
type ErrorMapper = dyn Fn(&BoxError) -> Response<Bytes> + Send + Sync;

/// Converts buffered HTTP messages and executes them through Netplane.
pub struct HttpAdapter<RequestType: Send, ResponseType> {
    handle: Handle<RequestType, ResponseType>,
    decode: Arc<Decoder<RequestType>>,
    encode: Arc<Encoder<ResponseType>>,
    map_error: Arc<ErrorMapper>,
    limits: HttpLimits,
    ingress: Arc<Semaphore>,
}

impl<RequestType: Send, ResponseType> Clone for HttpAdapter<RequestType, ResponseType> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            decode: Arc::clone(&self.decode),
            encode: Arc::clone(&self.encode),
            map_error: Arc::clone(&self.map_error),
            limits: self.limits,
            ingress: Arc::clone(&self.ingress),
        }
    }
}

impl<RequestType, ResponseType> HttpAdapter<RequestType, ResponseType>
where
    RequestType: Send + 'static,
    ResponseType: Send + 'static,
{
    /// Build an adapter with typed conversion and sanitized default errors.
    ///
    /// # Errors
    /// Returns [`InvalidLimits`] when any bound or the timeout is zero.
    pub fn new(
        handle: Handle<RequestType, ResponseType>,
        limits: HttpLimits,
        decode: impl Fn(Request<Bytes>) -> Result<RequestType, BoxError> + Send + Sync + 'static,
        encode: impl Fn(ResponseType) -> Result<Response<Bytes>, BoxError> + Send + Sync + 'static,
    ) -> Result<Self, InvalidLimits> {
        if limits.max_body_bytes == 0
            || limits.max_concurrent_body_reads == 0
            || limits.body_read_timeout.is_zero()
            || limits.max_response_bytes == 0
        {
            return Err(InvalidLimits);
        }
        Ok(Self {
            handle,
            decode: Arc::new(decode),
            encode: Arc::new(encode),
            map_error: Arc::new(|_| empty(StatusCode::INTERNAL_SERVER_ERROR)),
            limits,
            ingress: Arc::new(Semaphore::new(limits.max_concurrent_body_reads)),
        })
    }

    /// Set an application error mapping. Avoid exposing internal error text.
    #[must_use]
    pub fn with_error_mapper(
        mut self,
        mapper: impl Fn(&BoxError) -> Response<Bytes> + Send + Sync + 'static,
    ) -> Self {
        self.map_error = Arc::new(mapper);
        self
    }

    /// Convert this adapter to a catch-all Axum router for a persistent listener.
    pub fn router(self) -> Router {
        Router::new()
            .fallback(serve::<RequestType, ResponseType>)
            .with_state(self)
    }

    /// Execute a buffered HTTP request. Useful for deterministic ownership tests.
    pub async fn execute(&self, request: Request<Body>) -> Response<Body> {
        let Ok(permit) = Arc::clone(&self.ingress).try_acquire_owned() else {
            return body_response(empty(StatusCode::SERVICE_UNAVAILABLE));
        };
        if request
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .is_some_and(|length| length > self.limits.max_body_bytes)
        {
            return body_response(empty(StatusCode::PAYLOAD_TOO_LARGE));
        }
        let (parts, body) = request.into_parts();
        let collected = tokio::time::timeout(
            self.limits.body_read_timeout,
            to_bytes(body, self.limits.max_body_bytes),
        )
        .await;
        let bytes = match collected {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                tracing::debug!(target: "netplane_http", error = %error, "body rejected");
                let status = if std::error::Error::source(&error)
                    .is_some_and(<dyn std::error::Error>::is::<LengthLimitError>)
                {
                    StatusCode::PAYLOAD_TOO_LARGE
                } else {
                    StatusCode::BAD_REQUEST
                };
                return body_response(empty(status));
            }
            Err(_) => return body_response(empty(StatusCode::REQUEST_TIMEOUT)),
        };
        let decoded = match (self.decode)(Request::from_parts(parts, bytes)) {
            Ok(decoded) => decoded,
            Err(error) => {
                tracing::debug!(target: "netplane_http", error = %error, "request decode failed");
                return body_response(empty(StatusCode::BAD_REQUEST));
            }
        };
        drop(permit);
        let execution = match self.handle.try_execute(decoded) {
            Ok(execution) => execution,
            Err(AdmissionError::AtCapacity { .. } | AdmissionError::Closed) => {
                return body_response(empty(StatusCode::SERVICE_UNAVAILABLE));
            }
        };
        let response = match execution.await {
            Ok(response) => match (self.encode)(response) {
                Ok(response) => response,
                Err(error) => {
                    tracing::error!(target: "netplane_http", error = %error, "response encode failed");
                    empty(StatusCode::INTERNAL_SERVER_ERROR)
                }
            },
            Err(error) => {
                tracing::error!(target: "netplane_http", error = %error, "service failed");
                (self.map_error)(&error)
            }
        };
        if response.body().len() > self.limits.max_response_bytes {
            tracing::error!(target: "netplane_http", "response exceeds configured bound");
            return body_response(empty(StatusCode::INTERNAL_SERVER_ERROR));
        }
        body_response(response)
    }
}

impl HttpAdapter<Request<Bytes>, Response<Bytes>> {
    /// Build an identity adapter for HTTP graphs.
    ///
    /// # Errors
    /// Returns [`InvalidLimits`] when any bound or the timeout is zero.
    pub fn identity(
        handle: Handle<Request<Bytes>, Response<Bytes>>,
        limits: HttpLimits,
    ) -> Result<Self, InvalidLimits> {
        Self::new(handle, limits, Ok, Ok)
    }
}

async fn serve<RequestType, ResponseType>(
    State(adapter): State<HttpAdapter<RequestType, ResponseType>>,
    request: Request<Body>,
) -> Response<Body>
where
    RequestType: Send + 'static,
    ResponseType: Send + 'static,
{
    adapter.execute(request).await
}

fn empty(status: StatusCode) -> Response<Bytes> {
    let mut response = Response::new(Bytes::new());
    *response.status_mut() = status;
    response
}

fn body_response(response: Response<Bytes>) -> Response<Body> {
    response.map(Body::from)
}
