use bytes::{Bytes, BytesMut};
use http::{HeaderMap, Request, Response, StatusCode};
use std::time::Duration;
use tower::BoxError;

#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error("upstream transport failed")]
    Transport(#[source] reqwest::Error),
    #[error("upstream timed out")]
    Timeout,
    #[error("upstream response exceeds limit")]
    ResponseTooLarge,
    #[error("unsupported request target")]
    InvalidRequest,
}

impl From<reqwest::Error> for UpstreamError {
    fn from(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout
        } else {
            Self::Transport(error)
        }
    }
}

pub fn client(timeout: Duration) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .http1_only()
        .pool_max_idle_per_host(8)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()
}

#[derive(Clone)]
pub struct HttpUpstream {
    client: reqwest::Client,
    origin: url::Url,
    max_response_bytes: usize,
}

impl HttpUpstream {
    pub fn new(
        client: reqwest::Client,
        origin: &str,
        max_response_bytes: usize,
    ) -> Result<Self, BoxError> {
        let origin = url::Url::parse(origin)?;
        if origin.scheme() != "http"
            || origin.host_str().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
            || max_response_bytes == 0
        {
            return Err("expected an HTTP origin and a positive response limit".into());
        }
        Ok(Self {
            client,
            origin,
            max_response_bytes,
        })
    }

    pub async fn execute(&self, request: Request<Bytes>) -> Result<Response<Bytes>, UpstreamError> {
        if request.uri().scheme().is_some()
            || request.uri().authority().is_some()
            || !request.uri().path().starts_with('/')
        {
            return Err(UpstreamError::InvalidRequest);
        }
        // Set components instead of joining: even //host/path cannot change the origin.
        let mut url = self.origin.clone();
        url.set_path(request.uri().path());
        url.set_query(request.uri().query());
        let (mut parts, body) = request.into_parts();
        strip_headers(&mut parts.headers);
        parts.headers.remove(http::header::HOST);
        parts.headers.remove(http::header::CONTENT_LENGTH);
        let mut upstream = self
            .client
            .request(parts.method, url)
            .headers(parts.headers)
            .body(body)
            .send()
            .await?;
        if upstream
            .content_length()
            .is_some_and(|size| size > self.max_response_bytes as u64)
        {
            return Err(UpstreamError::ResponseTooLarge);
        }
        let mut response = Response::new(Bytes::new());
        *response.status_mut() = upstream.status();
        *response.headers_mut() = upstream.headers().clone();
        strip_headers(response.headers_mut());
        let mut body = BytesMut::new();
        while let Some(chunk) = upstream.chunk().await? {
            if chunk.len() > self.max_response_bytes.saturating_sub(body.len()) {
                return Err(UpstreamError::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        *response.body_mut() = body.freeze();
        Ok(response)
    }
}

pub fn strip_headers(headers: &mut HeaderMap) {
    let nominated: Vec<_> = headers
        .get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| http::HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    for name in nominated {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "proxy-connection",
    ] {
        headers.remove(name);
    }
}

pub fn map_error(error: &BoxError) -> Response<Bytes> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error.as_ref());
    let mut status = StatusCode::BAD_GATEWAY;
    while let Some(error) = current {
        if let Some(upstream) = error.downcast_ref::<UpstreamError>() {
            status = match upstream {
                UpstreamError::Timeout => StatusCode::GATEWAY_TIMEOUT,
                UpstreamError::InvalidRequest => StatusCode::BAD_REQUEST,
                UpstreamError::Transport(_) | UpstreamError::ResponseTooLarge => {
                    StatusCode::BAD_GATEWAY
                }
            };
            break;
        }
        current = error.source();
    }
    let mut response = Response::new(Bytes::new());
    *response.status_mut() = status;
    response
}
