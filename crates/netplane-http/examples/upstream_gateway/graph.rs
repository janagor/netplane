use super::upstream::HttpUpstream;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use netplane::{Capacity, GraphBuilder, GraphVersion};
use tower::{BoxError, service_fn};

pub fn selected(request: &Request<Bytes>) -> &'static str {
    let Some(key) = request.headers().get("x-request-key") else {
        return "invalid";
    };
    let key = key.as_bytes();
    if key.is_empty()
        || key.len() > 128
        || !key
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return "invalid";
    }
    let hash = key.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    if hash % 100 < 20 { "canary" } else { "stable" }
}

pub fn build(
    stable: HttpUpstream,
    canary: HttpUpstream,
) -> Result<GraphVersion<Request<Bytes>, Response<Bytes>>, BoxError> {
    Ok(GraphBuilder::new()
        .service(
            "stable",
            service_fn(move |request| {
                let upstream = stable.clone();
                async move { upstream.execute(request).await }
            }),
        )
        .service(
            "canary",
            service_fn(move |request| {
                let upstream = canary.clone();
                async move { upstream.execute(request).await }
            }),
        )
        .service(
            "fallback",
            service_fn(|_| async {
                let mut response = Response::new(Bytes::new());
                *response.status_mut() = StatusCode::BAD_REQUEST;
                Ok::<_, super::upstream::UpstreamError>(response)
            }),
        )
        .router("policy", selected)
        .route("policy", "stable", "stable")
        .route("policy", "canary", "canary")
        .fallback("policy", "fallback")
        .entry("policy")
        .without_outer_capacity()
        .backend_capacity(Capacity {
            max_in_flight: 8,
            buffer_capacity: 16,
        })
        .validate()?)
}
