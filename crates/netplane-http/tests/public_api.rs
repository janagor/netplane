//! External-consumer smoke test for the optional HTTP adapter surface.

use std::{convert::Infallible, error::Error, time::Duration};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use netplane::{Capacity, GraphDraft, Netplane, RuntimeLimits};
use netplane_http::{HttpAdapter, HttpLimits};
use tower::service_fn;

#[tokio::test]
async fn external_consumer_can_adapt_execute_and_observe_overload()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let version = GraphDraft::new(service_fn(|request: Request<Bytes>| async move {
        Ok::<_, Infallible>(Response::new(request.into_body()))
    }))
    .capacity(Capacity {
        max_in_flight: 2,
        buffer_capacity: 2,
    })
    .validate()?;
    let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    let adapter = HttpAdapter::new(
        handle.clone(),
        HttpLimits {
            max_body_bytes: 64,
            max_concurrent_body_reads: 2,
            body_read_timeout: Duration::from_secs(1),
            max_response_bytes: 64,
        },
        Ok,
        Ok,
    )?;

    let held = handle.try_execute(Request::new(Bytes::from_static(b"held")))?;
    let overloaded = adapter
        .execute(Request::new(axum::body::Body::from("busy")))
        .await;
    assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(held);

    let response = adapter
        .execute(Request::new(axum::body::Body::from("ok")))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = http_body_util::BodyExt::collect(response.into_body())
        .await?
        .to_bytes();
    assert_eq!(body.as_ref(), b"ok");

    let _ = adapter.router();
    Ok(())
}
