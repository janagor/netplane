//! Request buffering and ingress permit boundaries.

use axum::body::Body;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use netplane::{GraphDraft, Netplane, RuntimeLimits};
use netplane_http::{HttpAdapter, HttpLimits};
use std::{
    convert::Infallible,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};
use tokio::sync::Notify;
use tower::{BoxError, service_fn};

type HttpRequest = Request<Bytes>;

fn fixture(
    limits: HttpLimits,
) -> Result<
    (
        Netplane<Bytes, Bytes>,
        HttpAdapter<Bytes, Bytes>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ),
    BoxError,
> {
    let calls = Arc::new(AtomicUsize::new(0));
    let decodes = Arc::new(AtomicUsize::new(0));
    let backend_calls = Arc::clone(&calls);
    let version = GraphDraft::new(service_fn(move |body: Bytes| {
        backend_calls.fetch_add(1, Ordering::SeqCst);
        async move { Ok::<_, Infallible>(body) }
    }))
    .validate()?;
    let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let decoder_calls = Arc::clone(&decodes);
    let adapter = HttpAdapter::new(
        runtime.handle(),
        limits,
        move |request: HttpRequest| {
            decoder_calls.fetch_add(1, Ordering::SeqCst);
            Ok(request.into_body())
        },
        |body| Ok(Response::new(body)),
    )?;
    Ok((runtime, adapter, calls, decodes))
}

const fn limits() -> HttpLimits {
    HttpLimits {
        max_body_bytes: 4,
        max_concurrent_body_reads: 1,
        body_read_timeout: Duration::from_secs(1),
        max_response_bytes: 4,
    }
}

#[tokio::test]
async fn body_at_exact_limit_is_accepted() -> Result<(), BoxError> {
    let (_, adapter, calls, decodes) = fixture(limits())?;
    let response = adapter.execute(Request::new(Body::from("four"))).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(decodes.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn body_over_limit_without_content_length_is_rejected() -> Result<(), BoxError> {
    let (_, adapter, calls, decodes) = fixture(limits())?;
    let body = Body::from_stream(futures_util::stream::iter([Ok::<_, io::Error>(
        Bytes::from_static(b"five!"),
    )]));
    let response = adapter.execute(Request::new(body)).await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(decodes.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn content_length_over_limit_is_rejected_early() -> Result<(), BoxError> {
    let (_, adapter, calls, decodes) = fixture(limits())?;
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&polls);
    let body = Body::from_stream(futures_util::stream::poll_fn(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
        Poll::<Option<Result<Bytes, io::Error>>>::Pending
    }));
    let request = Request::builder()
        .header(http::header::CONTENT_LENGTH, "5")
        .body(body)?;
    assert_eq!(
        adapter.execute(request).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(decodes.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn body_stream_error_is_bad_request() -> Result<(), BoxError> {
    let (runtime, adapter, calls, decodes) = fixture(limits())?;
    let body = Body::from_stream(futures_util::stream::iter([Err::<Bytes, _>(
        io::Error::other("broken body"),
    )]));
    assert_eq!(
        adapter.execute(Request::new(body)).await.status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(decodes.load(Ordering::SeqCst), 0);
    let admitted = runtime.handle().try_execute(Bytes::new())?;
    drop(admitted);
    assert_eq!(
        adapter.execute(Request::new(Body::empty())).await.status(),
        StatusCode::OK
    );
    Ok(())
}

#[tokio::test]
async fn ingress_capacity_is_shared_across_adapter_clones() -> Result<(), BoxError> {
    let (runtime, adapter, calls, _) = fixture(HttpLimits {
        body_read_timeout: Duration::from_secs(30),
        ..limits()
    })?;
    let polled = Arc::new(Notify::new());
    let signal = Arc::clone(&polled);
    let body = Body::from_stream(futures_util::stream::poll_fn(move |_| {
        signal.notify_one();
        Poll::<Option<Result<Bytes, io::Error>>>::Pending
    }));
    let clone = adapter.clone();
    let held = tokio::spawn(async move { clone.execute(Request::new(body)).await });
    tokio::time::timeout(Duration::from_secs(5), polled.notified()).await?;
    assert_eq!(
        adapter.execute(Request::new(Body::empty())).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let direct = runtime.handle().try_execute(Bytes::new())?;
    drop(direct);
    held.abort();
    let _ = held.await;
    assert_eq!(
        adapter.execute(Request::new(Body::empty())).await.status(),
        StatusCode::OK
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn ingress_permit_is_released_after_timeout() -> Result<(), BoxError> {
    let (_, adapter, calls, _) = fixture(limits())?;
    let body = Body::from_stream(futures_util::stream::pending::<Result<Bytes, io::Error>>());
    assert_eq!(
        adapter.execute(Request::new(body)).await.status(),
        StatusCode::REQUEST_TIMEOUT
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        adapter.execute(Request::new(Body::empty())).await.status(),
        StatusCode::OK
    );
    Ok(())
}
