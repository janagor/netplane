//! Encoded body bounds and ownership after runtime execution.

#[path = "common/http_response.rs"]
mod http_response;

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
    time::Duration,
};
use tower::{BoxError, service_fn};

type HttpRequest = Request<Bytes>;

fn limits() -> HttpLimits {
    HttpLimits {
        max_response_bytes: 4,
        ..HttpLimits::default()
    }
}

#[tokio::test]
async fn encoded_response_at_limit_is_accepted() -> Result<(), BoxError> {
    let version = GraphDraft::new(service_fn(|_: HttpRequest| async {
        Ok::<_, Infallible>(Bytes::from_static(b"four"))
    }))
    .validate()?;
    let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let adapter = HttpAdapter::new(
        runtime.handle(),
        limits(),
        |request: HttpRequest| Ok(request),
        |bytes| Ok(Response::new(bytes)),
    )?;
    let response = adapter.execute(Request::new(Body::empty())).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 4).await?,
        Bytes::from_static(b"four")
    );
    Ok(())
}

#[tokio::test]
async fn custom_encoder_oversize_is_rejected() -> Result<(), BoxError> {
    let version = GraphDraft::new(service_fn(|_: HttpRequest| async {
        Ok::<_, Infallible>(())
    }))
    .validate()?;
    let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let encode_calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&encode_calls);
    let adapter = HttpAdapter::new(
        runtime.handle(),
        limits(),
        |request: HttpRequest| Ok(request),
        move |()| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(Bytes::from_static(b"oversize")))
        },
    )?;
    assert_eq!(
        adapter.execute(Request::new(Body::empty())).await.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(encode_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn error_mapper_response_respects_size_limit() -> Result<(), BoxError> {
    let version = GraphDraft::new(service_fn(|_: HttpRequest| async {
        Err::<(), _>(io::Error::other("service failed"))
    }))
    .validate()?;
    let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let mapper_calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&mapper_calls);
    let adapter = HttpAdapter::new(
        runtime.handle(),
        limits(),
        |request: HttpRequest| Ok(request),
        |()| Ok(Response::new(Bytes::new())),
    )?
    .with_error_mapper(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
        Response::new(Bytes::from_static(b"oversize"))
    });
    assert_eq!(
        adapter.execute(Request::new(Body::empty())).await.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(mapper_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

struct Tracked {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn encoded_body_outlives_execution_admission() -> Result<(), BoxError> {
    let drops = Arc::new(AtomicUsize::new(0));
    let owned_drops = Arc::clone(&drops);
    let version = GraphDraft::new(service_fn(move |_: HttpRequest| {
        let drops = Arc::clone(&owned_drops);
        async move {
            Ok::<_, Infallible>(Bytes::from_owner(Tracked {
                bytes: b"held".to_vec(),
                drops,
            }))
        }
    }))
    .validate()?;
    let mut runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let adapter = HttpAdapter::new(
        runtime.handle(),
        limits(),
        |request: HttpRequest| Ok(request),
        |body| Ok(Response::new(body)),
    )?;
    let response = adapter.execute(Request::new(Body::empty())).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let second = runtime.handle().try_execute(Request::new(Bytes::new()))?;
    drop(second);
    runtime.shutdown(Duration::from_secs(1)).await?;
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(response);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn held_response_body_does_not_block_new_admission() -> Result<(), BoxError> {
    let version = GraphDraft::new(service_fn(|_: HttpRequest| async {
        Ok::<_, Infallible>(Bytes::from_static(b"held"))
    }))
    .validate()?;
    let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let adapter = HttpAdapter::new(
        runtime.handle(),
        limits(),
        |request: HttpRequest| Ok(request),
        |body| Ok(Response::new(body)),
    )?;
    let held = adapter.execute(Request::new(Body::empty())).await;
    assert_eq!(held.status(), StatusCode::OK);
    assert_eq!(
        adapter.execute(Request::new(Body::empty())).await.status(),
        StatusCode::OK
    );
    drop(held);
    Ok(())
}

#[tokio::test]
async fn slow_response_reader_does_not_hold_runtime_admission() -> Result<(), BoxError> {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    const GUARD: Duration = Duration::from_secs(15);
    let large = Bytes::from(vec![b'x'; 4 * 1024 * 1024]);
    let version = GraphDraft::new(service_fn(move |request: HttpRequest| {
        let large = large.clone();
        async move {
            let body = if request.uri().path() == "/large" {
                large
            } else {
                Bytes::from_static(b"ok")
            };
            Ok::<_, Infallible>(Response::new(body))
        }
    }))
    .validate()?;
    let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let adapter = HttpAdapter::identity(
        runtime.handle(),
        HttpLimits {
            max_response_bytes: 4 * 1024 * 1024,
            ..HttpLimits::default()
        },
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, adapter.router())
            .with_graceful_shutdown(async {
                let _ = stop_rx.await;
            })
            .await
    });
    let mut slow = tokio::time::timeout(GUARD, TcpStream::connect(("127.0.0.1", port))).await??;
    tokio::time::timeout(
        GUARD,
        slow.write_all(b"GET /large HTTP/1.1\r\nHost: localhost\r\n\r\n"),
    )
    .await??;
    let mut prefix = [0_u8; 128];
    let count = tokio::time::timeout(GUARD, slow.read(&mut prefix)).await??;
    assert!(count > 0);
    let mut small = tokio::time::timeout(GUARD, TcpStream::connect(("127.0.0.1", port))).await??;
    tokio::time::timeout(
        GUARD,
        small.write_all(b"GET /small HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
    )
    .await??;
    let mut wire = Vec::new();
    tokio::time::timeout(GUARD, small.read_to_end(&mut wire)).await??;
    let response = http_response::parse(&wire)?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.body(), &Bytes::from_static(b"ok"));
    drop(slow);
    let _ = stop_tx.send(());
    tokio::time::timeout(GUARD, server).await???;
    Ok(())
}
