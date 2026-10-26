//! Publication, cancellation, and shutdown at the HTTP to runtime boundary.

#[path = "common/http_response.rs"]
mod http_response;

use axum::body::Body;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use netplane::{GraphDraft, Netplane, RuntimeLimits};
use netplane_http::{HttpAdapter, HttpLimits};
use std::{
    convert::Infallible,
    future::Future,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tower::{BoxError, service_fn};

const GUARD: Duration = Duration::from_secs(15);
type HttpRequest = Request<Bytes>;
type HttpResponse = Response<Bytes>;

async fn bounded<T>(future: impl Future<Output = T>) -> Result<T, BoxError> {
    Ok(tokio::time::timeout(GUARD, future).await?)
}

fn graph(
    label: &'static str,
    entered: Option<Arc<Notify>>,
    release: Option<Arc<Notify>>,
) -> Result<netplane::GraphVersion<HttpRequest, HttpResponse>, BoxError> {
    Ok(GraphDraft::new(service_fn(move |_: HttpRequest| {
        let entered = entered.clone();
        let release = release.clone();
        async move {
            if let Some(entered) = entered {
                entered.notify_one();
            }
            if let Some(release) = release {
                release.notified().await;
            }
            let mut response = Response::new(Bytes::from_static(label.as_bytes()));
            response
                .headers_mut()
                .insert("x-version", http::HeaderValue::from_static(label));
            Ok::<_, Infallible>(response)
        }
    }))
    .validate()?)
}

fn pending_body(polled: Arc<Notify>) -> Body {
    Body::from_stream(futures_util::stream::poll_fn(move |_| {
        polled.notify_one();
        Poll::<Option<Result<Bytes, io::Error>>>::Pending
    }))
}

#[tokio::test]
async fn publication_during_upload_selects_new_version() -> Result<(), BoxError> {
    let mut runtime = Netplane::new(
        graph("v1", None, None)?,
        RuntimeLimits { max_outstanding: 1 },
    )?;
    let adapter = HttpAdapter::identity(runtime.handle(), HttpLimits::default())?;
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(2);
    let polled = Arc::new(Notify::new());
    let observed = Arc::clone(&polled);
    let stream = futures_util::stream::unfold((rx, observed), |(mut rx, observed)| async move {
        observed.notify_one();
        rx.recv()
            .await
            .map(|bytes| (Ok::<_, io::Error>(bytes), (rx, observed)))
    });
    let held_adapter = adapter.clone();
    let upload = tokio::spawn(async move {
        held_adapter
            .execute(Request::new(Body::from_stream(stream)))
            .await
    });
    bounded(polled.notified()).await?;
    runtime.publish(graph("v2", None, None)?)?;
    tx.send(Bytes::from_static(b"done")).await?;
    drop(tx);
    let response = bounded(upload).await??;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-version"], "v2");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    runtime.publish(graph(
        "v2",
        Some(Arc::clone(&entered)),
        Some(Arc::clone(&release)),
    )?)?;
    let old_adapter = adapter.clone();
    let old = tokio::spawn(async move { old_adapter.execute(Request::new(Body::empty())).await });
    bounded(entered.notified()).await?;
    runtime.publish(graph("v3", None, None)?)?;
    release.notify_one();
    assert_eq!(bounded(old).await??.headers()["x-version"], "v2");
    assert_eq!(
        adapter.execute(Request::new(Body::empty())).await.headers()["x-version"],
        "v3"
    );
    Ok(())
}

#[tokio::test]
async fn cancel_during_body_collection_releases_ingress_only() -> Result<(), BoxError> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let version = GraphDraft::new(service_fn(move |_: HttpRequest| {
        counted.fetch_add(1, Ordering::SeqCst);
        async { Ok::<_, Infallible>(Response::new(Bytes::new())) }
    }))
    .validate()?;
    let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let adapter = HttpAdapter::identity(
        runtime.handle(),
        HttpLimits {
            max_concurrent_body_reads: 1,
            ..HttpLimits::default()
        },
    )?;
    let polled = Arc::new(Notify::new());
    let held_adapter = adapter.clone();
    let body_polled = Arc::clone(&polled);
    let held = tokio::spawn(async move {
        held_adapter
            .execute(Request::new(pending_body(body_polled)))
            .await
    });
    bounded(polled.notified()).await?;
    assert_eq!(
        adapter.execute(Request::new(Body::empty())).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    held.abort();
    let _ = bounded(held).await?;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        adapter.execute(Request::new(Body::empty())).await.status(),
        StatusCode::OK
    );
    Ok(())
}

#[tokio::test]
async fn cancel_after_admission_releases_runtime_budget() -> Result<(), BoxError> {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let runtime = Netplane::new(
        graph("v1", Some(Arc::clone(&entered)), Some(Arc::clone(&release)))?,
        RuntimeLimits { max_outstanding: 1 },
    )?;
    let adapter = HttpAdapter::identity(runtime.handle(), HttpLimits::default())?;
    let held_adapter = adapter.clone();
    let held = tokio::spawn(async move { held_adapter.execute(Request::new(Body::empty())).await });
    bounded(entered.notified()).await?;
    assert_eq!(
        adapter.execute(Request::new(Body::empty())).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    held.abort();
    let _ = bounded(held).await?;
    let fresh = runtime.handle().try_execute(Request::new(Bytes::new()))?;
    drop(fresh);
    release.notify_waiters();
    Ok(())
}

#[tokio::test]
async fn body_read_capacity_and_runtime_admission_are_independent() -> Result<(), BoxError> {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let runtime = Netplane::new(
        graph("v1", Some(Arc::clone(&entered)), Some(Arc::clone(&release)))?,
        RuntimeLimits { max_outstanding: 1 },
    )?;
    let adapter = HttpAdapter::identity(
        runtime.handle(),
        HttpLimits {
            max_concurrent_body_reads: 1,
            ..HttpLimits::default()
        },
    )?;
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(1);
    let polled = Arc::new(Notify::new());
    let signal = Arc::clone(&polled);
    let stream = futures_util::stream::unfold((rx, signal), |(mut rx, signal)| async move {
        signal.notify_one();
        rx.recv()
            .await
            .map(|bytes| (Ok::<_, io::Error>(bytes), (rx, signal)))
    });
    let held_adapter = adapter.clone();
    let upload = tokio::spawn(async move {
        held_adapter
            .execute(Request::new(Body::from_stream(stream)))
            .await
    });
    bounded(polled.notified()).await?;
    let direct = runtime.handle().try_execute(Request::new(Bytes::new()))?;
    let direct = tokio::spawn(direct);
    bounded(entered.notified()).await?;
    tx.send(Bytes::from_static(b"done")).await?;
    drop(tx);
    assert_eq!(
        bounded(upload).await??.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    release.notify_one();
    let _ = bounded(direct).await??;
    Ok(())
}

async fn listener(
    adapter: HttpAdapter<HttpRequest, HttpResponse>,
) -> Result<
    (
        u16,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), io::Error>>,
    ),
    BoxError,
> {
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let port = socket.local_addr()?.port();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(socket, adapter.router())
            .with_graceful_shutdown(async {
                let _ = stop_rx.await;
            })
            .await
    });
    Ok((port, stop_tx, server))
}

#[tokio::test]
async fn shutdown_with_pending_body_reader_does_not_wait_for_netplane_admission()
-> Result<(), BoxError> {
    let mut runtime = Netplane::new(
        graph("v1", None, None)?,
        RuntimeLimits { max_outstanding: 1 },
    )?;
    let adapter = HttpAdapter::identity(
        runtime.handle(),
        HttpLimits {
            max_concurrent_body_reads: 1,
            ..HttpLimits::default()
        },
    )?;
    let probe = adapter.clone();
    let (port, stop, server) = listener(adapter).await?;
    let mut client = bounded(TcpStream::connect(("127.0.0.1", port))).await??;
    bounded(client.write_all(
        b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nConnection: close\r\n\r\nab",
    ))
    .await??;
    // Header parsing happens before the adapter, so the socket write cannot
    // signal permit ownership. Before shutdown the runtime is open and these
    // empty probe requests can return 503 only while the upload holds the
    // adapter's sole ingress permit.
    bounded(async {
        loop {
            let response = probe.execute(Request::new(Body::empty())).await;
            if response.status() == StatusCode::SERVICE_UNAVAILABLE {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    bounded(runtime.shutdown(Duration::from_secs(1))).await??;
    bounded(client.write_all(b"cd")).await??;
    let mut wire = Vec::new();
    bounded(client.read_to_end(&mut wire)).await??;
    assert_eq!(
        http_response::parse(&wire)?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let _ = stop.send(());
    bounded(server).await???;
    Ok(())
}

#[tokio::test]
async fn shutdown_with_incomplete_headers_is_transport_owned() -> Result<(), BoxError> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let version = GraphDraft::new(service_fn(move |_: HttpRequest| {
        counted.fetch_add(1, Ordering::SeqCst);
        async { Ok::<_, Infallible>(Response::new(Bytes::new())) }
    }))
    .validate()?;
    let mut runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let adapter = HttpAdapter::identity(runtime.handle(), HttpLimits::default())?;
    let (port, stop, server) = listener(adapter).await?;
    let mut client = bounded(TcpStream::connect(("127.0.0.1", port))).await??;
    bounded(client.write_all(b"GET / HTTP/1.1\r\nHost: local")).await??;
    bounded(runtime.shutdown(Duration::from_secs(1))).await??;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(client);
    let _ = stop.send(());
    bounded(server).await???;
    Ok(())
}
