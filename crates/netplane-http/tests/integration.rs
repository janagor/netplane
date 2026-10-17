//! Real socket and adapter behavior tests.

#[path = "common/http_response.rs"]
mod http_response;

use std::{convert::Infallible, future::Future, io, sync::Arc, task::Poll, time::Duration};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use netplane::{Capacity, GraphBuilder, Netplane, RuntimeLimits};
use netplane_http::{HttpAdapter, HttpLimits, InvalidLimits};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
};
use tower::{BoxError, ServiceBuilder, service_fn};

type HttpRequest = Request<Bytes>;
type HttpResponse = Response<Bytes>;
const TEST_TIMEOUT: Duration = Duration::from_secs(15);

async fn within<T>(future: impl Future<Output = T>) -> Result<T, BoxError> {
    Ok(tokio::time::timeout(TEST_TIMEOUT, future).await?)
}

fn graph(
    label: &'static str,
    entered: Option<Arc<Notify>>,
    release: Option<Arc<Notify>>,
) -> Result<netplane::GraphVersion<HttpRequest, HttpResponse>, BoxError> {
    let backend = move |request: HttpRequest| {
        let entered = entered.clone();
        let release = release.clone();
        async move {
            if request.uri().path() == "/a" {
                if let Some(entered) = entered {
                    entered.notify_one();
                }
                if let Some(release) = release {
                    release.notified().await;
                }
            }
            Ok::<_, Infallible>(Response::new(Bytes::from(label)))
        }
    };
    let backend = ServiceBuilder::new()
        .map_response(move |mut response: HttpResponse| {
            response
                .headers_mut()
                .insert("x-policy", http::HeaderValue::from_static(label));
            response
        })
        .service(service_fn(backend));
    let version = GraphBuilder::new()
        .service("a", backend)
        .service(
            "b",
            service_fn(move |_: HttpRequest| async move {
                Ok::<_, Infallible>(Response::new(Bytes::from(format!("b-{label}"))))
            }),
        )
        .service(
            "fallback",
            service_fn(move |_: HttpRequest| async move {
                Ok::<_, Infallible>(Response::new(Bytes::from(format!("fallback-{label}"))))
            }),
        )
        .router("router", |request: &HttpRequest| {
            match request.uri().path() {
                "/a" => "a",
                "/b" => "b",
                _ => "fallback",
            }
        })
        .route("router", "a", "a")
        .route("router", "b", "b")
        .fallback("router", "fallback")
        .entry("router")
        .capacity(Capacity {
            max_in_flight: 4,
            buffer_capacity: 4,
        })
        .validate()?;
    Ok(version)
}

async fn get(port: u16, path: &str) -> Result<HttpResponse, BoxError> {
    let mut stream = within(tokio::net::TcpStream::connect(("127.0.0.1", port))).await??;
    within(stream.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes(),
    ))
    .await??;
    let mut bytes = Vec::new();
    within(stream.read_to_end(&mut bytes)).await??;
    http_response::parse(&bytes)
}

#[tokio::test]
async fn live_publication_retains_old_request_and_admission() -> Result<(), BoxError> {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut runtime = Netplane::new(
        graph("v1", Some(Arc::clone(&entered)), Some(Arc::clone(&release)))?,
        RuntimeLimits { max_outstanding: 1 },
    )?;
    let adapter = HttpAdapter::identity(runtime.handle(), HttpLimits::default())?;
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
    assert_eq!(get(port, "/b").await?.body(), &Bytes::from_static(b"b-v1"));
    assert_eq!(
        get(port, "/missing").await?.body(),
        &Bytes::from_static(b"fallback-v1")
    );
    let old = tokio::spawn(async move { get(port, "/a").await });
    within(entered.notified()).await?;
    runtime.publish(graph("v2", None, None)?)?;
    let overloaded = get(port, "/a").await?;
    assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
    release.notify_one();
    let old = within(old).await???;
    assert_eq!(old.status(), StatusCode::OK);
    assert_eq!(old.headers()["x-policy"], "v1");
    assert_eq!(old.body(), &Bytes::from_static(b"v1"));
    let new = get(port, "/a").await?;
    assert_eq!(new.status(), StatusCode::OK);
    assert_eq!(new.headers()["x-policy"], "v2");
    assert_eq!(new.body(), &Bytes::from_static(b"v2"));
    assert_eq!(get(port, "/b").await?.body(), &Bytes::from_static(b"b-v2"));
    assert_eq!(
        get(port, "/missing").await?.body(),
        &Bytes::from_static(b"fallback-v2")
    );
    let _ = stop_tx.send(());
    within(runtime.shutdown(Duration::from_secs(2))).await??;
    within(server).await???;
    Ok(())
}

#[tokio::test]
async fn decode_and_service_errors_are_sanitized() -> Result<(), BoxError> {
    let version = netplane::GraphDraft::new(service_fn(|request: String| async move {
        if request == "fail" {
            Err::<String, _>(io::Error::other("private failure"))
        } else {
            Ok(request)
        }
    }))
    .validate()?;
    let mut runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let adapter = HttpAdapter::new(
        runtime.handle(),
        HttpLimits::default(),
        |request: HttpRequest| {
            let value = String::from_utf8(request.body().to_vec())?;
            if value == "bad" {
                return Err(io::Error::other("invalid").into());
            }
            Ok(value)
        },
        |response: String| Ok(Response::new(Bytes::from(response))),
    )?;
    let bad = adapter
        .execute(Request::new(axum::body::Body::from("bad")))
        .await;
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
    let small = HttpAdapter::new(
        runtime.handle(),
        HttpLimits {
            max_body_bytes: 2,
            ..HttpLimits::default()
        },
        |request: HttpRequest| Ok(String::from_utf8(request.body().to_vec())?),
        |response: String| Ok(Response::new(Bytes::from(response))),
    )?;
    let oversized = small
        .execute(Request::new(axum::body::Body::from("large")))
        .await;
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let failed = adapter
        .execute(Request::new(axum::body::Body::from("fail")))
        .await;
    assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
    runtime.shutdown(Duration::from_secs(1)).await?;
    let closed = adapter
        .execute(Request::new(axum::body::Body::from("ok")))
        .await;
    assert_eq!(closed.status(), StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

#[tokio::test]
async fn dropped_handler_releases_admission_and_drain_can_retry() -> Result<(), BoxError> {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let service_entered = Arc::clone(&entered);
    let service_release = Arc::clone(&release);
    let version = netplane::GraphDraft::new(service_fn(move |_: HttpRequest| {
        let entered = Arc::clone(&service_entered);
        let release = Arc::clone(&service_release);
        async move {
            entered.notify_one();
            release.notified().await;
            Ok::<_, Infallible>(Response::new(Bytes::new()))
        }
    }))
    .validate()?;
    let mut runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let adapter = HttpAdapter::identity(runtime.handle(), HttpLimits::default())?;
    let handler = tokio::spawn(async move {
        adapter
            .execute(Request::new(axum::body::Body::empty()))
            .await
    });
    within(entered.notified()).await?;
    let timed_out = runtime.shutdown(Duration::ZERO).await;
    assert!(matches!(
        timed_out,
        Err(netplane::DrainError::TimedOut { .. })
    ));
    handler.abort();
    let _ = within(handler).await?;
    within(runtime.shutdown(Duration::from_secs(1))).await??;
    release.notify_one();
    Ok(())
}

#[tokio::test]
async fn socket_post_preserves_body_headers_and_query() -> Result<(), BoxError> {
    let version = netplane::GraphDraft::new(service_fn(|request: String| async move {
        Ok::<_, Infallible>(request)
    }))
    .validate()?;
    let mut runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 2 })?;
    let adapter = HttpAdapter::new(
        runtime.handle(),
        HttpLimits::default(),
        |request: HttpRequest| {
            let header = request
                .headers()
                .get("x-test")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| io::Error::other("missing header"))?;
            Ok(format!(
                "{}|{header}|{}",
                request.uri(),
                String::from_utf8_lossy(request.body())
            ))
        },
        |response: String| Ok(Response::new(Bytes::from(response))),
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
    let mut stream = within(tokio::net::TcpStream::connect(("127.0.0.1", port))).await??;
    within(stream.write_all(b"POST /submit?x=1 HTTP/1.1\r\nHost: localhost\r\nX-Test: yes\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndata")).await??;
    let mut bytes = Vec::new();
    within(stream.read_to_end(&mut bytes)).await??;
    let response = http_response::parse(&bytes)?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.body(),
        &Bytes::from_static(b"/submit?x=1|yes|data")
    );
    let _ = stop_tx.send(());
    within(runtime.shutdown(Duration::from_secs(1))).await??;
    within(server).await???;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn pending_body_times_out_without_admission() -> Result<(), BoxError> {
    let version = netplane::GraphDraft::new(service_fn(|_: HttpRequest| async {
        Ok::<_, Infallible>(Response::new(Bytes::new()))
    }))
    .validate()?;
    let mut runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let adapter = HttpAdapter::identity(
        runtime.handle(),
        HttpLimits {
            body_read_timeout: Duration::from_millis(1),
            ..HttpLimits::default()
        },
    )?;
    let pending = futures_util::stream::pending::<Result<Bytes, io::Error>>();
    let response = adapter
        .execute(Request::new(axum::body::Body::from_stream(pending)))
        .await;
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    runtime.shutdown(Duration::ZERO).await?;
    Ok(())
}

#[tokio::test]
async fn encoded_response_limit_is_enforced() -> Result<(), BoxError> {
    let version = netplane::GraphDraft::new(service_fn(|_: HttpRequest| async {
        Ok::<_, Infallible>(Response::new(Bytes::from_static(b"large")))
    }))
    .validate()?;
    let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let adapter = HttpAdapter::identity(
        runtime.handle(),
        HttpLimits {
            max_response_bytes: 2,
            ..HttpLimits::default()
        },
    )?;
    let response = adapter
        .execute(Request::new(axum::body::Body::empty()))
        .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    Ok(())
}

#[tokio::test]
async fn each_zero_http_limit_is_rejected() -> Result<(), BoxError> {
    let version = netplane::GraphDraft::new(service_fn(|_: HttpRequest| async {
        Ok::<_, Infallible>(Response::new(Bytes::new()))
    }))
    .validate()?;
    let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let defaults = HttpLimits::default();
    let invalid = [
        HttpLimits {
            max_body_bytes: 0,
            ..defaults
        },
        HttpLimits {
            max_concurrent_body_reads: 0,
            ..defaults
        },
        HttpLimits {
            body_read_timeout: Duration::ZERO,
            ..defaults
        },
        HttpLimits {
            max_response_bytes: 0,
            ..defaults
        },
    ];
    for limits in invalid {
        assert!(matches!(
            HttpAdapter::identity(runtime.handle(), limits),
            Err(InvalidLimits)
        ));
    }
    assert!(HttpAdapter::identity(runtime.handle(), defaults).is_ok());
    Ok(())
}

#[tokio::test]
async fn ingress_saturation_recovers_after_body_reader_cancellation() -> Result<(), BoxError> {
    let version = netplane::GraphDraft::new(service_fn(|_: HttpRequest| async {
        Ok::<_, Infallible>(Response::new(Bytes::from_static(b"ok")))
    }))
    .validate()?;
    let mut runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
    let adapter = HttpAdapter::identity(
        runtime.handle(),
        HttpLimits {
            max_concurrent_body_reads: 1,
            ..HttpLimits::default()
        },
    )?;
    let started = Arc::new(Notify::new());
    let body_started = Arc::clone(&started);
    let pending = futures_util::stream::poll_fn(move |_| {
        body_started.notify_one();
        Poll::<Option<Result<Bytes, io::Error>>>::Pending
    });
    let held_adapter = adapter.clone();
    let held = tokio::spawn(async move {
        held_adapter
            .execute(Request::new(axum::body::Body::from_stream(pending)))
            .await
    });
    within(started.notified()).await?;
    let unpolled = runtime.handle().try_execute(Request::new(Bytes::new()))?;
    drop(unpolled);
    let rejected = adapter
        .execute(Request::new(axum::body::Body::empty()))
        .await;
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    held.abort();
    let _ = within(held).await?;
    let accepted = adapter
        .execute(Request::new(axum::body::Body::empty()))
        .await;
    assert_eq!(accepted.status(), StatusCode::OK);
    within(runtime.shutdown(Duration::from_secs(1))).await??;
    Ok(())
}

#[tokio::test]
async fn graceful_listener_shutdown_drains_active_socket_request() -> Result<(), BoxError> {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut runtime = Netplane::new(
        graph("v1", Some(Arc::clone(&entered)), Some(Arc::clone(&release)))?,
        RuntimeLimits { max_outstanding: 1 },
    )?;
    let adapter = HttpAdapter::identity(runtime.handle(), HttpLimits::default())?;
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
    let client = tokio::spawn(async move { get(port, "/a").await });
    within(entered.notified()).await?;
    let drain = runtime.shutdown(Duration::from_secs(2));
    let _ = stop_tx.send(());
    let drain = tokio::spawn(drain);
    tokio::task::yield_now().await;
    assert!(!drain.is_finished());
    release.notify_one();
    let response = within(client).await???;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-policy"], "v1");
    assert_eq!(response.body(), &Bytes::from_static(b"v1"));
    within(drain).await???;
    within(server).await???;
    Ok(())
}
