use super::{graph, upstream::*};
use bytes::Bytes;
use http::{Request, StatusCode};
use netplane::{Netplane, RuntimeLimits};
use std::{sync::Arc, time::Duration};
use tower::BoxError;
#[path = "server.rs"]
mod server;

struct Server {
    origin: String,
    task: tokio::task::JoinHandle<()>,
    gate: Arc<server::Gate>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn start(name: &str) -> Result<Server, BoxError> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let gate = Arc::new(server::Gate::default());
    let app = server::router(name.into(), gate.clone());
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(Server { origin, task, gate })
}
fn request(path: &str, key: &str) -> Result<Request<Bytes>, BoxError> {
    Ok(Request::builder()
        .method(if path.split('?').next() == Some("/process") {
            "POST"
        } else {
            "GET"
        })
        .uri(path)
        .header("x-request-key", key)
        .body(Bytes::from_static(b"hello"))?)
}
fn backend(client: &reqwest::Client, origin: &str) -> Result<HttpUpstream, BoxError> {
    HttpUpstream::new(client.clone(), origin, 1024)
}
fn key(route: &str) -> Result<String, BoxError> {
    for i in 0..1000 {
        let key = format!("key-{i}");
        if graph::selected(&request("/process", &key)?) == route {
            return Ok(key);
        }
    }
    Err("no matching key".into())
}

#[tokio::test]
async fn forwarding_routing_and_statuses() -> Result<(), BoxError> {
    let a = start("stable").await?;
    let b = start("canary").await?;
    let client = client(Duration::from_secs(2))?;
    let mut runtime = Netplane::new(
        graph::build(backend(&client, &a.origin)?, backend(&client, &b.origin)?)?,
        RuntimeLimits { max_outstanding: 4 },
    )?;
    let handle = runtime.handle();
    for route in ["stable", "canary"] {
        let response = handle
            .execute(request("/process?x=1", &key(route)?)?)
            .await?;
        assert_eq!(response.headers()["x-backend"], route);
        assert_eq!(
            response.body(),
            &format!("backend={route};processed_bytes=5")
        );
    }
    for (path, status) in [
        ("/failure", StatusCode::SERVICE_UNAVAILABLE),
        ("/missing", StatusCode::NOT_FOUND),
    ] {
        assert_eq!(
            handle
                .execute(request(path, &key("stable")?)?)
                .await?
                .status(),
            status
        );
    }
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn publication_capacity_cancellation_and_drain() -> Result<(), BoxError> {
    let a = start("old").await?;
    let b = start("new").await?;
    let client = client(Duration::from_secs(5))?;
    let mut runtime = Netplane::new(
        graph::build(backend(&client, &a.origin)?, backend(&client, &a.origin)?)?,
        RuntimeLimits { max_outstanding: 2 },
    )?;
    let handle = runtime.handle();
    let old = tokio::spawn(handle.execute(request("/slow", "key")?));
    tokio::time::timeout(Duration::from_secs(2), a.gate.entered.notified()).await?;
    runtime.publish(graph::build(
        backend(&client, &b.origin)?,
        backend(&client, &b.origin)?,
    )?)?;
    let unpolled = handle.try_execute(request("/health", "key")?)?;
    assert!(handle.try_execute(request("/health", "key")?).is_err());
    drop(unpolled);
    assert_eq!(
        handle.execute(request("/process", "key")?).await?.headers()["x-backend"],
        "new"
    );
    let drain = runtime.shutdown(Duration::from_secs(2));
    assert!(handle.try_execute(request("/health", "key")?).is_err());
    a.gate.release.notify_one();
    assert_eq!(old.await??.headers()["x-backend"], "old");
    drain.await?;
    Ok(())
}

#[tokio::test]
async fn timeout_refusal_and_response_bound() -> Result<(), BoxError> {
    let a = start("stable").await?;
    let client = client(Duration::from_millis(100))?;
    let error = backend(&client, &a.origin)?
        .execute(request("/slow", "key")?)
        .await
        .err()
        .ok_or("expected timeout")?;
    assert!(matches!(error, UpstreamError::Timeout));
    assert_eq!(
        map_error(&(Box::new(error) as BoxError)).status(),
        StatusCode::GATEWAY_TIMEOUT
    );
    let upstream = HttpUpstream::new(client.clone(), &a.origin, 2)?;
    let error = upstream
        .execute(request("/process", "key")?)
        .await
        .err()
        .ok_or("expected size error")?;
    assert!(matches!(error, UpstreamError::ResponseTooLarge));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let mut runtime = Netplane::new(
        graph::build(backend(&client, &origin)?, backend(&client, &origin)?)?,
        RuntimeLimits { max_outstanding: 2 },
    )?;
    let adapter = netplane_http::HttpAdapter::identity(
        runtime.handle(),
        netplane_http::HttpLimits::default(),
    )?
    .with_error_mapper(map_error);
    let response = adapter
        .execute(request("/health", "key")?.map(axum::body::Body::from))
        .await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn reuse_headers_target_and_chunked_limit() -> Result<(), BoxError> {
    tokio::time::timeout(Duration::from_secs(8), raw_upstream_scenario()).await?
}

async fn raw_upstream_scenario() -> Result<(), BoxError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        // Exactly one accept: both sequential exchanges must reuse this socket.
        let (mut socket, _) =
            tokio::time::timeout(Duration::from_secs(3), listener.accept()).await??;
        for index in 0..2 {
            let mut headers = Vec::new();
            while !headers.ends_with(b"\r\n\r\n") {
                headers
                    .push(tokio::time::timeout(Duration::from_secs(3), socket.read_u8()).await??);
            }
            let headers = String::from_utf8(headers)?;
            assert!(headers.starts_with("GET //untrusted.test/a%2Fb/%25?q=%2F HTTP/1.1"));
            assert!(headers.contains(&format!("host: {address}")));
            assert!(!headers.contains("x-private"));
            assert!(!headers.contains("x-other"));
            assert!(!headers.contains("inbound.test"));
            if index == 0 {
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: x-secret\r\nConnection: x-other\r\nX-Secret: hidden\r\nX-Other: hidden\r\n\r\nokay").await?;
            } else {
                socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nab\r\n2\r\ncd\r\n1\r\ne\r\n0\r\n\r\n").await?;
            }
        }
        Ok::<_, BoxError>(())
    });
    let upstream = HttpUpstream::new(
        client(Duration::from_secs(2))?,
        &format!("http://{address}"),
        4,
    )?;
    for index in 0..2 {
        let request = Request::builder()
            .uri("//untrusted.test/a%2Fb/%25?q=%2F")
            .header("host", "inbound.test")
            .header("connection", "x-private")
            .header("connection", "x-other")
            .header("x-other", "hidden")
            .header("x-private", "hidden")
            .body(Bytes::new())?;
        let response = upstream.execute(request).await;
        if index == 0 {
            let response = response?;
            assert_eq!(response.body(), "okay");
            assert!(!response.headers().contains_key("x-other"));
            assert!(!response.headers().contains_key("x-secret"));
            assert!(!response.headers().contains_key("connection"));
        } else {
            assert!(matches!(response, Err(UpstreamError::ResponseTooLarge)));
        }
    }
    tokio::time::timeout(Duration::from_secs(3), task).await???;
    Ok(())
}

#[tokio::test]
async fn dropped_dispatched_request_releases_admission() -> Result<(), BoxError> {
    let a = start("stable").await?;
    let client = client(Duration::from_secs(2))?;
    let mut runtime = Netplane::new(
        graph::build(backend(&client, &a.origin)?, backend(&client, &a.origin)?)?,
        RuntimeLimits { max_outstanding: 1 },
    )?;
    let handle = runtime.handle();
    let task = tokio::spawn(handle.execute(request("/slow", "key")?));
    tokio::time::timeout(Duration::from_secs(1), a.gate.entered.notified()).await?;
    task.abort();
    assert!(task.await.is_err());
    assert_eq!(
        handle.execute(request("/health", "key")?).await?.status(),
        StatusCode::OK
    );
    let b = start("replacement").await?;
    runtime.publish(graph::build(
        backend(&client, &b.origin)?,
        backend(&client, &b.origin)?,
    )?)?;
    assert_eq!(
        handle.execute(request("/process", "key")?).await?.headers()["x-backend"],
        "replacement"
    );
    a.gate.release.notify_one();
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn saturated_backend_leaves_canary_available() -> Result<(), BoxError> {
    let a = start("stable").await?;
    let b = start("canary").await?;
    let client = client(Duration::from_secs(5))?;
    let mut runtime = Netplane::new(
        graph::build(backend(&client, &a.origin)?, backend(&client, &b.origin)?)?,
        RuntimeLimits {
            max_outstanding: 12,
        },
    )?;
    let handle = runtime.handle();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        tasks.push(tokio::spawn(
            handle.execute(request("/slow", &key("stable")?)?),
        ));
        tokio::time::timeout(Duration::from_secs(1), a.gate.entered.notified()).await?;
    }
    let mut queued = Box::pin(handle.execute(request("/health", &key("stable")?)?));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(queued.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            handle.execute(request("/process", &key("canary")?)?)
        )
        .await??
        .headers()["x-backend"],
        "canary"
    );
    drop(queued);
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[test]
fn classifies_wrapped_upstream_errors() {
    #[derive(Debug, thiserror::Error)]
    #[error("middleware failed")]
    struct Wrapped(#[source] UpstreamError);
    for (error, expected) in [
        (UpstreamError::InvalidRequest, StatusCode::BAD_REQUEST),
        (UpstreamError::Timeout, StatusCode::GATEWAY_TIMEOUT),
        (UpstreamError::ResponseTooLarge, StatusCode::BAD_GATEWAY),
    ] {
        let error: BoxError = Box::new(Wrapped(error));
        let response = map_error(&error);
        assert_eq!(response.status(), expected);
        assert!(response.body().is_empty());
    }
}

#[path = "ingress_tests.rs"]
mod ingress_tests;

#[tokio::test]
async fn invalid_target_returns_bad_request_without_connecting() -> Result<(), BoxError> {
    let upstream = backend(&client(Duration::from_secs(1))?, "http://127.0.0.1:1")?;
    let error: BoxError = Box::new(
        upstream
            .execute(request("http://untrusted.test/health", "key")?)
            .await
            .err()
            .ok_or("expected target rejection")?,
    );
    assert!(matches!(
        error.downcast_ref::<UpstreamError>(),
        Some(UpstreamError::InvalidRequest)
    ));
    assert_eq!(map_error(&error).status(), StatusCode::BAD_REQUEST);
    Ok(())
}
