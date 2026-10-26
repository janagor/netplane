use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, StatusCode};
use netplane::{ControllerOptions, Netplane, ReconfigurationController};
use netplane_http::HttpAdapter;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tower::BoxError;

use super::{
    HttpRequest,
    config::{GatewayConfig, InvalidPolicy},
    graph::build_graph,
    routing::{canary_bucket, select_backend, valid_request_key},
};

#[path = "../../tests/common/http_response.rs"]
mod http_response;

fn request(
    method: Method,
    path: &str,
    key: Option<&str>,
    body: &'static [u8],
) -> Result<HttpRequest, BoxError> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .body(Bytes::from_static(body))?;
    if let Some(key) = key {
        request
            .headers_mut()
            .insert("x-request-key", key.parse::<http::HeaderValue>()?);
    }
    Ok(request)
}

#[test]
fn rejects_invalid_policy() {
    let mut policy = GatewayConfig::default().policy;
    policy.revision = 0;
    assert_eq!(policy.validate(), Err(InvalidPolicy::ZeroRevision));
    policy.revision = 1;
    policy.canary_percent = 101;
    assert_eq!(policy.validate(), Err(InvalidPolicy::CanaryPercent(101)));
}

#[test]
fn canary_hash_is_stable() {
    for (key, bucket) in [
        ("customer-1", 19),
        ("customer-2", 30),
        ("customer-3", 41),
        ("user-1", 8),
    ] {
        assert_eq!(canary_bucket(key.as_bytes()), bucket);
    }
    assert!(valid_request_key(b"abc-._123"));
    assert!(!valid_request_key(b""));
    assert!(!valid_request_key(b"bad key"));
    assert!(!valid_request_key(&[b'a'; 129]));
}

#[test]
fn canary_rollout_is_monotonic() -> Result<(), BoxError> {
    let mut policy = GatewayConfig::default().policy;
    for index in 0..500 {
        let key = format!("key-{index}");
        let request = request(Method::POST, "/process", Some(&key), b"")?;
        policy.canary_percent = 10;
        let at_ten = select_backend(&request, &policy);
        policy.canary_percent = 25;
        if at_ten == "canary" {
            assert_eq!(select_backend(&request, &policy), "canary");
        }
    }
    Ok(())
}

#[tokio::test]
async fn routes_to_declared_backends() -> Result<(), BoxError> {
    let config = GatewayConfig::default();
    for (percent, key, expected) in [
        (0, "customer-1", "stable"),
        (25, "customer-1", "canary"),
        (25, "customer-2", "stable"),
        (100, "customer-2", "canary"),
    ] {
        let mut policy = config.policy.clone();
        policy.canary_percent = percent;
        let graph = build_graph(&policy, config.outer_capacity, config.backend_capacity)?;
        let runtime = Netplane::new(graph, config.runtime_limits)?;
        let response = runtime
            .handle()
            .execute(request(Method::POST, "/process", Some(key), b"hello")?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-backend"], expected);
        assert_eq!(response.headers()["x-policy-revision"], "1");
        assert_eq!(
            response.body(),
            &Bytes::from(format!("backend={expected};processed_bytes=5"))
        );
    }
    Ok(())
}

#[tokio::test]
async fn fallback_returns_correct_status() -> Result<(), BoxError> {
    let config = GatewayConfig::default();
    let graph = build_graph(
        &config.policy,
        config.outer_capacity,
        config.backend_capacity,
    )?;
    let runtime = Netplane::new(graph, config.runtime_limits)?;
    for (method, path, key, expected) in [
        (Method::GET, "/unknown", None, StatusCode::NOT_FOUND),
        (
            Method::GET,
            "/process",
            None,
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (Method::POST, "/process", None, StatusCode::BAD_REQUEST),
        (
            Method::POST,
            "/process",
            Some("bad key"),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let response = runtime
            .handle()
            .execute(request(method, path, key, b"")?)
            .await?;
        assert_eq!(response.status(), expected);
        assert_eq!(response.headers()["x-backend"], "fallback");
        if expected == StatusCode::METHOD_NOT_ALLOWED {
            assert_eq!(response.headers()[http::header::ALLOW], "POST");
        }
    }
    Ok(())
}

#[tokio::test]
async fn graph_metadata_matches_topology() -> Result<(), BoxError> {
    let config = GatewayConfig::default();
    let graph = build_graph(
        &config.policy,
        config.outer_capacity,
        config.backend_capacity,
    )?;
    let metadata = graph.metadata();
    assert_eq!(metadata.entry, "policy");
    assert_eq!(metadata.nodes.len(), 4);
    assert_eq!(metadata.routes.len(), 2);
    assert_eq!(metadata.fallback.as_deref(), Some("fallback"));
    assert_eq!(metadata.capacity, Some(config.outer_capacity));
    assert_eq!(metadata.backend_capacity, Some(config.backend_capacity));
    Ok(())
}

#[tokio::test]
async fn simulated_backend_failure_is_visible() -> Result<(), BoxError> {
    let mut config = GatewayConfig::default();
    config.policy.canary.fail = true;
    let graph = build_graph(
        &config.policy,
        config.outer_capacity,
        config.backend_capacity,
    )?;
    let runtime = Netplane::new(graph, config.runtime_limits)?;
    let response = runtime
        .handle()
        .execute(request(
            Method::POST,
            "/process",
            Some("customer-1"),
            b"hello",
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["x-backend"], "canary");
    Ok(())
}

#[tokio::test]
async fn shutdown_drains_admitted_requests() -> Result<(), BoxError> {
    let mut config = GatewayConfig::default();
    config.policy.stable.delay = Duration::from_millis(10);
    let graph = build_graph(
        &config.policy,
        config.outer_capacity,
        config.backend_capacity,
    )?;
    let runtime = Netplane::new(graph, config.runtime_limits)?;
    let controller = ReconfigurationController::new(runtime, ControllerOptions::default());
    let handle = controller.handle();
    let accepted = handle.execute(request(Method::POST, "/process", Some("customer-2"), b"x")?);
    let drain = controller.shutdown(Duration::from_secs(1));
    assert!(
        handle
            .try_execute(request(Method::POST, "/process", Some("customer-2"), b"x")?)
            .is_err()
    );
    let (response, drained) = tokio::join!(accepted, drain);
    assert_eq!(response?.status(), StatusCode::OK);
    assert!(drained.is_ok());
    Ok(())
}

async fn http_request(
    port: u16,
    method: Method,
    path: &str,
    key: Option<&str>,
) -> Result<http::Response<Bytes>, BoxError> {
    let mut stream = tokio::time::timeout(
        Duration::from_secs(3),
        TcpStream::connect(("127.0.0.1", port)),
    )
    .await??;
    let key_header = key.map_or(String::new(), |key| format!("X-Request-Key: {key}\r\n"));
    let body = if method == Method::POST { "hello" } else { "" };
    let wire = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n{key_header}\r\n{body}",
        body.len()
    );
    tokio::time::timeout(Duration::from_secs(3), stream.write_all(wire.as_bytes())).await??;
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut received)).await??;
    let response = http_response::parse(&received)?;
    assert_eq!(response.headers()["x-policy-revision"], "1");
    Ok(response)
}

#[tokio::test]
async fn handles_concurrent_http_requests() -> Result<(), BoxError> {
    let config = GatewayConfig::default();
    let graph = build_graph(
        &config.policy,
        config.outer_capacity,
        config.backend_capacity,
    )?;
    let runtime = Netplane::new(graph, config.runtime_limits)?;
    let controller = ReconfigurationController::new(runtime, ControllerOptions::default());
    let adapter = HttpAdapter::identity(controller.handle(), config.http_limits)?;
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
    let mut clients = Vec::new();
    for index in 0..16 {
        clients.push(tokio::spawn(async move {
            let (key, backend) = if index % 2 == 0 {
                ("customer-1", "canary")
            } else {
                ("customer-2", "stable")
            };
            let response = http_request(port, Method::POST, "/process", Some(key)).await?;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["x-backend"], backend);
            assert_eq!(
                response.body(),
                &Bytes::from(format!("backend={backend};processed_bytes=5"))
            );
            Ok::<_, BoxError>(())
        }));
    }
    for client in clients {
        client.await??;
    }
    let response = http_request(port, Method::POST, "/process", None).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()["x-backend"], "fallback");
    let response = http_request(port, Method::POST, "/unknown", Some("customer-1")).await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(response.headers()["x-backend"], "fallback");
    let response = http_request(port, Method::GET, "/process", None).await?;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers()["x-backend"], "fallback");
    assert_eq!(response.headers()[http::header::ALLOW], "POST");
    let drain = controller.shutdown(Duration::from_secs(1));
    let _ = stop_tx.send(());
    let (server_result, drain_result) = tokio::join!(server, drain);
    server_result??;
    drain_result?;
    Ok(())
}

#[path = "admin_tests.rs"]
mod admin_tests;

#[test]
fn admin_token_configuration_rejects_invalid_values() -> Result<(), BoxError> {
    use std::env::VarError;
    assert!(super::configured_admin_token(Err(VarError::NotPresent))?.is_none());
    assert!(super::configured_admin_token(Ok(String::new())).is_err());
    assert!(super::configured_admin_token(Ok("abcdefghijklmnop\n".into())).is_err());
    assert!(super::configured_admin_token(Ok("abcdefghijklmnop".into()))?.is_some());
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        let non_unicode = std::ffi::OsString::from_vec(vec![0xff]);
        assert!(super::configured_admin_token(Err(VarError::NotUnicode(non_unicode))).is_err());
    }
    Ok(())
}
