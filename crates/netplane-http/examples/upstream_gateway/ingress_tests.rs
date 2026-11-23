use super::{backend, client, graph, key, start};
use bytes::Bytes;
use http::StatusCode;
use netplane::{Netplane, RuntimeLimits};
use netplane_http::{HttpAdapter, HttpLimits};
use std::time::Duration;
use tower::BoxError;

#[tokio::test]
async fn tcp_ingress_routes_and_maps_failures() -> Result<(), BoxError> {
    tokio::time::timeout(Duration::from_secs(10), ingress_scenario()).await?
}

async fn ingress_scenario() -> Result<(), BoxError> {
    let stable = start("stable").await?;
    let canary = start("canary").await?;
    let outbound = client(Duration::from_millis(200))?;
    let mut runtime = Netplane::new(
        graph::build(
            backend(&outbound, &stable.origin)?,
            backend(&outbound, &canary.origin)?,
        )?,
        RuntimeLimits { max_outstanding: 8 },
    )?;
    let adapter = HttpAdapter::identity(runtime.handle(), HttpLimits::default())?
        .with_error_mapper(super::map_error);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, adapter.router()).await;
    });
    // Reuse the fixture's abort-on-drop ownership for the gateway listener too.
    let gateway = super::Server {
        origin,
        task,
        gate: std::sync::Arc::default(),
    };
    let inbound = client(Duration::from_secs(3))?;
    for name in ["stable", "canary"] {
        let response = inbound
            .post(format!("{}/process", gateway.origin))
            .header("x-request-key", key(name)?)
            .body("hello")
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-backend"], name);
        assert_eq!(
            response.bytes().await?,
            Bytes::from(format!("backend={name};processed_bytes=5"))
        );
    }
    assert_eq!(
        inbound
            .post(format!("{}/process", gateway.origin))
            .send()
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    let response = inbound
        .get(format!("{}/failure", gateway.origin))
        .header("x-request-key", key("stable")?)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let response = inbound
        .get(format!("{}/slow", gateway.origin))
        .header("x-request-key", key("stable")?)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(response.bytes().await?.is_empty());
    stable.gate.release.notify_one();

    // Hold the ephemeral port until immediately before connecting to avoid
    // accidentally choosing the gateway or either upstream's live port.
    let refused = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let refused_origin = format!("http://{}", refused.local_addr()?);
    runtime.publish(graph::build(
        backend(&outbound, &refused_origin)?,
        backend(&outbound, &refused_origin)?,
    )?)?;
    drop(refused);
    let response = inbound
        .get(format!("{}/health", gateway.origin))
        .header("x-request-key", key("stable")?)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(response.bytes().await?.is_empty());
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
