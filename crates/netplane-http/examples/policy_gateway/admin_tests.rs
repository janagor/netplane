use super::{GatewayConfig, build_graph, http_response};
use bytes::Bytes;
use http::{Method, Request, StatusCode};
use netplane::{ControllerOptions, Netplane, ReconfigurationController};
use netplane_http::HttpAdapter;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tower::BoxError;
fn control_fixture(
    config: GatewayConfig,
) -> Result<
    (
        std::sync::Arc<crate::control::GatewayControl>,
        tokio::sync::oneshot::Receiver<crate::control::Drain>,
    ),
    BoxError,
> {
    let graph = build_graph(
        &config.policy,
        config.outer_capacity,
        config.backend_capacity,
    )?;
    let runtime = Netplane::new(graph, config.runtime_limits)?;
    let controller = std::sync::Arc::new(ReconfigurationController::new(
        runtime,
        ControllerOptions::default(),
    ));
    let (tx, rx) = tokio::sync::oneshot::channel();
    Ok((
        std::sync::Arc::new(crate::control::GatewayControl::new(controller, config, tx)?),
        rx,
    ))
}

#[tokio::test]
async fn control_tracks_replacements_rollback_and_eviction() -> Result<(), BoxError> {
    use crate::control::{ControlError, PolicyInput, ReconfigureRequest, RollbackRequest};
    let mut config = GatewayConfig::default();
    config.policy.canary_percent = 0;
    let (control, drain_rx) = control_fixture(config)?;
    let initial = control.status().await?;
    assert_eq!(initial.active.as_ref().ok_or("missing active")?.version, 1);
    assert_eq!(
        initial
            .active
            .as_ref()
            .ok_or("missing active")?
            .policy
            .canary_percent,
        0
    );
    let request = |version, percent| ReconfigureRequest {
        expected_version: version,
        policy: PolicyInput {
            canary_percent: percent,
        },
    };
    let published = control.reconfigure(request(1, 100)).await?;
    assert_eq!(published.published_version, 2);
    assert!(matches!(
        control.reconfigure(request(1, 50)).await,
        Err(ControlError::Conflict)
    ));
    assert!(matches!(
        control.reconfigure(request(2, 101)).await,
        Err(ControlError::InvalidPolicy)
    ));
    let rolled = control
        .rollback(RollbackRequest {
            expected_version: 2,
            target_version: 1,
        })
        .await?;
    assert_eq!(rolled.published_version, 3);
    assert_eq!(rolled.source_version, Some(1));
    assert_eq!(
        control
            .status()
            .await?
            .active
            .ok_or("missing active")?
            .policy
            .canary_percent,
        0
    );
    control.reconfigure(request(3, 20)).await?;
    control.reconfigure(request(4, 30)).await?;
    let status = control.status().await?;
    assert_eq!(
        status
            .retained
            .iter()
            .map(|v| v.version)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    assert!(matches!(
        control
            .rollback(RollbackRequest {
                expected_version: 5,
                target_version: 1
            })
            .await,
        Err(ControlError::NotFound)
    ));
    control.shutdown().await?;
    drain_rx.await?.await?;
    assert!(matches!(
        control.reconfigure(request(5, 10)).await,
        Err(ControlError::ShuttingDown)
    ));
    Ok(())
}

#[tokio::test]
async fn admin_authentication_and_json_errors() -> Result<(), BoxError> {
    use axum::body::{Body, to_bytes};
    use tower::ServiceExt;
    let (control, _drain_rx) = control_fixture(GatewayConfig::default())?;
    let app = crate::admin::router(control, "abcdefghijklmnop".to_owned());
    let request = |token: Option<&str>, body: &str| {
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri("/reconfigure")
            .header("content-type", "application/json");
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::from(body.to_owned()))
    };
    assert_eq!(
        app.clone().oneshot(request(None, "{}")?).await?.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(request(Some("wrong"), "{}")?)
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(request(Some("abcdefghijklmnop"), "{")?)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        app.clone()
            .oneshot(request(Some("abcdefghijklmnop"), &"x".repeat(4097))?)
            .await?
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                Some("abcdefghijklmnop"),
                r#"{"expected_version":1,"policy":{"canary_percent":256}}"#
            )?)
            .await?
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                Some("abcdefghijklmnop"),
                r#"{"expected_version":1,"policy":{"canary_percent":"many"}}"#
            )?)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    let response = app
        .oneshot(request(
            Some("abcdefghijklmnop"),
            r#"{"expected_version":1,"policy":{"canary_percent":100}}"#,
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 4096).await?;
    let value: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(value["published_version"], 2);
    Ok(())
}

async fn wire_request(
    port: u16,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> Result<http::Response<Bytes>, BoxError> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    let auth = token.map_or(String::new(), |value| {
        format!("Authorization: Bearer {value}\r\n")
    });
    let wire = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nX-Request-Key: customer-1\r\nContent-Type: application/json\r\n{auth}Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(wire.as_bytes()).await?;
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut received)).await??;
    http_response::parse(&received)
}

#[tokio::test]
async fn live_admin_listener_replaces_and_rolls_back() -> Result<(), BoxError> {
    let mut config = GatewayConfig::default();
    config.policy.canary_percent = 0;
    let gate = std::sync::Arc::new(crate::backends::BackendGate::default());
    config.policy.stable.gate = Some(gate.clone());
    config.policy.stable.count_calls = true;
    let graph = build_graph(
        &config.policy,
        config.outer_capacity,
        config.backend_capacity,
    )?;
    let runtime = Netplane::new(graph, config.runtime_limits)?;
    let controller = std::sync::Arc::new(ReconfigurationController::new(
        runtime,
        ControllerOptions::default(),
    ));
    let adapter = HttpAdapter::identity(controller.handle(), config.http_limits)?;
    let (drain_tx, drain_rx) = tokio::sync::oneshot::channel();
    let control = std::sync::Arc::new(crate::control::GatewayControl::new(
        controller, config, drain_tx,
    )?);
    let public = TcpListener::bind("127.0.0.1:0").await?;
    let public_port = public.local_addr()?.port();
    let admin = TcpListener::bind("127.0.0.1:0").await?;
    let admin_port = admin.local_addr()?.port();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut public_stop = stop_rx.clone();
    let public_server = tokio::spawn(async move {
        axum::serve(public, adapter.router())
            .with_graceful_shutdown(async move {
                while !*public_stop.borrow_and_update() && public_stop.changed().await.is_ok() {}
            })
            .await
    });
    let mut admin_stop = stop_rx;
    let admin_server = tokio::spawn(async move {
        axum::serve(
            admin,
            crate::admin::router(control, "abcdefghijklmnop".into()),
        )
        .with_graceful_shutdown(async move {
            while !*admin_stop.borrow_and_update() && admin_stop.changed().await.is_ok() {}
        })
        .await
    });
    verify_live_flow(public_port, admin_port, gate).await?;
    let _ = stop_tx.send(true);
    drain_rx.await?.await?;
    public_server.await??;
    admin_server.await??;
    Ok(())
}

async fn verify_live_flow(
    public_port: u16,
    admin_port: u16,
    gate: std::sync::Arc<crate::backends::BackendGate>,
) -> Result<(), BoxError> {
    let token = Some("abcdefghijklmnop");
    assert_eq!(
        wire_request(public_port, "POST", "/process", None, "hello")
            .await?
            .headers()["x-backend"],
        "stable"
    );
    gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    let held =
        tokio::spawn(
            async move { wire_request(public_port, "POST", "/process", None, "held").await },
        );
    tokio::time::timeout(Duration::from_secs(3), gate.entered.notified()).await?;
    let replacement = wire_request(
        admin_port,
        "POST",
        "/reconfigure",
        token,
        r#"{"expected_version":1,"policy":{"canary_percent":100}}"#,
    )
    .await?;
    assert_eq!(replacement.status(), StatusCode::OK);
    assert_eq!(
        wire_request(public_port, "POST", "/process", None, "hello")
            .await?
            .headers()["x-backend"],
        "canary"
    );
    gate.release.notify_one();
    let held_response = held.await??;
    assert_eq!(held_response.headers()["x-backend"], "stable");
    assert_eq!(held_response.headers()["x-policy-revision"], "1");
    assert_eq!(held_response.headers()["x-service-count"], "2");
    assert_eq!(
        wire_request(
            admin_port,
            "POST",
            "/reconfigure",
            token,
            r#"{"expected_version":1,"policy":{"canary_percent":0}}"#
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );
    let rollback = wire_request(
        admin_port,
        "POST",
        "/rollback",
        token,
        r#"{"expected_version":2,"target_version":1}"#,
    )
    .await?;
    assert_eq!(rollback.status(), StatusCode::OK);
    let restored = wire_request(public_port, "POST", "/process", None, "hello").await?;
    assert_eq!(restored.headers()["x-backend"], "stable");
    assert_eq!(restored.headers()["x-service-count"], "3");
    let status = wire_request(admin_port, "GET", "/status", token, "").await?;
    let value: serde_json::Value = serde_json::from_slice(status.body())?;
    assert_eq!(value["active"]["version"], 3);
    assert_eq!(value["last_publication"]["source_version"], 1);
    assert_eq!(
        wire_request(admin_port, "POST", "/shutdown", token, "")
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_admin_reconfigurations_have_one_winner() -> Result<(), BoxError> {
    use axum::body::{Body, to_bytes};
    use tower::ServiceExt;
    let (control, _drain_rx) = control_fixture(GatewayConfig::default())?;
    let app = crate::admin::router(control, "abcdefghijklmnop".into());
    let submit = |percent: u64| {
        Request::builder()
            .method(Method::POST)
            .uri("/reconfigure")
            .header("authorization", "Bearer abcdefghijklmnop")
            .header("content-type", "application/json")
            .body(Body::from(format!(
                r#"{{"expected_version":1,"policy":{{"canary_percent":{percent}}}}}"#
            )))
    };
    let (first, second) = tokio::join!(
        app.clone().oneshot(submit(0)?),
        app.clone().oneshot(submit(100)?)
    );
    let statuses = [first?.status(), second?.status()];
    assert!(statuses.contains(&StatusCode::OK));
    assert!(statuses.contains(&StatusCode::CONFLICT));
    let status_request = Request::builder()
        .uri("/status")
        .header("authorization", "Bearer abcdefghijklmnop")
        .body(Body::empty())?;
    let status = app.oneshot(status_request).await?;
    let body = to_bytes(status.into_body(), 4096).await?;
    let value: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(value["active"]["version"], 2);
    assert!(
        value["active"]["policy"]["canary_percent"] == 0
            || value["active"]["policy"]["canary_percent"] == 100
    );
    Ok(())
}

#[tokio::test]
async fn completed_shutdown_has_null_active_status() -> Result<(), BoxError> {
    let (control, drain_rx) = control_fixture(GatewayConfig::default())?;
    control.shutdown().await?;
    drain_rx.await?.await?;
    let status = control.status().await?;
    assert!(status.active.is_none());
    assert!(status.retained.is_empty());
    assert_eq!(status.retained_count, 0);
    Ok(())
}

#[tokio::test]
async fn lost_shutdown_notification_remains_an_error() -> Result<(), BoxError> {
    use crate::control::ControlError;
    let (control, drain_rx) = control_fixture(GatewayConfig::default())?;
    drop(drain_rx);
    assert!(matches!(
        control.shutdown().await,
        Err(ControlError::Internal)
    ));
    assert!(matches!(
        control.shutdown().await,
        Err(ControlError::Internal)
    ));
    Ok(())
}

#[tokio::test]
async fn server_failure_triggers_drain_and_is_reported() -> Result<(), BoxError> {
    let (control, drain_rx) = control_fixture(GatewayConfig::default())?;
    let (stop_tx, _) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(async { Err(std::io::Error::other("listener failed")) });
    let result = crate::supervise(control.clone(), drain_rx, stop_tx, server, None).await;
    assert!(result.is_err());
    assert!(control.status().await?.active.is_none());
    Ok(())
}
