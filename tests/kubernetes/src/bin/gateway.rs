//! Fixture-only diagnostics and fault controls around the real integration.
#[path = "../fault.rs"]
mod fault;
#[path = "../observations.rs"]
mod observations;
#[path = "../preparations.rs"]
mod preparations;
#[path = "../sample.rs"]
mod sample;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use bytes::Bytes;
use http::{Request, Response};
use netplane::{ControllerOptions, Netplane, ReconfigurationController, RuntimeLimits};
use netplane_control::{
    GraphPreparer, Reconciler, ReconciliationDriver, ReconciliationStatus, UpdatePolicy,
};
use netplane_kubernetes::{
    Discovery, DiscoveryConfig, DiscoveryHandle, DiscoverySnapshot, HttpGraphPreparer,
    IntegrationStatus, KubernetesIntegration, PortSelector, ServiceBinding, UpstreamLimits,
    map_http_error, translate,
};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tower::BoxError;

type Controller = ReconfigurationController<Request<Bytes>, Response<Bytes>>;
#[derive(Clone)]
struct App {
    controller: Arc<Controller>,
    releases: observations::Releases,
    preparations: preparations::Counters,
    reconciliation: watch::Receiver<ReconciliationStatus>,
    discovery: DiscoveryHandle,
    integration: watch::Receiver<IntegrationStatus>,
    stopped: Arc<std::sync::atomic::AtomicBool>,
    binding: ServiceBinding,
    fault: fault::Fault,
    stop: CancellationToken,
    http: reqwest::Client,
}
async fn status(State(app): State<App>) -> Result<Json<Value>, StatusCode> {
    sample::observation(&app).map(Json)
}
async fn topology(State(app): State<App>) -> Result<Json<Value>, StatusCode> {
    sample::observation(&app).map(|sample| Json(sample["endpoints"].clone()))
}
async fn ready(State(app): State<App>) -> StatusCode {
    if sample::observation(&app).is_ok_and(|sample| {
        sample["converged"] == true
            && sample["endpoints"]
                .as_array()
                .is_some_and(|e| !e.is_empty())
    }) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn preparation_gate(State(app): State<App>, Json(paused): Json<bool>) {
    app.preparations.paused.send_replace(paused);
}
async fn proxy(
    State(app): State<App>,
    Path(path): Path<String>,
    headers: http::HeaderMap,
) -> Response<axum::body::Body> {
    let path = if path == "block" {
        "/test/block"
    } else {
        "/identity"
    };
    let mut request = Request::new(Bytes::new());
    *request.uri_mut() = http::Uri::from_static(path);
    *request.headers_mut() = headers;
    match app.controller.handle().execute(request).await {
        Ok(response) => response.map(axum::body::Body::from),
        Err(error) => map_http_error(&error).map(axum::body::Body::from),
    }
}
#[derive(Deserialize)]
struct Control {
    address: std::net::SocketAddr,
    action: String,
    ready: Option<bool>,
}
async fn control(
    State(app): State<App>,
    Json(command): Json<Control>,
) -> Result<Json<Value>, StatusCode> {
    let url = format!("http://{}/test/{}", command.address, command.action);
    let request = if command.action == "status" {
        app.http.get(url)
    } else {
        app.http.post(url).json(&command.ready)
    };
    let response = request.send().await.map_err(|_| StatusCode::BAD_GATEWAY)?;
    if !response.status().is_success() {
        return Err(StatusCode::BAD_GATEWAY);
    }
    let body = response
        .bytes()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::from_slice(&body).unwrap_or(Value::Null)))
}
async fn api_fault(State(app): State<App>, Json(disconnected): Json<bool>) {
    app.fault.set(disconnected);
}
async fn shutdown(State(app): State<App>) {
    app.stop.cancel();
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let releases = observations::install();
    let namespace = std::env::var("POD_NAMESPACE")?;
    let binding = ServiceBinding {
        namespace: namespace.clone(),
        service_name: "backend".into(),
        port: PortSelector::Name("http".into()),
        backend_capacity: NonZeroUsize::new(8).ok_or("capacity")?,
    };
    let stop = CancellationToken::new();
    let mut tasks = JoinSet::<Result<(), BoxError>>::new();
    let (client, fault) = fault::client(stop.clone(), &mut tasks).await?;
    let preparer = preparer();
    let counters = preparer.counters.clone();
    let initial = translate(
        &DiscoverySnapshot {
            revision: 0,
            services: BTreeMap::new(),
        },
        &binding,
    )?;
    let runtime = Netplane::new(
        preparer.prepare(initial).await?,
        RuntimeLimits {
            max_outstanding: 32,
        },
    )?;
    let controller = Arc::new(Controller::new(
        runtime,
        ControllerOptions {
            max_retained_versions: 0,
        },
    ));
    let reconciler = Reconciler::new(controller.clone(), preparer);
    let reconciliation = reconciler.subscribe_status();
    let (driver, updates) = ReconciliationDriver::new(reconciler, UpdatePolicy::default())?;
    let (discovery, handle) = Discovery::new(
        client,
        DiscoveryConfig {
            namespace,
            service_names: ["backend".into()].into(),
        },
    )?;
    let integration = KubernetesIntegration::new(&handle, binding.clone(), updates.clone());
    let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let app = App {
        preparations: counters,
        releases,
        integration: integration.status(),
        stopped: stopped.clone(),
        controller: controller.clone(),
        reconciliation,
        discovery: handle,
        binding,
        fault,
        stop: stop.clone(),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?,
    };
    let router = router(app);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    let token = stop.clone();
    tasks.spawn(async move {
        discovery.run(token).await?;
        Ok(())
    });
    let token = stop.clone();
    tasks.spawn(async move {
        integration.run(token).await?;
        Ok(())
    });
    tasks.spawn(async move {
        driver.run(|_| false).await;
        Ok(())
    });
    // Keep diagnostics available after runtime shutdown for lifecycle assertions.
    let mut server = tokio::spawn(async move { axum::serve(listener, router).await });
    let terminate = termination();
    tokio::pin!(terminate);
    let exiting = tokio::select! {
        () = stop.cancelled() => false,
        () = &mut terminate => { stop.cancel(); true },
        _ = tokio::signal::ctrl_c() => { stop.cancel(); true },
    };
    drain(&updates, &controller, &mut tasks, &stopped).await?;
    println!("owned_background_tasks=0");
    if exiting {
        server.abort();
    } else {
        tokio::select! {
            result = &mut server => { result??; },
            () = termination() => server.abort(),
            _ = tokio::signal::ctrl_c() => server.abort(),
        }
    }
    Ok(())
}

async fn termination() {
    #[cfg(unix)]
    if let Ok(mut signal) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        signal.recv().await;
        return;
    }
    std::future::pending::<()>().await;
}

fn router(app: App) -> Router {
    Router::new()
        .route("/status", get(status))
        .route("/observation", get(status))
        .route("/test/preparation", post(preparation_gate))
        .route("/topology", get(topology))
        .route("/readyz", get(ready))
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/proxy/{path}", get(proxy))
        .route("/test/control", post(control))
        .route("/test/api", post(api_fault))
        .route("/test/shutdown", post(shutdown))
        .with_state(app)
}

async fn drain(
    updates: &netplane_control::UpdateSender<netplane_kubernetes::DesiredState>,
    controller: &Controller,
    tasks: &mut JoinSet<Result<(), BoxError>>,
    stopped: &std::sync::atomic::AtomicBool,
) -> Result<(), BoxError> {
    updates.shutdown();
    controller.shutdown(Duration::from_secs(60)).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok::<_, BoxError>(())
    })
    .await??;
    stopped.store(true, std::sync::atomic::Ordering::SeqCst);
    Ok(())
}

fn preparer() -> preparations::Preparer {
    let preparer = HttpGraphPreparer::new(
        UpstreamLimits {
            max_backends: 16,
            exchange_timeout: Duration::from_secs(120),
            ..UpstreamLimits::default()
        },
        |r| {
            r.headers()
                .get("x-request-key")
                .map_or(b"default", |h| h.as_bytes())
        },
    );
    preparations::Preparer {
        inner: preparer,
        counters: preparations::Counters::default(),
    }
}
