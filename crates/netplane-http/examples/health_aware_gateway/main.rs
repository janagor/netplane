//! Three independently controlled upstreams and a health-aware HTTP gateway.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::State,
    routing::{get, post},
};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use netplane::{
    Capacity, GraphBuilder, GraphVersion, Netplane, RuntimeLimits,
    health::{
        ActiveHealthConfig, BackendIdentity, HealthHandle, HealthLayer, HealthMode, HealthPolicy,
        HealthRegistry, HealthScheduler, HealthState,
    },
    routing::{HealthAwareRouting, WeightedRouting},
};
use netplane_http::{
    HttpAdapter, HttpLimits,
    health::{HttpHealthProbe, HttpOutcomeClassifier},
    upstream::{self, HttpUpstream},
};
use tokio::{net::TcpListener, task::JoinSet};
use tower::{BoxError, Layer, service_fn};

#[cfg(test)]
mod tests;

#[derive(Clone)]
struct UpstreamState {
    name: &'static str,
    failed: Arc<AtomicBool>,
}

fn upstream_router(name: &'static str) -> Router {
    Router::new()
        .route("/health", get(upstream_response))
        .route(
            "/fail",
            post(|State(state): State<UpstreamState>| async move {
                state.failed.store(true, Ordering::Relaxed);
                StatusCode::NO_CONTENT
            }),
        )
        .route(
            "/recover",
            post(|State(state): State<UpstreamState>| async move {
                state.failed.store(false, Ordering::Relaxed);
                StatusCode::NO_CONTENT
            }),
        )
        .fallback(upstream_response)
        .with_state(UpstreamState {
            name,
            failed: Arc::new(AtomicBool::new(false)),
        })
}

async fn upstream_response(State(state): State<UpstreamState>) -> (StatusCode, &'static str) {
    (
        if state.failed.load(Ordering::Relaxed) {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::OK
        },
        state.name,
    )
}

fn build_graph(
    upstreams: Vec<(String, HttpUpstream, HealthHandle)>,
) -> Result<GraphVersion<Request<Bytes>, Response<Bytes>>, BoxError> {
    let weights = [60, 30, 10];
    let weighted = WeightedRouting::try_new(
        upstreams
            .iter()
            .zip(weights)
            .map(|((name, _, _), weight)| (name.clone(), weight)),
    )?;
    let policy = HealthAwareRouting::try_new(
        weighted,
        upstreams
            .iter()
            .map(|(name, _, health)| (name.clone(), health.clone())),
    )?;
    let mut builder = GraphBuilder::new()
        .try_router("weighted", move |request: &Request<Bytes>| {
            let key = request.headers().get("x-routing-key").map_or_else(
                || request.uri().path().as_bytes(),
                http::HeaderValue::as_bytes,
            );
            policy.select(key).map(str::to_owned)
        })
        .entry("weighted")
        .without_outer_capacity()
        .fallback("weighted", "backend-a");
    for (name, upstream, health) in upstreams {
        let service = service_fn(move |request| {
            let upstream = upstream.clone();
            async move { upstream.execute(request).await }
        });
        builder = builder
            .service_with_capacity(
                name.clone(),
                HealthLayer::new(health, HttpOutcomeClassifier::default()).layer(service),
                Capacity {
                    max_in_flight: 16,
                    buffer_capacity: 16,
                },
            )
            .route("weighted", name.clone(), name);
    }
    Ok(builder.validate()?)
}

fn diagnostics(registry: &HealthRegistry, publication: u64) -> serde_json::Value {
    let snapshot = registry.snapshot();
    serde_json::json!({
        "publication_id": publication,
        "active_probes": snapshot.summary.active_probes,
        "health_transitions": snapshot.summary.health_transitions,
        "backends": snapshot.backends.into_iter().map(|(identity, health)| serde_json::json!({
            "id": identity.instance,
            "state": match health.state { HealthState::Healthy => "healthy", HealthState::Unhealthy => "unhealthy", HealthState::Recovering => "recovering" },
            "eligible": health.eligible,
            "consecutive_failures": health.consecutive_failures,
            "consecutive_successes": health.consecutive_successes,
        })).collect::<Vec<_>>()
    })
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let registry = HealthRegistry::default();
    let scheduler = HealthScheduler::new(ActiveHealthConfig {
        interval: Duration::from_secs(1),
        timeout: Duration::from_millis(500),
        max_concurrent_checks: NonZeroUsize::new(2).ok_or("probe concurrency")?,
        max_registered_backends: NonZeroUsize::new(3).ok_or("backend limit")?,
    })?;
    let scheduler_handle = scheduler.handle();
    let scheduler_task = tokio::spawn(scheduler.run());
    let mut servers = JoinSet::new();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let client = upstream::client(Duration::from_secs(2))?;
    let policy = HealthPolicy {
        failure_threshold: NonZeroU32::new(3).ok_or("failure threshold")?,
        recovery_threshold: NonZeroU32::new(2).ok_or("recovery threshold")?,
        recovery_delay: Duration::from_secs(3),
    };
    let mut upstreams = Vec::new();
    let mut registrations = Vec::new();
    for (name, port) in [
        ("backend-a", 4101),
        ("backend-b", 4102),
        ("backend-c", 4103),
    ] {
        let listener = TcpListener::bind(("127.0.0.1", port)).await?;
        let mut stopped = stopped.clone();
        servers.spawn(async move {
            axum::serve(listener, upstream_router(name))
                .with_graceful_shutdown(async move {
                    let _ = stopped.changed().await;
                })
                .await
        });
        let health = registry.register_with_mode(
            BackendIdentity {
                service: "demo".into(),
                instance: name.into(),
                incarnation: "demo-start".into(),
            },
            policy.clone(),
            HealthMode::Active,
        )?;
        let upstream =
            HttpUpstream::new(client.clone(), &format!("http://127.0.0.1:{port}"), 1024)?;
        registrations.push(scheduler_handle.register(
            health.clone(),
            HttpHealthProbe::new(upstream.clone(), "/health")?,
        )?);
        upstreams.push((name.into(), upstream, health));
    }
    let mut runtime = Netplane::new(
        build_graph(upstreams)?,
        RuntimeLimits {
            max_outstanding: 64,
        },
    )?;
    let publication = runtime
        .active_version_id()
        .ok_or("active publication")?
        .get();
    let adapter = HttpAdapter::identity(runtime.handle(), HttpLimits::default())?
        .with_error_mapper(upstream::map_error);
    let app = Router::new()
        .route(
            "/admin/health",
            get(move || {
                let registry = registry.clone();
                async move { axum::Json(diagnostics(&registry, publication)) }
            }),
        )
        .fallback_service(adapter.router());
    let listener = TcpListener::bind("127.0.0.1:4100").await?;
    let (gateway_stop, gateway_stopped) = tokio::sync::oneshot::channel();
    servers.spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = gateway_stopped.await;
            })
            .await
    });
    println!(
        "gateway :4100; diagnostics /admin/health; upstreams :4101/:4102/:4103 POST /fail or /recover; Ctrl-C drains"
    );
    tokio::signal::ctrl_c().await?;
    let drain = runtime.shutdown(Duration::from_secs(10));
    let _ = gateway_stop.send(());
    drain.await?;
    scheduler_handle.shutdown();
    scheduler_task.await?;
    let _ = stop.send(true);
    drop(registrations);
    while let Some(server) = servers.join_next().await {
        server??;
    }
    Ok(())
}
