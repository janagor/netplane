use axum::{
    Router,
    body::Bytes,
    extract::State,
    response::IntoResponse,
    routing::{get, post},
};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Default)]
pub struct Gate {
    pub entered: Notify,
    pub release: Notify,
}

#[derive(Clone)]
struct Backend {
    name: String,
    gate: Arc<Gate>,
}

pub fn router(name: String, gate: Arc<Gate>) -> Router {
    Router::new()
        .route("/process", post(process))
        .route("/health", get(|| async { "ok" }))
        .route(
            "/failure",
            get(|| async { http::StatusCode::SERVICE_UNAVAILABLE }),
        )
        .route("/slow", get(slow))
        .with_state(Backend { name, gate })
}

async fn process(State(backend): State<Backend>, body: Bytes) -> impl IntoResponse {
    (
        [("x-backend", backend.name.clone())],
        format!("backend={};processed_bytes={}", backend.name, body.len()),
    )
}

async fn slow(State(backend): State<Backend>) -> impl IntoResponse {
    backend.gate.entered.notify_one();
    backend.gate.release.notified().await;
    ([("x-backend", backend.name.clone())], backend.name)
}
