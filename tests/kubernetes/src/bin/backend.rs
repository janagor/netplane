//! Controllable HTTP workload used only by the local cluster suite.
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::watch;
use tower::BoxError;

struct Backend {
    ready: AtomicBool,
    active: AtomicUsize,
    completed: AtomicUsize,
    release: watch::Sender<u64>,
}
fn identity() -> Json<Value> {
    Json(json!({"pod": std::env::var("POD_NAME").unwrap_or_default(),
        "version": std::env::var("APP_VERSION").unwrap_or_default()}))
}
struct ActiveRequest(Arc<Backend>);
impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
async fn block(State(state): State<Arc<Backend>>) -> Json<Value> {
    let mut release = state.release.subscribe();
    state.active.fetch_add(1, Ordering::SeqCst);
    let _active = ActiveRequest(state.clone());
    let _ = release.changed().await;
    state.completed.fetch_add(1, Ordering::SeqCst);
    identity()
}
async fn status(State(state): State<Arc<Backend>>) -> Json<Value> {
    Json(json!({"active": state.active.load(Ordering::SeqCst),
        "completed": state.completed.load(Ordering::SeqCst)}))
}
async fn readiness(State(state): State<Arc<Backend>>, Json(ready): Json<bool>) {
    state.ready.store(ready, Ordering::SeqCst);
}
async fn ready(State(state): State<Arc<Backend>>) -> StatusCode {
    if state.ready.load(Ordering::SeqCst) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn release(State(state): State<Arc<Backend>>) {
    state.release.send_modify(|generation| *generation += 1);
}
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let state = Arc::new(Backend {
        ready: AtomicBool::new(true),
        active: AtomicUsize::new(0),
        completed: AtomicUsize::new(0),
        release: watch::channel(0).0,
    });
    let router = Router::new()
        .route("/identity", get(|| async { identity() }))
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(ready))
        .route("/test/readiness", post(readiness))
        .route("/test/block", get(block))
        .route("/test/release", post(release))
        .route("/test/status", get(status))
        .with_state(state);
    axum::serve(tokio::net::TcpListener::bind("0.0.0.0:8080").await?, router).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cancelled_block_releases_active_request_count() {
        let state = Arc::new(Backend {
            ready: AtomicBool::new(true),
            active: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            release: watch::channel(0).0,
        });
        {
            let blocked = block(State(state.clone()));
            tokio::pin!(blocked);
            assert!(futures_util::poll!(blocked.as_mut()).is_pending());
            assert_eq!(state.active.load(Ordering::SeqCst), 1);
        }
        assert_eq!(state.active.load(Ordering::SeqCst), 0);
        assert_eq!(state.completed.load(Ordering::SeqCst), 0);
    }
}
