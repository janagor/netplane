//! Persistent policy-routing gateway. Run with `cargo run -p netplane-http --example policy_gateway`.

mod backends;
mod config;
mod graph;
mod routing;

#[cfg(test)]
mod tests;

use bytes::Bytes;
use http::{Request, Response};
use netplane::{ControllerOptions, Netplane, ReconfigurationController};
use netplane_http::HttpAdapter;
use tokio::net::TcpListener;
use tower::BoxError;

type HttpRequest = Request<Bytes>;
type HttpResponse = Response<Bytes>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let config = config::GatewayConfig::default();
    let graph = graph::build_graph(
        &config.policy,
        config.outer_capacity,
        config.backend_capacity,
    )?;
    let runtime = Netplane::new(graph, config.runtime_limits)?;
    let controller = ReconfigurationController::new(runtime, ControllerOptions::default());
    let adapter = HttpAdapter::identity(controller.handle(), config.http_limits)?;
    let listener = TcpListener::bind(config.listen).await?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, adapter.router())
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    println!(
        "policy gateway listening on http://{}; press Ctrl-C to stop",
        config.listen
    );
    let early_server_result = tokio::select! {
        result = &mut server => Some(result),
        signal = tokio::signal::ctrl_c() => {
            signal?;
            None
        }
    };
    let drain = controller.shutdown(config.shutdown_grace_period);
    let _ = shutdown_tx.send(());
    let (server_result, drain_result) = if let Some(result) = early_server_result {
        (result, drain.await)
    } else {
        tokio::join!(server, drain)
    };
    server_result??;
    drain_result?;
    Ok(())
}
