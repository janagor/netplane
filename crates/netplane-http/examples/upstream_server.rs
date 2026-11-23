//! Independent loopback upstream: `upstream_server stable 127.0.0.1:4001`.
#[path = "upstream_gateway/server.rs"]
mod server;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), tower::BoxError> {
    let mut args = std::env::args().skip(1);
    let name = args.next().unwrap_or_else(|| "stable".into());
    let address: std::net::SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:4001".into())
        .parse()?;
    if !address.ip().is_loopback() {
        return Err("use a loopback listener".into());
    }
    let listener = tokio::net::TcpListener::bind(address).await?;
    let gate = Arc::new(server::Gate::default());
    let release = gate.clone();
    let router = server::router(name, gate).route(
        "/release",
        axum::routing::post(move || {
            let gate = release.clone();
            async move {
                gate.release.notify_one();
                http::StatusCode::NO_CONTENT
            }
        }),
    );
    println!("upstream http://{address}; POST /release releases one /slow request");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
