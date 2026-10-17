//! Run with `cargo run -p netplane-http --example reconfigure`.
//! Start `/a` in one client, press Enter to publish v2, request `/a`
//! in another client, then press Enter again to release the old request.

use std::{convert::Infallible, sync::Arc, time::Duration};

use bytes::Bytes;
use http::{Request, Response};
use netplane::{GraphBuilder, Netplane, RuntimeLimits};
use netplane_http::{HttpAdapter, HttpLimits};
use tokio::{net::TcpListener, sync::Notify};
use tower::{BoxError, ServiceBuilder, service_fn};

type HttpRequest = Request<Bytes>;
type HttpResponse = Response<Bytes>;

fn graph(
    version: &'static str,
    hold_a: Option<Arc<Notify>>,
) -> Result<netplane::GraphVersion<HttpRequest, HttpResponse>, BoxError> {
    let backend = move |name: &'static str| {
        let hold_a = hold_a.clone();
        let service = service_fn(move |_: HttpRequest| {
            let hold_a = hold_a.clone();
            async move {
                if name == "a" {
                    if let Some(release) = hold_a {
                        release.notified().await;
                    }
                }
                Ok::<_, Infallible>(Response::new(Bytes::from(format!("{name}:{version}"))))
            }
        });
        ServiceBuilder::new()
            .map_response(move |mut response: HttpResponse| {
                response
                    .headers_mut()
                    .insert("x-policy", http::HeaderValue::from_static(version));
                response
            })
            .service(service)
    };
    Ok(GraphBuilder::new()
        .service("a", backend("a"))
        .service("b", backend("b"))
        .service("fallback", backend("fallback"))
        .router("routes", |request: &HttpRequest| {
            match request.uri().path() {
                "/a" => "a",
                "/b" => "b",
                _ => "fallback",
            }
        })
        .route("routes", "a", "a")
        .route("routes", "b", "b")
        .fallback("routes", "fallback")
        .entry("routes")
        .validate()?)
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let release_old = Arc::new(Notify::new());
    let mut runtime = Netplane::new(
        graph("v1", Some(Arc::clone(&release_old)))?,
        RuntimeLimits {
            max_outstanding: 128,
        },
    )?;
    let adapter = HttpAdapter::identity(runtime.handle(), HttpLimits::default())?;
    let listener = TcpListener::bind("127.0.0.1:3000").await?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, adapter.router())
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    println!("listening on http://127.0.0.1:3000; start a /a request, then press Enter");
    read_enter().await?;
    runtime.publish(graph("v2", None)?)?;
    println!("v2 published; request /a again, then press Enter to release the old request");
    read_enter().await?;
    release_old.notify_one();
    println!("old request released; press Ctrl-C to stop");
    tokio::signal::ctrl_c().await?;
    let drain = runtime.shutdown(Duration::from_secs(10));
    let _ = shutdown_tx.send(());
    let (server_result, drain_result) = tokio::join!(server, drain);
    server_result??;
    drain_result?;
    Ok(())
}

async fn read_enter() -> Result<(), BoxError> {
    tokio::task::spawn_blocking(|| {
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)
    })
    .await??;
    Ok(())
}
