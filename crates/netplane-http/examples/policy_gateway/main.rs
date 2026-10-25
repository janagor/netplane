//! Persistent policy-routing gateway. Run with `cargo run -p netplane-http --example policy_gateway`.

mod admin;
mod backends;
mod config;
mod control;
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
    let admin_token = configured_admin_token(std::env::var("NETPLANE_ADMIN_TOKEN"))?;
    let public_listener = TcpListener::bind(config.listen).await?;
    let admin_listener = if admin_token.is_some() {
        Some(TcpListener::bind("127.0.0.1:3001").await?)
    } else {
        None
    };
    let graph = graph::build_graph(
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
    let control = std::sync::Arc::new(control::GatewayControl::new(
        controller,
        config.clone(),
        drain_tx,
    )?);
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut public_stop = stop_rx.clone();
    let public_server = tokio::spawn(async move {
        axum::serve(public_listener, adapter.router())
            .with_graceful_shutdown(async move {
                while !*public_stop.borrow_and_update() && public_stop.changed().await.is_ok() {}
            })
            .await
    });
    let admin_server = if let (Some(listener), Some(token)) = (admin_listener, admin_token) {
        let router = admin::router(control.clone(), token);
        let mut admin_stop = stop_rx;
        println!("admin listener on http://127.0.0.1:3001");
        Some(tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    while !*admin_stop.borrow_and_update() && admin_stop.changed().await.is_ok() {}
                })
                .await
        }))
    } else {
        None
    };
    println!(
        "policy gateway listening on http://{}; press Ctrl-C to stop",
        config.listen
    );
    supervise(control, drain_rx, stop_tx, public_server, admin_server).await
}

enum Trigger {
    AdminDrain(control::Drain),
    CtrlC(Result<(), std::io::Error>),
    PublicExit(Result<Result<(), std::io::Error>, tokio::task::JoinError>),
    AdminExit(Result<Result<(), std::io::Error>, tokio::task::JoinError>),
}

async fn supervise(
    control: std::sync::Arc<control::GatewayControl>,
    mut drain_rx: tokio::sync::oneshot::Receiver<control::Drain>,
    stop_tx: tokio::sync::watch::Sender<bool>,
    mut public_server: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    mut admin_server: Option<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
) -> Result<(), BoxError> {
    let mut public_finished = false;
    let mut admin_finished = false;
    let mut server_failure: Option<BoxError> = None;
    let trigger = tokio::select! {
        result = &mut public_server => Trigger::PublicExit(result),
        result = async {
            match admin_server.as_mut() {
                Some(server) => server.await,
                None => std::future::pending().await,
            }
        } => Trigger::AdminExit(result),
        signal = tokio::signal::ctrl_c() => Trigger::CtrlC(signal),
        drain = &mut drain_rx => Trigger::AdminDrain(drain?),
    };
    let drain = match trigger {
        Trigger::AdminDrain(drain) => drain,
        Trigger::CtrlC(signal) => {
            if let Err(error) = signal {
                server_failure = Some(Box::new(error));
            }
            control.shutdown().await?;
            drain_rx.await?
        }
        Trigger::PublicExit(result) => {
            public_finished = true;
            server_failure = Some(server_exit("public", result));
            control.shutdown().await?;
            drain_rx.await?
        }
        Trigger::AdminExit(result) => {
            admin_finished = true;
            server_failure = Some(server_exit("admin", result));
            control.shutdown().await?;
            drain_rx.await?
        }
    };
    let _ = stop_tx.send(true);
    let drain_result = drain.await;
    if !public_finished {
        match public_server.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                server_failure.get_or_insert_with(|| Box::new(error));
            }
            Err(error) => {
                server_failure.get_or_insert_with(|| Box::new(error));
            }
        }
    }
    if !admin_finished {
        if let Some(server) = admin_server {
            match server.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    server_failure.get_or_insert_with(|| Box::new(error));
                }
                Err(error) => {
                    server_failure.get_or_insert_with(|| Box::new(error));
                }
            }
        }
    }
    if let Some(error) = server_failure {
        return Err(error);
    }
    drain_result?;
    Ok(())
}

fn server_exit(
    name: &str,
    result: Result<Result<(), std::io::Error>, tokio::task::JoinError>,
) -> BoxError {
    match result {
        Ok(Ok(())) => format!("{name} listener exited unexpectedly").into(),
        Ok(Err(error)) => Box::new(error),
        Err(error) => Box::new(error),
    }
}

fn configured_admin_token(
    value: Result<String, std::env::VarError>,
) -> Result<Option<String>, BoxError> {
    let token = match value {
        Ok(token) => token,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err("NETPLANE_ADMIN_TOKEN must be Unicode".into());
        }
    };
    if token.len() < 16 {
        return Err("NETPLANE_ADMIN_TOKEN must contain at least 16 bytes".into());
    }
    http::HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| "NETPLANE_ADMIN_TOKEN is not valid in an HTTP header")?;
    Ok(Some(token))
}
