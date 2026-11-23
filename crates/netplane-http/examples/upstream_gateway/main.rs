//! Gateway to independent HTTP upstreams; see the adjacent README for commands.
mod config;
mod graph;
#[cfg(test)]
mod tests;
mod upstream;

use netplane::{Netplane, RuntimeLimits};
use netplane_http::{HttpAdapter, HttpLimits};
use std::time::Duration;
use tower::BoxError;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let client = upstream::client(config::EXCHANGE_TIMEOUT)?;
    let stable = config::origin("NETPLANE_STABLE", "http://127.0.0.1:4001")?;
    let canary = config::origin("NETPLANE_CANARY", "http://127.0.0.1:4002")?;
    let build = |stable: &str, canary: &str| {
        graph::build(
            upstream::HttpUpstream::new(client.clone(), stable, config::RESPONSE_LIMIT)?,
            upstream::HttpUpstream::new(client.clone(), canary, config::RESPONSE_LIMIT)?,
        )
    };
    let mut runtime = Netplane::new(
        build(&stable, &canary)?,
        RuntimeLimits {
            max_outstanding: 64,
        },
    )?;
    let adapter = HttpAdapter::identity(runtime.handle(), HttpLimits::default())?
        .with_error_mapper(upstream::map_error);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, adapter.router())
            .with_graceful_shutdown(async {
                let _ = stop_rx.await;
            })
            .await
    });
    println!("gateway http://127.0.0.1:3000; enter swaps stable/canary origins; Ctrl-C drains");
    let mut lines =
        tokio::io::AsyncBufReadExt::lines(tokio::io::BufReader::new(tokio::io::stdin()));
    let mut swapped = false;
    let mut stdin_open = true;
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => { signal?; break; }
            result = &mut server => { result??; return Err("listener exited".into()); }
            line = lines.next_line(), if stdin_open => {
                if line?.is_none() { stdin_open = false; continue; }
                swapped = !swapped;
                runtime.publish(if swapped { build(&canary, &stable)? } else { build(&stable, &canary)? })?;
                println!("published replacement; origins swapped={swapped}");
            }
        }
    }
    let drain = runtime.shutdown(Duration::from_secs(15));
    let _ = stop_tx.send(());
    drain.await?;
    server.await??;
    Ok(())
}
