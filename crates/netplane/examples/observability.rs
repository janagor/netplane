//! Demonstrates application-owned tracing configuration and runtime lifecycle events.

use std::{convert::Infallible, error::Error, time::Duration};

use netplane::{GraphDraft, Netplane, RuntimeLimits};
use tower::service_fn;
use tracing_subscriber::EnvFilter;

fn graph() -> Result<netplane::GraphVersion<u64, u64>, netplane::ConfigError> {
    GraphDraft::new(service_fn(|value: u64| async move {
        Ok::<_, Infallible>(value)
    }))
    .validate()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("netplane=debug")),
        )
        .init();
    let mut runtime = Netplane::new(graph()?, RuntimeLimits { max_outstanding: 2 })?;
    let handle = runtime.handle();
    let _ = handle.execute(1).await?;
    let cancelled = handle.execute(2);
    runtime.publish(graph()?)?;
    drop(cancelled);
    let _ = handle.execute(3).await?;
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
