//! Forward one explicitly bound Kubernetes Service through a persistent HTTP listener.
use bytes::Bytes;
use clap::Parser;
use http::Request;
use netplane::{ControllerOptions, Netplane, ReconfigurationController, RuntimeLimits};
use netplane_control::{GraphPreparer, Reconciler, ReconciliationDriver, UpdatePolicy};
use netplane_http::{HttpAdapter, HttpLimits};
use netplane_kubernetes::{
    Discovery, DiscoveryConfig, DiscoverySnapshot, HttpGraphPreparer, KubernetesIntegration,
    PortSelector, ServiceBinding, UpstreamLimits, map_http_error, translate,
};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tower::BoxError;

#[derive(Parser)]
struct Arguments {
    #[arg(long, default_value = "default")]
    namespace: String,
    #[arg(long)]
    service: String,
    #[arg(
        long,
        required_unless_present = "port_number",
        conflicts_with = "port_number"
    )]
    port_name: Option<String>,
    #[arg(long, required_unless_present = "port_name")]
    port_number: Option<u16>,
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    #[arg(long, default_value = "32")]
    backend_capacity: NonZeroUsize,
    #[arg(long, default_value = "1024")]
    max_outstanding: usize,
    #[arg(long, default_value = "256")]
    max_backends: usize,
}
#[derive(Clone)]
struct RoutingKey([u8; 8]);
static NEXT_KEY: AtomicU64 = AtomicU64::new(1);
fn routing_key(request: &Request<Bytes>) -> &[u8] {
    request.headers().get("x-request-key").map_or_else(
        || {
            request
                .extensions()
                .get::<RoutingKey>()
                .map_or(&[][..], |key| &key.0)
        },
        |key| key.as_bytes(),
    )
}
fn assign_routing_key(mut request: Request<Bytes>) -> Request<Bytes> {
    request.extensions_mut().insert(RoutingKey(
        NEXT_KEY.fetch_add(1, Ordering::Relaxed).to_le_bytes(),
    ));
    request
}

fn binding(args: &Arguments) -> Result<ServiceBinding, BoxError> {
    let port = match (&args.port_name, args.port_number) {
        (Some(name), _) => PortSelector::Name(name.clone()),
        (_, Some(number)) => PortSelector::Number(number),
        _ => return Err("select --port-name or --port-number".into()),
    };
    Ok(ServiceBinding {
        namespace: args.namespace.clone(),
        service_name: args.service.clone(),
        port,
        backend_capacity: args.backend_capacity,
    })
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .try_init()?;
    let args = Arguments::parse();
    let binding = binding(&args)?;
    let initial = translate(
        &DiscoverySnapshot {
            revision: 0,
            services: BTreeMap::new(),
        },
        &binding,
    )?;
    let preparer = HttpGraphPreparer::new(
        UpstreamLimits {
            max_backends: args.max_backends,
            ..UpstreamLimits::default()
        },
        routing_key,
    );
    let runtime = Netplane::new(
        preparer.prepare(initial).await?,
        RuntimeLimits {
            max_outstanding: args.max_outstanding,
        },
    )?;
    let controller = Arc::new(ReconfigurationController::new(
        runtime,
        ControllerOptions {
            max_retained_versions: 0,
        },
    ));
    let (driver, updates) = ReconciliationDriver::new(
        Reconciler::new(controller.clone(), preparer),
        UpdatePolicy::default(),
    )?;
    let mut reconciliation_status = driver.status();
    let (discovery, handle) = Discovery::new(
        kube::Client::try_default().await?,
        DiscoveryConfig {
            namespace: args.namespace,
            service_names: [args.service].into(),
        },
    )?;
    let integration = KubernetesIntegration::new(&handle, binding, updates.clone());
    let mut integration_status = integration.status();
    let adapter = HttpAdapter::new(
        controller.handle(),
        HttpLimits::default(),
        |request| Ok(assign_routing_key(request)),
        Ok,
    )?
    .with_error_mapper(map_http_error);
    let listener = TcpListener::bind(args.listen).await?;
    println!(
        "listening={} key=x-request-key (defaults to a unique key per request)",
        listener.local_addr()?
    );
    let stop = CancellationToken::new();
    let mut tasks = JoinSet::<Result<(), BoxError>>::new();
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
    let token = stop.clone();
    tasks.spawn(async move {
        axum::serve(listener, adapter.router())
            .with_graceful_shutdown(token.cancelled_owned())
            .await?;
        Ok(())
    });
    let outcome = loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => break signal.map_err(BoxError::from),
            result = tasks.join_next() => break match result {
                Some(Ok(result)) => result,
                Some(Err(error)) => Err(error.into()),
                None => Ok(()),
            },
            changed = integration_status.changed() => {
                if changed.is_ok() { println!("discovery={:?}", *integration_status.borrow_and_update()); }
            }
            changed = reconciliation_status.changed() => {
                if changed.is_ok() { println!("reconciliation={:?}", *reconciliation_status.borrow_and_update()); }
            }
        }
    };
    shutdown(&stop, &updates, &controller, &mut tasks).await?;
    outcome
}

async fn shutdown(
    stop: &CancellationToken,
    updates: &netplane_control::UpdateSender<netplane_kubernetes::DesiredState>,
    controller: &ReconfigurationController<Request<Bytes>, http::Response<Bytes>>,
    tasks: &mut JoinSet<Result<(), BoxError>>,
) -> Result<(), BoxError> {
    stop.cancel();
    updates.shutdown();
    // Close admission immediately while accepted requests retain their versions.
    let drained = controller.shutdown(Duration::from_secs(10)).await;
    let joined = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok::<_, BoxError>(())
    })
    .await;
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    drained?;
    joined??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{assign_routing_key, routing_key};
    use bytes::Bytes;
    use http::Request;

    #[test]
    fn repeated_paths_get_distinct_default_keys_and_explicit_keys_stay_sticky() {
        let first = assign_routing_key(Request::new(Bytes::new()));
        let second = assign_routing_key(Request::new(Bytes::new()));
        assert_ne!(routing_key(&first), routing_key(&second));
        let mut sticky = Request::new(Bytes::new());
        sticky.headers_mut().insert(
            "x-request-key",
            http::HeaderValue::from_static("stable-key"),
        );
        let sticky = assign_routing_key(sticky);
        assert_eq!(routing_key(&sticky), b"stable-key");
    }
}
