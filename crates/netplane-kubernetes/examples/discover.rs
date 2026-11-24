//! Print namespace-scoped topology until Ctrl-C.
use clap::Parser;
use netplane_kubernetes::{Discovery, DiscoveryConfig};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
struct Arguments {
    #[arg(long, default_value = "default")]
    namespace: String,
    #[arg(long = "service")]
    services: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Arguments::parse();
    let client = kube::Client::try_default().await?;
    let (discovery, handle) = Discovery::new(
        client,
        DiscoveryConfig {
            namespace: args.namespace,
            service_names: args.services.into_iter().collect(),
        },
    )?;
    let cancellation = CancellationToken::new();
    let mut snapshots = handle.subscribe();
    let mut status = handle.subscribe_status();
    let mut task = tokio::spawn(discovery.run(cancellation.clone()));
    loop {
        tokio::select! {
            result = &mut task => { result??; return Ok(()); }
            signal = tokio::signal::ctrl_c() => { signal?; cancellation.cancel(); break; }
            changed = status.changed() => {
                if changed.is_ok() {
                    let current_status = status.borrow_and_update().clone();
                    println!("Discovery status: {current_status:?}");
                }
            }
            changed = snapshots.changed() => {
                if changed.is_err() { break; }
                let snapshot = snapshots.borrow_and_update().clone();
                if let Some(snapshot) = snapshot {
                    println!("Discovery revision: {}", snapshot.revision);
                    for service in snapshot.services.values() {
                        println!("{}/{}", service.key.namespace, service.key.name);
                        for endpoint in &service.endpoints {
                            println!("  {}  {:?}  {}", std::net::SocketAddr::new(endpoint.address, endpoint.port), endpoint.protocol, if endpoint.is_ready() { "ready" } else { "ineligible" });
                        }
                    }
                }
            }
        }
    }
    cancellation.cancel();
    task.await??;
    Ok(())
}
