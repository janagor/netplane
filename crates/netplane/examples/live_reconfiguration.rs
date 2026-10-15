//! Run with `cargo run -p netplane --example live_reconfiguration`.

use std::{convert::Infallible, error::Error, io, time::Duration};
use tokio::sync::oneshot;
use tower::{ServiceBuilder, ServiceExt, filter::FilterLayer, service_fn};

use netplane::{Capacity, GraphDraft, Netplane};

#[derive(Clone, Copy)]
/// Request data used by both published compositions.
pub struct Request {
    /// Backend routing key.
    pub key: u32,
    /// Whether the version B simulated authorization filter permits the request.
    pub authorized: bool,
}

/// Observed outputs from one version transition.
pub struct Results {
    /// Response from the request selected on version A.
    pub old_request: String,
    /// Response routed to Backend A on version B.
    pub routed_a: String,
    /// Response routed to Backend B on version B.
    pub routed_b: String,
    /// Whether version B rejected an unauthorized request.
    pub rejected: bool,
}

const CAPACITY: Capacity = Capacity {
    max_in_flight: 2,
    buffer_capacity: 4,
};

/// Execute one transition while an old-version request is in progress.
///
/// # Errors
/// Returns an error if setup, publication, execution, or synchronization fails.
pub async fn run_demo() -> Result<Results, Box<dyn Error + Send + Sync>> {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let mut entered_tx = Some(entered_tx);
    let mut release_rx = Some(release_rx);

    // Version A has one direct backend. Its first request pauses after dispatch.
    let direct_a = service_fn(move |request: Request| {
        let entered = entered_tx.take();
        let release = release_rx.take();
        async move {
            if let Some(entered) = entered {
                let _ = entered.send(());
            }
            if let Some(release) = release {
                let _ = release.await;
            }
            Ok::<_, Infallible>(format!("A:{}", request.key))
        }
    });
    let graph_a = GraphDraft::new(direct_a).capacity(CAPACITY).validate()?;
    let mut runtime = Netplane::new(graph_a);
    let handle = runtime.handle();
    let old_request = tokio::spawn(handle.execute(Request {
        key: 1,
        authorized: false,
    }));
    entered_rx.await?;

    // Version B checks simulated authorization, then routes even keys to A and odd keys to B.
    let backend_a =
        service_fn(
            |request: Request| async move { Ok::<_, Infallible>(format!("A:{}", request.key)) },
        );
    let backend_b =
        service_fn(
            |request: Request| async move { Ok::<_, Infallible>(format!("B:{}", request.key)) },
        );
    let router = service_fn(move |request: Request| {
        let a = backend_a;
        let b = backend_b;
        async move {
            if request.key.is_multiple_of(2) {
                a.oneshot(request).await
            } else {
                b.oneshot(request).await
            }
        }
    });
    let authorized_router = ServiceBuilder::new()
        .layer(FilterLayer::new(|request: Request| {
            request
                .authorized
                .then_some(request)
                .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "unauthorized"))
        }))
        .service(router);
    let graph_b = GraphDraft::new(authorized_router)
        .capacity(CAPACITY)
        .validate()?;
    runtime.publish(graph_b)?;

    let routed_a = handle
        .execute(Request {
            key: 2,
            authorized: true,
        })
        .await?;
    let routed_b = handle
        .execute(Request {
            key: 3,
            authorized: true,
        })
        .await?;
    let rejected = match handle
        .execute(Request {
            key: 4,
            authorized: false,
        })
        .await
    {
        Err(error) => error
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.kind() == io::ErrorKind::PermissionDenied),
        Ok(_) => false,
    };
    release_tx
        .send(())
        .map_err(|()| io::Error::other("old request finished before release"))?;
    let old_request = old_request.await??;
    runtime.shutdown(Duration::from_secs(5)).await?;

    Ok(Results {
        old_request,
        routed_a,
        routed_b,
        rejected,
    })
}

#[cfg(not(test))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let results = run_demo().await?;
    println!("old request: {}", results.old_request);
    println!("new route A: {}", results.routed_a);
    println!("new route B: {}", results.routed_b);
    println!("unauthorized request rejected: {}", results.rejected);
    Ok(())
}
