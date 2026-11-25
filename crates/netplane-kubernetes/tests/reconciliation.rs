//! Cluster-independent HTTP publication and lifetime tests.
#[path = "reconciliation/health.rs"]
mod health;
mod support;
use bytes::Bytes;
use netplane_control::{ApplyOutcome, GraphPreparer};
use netplane_http::{HttpAdapter, HttpLimits};
use netplane_kubernetes::{HttpGraphPreparer, UpstreamLimits, map_http_error};
use std::time::Duration;
use support::*;

#[tokio::test]
async fn publication_retains_accepted_version_and_empty_recovers() -> Result {
    tokio::time::timeout(Duration::from_secs(8), async {
        let a = Server::start("A", true).await?;
        let b = Server::start("B", false).await?;
        let (controller, reconciler) = runtime().await?;
        reconciler.apply(desired(&[a.address])?).await?;
        let handle = controller.handle();
        let unpolled = handle.execute(request());
        let in_flight = tokio::spawn(handle.execute(request()));
        a.entered.acquire().await?.forget();
        reconciler.apply(desired(&[b.address])?).await?;
        assert_eq!(
            handle.execute(request()).await?.body(),
            &Bytes::from_static(b"B")
        );
        assert!(matches!(
            reconciler.apply(desired(&[b.address])?).await?,
            ApplyOutcome::Unchanged(_)
        ));
        a.release.add_permits(2);
        assert_eq!(in_flight.await??.body(), &Bytes::from_static(b"A"));
        assert_eq!(unpolled.await?.body(), &Bytes::from_static(b"A"));
        reconciler.apply(desired(&[])?).await?;
        let adapter = HttpAdapter::identity(handle.clone(), HttpLimits::default())?
            .with_error_mapper(map_http_error);
        let response = adapter
            .execute(http::Request::new(axum::body::Body::empty()))
            .await;
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        reconciler.apply(desired(&[b.address])?).await?;
        assert_eq!(
            handle.execute(request()).await?.body(),
            &Bytes::from_static(b"B")
        );
        controller.shutdown(Duration::from_secs(1)).await?;
        a.stop().await?;
        b.stop().await?;
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn selected_endpoint_capacity_cancellation_and_churn() -> Result {
    tokio::time::timeout(Duration::from_secs(8), async {
        let server = Server::start("A", true).await?;
        let (controller, reconciler) = runtime().await?;
        reconciler.apply(desired(&[server.address])?).await?;
        let handle = controller.handle();
        let first = tokio::spawn(handle.execute(request()));
        server.entered.acquire().await?.forget();
        let mut queued = Box::pin(handle.execute(request()));
        assert!(futures_util::poll!(&mut queued).is_pending());
        assert_eq!(server.entered.available_permits(), 0);
        drop(queued);
        first.abort();
        assert!(first.await.is_err());
        server.release.add_permits(1);
        for _ in 0..30 {
            reconciler.apply(desired(&[])?).await?;
            reconciler.apply(desired(&[server.address])?).await?;
        }
        // A fresh version is independently bounded and can progress after cancellation.
        server.release.add_permits(1);
        assert_eq!(
            handle.execute(request()).await?.body(),
            &Bytes::from_static(b"A")
        );
        controller.shutdown(Duration::from_secs(1)).await?;
        server.stop().await?;
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn oversized_candidate_keeps_last_valid_graph() -> Result {
    let a = Server::start("A", false).await?;
    let (controller, _) = runtime().await?;
    let preparer = HttpGraphPreparer::new(
        UpstreamLimits {
            max_backends: 1,
            ..UpstreamLimits::default()
        },
        |request| request.uri().path().as_bytes(),
    );
    let reconciler = netplane_control::Reconciler::new(controller.clone(), preparer);
    reconciler.apply(desired(&[a.address])?).await?;
    let mut other = a.address;
    other.set_port(a.address.port().saturating_sub(1));
    assert!(
        reconciler
            .apply(desired(&[a.address, other])?)
            .await
            .is_err()
    );
    assert_eq!(
        controller.handle().execute(request()).await?.body(),
        &Bytes::from_static(b"A")
    );
    assert!(preparer_invalid().validate(&desired(&[])?).is_err());
    controller.shutdown(Duration::from_secs(1)).await?;
    a.stop().await
}
fn preparer_invalid() -> HttpGraphPreparer {
    HttpGraphPreparer::new(
        UpstreamLimits {
            exchange_timeout: Duration::ZERO,
            ..UpstreamLimits::default()
        },
        |request| request.uri().path().as_bytes(),
    )
}

#[tokio::test]
async fn pod_replacement_isolates_pools_and_old_pool_closes_after_last_request_drop() -> Result {
    use std::sync::Arc;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Semaphore,
    };
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let opened = Arc::new(Semaphore::new(0));
        let closed = Arc::new(Semaphore::new(0));
        let (seen, finished) = (opened.clone(), closed.clone());
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await?;
                seen.add_permits(1);
                let finished = finished.clone();
                connections.spawn(async move {
                    let mut request = Vec::new();
                    let mut byte = [0];
                    while !request.ends_with(b"\r\n\r\n") {
                        socket.read_exact(&mut byte).await?;
                        request.push(byte[0]);
                    }
                    socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nA")
                        .await?;
                    assert_eq!(socket.read(&mut byte).await?, 0);
                    finished.add_permits(1);
                    Ok::<_, std::io::Error>(())
                });
            }
            while let Some(result) = connections.join_next().await {
                result??;
            }
            Ok::<_, tower::BoxError>(())
        });
        let (controller, reconciler) = runtime().await?;
        reconciler
            .apply(desired_for_uid(&[address], "old-pod")?)
            .await?;
        controller.handle().execute(request()).await?;
        opened.acquire().await?.forget();
        let old_request = controller.handle().execute(request());
        reconciler
            .apply(desired_for_uid(&[address], "new-pod")?)
            .await?;
        controller.handle().execute(request()).await?;
        opened.acquire().await?.forget(); // same IP, different Pod UID opens another connection
        assert_eq!(closed.available_permits(), 0); // old accepted future retains its client
        drop(old_request);
        closed.acquire().await?.forget();
        controller.shutdown(Duration::from_secs(1)).await?;
        closed.acquire().await?.forget();
        server.await??;
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn unavailable_graph_handles_repeated_requests_without_poisoning_worker() -> Result {
    let (controller, _) = runtime().await?;
    let adapter = HttpAdapter::identity(controller.handle(), HttpLimits::default())?
        .with_error_mapper(map_http_error);
    for _ in 0..64 {
        assert_eq!(
            adapter
                .execute(http::Request::new(axum::body::Body::empty()))
                .await
                .status(),
            http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(controller.handle().status().outstanding, 0);
    }
    controller.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn saturated_endpoint_does_not_block_another_endpoint() -> Result {
    tokio::time::timeout(Duration::from_secs(5), async {
        let a = Server::start("A", true).await?;
        let b = Server::start("B", false).await?;
        let (controller, reconciler) = runtime().await?;
        let desired = desired(&[a.address, b.address])?;
        let policy = netplane::routing::UniformRouting::try_new(vec![
            "endpoint-0".into(),
            "endpoint-1".into(),
        ])?;
        let a_index = desired
            .backends()
            .iter()
            .position(|backend| backend.id.address == a.address)
            .ok_or("endpoint A")?;
        let key_for = |index: usize| -> Result<String> {
            (0..100)
                .map(|number| format!("/request-{number}"))
                .find(|key| policy.select(key.as_bytes()) == format!("endpoint-{index}"))
                .ok_or_else(|| "key".into())
        };
        let key_a = key_for(a_index)?;
        let key_b = key_for(1 - a_index)?;
        let make_request = |key: &str| -> Result<http::Request<Bytes>> {
            Ok(http::Request::builder().uri(key).body(Bytes::new())?)
        };
        reconciler.apply(desired).await?;
        let handle = controller.handle();
        let first = tokio::spawn(handle.execute(make_request(&key_a)?));
        a.entered.acquire().await?.forget();
        let mut queued = Box::pin(handle.execute(make_request(&key_a)?));
        assert!(futures_util::poll!(&mut queued).is_pending());
        assert_eq!(
            handle.execute(make_request(&key_b)?).await?.body(),
            &Bytes::from_static(b"B")
        );
        assert_eq!(a.entered.available_permits(), 0);
        drop(queued);
        a.release.add_permits(1);
        first.await??;
        controller.shutdown(Duration::from_secs(1)).await?;
        a.stop().await?;
        b.stop().await
    })
    .await?
}
