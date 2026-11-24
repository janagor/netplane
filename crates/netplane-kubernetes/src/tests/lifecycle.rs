use super::support::{bounded, events, runner};
use super::*;
use crate::WatchStatus;
use futures_util::stream;
use kube::runtime::watcher::{Error, Event};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn synchronization_recovery_and_immutable_snapshots() -> Result {
    bounded(async {
        let (discovery, handle) = runner()?;
        let (services, service_stream) = events();
        let (slices, slice_stream) = events();
        let cancellation = CancellationToken::new();
        let task =
            tokio::spawn(discovery.run_streams(service_stream, slice_stream, cancellation.clone()));
        let mut snapshots = handle.subscribe();
        let slow = handle.subscribe();
        services.send(Ok(Event::Init)).await?;
        services.send(Ok(Event::InitApply(service()?))).await?;
        services.send(Ok(Event::InitDone)).await?;

        assert!(handle.snapshot().is_none());
        slices.send(Ok(Event::Init)).await?;
        slices.send(Ok(Event::InitApply(slice()?))).await?;
        slices.send(Ok(Event::InitDone)).await?;
        snapshots.changed().await?;
        let original = handle.snapshot().ok_or("snapshot")?;
        assert_eq!(original.revision, 1);
        slices
            .send(Err(Error::WatchFailed(kube::Error::Api(
                kube::core::Status::failure("forbidden", "Forbidden")
                    .with_code(403)
                    .boxed(),
            ))))
            .await?;

        assert_eq!(handle.status().endpoint_slices, WatchStatus::Recovering);
        assert_eq!(handle.snapshot().ok_or("snapshot")?.revision, 1);
        slices.send(Ok(Event::Init)).await?;
        slices.send(Ok(Event::InitApply(slice()?))).await?;

        services.send(Ok(Event::Delete(service()?))).await?;

        assert_eq!(handle.snapshot().ok_or("snapshot")?.revision, 1);
        slices.send(Ok(Event::InitDone)).await?;
        snapshots.changed().await?;
        assert!(handle.snapshot().ok_or("snapshot")?.services.is_empty());
        assert_eq!(slow.borrow().as_ref().ok_or("slow snapshot")?.revision, 2);
        assert_eq!(original.services.len(), 1);
        services.send(Ok(Event::Apply(service()?))).await?;
        snapshots.changed().await?;
        let revision = handle.snapshot().ok_or("snapshot")?.revision;
        let mut metadata = service()?;
        metadata.metadata.resource_version = Some("999".into());
        services.send(Ok(Event::Apply(metadata))).await?;

        assert_eq!(handle.snapshot().ok_or("snapshot")?.revision, revision);
        cancellation.cancel();
        task.await??;
        assert_eq!(handle.status().services, WatchStatus::Stopped);
        Ok(())
    })
    .await
}
#[tokio::test]
async fn empty_initialization_and_slice_deletion() -> Result {
    bounded(async {
        let (discovery, handle) = runner()?;
        let (services, service_stream) = events();
        let (slices, slice_stream) = events();
        let cancellation = CancellationToken::new();
        let task =
            tokio::spawn(discovery.run_streams(service_stream, slice_stream, cancellation.clone()));
        let mut snapshots = handle.subscribe();
        slices.send(Ok(Event::Init)).await?;
        slices.send(Ok(Event::InitDone)).await?;

        assert!(handle.snapshot().is_none());
        services.send(Ok(Event::Init)).await?;
        services.send(Ok(Event::InitDone)).await?;
        snapshots.changed().await?;
        assert!(handle.snapshot().ok_or("snapshot")?.services.is_empty());
        services.send(Ok(Event::Apply(service()?))).await?;
        snapshots.changed().await?;
        slices.send(Ok(Event::Apply(slice()?))).await?;
        snapshots.changed().await?;
        slices.send(Ok(Event::Delete(slice()?))).await?;
        snapshots.changed().await?;
        assert!(
            handle
                .snapshot()
                .ok_or("snapshot")?
                .services
                .values()
                .all(|s| s.endpoints.is_empty())
        );
        cancellation.cancel();
        task.await??;
        Ok(())
    })
    .await
}
#[tokio::test]
async fn cancellation_pending_streams_and_termination() -> Result {
    bounded(async {
        let (discovery, handle) = runner()?;
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        discovery
            .run_streams(stream::pending(), stream::pending(), cancellation)
            .await?;
        assert_eq!(handle.status().services, WatchStatus::Stopped);
        let (discovery, handle) = runner()?;
        assert!(
            discovery
                .run_streams(stream::empty(), stream::pending(), CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(handle.status().services, WatchStatus::Failed);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn repeated_errors_relist_and_readiness_updates_preserve_revision() -> Result {
    bounded(async {
        let (discovery, handle) = runner()?;
        let (services, service_stream) = events();
        let (slices, slice_stream) = events();
        let cancellation = CancellationToken::new();
        let task =
            tokio::spawn(discovery.run_streams(service_stream, slice_stream, cancellation.clone()));
        let mut snapshots = handle.subscribe();
        services.send(Ok(Event::Init)).await?;
        services.send(Ok(Event::InitApply(service()?))).await?;
        services.send(Ok(Event::InitDone)).await?;
        slices.send(Ok(Event::Init)).await?;
        slices.send(Ok(Event::InitApply(slice()?))).await?;
        slices.send(Ok(Event::InitDone)).await?;
        snapshots.changed().await?;
        let original = handle.snapshot().ok_or("snapshot")?;
        let mut statuses = handle.subscribe_status();
        for _ in 0..3 {
            slices
                .send(Err(Error::WatchError(
                    kube::core::Status::failure("expired", "Expired")
                        .with_code(410)
                        .boxed(),
                )))
                .await?;
        }
        while handle.status().endpoint_slices != WatchStatus::Recovering {
            statuses.changed().await?;
        }
        assert_eq!(
            handle.snapshot().ok_or("snapshot")?.revision,
            original.revision
        );
        slices.send(Ok(Event::Init)).await?;
        slices.send(Ok(Event::InitApply(slice()?))).await?;
        slices.send(Ok(Event::InitDone)).await?;
        while handle.status().endpoint_slices != WatchStatus::Watching {
            statuses.changed().await?;
        }
        assert_eq!(
            handle.snapshot().ok_or("snapshot")?.revision,
            original.revision
        );
        let mut updated = slice()?;
        updated.endpoints[0].conditions = Some(serde_json::from_value(json!({"ready":false}))?);
        slices.send(Ok(Event::Apply(updated))).await?;
        snapshots.changed().await?;
        let latest = handle.snapshot().ok_or("snapshot")?;
        assert_eq!(latest.revision, original.revision + 1);
        assert!(!latest.services.values().next().ok_or("service")?.endpoints[0].is_ready());
        assert!(
            original
                .services
                .values()
                .next()
                .ok_or("service")?
                .endpoints[0]
                .is_ready()
        );
        cancellation.cancel();
        task.await??;
        Ok(())
    })
    .await
}
