use super::{
    Result, service, slice,
    support::{Harness, bounded, runner},
};
use crate::WatchStatus;
use kube::runtime::watcher::{Error, Event};

fn failure() -> Error {
    Error::WatchError(
        kube::core::Status::failure("expired", "Expired")
            .with_code(410)
            .boxed(),
    )
}

#[tokio::test]
async fn updates_publish_from_committed_caches_during_either_watchers_recovery() -> Result {
    bounded(async {
        for service_failure in [true, false] {
            let harness = Harness::initialized().await?;
            let before = harness.handle.snapshot().ok_or("snapshot")?;
            if service_failure {
                harness.services.send(Err(failure())).await?;
                let mut updated = slice()?;
                updated.endpoints[0].conditions = None;
                harness.slices.send(Ok(Event::Apply(updated))).await?;
                assert_eq!(harness.handle.status().services, WatchStatus::Recovering);
            } else {
                harness.slices.send(Err(failure())).await?;
                let mut updated = service()?;
                updated
                    .spec
                    .as_mut()
                    .ok_or("spec")?
                    .publish_not_ready_addresses = Some(true);
                harness.services.send(Ok(Event::Apply(updated))).await?;
                assert_eq!(
                    harness.handle.status().endpoint_slices,
                    WatchStatus::Recovering
                );
            }
            let after = harness.handle.snapshot().ok_or("snapshot")?;
            assert_eq!(after.revision, before.revision + 1);
            assert_eq!(
                after
                    .services
                    .values()
                    .next()
                    .ok_or("service")?
                    .endpoints
                    .len(),
                1
            );
            harness.stop().await?;
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn simultaneous_relists_publish_only_after_both_commit() -> Result {
    bounded(async {
        let harness = Harness::initialized().await?;
        harness.services.send(Ok(Event::Init)).await?;
        let mut updated = service()?;
        updated
            .spec
            .as_mut()
            .ok_or("spec")?
            .publish_not_ready_addresses = Some(true);
        harness.services.send(Ok(Event::InitApply(updated))).await?;
        harness.slices.send(Ok(Event::Init)).await?;
        harness.services.send(Ok(Event::InitDone)).await?;
        let before = harness.handle.snapshot().ok_or("snapshot")?;
        assert_eq!(before.revision, 1);
        assert_eq!(
            before
                .services
                .values()
                .next()
                .ok_or("service")?
                .endpoints
                .len(),
            1
        );
        harness.slices.send(Ok(Event::InitDone)).await?;
        let after = harness.handle.snapshot().ok_or("snapshot")?;
        assert_eq!(after.revision, 2);
        let service = after.services.values().next().ok_or("service")?;
        assert!(service.publish_not_ready_addresses);
        assert!(service.endpoints.is_empty());
        harness.stop().await
    })
    .await
}

#[tokio::test]
async fn interrupted_relist_discards_staging_through_runner() -> Result {
    bounded(async {
        let harness = Harness::initialized().await?;
        harness.services.send(Ok(Event::Init)).await?;
        let mut stale = service()?;
        stale.metadata.name = Some("stale".into());
        harness.services.send(Ok(Event::InitApply(stale))).await?;
        harness.services.send(Err(failure())).await?;
        assert_eq!(harness.handle.snapshot().ok_or("snapshot")?.revision, 1);
        harness.services.send(Ok(Event::Init)).await?;
        harness
            .services
            .send(Ok(Event::InitApply(service()?)))
            .await?;
        harness.services.send(Ok(Event::InitDone)).await?;
        let after = harness.handle.snapshot().ok_or("snapshot")?;
        assert_eq!(after.revision, 1);
        assert_eq!(after.services.len(), 1);
        assert_eq!(harness.handle.status().services, WatchStatus::Watching);
        harness.stop().await
    })
    .await
}

#[tokio::test]
async fn recreation_rejects_old_slice_and_stale_service_delete() -> Result {
    bounded(async {
        let harness = Harness::initialized().await?;
        harness.services.send(Ok(Event::Delete(service()?))).await?;
        assert!(
            harness
                .handle
                .snapshot()
                .ok_or("snapshot")?
                .services
                .is_empty()
        );
        let mut recreated = service()?;
        recreated.metadata.uid = Some("uid-2".into());
        harness.services.send(Ok(Event::Apply(recreated))).await?;
        harness.services.send(Ok(Event::Delete(service()?))).await?;
        let current = harness.handle.snapshot().ok_or("snapshot")?;
        let service = current.services.values().next().ok_or("service")?;
        assert_eq!(service.uid, "uid-2");
        assert!(service.endpoints.is_empty());
        let mut updated_slice = slice()?;
        updated_slice
            .metadata
            .owner_references
            .as_mut()
            .ok_or("owner")?[0]
            .uid = "uid-2".into();
        harness.slices.send(Ok(Event::Apply(updated_slice))).await?;
        assert_eq!(
            harness
                .handle
                .snapshot()
                .ok_or("snapshot")?
                .services
                .values()
                .next()
                .ok_or("service")?
                .endpoints
                .len(),
            1
        );
        harness.stop().await
    })
    .await
}

#[tokio::test]
async fn terminal_watch_failure_retains_last_snapshot() -> Result {
    bounded(async {
        let harness = Harness::initialized().await?;
        let before = harness.handle.snapshot().ok_or("snapshot")?;
        drop(harness.services);
        assert!(harness.task.await?.is_err());
        assert_eq!(harness.handle.status().services, WatchStatus::Failed);
        let after = harness.handle.snapshot().ok_or("snapshot")?;
        assert!(std::sync::Arc::ptr_eq(&before, &after));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn runner_dropped_before_execution_closes_channels() -> Result {
    bounded(async {
        let (discovery, handle) = runner()?;
        let snapshots = handle.subscribe();
        drop(discovery);
        assert!(handle.snapshot().is_none());
        assert_eq!(handle.status().services, WatchStatus::Stopped);
        assert_eq!(handle.status().endpoint_slices, WatchStatus::Stopped);
        assert!(snapshots.has_changed().is_err());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn equivalent_unnamed_port_updates_retain_snapshot_and_revision() -> Result {
    bounded(async {
        let harness = Harness::initialized().await?;
        let mut svc = service()?;
        svc.spec
            .as_mut()
            .ok_or("spec")?
            .ports
            .as_mut()
            .ok_or("ports")?[0]
            .name = None;
        let mut endpoint_slice = slice()?;
        endpoint_slice.ports.as_mut().ok_or("ports")?[0].name = None;
        harness.services.send(Ok(Event::Apply(svc.clone()))).await?;
        harness
            .slices
            .send(Ok(Event::Apply(endpoint_slice.clone())))
            .await?;
        let before = harness.handle.snapshot().ok_or("snapshot")?;
        assert_eq!(
            before
                .services
                .values()
                .next()
                .ok_or("service")?
                .endpoints
                .len(),
            1
        );
        for name in [Some(String::new()), None] {
            svc.spec
                .as_mut()
                .ok_or("spec")?
                .ports
                .as_mut()
                .ok_or("ports")?[0]
                .name
                .clone_from(&name);
            harness.services.send(Ok(Event::Apply(svc.clone()))).await?;
            assert!(std::sync::Arc::ptr_eq(
                &before,
                &harness.handle.snapshot().ok_or("snapshot")?
            ));
            endpoint_slice.ports.as_mut().ok_or("ports")?[0].name = name;
            harness
                .slices
                .send(Ok(Event::Apply(endpoint_slice.clone())))
                .await?;
            let after = harness.handle.snapshot().ok_or("snapshot")?;
            assert_eq!(before.revision, after.revision);
            assert!(std::sync::Arc::ptr_eq(&before, &after));
        }
        harness.stop().await
    })
    .await
}
