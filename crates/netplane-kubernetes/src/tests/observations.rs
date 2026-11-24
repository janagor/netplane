use super::{
    Result, service, slice,
    support::{Harness, bounded},
};
use crate::WatchStatus;
use kube::runtime::watcher::{Error, Event};

#[tokio::test]
async fn observation_pairs_latest_snapshot_with_health_and_relist_completion() -> Result {
    bounded(async {
        let harness = Harness::initialized().await?;
        let observations = harness.handle.subscribe_observations();
        let initial = observations.borrow().clone();
        assert_eq!(
            initial.status.latest_revision,
            initial.snapshot.as_ref().map(|snapshot| snapshot.revision)
        );
        assert_eq!(initial.status.services, WatchStatus::Watching);
        harness
            .services
            .send(Err(Error::WatchFailed(kube::Error::Api(
                kube::core::Status::failure("offline", "Disconnected")
                    .with_code(503)
                    .boxed(),
            ))))
            .await?;
        let disconnected = observations.borrow().clone();
        assert_eq!(disconnected.status.services, WatchStatus::Recovering);
        assert_eq!(disconnected.snapshot, initial.snapshot);
        let mut changed = slice()?;
        changed.endpoints.clear();
        harness.slices.send(Ok(Event::Apply(changed))).await?;
        let degraded = observations.borrow().clone();
        assert_eq!(degraded.status.services, WatchStatus::Recovering);
        assert_eq!(
            degraded.status.latest_revision,
            degraded.snapshot.as_ref().map(|snapshot| snapshot.revision)
        );
        assert_ne!(degraded.snapshot, initial.snapshot);
        harness.services.send(Ok(Event::Init)).await?;
        harness
            .services
            .send(Ok(Event::InitApply(service()?)))
            .await?;
        assert_eq!(observations.borrow().snapshot, degraded.snapshot);
        assert_eq!(
            observations.borrow().status.services,
            WatchStatus::Recovering
        );
        harness.services.send(Ok(Event::InitDone)).await?;
        let recovered = observations.borrow().clone();
        assert_eq!(recovered.status.services, WatchStatus::Watching);
        assert_eq!(
            recovered.status.latest_revision,
            recovered
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.revision)
        );
        harness.stop().await?;
        assert_eq!(observations.borrow().status.services, WatchStatus::Stopped);
        Ok(())
    })
    .await
}
