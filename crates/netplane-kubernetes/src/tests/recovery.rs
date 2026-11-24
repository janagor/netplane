use super::support::{bounded, events};
use super::{Result, config, service, slice};
use crate::{Discovery, DiscoveryError, WatchStatus, state::ResourceCache};
use futures_util::stream;
use kube::{
    Client, Config,
    runtime::{
        WatchStreamExt,
        watcher::{Error, Event},
    },
};
use tokio_util::sync::CancellationToken;

#[test]
fn invalid_scopes_are_typed() {
    for namespace in ["", "*", "UPPER", "-leading", "trailing-", "two/names"] {
        let mut scope = config();
        scope.namespace = namespace.into();
        assert!(matches!(
            scope.validate(),
            Err(DiscoveryError::InvalidScope)
        ));
    }
    let mut scope = config();
    scope.service_names.insert("bad.name".into());
    assert!(scope.validate().is_err());
}
#[test]
fn independent_services_orphans_and_unsupported_address_types() -> Result {
    let mut services = ResourceCache::default();
    let mut slices = ResourceCache::default();
    slices.apply(Event::Apply(slice()?));
    assert!(crate::topology::build(&services, &slices, &config()).is_empty());
    services.apply(Event::Apply(service()?));
    let mut second = service()?;
    second.metadata.name = Some("second".into());
    second.spec.as_mut().ok_or("spec")?.cluster_ip = Some("None".into());
    services.apply(Event::Apply(second));
    let mut endpoint_slice = slice()?;
    endpoint_slice.address_type = "FQDN".into();
    slices.apply(Event::Apply(endpoint_slice));
    let topology = crate::topology::build(&services, &slices, &config());
    assert_eq!(topology.len(), 2);
    assert!(topology.values().all(|s| s.endpoints.is_empty()));
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn cancellation_interrupts_real_watcher_backoff() -> Result {
    bounded(async {
        let client = Client::try_from(Config::new("http://127.0.0.1:1".parse()?))?;
        let (discovery, handle) = Discovery::new(client, config())?;
        let failures = stream::repeat_with(|| {
            Err(Error::WatchFailed(kube::Error::Api(
                kube::core::Status::failure("forbidden", "Forbidden")
                    .with_code(403)
                    .boxed(),
            )))
        })
        .default_backoff();
        let cancellation = CancellationToken::new();
        let mut status = handle.subscribe_status();
        let task =
            tokio::spawn(discovery.run_streams(failures, stream::pending(), cancellation.clone()));
        status.changed().await?;
        assert_eq!(handle.status().services, WatchStatus::Recovering);
        assert!(handle.snapshot().is_none());
        cancellation.cancel();
        task.await??;
        assert_eq!(handle.status().services, WatchStatus::Stopped);
        Ok(())
    })
    .await
}
#[tokio::test]
async fn progress_without_snapshot_subscribers() -> Result {
    bounded(async {
        let client = Client::try_from(Config::new("http://127.0.0.1:1".parse()?))?;
        let (discovery, handle) = Discovery::new(client, config())?;
        let handle_status = handle.subscribe_status();
        drop(handle);
        let cancellation = CancellationToken::new();
        let stop = cancellation.clone();
        let (services, service_stream) = events();
        let (slices, slice_stream) = events();
        let mut status = handle_status;
        let task = tokio::spawn(discovery.run_streams(service_stream, slice_stream, cancellation));
        services.send(Ok(Event::Init)).await?;
        services.send(Ok(Event::InitDone)).await?;
        slices.send(Ok(Event::Init)).await?;
        slices.send(Ok(Event::InitDone)).await?;
        assert_eq!(status.borrow_and_update().latest_revision, Some(1));
        stop.cancel();
        task.await??;
        Ok(())
    })
    .await
}
