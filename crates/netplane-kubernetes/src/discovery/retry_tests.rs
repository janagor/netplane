use super::relisting_watch;
use futures_util::{StreamExt, stream};
use kube::runtime::{
    WatchStreamExt,
    watcher::{Error, Event},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;
fn failure(initial: bool) -> Error {
    let status = kube::core::Status::failure("injected", "InternalError")
        .with_code(500)
        .boxed();
    if initial {
        Error::InitialListFailed(kube::Error::Api(status))
    } else {
        Error::WatchError(status)
    }
}

#[tokio::test(start_paused = true)]
async fn repeated_initial_list_failures_and_successful_relists_have_bounded_retry_rate() -> Result {
    for initial in [true, false] {
        let factories = Arc::new(AtomicUsize::new(0));
        let count = factories.clone();
        let watched = relisting_watch(move || {
            count.fetch_add(1, Ordering::SeqCst);
            stream::unfold(0_u8, move |step| async move {
                let event: std::result::Result<Event<()>, Error> = match step {
                    0 => Ok(Event::Init),
                    1 if !initial => Ok(Event::InitDone),
                    _ => Err(failure(initial)),
                };
                Some((
                    event,
                    if step == 0 {
                        1
                    } else if step == 1 && !initial {
                        2
                    } else {
                        0
                    },
                ))
            })
        })
        .default_backoff();
        tokio::pin!(watched);
        assert!(matches!(watched.next().await, Some(Ok(Event::Init))));
        for _ in 0..6 {
            if !initial {
                assert!(matches!(watched.next().await, Some(Ok(Event::InitDone))));
            }
            assert!(matches!(watched.next().await, Some(Err(_))));
            let started = tokio::time::Instant::now();
            assert!(matches!(watched.next().await, Some(Ok(Event::Init))));
            // Successful relists can reset kube-rs backoff. Even then, retries
            // must retain its minimum delay rather than busy-looping.
            assert!(started.elapsed() >= Duration::from_millis(800));
        }
        assert_eq!(
            factories.load(Ordering::SeqCst),
            if initial { 1 } else { 7 }
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_backoff_stops_discovery_without_advancing_clock() -> Result {
    let watched = relisting_watch(|| stream::repeat_with(|| Err(failure(false)))).default_backoff();
    let (discovery, handle) = runner()?;
    let mut health = handle.subscribe_status();
    let stop = CancellationToken::new();
    let task = tokio::spawn(discovery.run_streams(watched, stream::pending(), stop.clone()));
    while handle.status().services != crate::WatchStatus::Recovering {
        health.changed().await?;
    }
    let started = tokio::time::Instant::now();
    stop.cancel();
    task.await??;
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(handle.status().services, crate::WatchStatus::Stopped);
    Ok(())
}

fn runner()
-> std::result::Result<(crate::Discovery, crate::DiscoveryHandle), Box<dyn std::error::Error>> {
    Ok(crate::Discovery::new(
        kube::Client::try_from(kube::Config::new("http://127.0.0.1:1".parse()?))?,
        crate::DiscoveryConfig {
            namespace: "default".into(),
            service_names: std::collections::BTreeSet::default(),
        },
    )?)
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_relist_drops_operation_and_retains_committed_snapshot() -> Result {
    use k8s_openapi::api::{core::v1::Service, discovery::v1::EndpointSlice};
    use std::sync::atomic::AtomicBool;
    struct Operation(Arc<AtomicBool>);
    impl Drop for Operation {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(tokio::sync::Notify::new());
    let count = Arc::new(AtomicUsize::new(0));
    let watched = relisting_watch({
        let dropped = dropped.clone();
        let entered = entered.clone();
        move || {
            let first = count.fetch_add(1, Ordering::SeqCst) == 0;
            let events = if first {
                let service = Service {
                    metadata: kube::api::ObjectMeta {
                        name: Some("backend".into()),
                        namespace: Some("default".into()),
                        ..Default::default()
                    },
                    spec: Some(k8s_openapi::api::core::v1::ServiceSpec::default()),
                    ..Service::default()
                };
                vec![
                    Ok(Event::Init),
                    Ok(Event::InitApply(service)),
                    Ok(Event::InitDone),
                    Err(failure(false)),
                ]
            } else {
                vec![Ok(Event::Init)]
            };
            let dropped = dropped.clone();
            let entered = entered.clone();
            stream::iter(events).chain(stream::once(async move {
                let _operation = Operation(dropped);
                entered.notify_one();
                std::future::pending().await
            }))
        }
    })
    .default_backoff();
    let slices = stream::iter([
        Ok::<Event<EndpointSlice>, Error>(Event::Init),
        Ok(Event::InitDone),
    ])
    .chain(stream::pending());
    let (discovery, handle) = runner()?;
    let stop = CancellationToken::new();
    let task = tokio::spawn(discovery.run_streams(watched, slices, stop.clone()));
    tokio::time::timeout(Duration::from_secs(30), entered.notified()).await?;
    let committed = handle.snapshot().ok_or("initial snapshot")?;
    assert_eq!(committed.revision, 1);
    assert_eq!(committed.services.len(), 1);
    assert_eq!(handle.status().services, crate::WatchStatus::Recovering);
    stop.cancel();
    task.await??;
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(handle.snapshot().ok_or("retained snapshot")?, committed);
    Ok(())
}
