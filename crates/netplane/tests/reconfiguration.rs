//! Version selection and publication behavior.
use netplane::{Capacity, ConfigError, GraphDraft, Netplane};
use std::{
    convert::Infallible,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Duration, timeout},
};
use tower::service_fn;

const fn capacity() -> Capacity {
    Capacity {
        max_in_flight: 2,
        buffer_capacity: 2,
    }
}

#[tokio::test]
async fn selected_version_survives_publication_before_poll_and_during_execution()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let (entered, started) = oneshot::channel();
        let (release, released) = oneshot::channel::<()>();
        let mut entered = Some(entered);
        let mut released = Some(released);
        let graph_a = GraphDraft::new(service_fn(move |value: usize| {
            let signal = entered.take();
            let wait = released.take();
            async move {
                if let Some(signal) = signal {
                    let _ = signal.send(());
                }
                if let Some(wait) = wait {
                    let _ = wait.await;
                }
                Ok::<_, Infallible>(value + 10)
            }
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(graph_a);
        let handle = runtime.handle();
        let before_poll = handle.execute(1);
        let running = tokio::spawn(handle.execute(2));
        started.await?;
        runtime.publish(
            GraphDraft::new(service_fn(|value: usize| async move {
                Ok::<_, Infallible>(value + 100)
            }))
            .capacity(capacity())
            .validate()?,
        );
        assert_eq!(handle.execute(3).await?, 103);
        let _ = release.send(());
        assert_eq!(running.await??, 12);
        assert_eq!(before_poll.await?, 11);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn invalid_draft_cannot_replace_active_version() -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let graph = GraphDraft::new(service_fn(|value: usize| async move {
            Ok::<_, Infallible>(value + 1)
        }))
        .capacity(capacity())
        .validate()?;
        let runtime = Netplane::new(graph);
        let handle = runtime.handle();
        let invalid = GraphDraft::new(service_fn(|value: usize| async move {
            Ok::<_, Infallible>(value + 10)
        }))
        .capacity(Capacity {
            max_in_flight: 0,
            buffer_capacity: 2,
        })
        .validate();
        assert!(matches!(invalid, Err(ConfigError::ZeroConcurrency)));
        assert_eq!(handle.execute(1).await?, 2);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn retained_future_keeps_old_service_alive() -> Result<(), Box<dyn Error + Send + Sync>> {
    struct DropProbe(Arc<AtomicUsize>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let probe = DropProbe(drops.clone());
    let old = GraphDraft::new(service_fn(move |value: usize| {
        let _ = &probe;
        async move { Ok::<_, Infallible>(value) }
    }))
    .capacity(capacity())
    .validate()?;
    let mut runtime = Netplane::new(old);
    let handle = runtime.handle();
    let pending = handle.execute(1);
    runtime.publish(
        GraphDraft::new(service_fn(|value: usize| async move {
            Ok::<_, Infallible>(value + 1)
        }))
        .capacity(capacity())
        .validate()?,
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(pending);
    timeout(Duration::from_secs(5), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn active_request_keeps_old_service_alive_until_completion()
-> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        struct DropProbe(Arc<AtomicUsize>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let probe = DropProbe(drops.clone());
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let mut entered_tx = Some(entered_tx);
        let mut release_rx = Some(release_rx);
        let old = GraphDraft::new(service_fn(move |value: usize| {
            let _ = &probe;
            let entered_tx = entered_tx.take();
            let release_rx = release_rx.take();
            async move {
                if let Some(entered_tx) = entered_tx {
                    let _ = entered_tx.send(());
                }
                if let Some(release_rx) = release_rx {
                    let _ = release_rx.await;
                }
                Ok::<_, Infallible>(value + 10)
            }
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(old);
        let handle = runtime.handle();
        let active = tokio::spawn(handle.execute(1));
        entered_rx.await?;
        runtime.publish(
            GraphDraft::new(service_fn(|value: usize| async move {
                Ok::<_, Infallible>(value + 100)
            }))
            .capacity(capacity())
            .validate()?,
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        release_tx
            .send(())
            .map_err(|()| "A request stopped before release")?;
        assert_eq!(active.await??, 11);
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_callers_see_only_complete_versions() -> Result<(), Box<dyn Error + Send + Sync>>
{
    timeout(Duration::from_secs(5), async {
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let (selected_tx, mut selected_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = watch::channel(false);
        let first = GraphDraft::new(service_fn(move |value: usize| {
            let entered_tx = entered_tx.clone();
            let mut release_rx = release_rx.clone();
            async move {
                let _ = entered_tx.send(value);
                while !*release_rx.borrow_and_update() {
                    if release_rx.changed().await.is_err() {
                        break;
                    }
                }
                Ok::<_, Infallible>(value + 10)
            }
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(first);
        let handle = runtime.handle();
        let callers: Vec<_> = (0..8)
            .map(|_| {
                let handle = handle.clone();
                let selected_tx = selected_tx.clone();
                tokio::spawn(async move {
                    let execution = handle.execute(1);
                    let _ = selected_tx.send(());
                    execution.await
                })
            })
            .collect();
        for _ in 0..callers.len() {
            selected_rx.recv().await.ok_or("caller did not select A")?;
        }
        for _ in 0..2 {
            assert_eq!(entered_rx.recv().await.ok_or("A request did not enter")?, 1);
        }
        runtime.publish(
            GraphDraft::new(service_fn(|value: usize| async move {
                Ok::<_, Infallible>(value + 100)
            }))
            .capacity(capacity())
            .validate()?,
        );
        let new_result = handle.execute(1).await?;
        assert_eq!(new_result, 101);
        release_tx.send(true)?;
        for caller in callers {
            assert_eq!(caller.await??, 11);
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn failed_version_does_not_prevent_replacement() -> Result<(), Box<dyn Error + Send + Sync>> {
    timeout(Duration::from_secs(5), async {
        let failing = GraphDraft::new(service_fn(|_: usize| async move {
            Err::<usize, std::io::Error>(std::io::Error::other("backend failed"))
        }))
        .capacity(capacity())
        .validate()?;
        let mut runtime = Netplane::new(failing);
        let handle = runtime.handle();
        assert!(handle.execute(1).await.is_err());
        runtime.publish(
            GraphDraft::new(service_fn(|value: usize| async move {
                Ok::<_, Infallible>(value + 1)
            }))
            .capacity(capacity())
            .validate()?,
        );
        assert_eq!(handle.execute(1).await?, 2);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
