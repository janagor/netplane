//! Global admission and request ownership across publications.
mod common;
#[path = "common/drop.rs"]
mod drop_helper;

use std::{
    convert::Infallible,
    error::Error,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use common::{CAPACITY, graph};
use netplane::{
    AdmissionError, ControllerOptions, GraphDraft, Netplane, ReconfigurationController,
    RuntimeLimits,
};
use tokio::{
    sync::{Semaphore, oneshot},
    time::timeout,
};
use tower::service_fn;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;
const WATCHDOG: Duration = common::DEADLINE;

#[tokio::test]
async fn requests_retain_selected_version_across_multiple_publications() -> TestResult {
    timeout(WATCHDOG, async {
        let (v1, mut gate1) = graph(1, CAPACITY)?;
        let control = ReconfigurationController::new(
            Netplane::new(v1, RuntimeLimits { max_outstanding: 3 })?,
            ControllerOptions {
                max_retained_versions: 0,
            },
        );
        let handle = control.handle();
        let r1 = tokio::spawn(handle.try_execute(1)?);
        assert_eq!(gate1.entered.recv().await, Some(1));

        let (v2, mut gate2) = graph(2, CAPACITY)?;
        control.publish_if_current(control.current_token().ok_or("missing token")?, v2)?;
        let r2 = tokio::spawn(handle.try_execute(2)?);
        assert_eq!(gate2.entered.recv().await, Some(2));

        let (v3, mut gate3) = graph(3, CAPACITY)?;
        control.publish_if_current(control.current_token().ok_or("missing token")?, v3)?;
        let r3 = tokio::spawn(handle.try_execute(3)?);
        assert_eq!(gate3.entered.recv().await, Some(3));

        let (v4, gate4) = graph(4, CAPACITY)?;
        control.publish_if_current(control.current_token().ok_or("missing token")?, v4)?;
        assert!(matches!(
            handle.try_execute(4),
            Err(AdmissionError::AtCapacity { limit: 3 })
        ));
        gate2.permits.add_permits(1);
        assert_eq!(r2.await??.version, 2);
        let r4 = tokio::spawn(handle.try_execute(4)?);
        assert!(matches!(
            handle.try_execute(5),
            Err(AdmissionError::AtCapacity { limit: 3 })
        ));
        gate4.permits.add_permits(1);
        assert_eq!(r4.await??.version, 4);
        gate3.permits.add_permits(1);
        assert_eq!(r3.await??.version, 3);
        gate1.permits.add_permits(1);
        assert_eq!(r1.await??.version, 1);
        drop_helper::wait_for_drop(&gate1.dropped, 1).await;
        drop_helper::wait_for_drop(&gate2.dropped, 1).await;
        drop_helper::wait_for_drop(&gate3.dropped, 1).await;
        control.shutdown(WATCHDOG).await?;
        drop_helper::wait_for_drop(&gate4.dropped, 1).await;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn unpolled_cancellation_releases_global_slot_across_versions() -> TestResult {
    timeout(WATCHDOG, async {
        let (v1, gate1) = graph(1, CAPACITY)?;
        let control = ReconfigurationController::new(
            Netplane::new(v1, RuntimeLimits { max_outstanding: 1 })?,
            ControllerOptions {
                max_retained_versions: 0,
            },
        );
        let handle = control.handle();
        let r1 = handle.try_execute(1)?;
        let (v2, gate2) = graph(2, CAPACITY)?;
        control.publish_if_current(control.current_token().ok_or("missing token")?, v2)?;
        assert!(matches!(
            handle.try_execute(2),
            Err(AdmissionError::AtCapacity { limit: 1 })
        ));
        drop(r1);
        drop_helper::wait_for_drop(&gate1.dropped, 1).await;
        let r2 = handle.try_execute(2)?;
        assert!(matches!(
            handle.try_execute(3),
            Err(AdmissionError::AtCapacity { limit: 1 })
        ));
        drop(r2);
        gate2.permits.add_permits(1);
        assert_eq!(handle.try_execute(3)?.await?.version, 2);
        assert_eq!(gate1.calls.load(Ordering::SeqCst), 0);
        control.shutdown(WATCHDOG).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn dispatched_cancellation_releases_netplane_admission_not_service_work() -> TestResult {
    timeout(WATCHDOG, async {
        let (started_tx, started_rx) = oneshot::channel();
        let (task_tx, mut task_rx) = oneshot::channel();
        let permits = Arc::new(Semaphore::new(0));
        let owned = Arc::clone(&permits);
        let mut started_tx = Some(started_tx);
        let mut task_tx = Some(task_tx);
        let v1 = GraphDraft::new(service_fn(move |_: usize| {
            let started = started_tx.take();
            let task_signal = task_tx.take();
            let permits = Arc::clone(&owned);
            async move {
                let task = tokio::spawn(async move {
                    if let Some(started) = started {
                        let _ = started.send(());
                    }
                    if let Ok(permit) = permits.acquire().await {
                        permit.forget();
                    }
                    if let Some(task_signal) = task_signal {
                        let _ = task_signal.send(());
                    }
                });
                task.await.map_err(std::io::Error::other)?;
                Ok::<_, std::io::Error>(1)
            }
        }))
        .validate()?;
        let control = ReconfigurationController::new(
            Netplane::new(v1, RuntimeLimits { max_outstanding: 1 })?,
            ControllerOptions {
                max_retained_versions: 0,
            },
        );
        let handle = control.handle();
        let caller = tokio::spawn(handle.try_execute(1)?);
        started_rx.await?;
        let v2 =
            GraphDraft::new(service_fn(|_: usize| async { Ok::<_, Infallible>(2) })).validate()?;
        control.publish_if_current(control.current_token().ok_or("missing token")?, v2)?;
        assert!(matches!(
            handle.try_execute(2),
            Err(AdmissionError::AtCapacity { limit: 1 })
        ));
        caller.abort();
        assert!(caller.await.is_err());
        assert_eq!(handle.try_execute(2)?.await?, 2);
        assert!(matches!(
            task_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        permits.add_permits(1);
        task_rx.await?;
        control.shutdown(WATCHDOG).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
