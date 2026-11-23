//! Publication counters and metadata remain coherent during concurrent control.
use std::{convert::Infallible, error::Error, sync::Arc};

use netplane::{
    ControllerError, ControllerOptions, GraphDraft, Netplane, ReconfigurationController,
    RuntimeLimits,
};
use tokio::sync::Barrier;
use tower::service_fn;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshots_track_replacements_and_rollbacks_coherently() -> TestResult {
    let graph = || {
        GraphDraft::new(service_fn(|value: usize| async move {
            Ok::<_, Infallible>(value)
        }))
        .validate()
    };
    let controller = Arc::new(ReconfigurationController::new(
        Netplane::new(
            graph()?,
            RuntimeLimits {
                max_outstanding: 16,
            },
        )?,
        ControllerOptions::default(),
    ));
    let barrier = Arc::new(Barrier::new(3));
    let mut writers = Vec::new();
    for _ in 0..2 {
        let controller = Arc::clone(&controller);
        let barrier = Arc::clone(&barrier);
        writers.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..100 {
                // Retry conflicts so each writer exercises both successful kinds.
                loop {
                    let snapshot = controller.inspect();
                    let token = snapshot.token.ok_or("missing token")?;
                    match controller.publish_if_current(token, graph()?) {
                        Ok(_) => break,
                        Err(ControllerError::Stale { .. }) => (),
                        Err(error) => return Err(error.into()),
                    }
                    tokio::task::yield_now().await;
                }
                loop {
                    let snapshot = controller.inspect();
                    let token = snapshot.token.ok_or("missing token")?;
                    let target = snapshot.retained.last().ok_or("missing history")?.id;
                    match controller.rollback_if_current(token, target) {
                        Ok(_) => break,
                        Err(ControllerError::Stale { .. }) => (),
                        Err(error) => return Err(error.into()),
                    }
                    tokio::task::yield_now().await;
                }
                tokio::task::yield_now().await;
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }));
    }
    barrier.wait().await;
    let mut samples = 0;
    loop {
        let snapshot = controller.inspect();
        let publications = snapshot.counters.replacements_total + snapshot.counters.rollbacks_total;
        assert_eq!(
            snapshot.active.ok_or("missing active")?.id.get(),
            publications + 1
        );
        samples += 1;
        if samples >= 1000 && writers.iter().all(tokio::task::JoinHandle::is_finished) {
            break;
        }
        tokio::task::yield_now().await;
    }
    for writer in writers {
        writer.await??;
    }
    let snapshot = controller.inspect();
    assert_eq!(snapshot.counters.replacements_total, 200);
    assert_eq!(snapshot.counters.rollbacks_total, 200);
    assert_eq!(snapshot.active.ok_or("missing active")?.id.get(), 401);
    Ok(())
}
