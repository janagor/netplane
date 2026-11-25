//! Control-plane acknowledgements and decisions use the existing publication mutex.
use netplane::{
    ControllerError, ControllerOptions, GraphDraft, GraphVersion, Netplane,
    ReconfigurationController, RuntimeLimits,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
fn graph() -> Result<GraphVersion<(), ()>, netplane::ConfigError> {
    GraphDraft::new(tower::service_fn(|()| async {
        Ok::<_, std::convert::Infallible>(())
    }))
    .validate()
}
struct AcknowledgedGuard(Arc<AtomicBool>);
impl Drop for AcknowledgedGuard {
    fn drop(&mut self) {
        assert!(
            self.0.load(Ordering::SeqCst),
            "commit must be acknowledged before freshness guard release"
        );
    }
}
#[tokio::test]
async fn acknowledge_commit_before_guard_release_and_reject_stale_callbacks() -> TestResult {
    let controller = ReconfigurationController::new(
        Netplane::new(graph()?, RuntimeLimits { max_outstanding: 4 })?,
        ControllerOptions::default(),
    );
    let token = controller.current_token().ok_or("no token")?;
    let baseline = controller.inspect().active.ok_or("no baseline")?.id;
    let acknowledged = Arc::new(AtomicBool::new(false));
    let outcome = controller.publish_if_current_guarded_with(
        token,
        graph()?,
        AcknowledgedGuard(acknowledged.clone()),
        |_| {
            acknowledged.store(true, Ordering::SeqCst);
        },
    )?;
    let mut stale_callback = false;
    assert!(matches!(
        controller.publish_if_current_guarded_with(token, graph()?, (), |_| {
            stale_callback = true;
        }),
        Err(ControllerError::Stale { .. })
    ));
    assert!(!stale_callback);
    assert!(matches!(
        controller.with_current_publication(baseline, || {
            stale_callback = true;
        }),
        Err(ControllerError::Stale { .. })
    ));
    assert!(!stale_callback);
    assert_eq!(
        controller.with_current_publication(outcome.published, || 42)?,
        42
    );
    let shutdown = controller.shutdown(Duration::from_secs(1));
    assert!(matches!(
        controller.with_current_publication(outcome.published, || {
            stale_callback = true;
        }),
        Err(ControllerError::ShuttingDown)
    ));
    assert!(!stale_callback);
    shutdown.await?;
    Ok(())
}
