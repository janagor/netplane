//! Rejected candidates and synchronous shutdown closure.
#[path = "common/drop.rs"]
mod drop_helper;

use std::{
    convert::Infallible,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use netplane::{
    AdmissionError, ControllerError, ControllerOptions, GraphBuilder, GraphDraft, GraphVersion,
    Netplane, ReconfigurationController, RuntimeLimits,
};
use tokio::{sync::oneshot, time::timeout};
use tower::service_fn;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;
const WATCHDOG: Duration = Duration::from_secs(5);

struct Probe(Arc<AtomicUsize>);

impl Drop for Probe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn simple(value: usize) -> Result<GraphVersion<usize, usize>, netplane::ConfigError> {
    GraphDraft::new(service_fn(move |_: usize| async move {
        Ok::<_, Infallible>(value)
    }))
    .validate()
}

fn candidate() -> Result<
    (
        GraphVersion<usize, usize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ),
    netplane::GraphBuildError,
> {
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));
    let probe_a = Probe(Arc::clone(&a));
    let probe_b = Probe(Arc::clone(&b));
    let graph = GraphBuilder::new()
        .service(
            "a",
            service_fn(move |_: usize| {
                let _ = &probe_a;
                async { Ok::<_, Infallible>(1) }
            }),
        )
        .service(
            "b",
            service_fn(move |_: usize| {
                let _ = &probe_b;
                async { Ok::<_, Infallible>(2) }
            }),
        )
        .router(
            "router",
            |request: &usize| if *request == 1 { "a" } else { "b" },
        )
        .route("router", "a", "a")
        .route("router", "b", "b")
        .fallback("router", "b")
        .entry("router")
        .validate()?;
    Ok((graph, a, b))
}

fn controller() -> TestResultController {
    let runtime = Netplane::new(simple(1)?, RuntimeLimits { max_outstanding: 2 })?;
    Ok(ReconfigurationController::new(
        runtime,
        ControllerOptions {
            max_retained_versions: 0,
        },
    ))
}

type TestResultController =
    Result<ReconfigurationController<usize, usize>, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn stale_candidate_releases_backend_services() -> TestResult {
    timeout(WATCHDOG, async {
        let control = controller()?;
        let stale = control.current_token().ok_or("missing token")?;
        let published = control.publish_if_current(stale, simple(2)?)?;
        let (candidate, a, b) = candidate()?;
        assert!(matches!(
            control.publish_if_current(stale, candidate),
            Err(ControllerError::Stale { .. })
        ));
        assert_eq!(
            control.inspect().active.ok_or("missing active")?.id,
            published.published
        );
        assert!(control.inspect().retained.is_empty());
        drop_helper::wait_for_drop(&a, 1).await;
        drop_helper::wait_for_drop(&b, 1).await;
        control.shutdown(WATCHDOG).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_rejected_candidate_releases_backend_services() -> TestResult {
    timeout(WATCHDOG, async {
        let control = controller()?;
        let token = control.current_token().ok_or("missing token")?;
        let (candidate, a, b) = candidate()?;
        let drain = control.shutdown(WATCHDOG);
        assert_eq!(
            control.publish_if_current(token, candidate),
            Err(ControllerError::ShuttingDown)
        );
        drop_helper::wait_for_drop(&a, 1).await;
        drop_helper::wait_for_drop(&b, 1).await;
        drain.await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_closure_prevents_prepared_candidate_publication() -> TestResult {
    timeout(WATCHDOG, async {
        let control = Arc::new(controller()?);
        let handle = control.handle();
        let held = handle.try_execute(1)?;
        let token = control.current_token().ok_or("missing token")?;
        let active = control.inspect().active.ok_or("missing active")?.id;
        let candidate = simple(2)?;
        let (release_tx, release_rx) = oneshot::channel();
        let publisher = Arc::clone(&control);
        let publication = tokio::spawn(async move {
            release_rx.await.map_err(std::io::Error::other)?;
            Ok::<_, Box<dyn Error + Send + Sync>>(publisher.publish_if_current(token, candidate))
        });
        let drain = control.shutdown(Duration::ZERO);
        release_tx.send(()).map_err(|()| "publisher gate closed")?;
        assert_eq!(publication.await??, Err(ControllerError::ShuttingDown));
        assert!(matches!(handle.try_execute(2), Err(AdmissionError::Closed)));
        assert_eq!(control.inspect().active.ok_or("missing active")?.id, active);
        drop(drain);
        drop(held);
        control.shutdown(WATCHDOG).await?;
        assert!(control.inspect().active.is_none());
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
