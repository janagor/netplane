//! Service and buffer-worker failures under concurrent admission.

use std::{
    error::Error,
    fmt,
    future::{Future, ready},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use netplane::{AdmissionError, GraphDraft, Netplane, RuntimeLimits};
use tokio::time::timeout;
use tower::Service;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[derive(Debug)]
struct Failure;
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("injected service failure")
    }
}
impl Error for Failure {}

struct FailingService {
    ready_fails: bool,
}

impl Service<usize> for FailingService {
    type Response = usize;
    type Error = Failure;
    type Future = Pin<Box<dyn Future<Output = Result<usize, Failure>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.ready_fails {
            Poll::Ready(Err(Failure))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, request: usize) -> Self::Future {
        Box::pin(ready(if request % 5 == 0 {
            Err(Failure)
        } else {
            Ok(request)
        }))
    }
}

#[tokio::test]
async fn mixed_call_failures_and_cancellation_return_all_capacity() -> TestResult {
    timeout(Duration::from_secs(5), async {
        let initial = GraphDraft::new(FailingService { ready_fails: false }).validate()?;
        let mut runtime = Netplane::new(
            initial,
            RuntimeLimits {
                max_outstanding: 16,
            },
        )?;
        let handle = runtime.handle();
        let pending: Vec<_> = (0..16)
            .map(|id| handle.try_execute(id))
            .collect::<Result<_, _>>()?;
        assert!(matches!(
            handle.try_execute(100),
            Err(AdmissionError::AtCapacity { limit: 16 })
        ));
        let next = GraphDraft::new(FailingService { ready_fails: false }).validate()?;
        runtime.publish(next)?;
        let mut failures = 0;
        for (id, future) in pending.into_iter().enumerate() {
            if id % 4 == 0 {
                drop(future);
                continue;
            }
            match future.await {
                Ok(value) => assert_eq!(value, id),
                Err(error) => {
                    assert!(error.downcast_ref::<Failure>().is_some());
                    failures += 1;
                }
            }
        }
        assert!(failures > 0);
        assert_eq!(handle.execute(101).await?, 101);
        runtime.shutdown(Duration::from_secs(1)).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn readiness_failure_closes_worker_and_releases_admission() -> TestResult {
    timeout(Duration::from_secs(5), async {
        let first = GraphDraft::new(FailingService { ready_fails: true }).validate()?;
        let mut runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 8 })?;
        let handle = runtime.handle();
        let futures: Vec<_> = (0..8)
            .map(|id| handle.try_execute(id))
            .collect::<Result<_, _>>()?;
        for future in futures {
            assert!(future.await.is_err());
        }
        let next = GraphDraft::new(FailingService { ready_fails: false }).validate()?;
        runtime.publish(next)?;
        assert_eq!(handle.try_execute(7)?.await?, 7);
        runtime.shutdown(Duration::from_secs(1)).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

struct CrashingWorker;

impl Service<usize> for CrashingWorker {
    type Response = usize;
    type Error = Failure;
    type Future = Pin<Box<dyn Future<Output = Result<usize, Failure>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        std::panic::resume_unwind(Box::new("injected buffer worker termination"))
    }

    fn call(&mut self, request: usize) -> Self::Future {
        Box::pin(ready(Ok(request)))
    }
}

#[tokio::test]
async fn unexpected_worker_termination_wakes_all_accepted_callers() -> TestResult {
    timeout(Duration::from_secs(5), async {
        let first = GraphDraft::new(CrashingWorker).validate()?;
        let mut runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 8 })?;
        let handle = runtime.handle();
        let futures: Vec<_> = (0..8)
            .map(|id| handle.try_execute(id))
            .collect::<Result<_, _>>()?;
        for future in futures {
            assert!(future.await.is_err());
        }
        let next = GraphDraft::new(FailingService { ready_fails: false }).validate()?;
        runtime.publish(next)?;
        assert_eq!(handle.execute(7).await?, 7);
        runtime.shutdown(Duration::from_secs(1)).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
