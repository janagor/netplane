//! Cancellation before polling, during queueing, and after dispatch.
mod common;

use std::{error::Error, future::Future, sync::atomic::Ordering, task::Poll};

use common::{CAPACITY, DEADLINE, graph};
use netplane::{Capacity, Netplane, RuntimeLimits};
use tokio::time::timeout;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn unpolled_and_dispatched_cancellation_release_admission() -> TestResult {
    timeout(DEADLINE, async {
        let (first, mut gate) = graph(0, CAPACITY)?;
        let runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 1 })?;
        let handle = runtime.handle();
        let unpolled = handle.try_execute(1)?;
        drop(unpolled);
        assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
        let running = tokio::spawn(handle.execute(2));
        assert_eq!(gate.entered.recv().await, Some(2));
        running.abort();
        assert!(matches!(running.await, Err(error) if error.is_cancelled()));
        let next = tokio::spawn(handle.try_execute(3)?);
        assert_eq!(gate.entered.recv().await, Some(3));
        gate.permits.add_permits(2);
        assert_eq!(next.await??.id, 3);
        assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn cancelled_queue_waiter_never_reaches_service_after_replacement() -> TestResult {
    timeout(DEADLINE, async {
        let capacity = Capacity {
            max_in_flight: 1,
            buffer_capacity: 1,
        };
        let (first, mut a) = graph(0, capacity)?;
        let mut runtime = Netplane::new(first, RuntimeLimits { max_outstanding: 4 })?;
        let handle = runtime.handle();
        let running = tokio::spawn(handle.execute(1));
        assert_eq!(a.entered.recv().await, Some(1));
        let mut waiting = Box::pin(handle.execute(2));
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(waiting);
        let followup = tokio::spawn(handle.execute(4));
        let (second, mut b) = graph(1, capacity)?;
        runtime.publish(second)?;
        assert_eq!(a.dropped.load(Ordering::SeqCst), 0);
        let replacement = tokio::spawn(handle.execute(3));
        assert_eq!(b.entered.recv().await, Some(3));
        b.permits.add_permits(1);
        assert_eq!(replacement.await??.version, 1);
        a.permits.add_permits(1);
        assert_eq!(running.await??.id, 1);
        assert_eq!(a.entered.recv().await, Some(4));
        a.permits.add_permits(1);
        assert_eq!(followup.await??.id, 4);
        assert_eq!(a.calls.load(Ordering::SeqCst), 2);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
