//! Controlled polling separates committed publication from rollout confirmation.
mod rollout_support;
use netplane_control::rollout::{RolloutDriver, RolloutState};
use rollout_support::*;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn cancel_after_publication_before_rollout_confirmation() -> TestResult {
    let (updates, reconciliation, controller) = fixture().await?;
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        Metrics::new(0),
        spec(&[25, 100]),
        Duration::from_secs(1),
    )?;
    let rollout = driver.run();
    tokio::pin!(rollout);
    // Submit the stage, then park this future while the reconciler commits.
    // Confirmation cannot reach the rollout machine until we poll it again.
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(rollout.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    converged(&updates).await;
    assert_eq!(handle.status().state, RolloutState::Applying);
    assert_eq!(controller.handle().execute(()).await?.weight, 25);
    handle.cancel()?;
    rollout.await;
    assert_eq!(handle.status().state, RolloutState::Cancelled);
    assert_eq!(handle.status().applied_canary_weight, Some(25));
    assert!(
        updates.status().is_converged(),
        "committed desired configuration must remain intact"
    );
    assert_eq!(
        updates.status().desired_revision,
        updates.status().applied_revision
    );
    let (next, next_handle) = RolloutDriver::start(
        updates.clone(),
        Metrics::new(100),
        spec(&[25, 100]),
        Duration::from_secs(1),
    )?;
    let next_task = tokio::spawn(next.run());
    ticks(1).await;
    state(&next_handle, RolloutState::Completed).await;
    next_task.await?;
    updates.shutdown();
    reconciliation.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn unused_rollout_drop_does_not_withdraw_baseline() -> TestResult {
    let (updates, reconciliation, _) = fixture().await?;
    let before = updates.status();
    let (driver, _) = RolloutDriver::start(
        updates.clone(),
        Metrics::new(0),
        spec(&[100]),
        Duration::from_secs(1),
    )?;
    drop(driver);
    assert_eq!(updates.status(), before);
    updates.shutdown();
    reconciliation.await?;
    Ok(())
}
