//! Coalesce an input burst and wait for its final desired state.
use netplane::{
    ControllerOptions, GraphDraft, GraphVersion, Netplane, ReconfigurationController, RuntimeLimits,
};
use netplane_control::{
    DriverStatus, GraphPreparer, Reconciler, ReconciliationDriver, UpdatePolicy,
};
use std::{convert::Infallible, sync::Arc};

struct Preparer;
fn graph(value: u32) -> Result<GraphVersion<(), u32>, netplane::ConfigError> {
    GraphDraft::new(tower::service_fn(move |()| async move {
        Ok::<_, Infallible>(value)
    }))
    .validate()
}
impl GraphPreparer<u32, (), u32> for Preparer {
    type Error = netplane::ConfigError;
    fn validate(&self, _: &u32) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn prepare(&self, desired: u32) -> Result<GraphVersion<(), u32>, Self::Error> {
        graph(desired)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let controller = Arc::new(ReconfigurationController::new(
        Netplane::new(graph(0)?, RuntimeLimits { max_outstanding: 8 })?,
        ControllerOptions::default(),
    ));
    let (driver, updates) = ReconciliationDriver::new(
        Reconciler::new(Arc::clone(&controller), Preparer),
        UpdatePolicy::default(),
    )?;
    let mut status = driver.status();
    let worker = tokio::spawn(driver.run(|_| false));
    for desired in 1..=1000 {
        updates.submit(desired)?;
    }
    loop {
        if matches!(
            status.borrow_and_update().as_ref(),
            DriverStatus::Applied(1000, _)
        ) {
            break;
        }
        status.changed().await?;
    }
    assert_eq!(controller.handle().execute(()).await?, 1000);
    updates.shutdown();
    worker.await?;
    controller
        .shutdown(std::time::Duration::from_secs(1))
        .await?;
    Ok(())
}
