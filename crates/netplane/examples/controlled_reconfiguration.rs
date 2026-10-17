//! Validate a replacement, handle a version conflict, and roll back.

use std::{convert::Infallible, error::Error, time::Duration};

use netplane::{
    ControllerError, ControllerOptions, GraphDraft, Netplane, ReconfigurationController,
    RuntimeLimits,
};
use tower::service_fn;

fn graph(value: usize) -> Result<netplane::GraphVersion<usize, usize>, netplane::ConfigError> {
    GraphDraft::new(service_fn(move |_: usize| async move {
        Ok::<_, Infallible>(value)
    }))
    .validate()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let runtime = Netplane::new(graph(1)?, RuntimeLimits { max_outstanding: 8 })?;
    let controller = ReconfigurationController::new(runtime, ControllerOptions::default());
    let snapshot = controller.inspect();
    let original = snapshot.active.ok_or("missing active graph")?.id;
    let expected = snapshot.token.ok_or("runtime closed")?;
    let candidate = graph(2)?;
    let replacement = controller.publish_if_current(expected, candidate)?;
    println!("published generation {}", replacement.published.get());

    // A caller holding the old token must inspect before trying again.
    match controller.publish_if_current(expected, graph(3)?) {
        Err(ControllerError::Stale { actual, .. }) => {
            println!("active generation is {}", actual.get());
        }
        result => return Err(format!("unexpected publication result: {result:?}").into()),
    }

    let rollback = controller.rollback_if_current(
        controller.inspect().token.ok_or("runtime closed")?,
        original,
    )?;
    println!("restored graph as generation {}", rollback.published.get());
    assert_eq!(controller.handle().execute(0).await?, 1);
    controller.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
