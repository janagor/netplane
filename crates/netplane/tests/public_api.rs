//! External-consumer smoke test for the stable runtime entry points.

use std::{convert::Infallible, error::Error, time::Duration};

use netplane::{
    AdmissionError, Capacity, ControllerOptions, GraphBuilder, GraphDraft, Netplane,
    ReconfigurationController, RuntimeLimits,
};
use tower::service_fn;

const fn capacity() -> Capacity {
    Capacity {
        max_in_flight: 2,
        buffer_capacity: 2,
    }
}

#[tokio::test]
async fn external_consumer_can_route_reconfigure_and_drain()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let initial = GraphDraft::new(service_fn(|value: u64| async move {
        Ok::<_, Infallible>(value + 1)
    }))
    .capacity(capacity())
    .validate()?;
    let runtime = Netplane::new(initial, RuntimeLimits { max_outstanding: 1 })?;
    let controller = ReconfigurationController::new(
        runtime,
        ControllerOptions {
            max_retained_versions: 2,
        },
    );
    let handle = controller.handle();
    let old = handle.try_execute(1)?;
    assert!(matches!(
        handle.try_execute(2),
        Err(AdmissionError::AtCapacity { limit: 1 })
    ));

    let snapshot = controller.inspect();
    let routed = GraphBuilder::<u64, u64>::new()
        .service_with_capacity(
            "primary",
            service_fn(|value: u64| async move { Ok::<_, Infallible>(value + 10) }),
            capacity(),
        )
        .router("router", |_value: &u64| "primary")
        .route("router", "primary", "primary")
        .fallback("router", "primary")
        .entry("router")
        .capacity(capacity())
        .backend_capacity(capacity())
        .validate()?;
    let token = snapshot.token.ok_or("missing publication token")?;
    let outcome = controller.publish_if_current(token, routed)?;
    let published = controller.inspect();
    let backend_capacities = &published
        .active
        .as_ref()
        .ok_or("missing active version")?
        .metadata
        .backend_capacities;
    assert_eq!(backend_capacities.len(), 1);
    assert_eq!(backend_capacities[0].name, "primary");
    assert_eq!(backend_capacities[0].capacity, capacity());
    assert_eq!(old.await?, 2);
    assert_eq!(handle.execute(1).await?, 11);
    let next = controller.current_token().ok_or("missing next token")?;
    controller.rollback_if_current(next, outcome.previous)?;
    assert_eq!(handle.execute(1).await?, 2);
    controller.shutdown(Duration::from_secs(1)).await?;
    assert!(matches!(handle.try_execute(1), Err(AdmissionError::Closed)));
    Ok(())
}
