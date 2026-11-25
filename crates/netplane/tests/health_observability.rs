//! Reentrant tracing inspection runs in isolation from unsubscribed health tests.
use netplane::health::{BackendIdentity, HealthError, HealthOutcome, HealthPolicy, HealthRegistry};
use std::{num::NonZeroU32, time::Duration};

#[test]
fn health_events_allow_reentrant_registry_inspection() -> Result<(), HealthError> {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tracing_subscriber::prelude::*;
    struct Inspect {
        registry: HealthRegistry,
        observed: Arc<AtomicUsize>,
    }
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Inspect {
        fn on_event(&self, _: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            let _ = self.registry.snapshot();
            self.observed.fetch_add(1, Ordering::Relaxed);
        }
    }
    let registry = HealthRegistry::default();
    let observed = Arc::new(AtomicUsize::new(0));
    let subscriber = tracing_subscriber::registry().with(Inspect {
        registry: registry.clone(),
        observed: Arc::clone(&observed),
    });
    let _scope = tracing::subscriber::set_default(subscriber);
    let handle = registry.register(
        BackendIdentity {
            service: "payments".into(),
            instance: "endpoint-a".into(),
            incarnation: "one".into(),
        },
        HealthPolicy {
            failure_threshold: NonZeroU32::MIN.saturating_add(2),
            recovery_threshold: NonZeroU32::MIN,
            recovery_delay: Duration::from_secs(10),
        },
    )?;
    for _ in 0..3 {
        handle.report(handle.begin_observation(), HealthOutcome::Failure);
    }
    drop(handle);
    assert!(observed.load(Ordering::Relaxed) >= 3);
    Ok(())
}
