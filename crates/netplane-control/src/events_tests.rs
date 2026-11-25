//! Regression coverage for audit payloads and reentrant delivery.
use crate::{
    ApplyError, ReconciliationFailure as Failure, ReconciliationPhase as Phase, events::Observation,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::SubscriberExt};

#[derive(Default)]
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}
struct Capture {
    events: Arc<Mutex<Vec<Fields>>>,
    reentrant: Arc<Observation>,
    once: AtomicBool,
}
impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "netplane_control" {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.0.get("runtime_id").is_some_and(|id| id == "78")
            && !self.once.swap(true, Ordering::SeqCst)
        {
            // Deliver the nested transition before recording the outer callback.
            self.reentrant.desired(2).emit();
        }
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(fields);
    }
}

#[test]
fn failure_payloads_are_independent_and_reentrant_delivery_can_reverse_sequences()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let reentrant = Arc::new(Observation::new(78, None));
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture {
        events: Arc::clone(&events),
        reentrant: Arc::clone(&reentrant),
        once: AtomicBool::new(false),
    }))?;
    let observation = Observation::new(77, None);
    observation.desired(1).emit();
    observation.desired(2).emit();
    observation.failed(1, &ApplyError::Preparation("private obsolete error"), false);
    assert_eq!(observation.status().last_failure, None);
    assert_eq!(observation.status().phase, Phase::Pending);
    observation.failed(2, &ApplyError::Validation("private policy data"), false);
    let before = observation.status();
    observation.failed(1, &ApplyError::Preparation("private upstream error"), false);
    let after = observation.status();
    assert_eq!(after.desired_revision, before.desired_revision);
    assert_eq!(after.phase, Phase::Blocked);
    assert_eq!(after.last_failure, Some(Failure::Validation));
    observation.stop();
    observation.failed(2, &ApplyError::<()>::Busy, false);
    assert_eq!(observation.status().phase, Phase::Stopped);
    assert_eq!(observation.status().last_failure, Some(Failure::Validation));
    // Other unit tests can register this callsite while the global subscriber
    // is installed. Refresh its interest before checking recursive delivery.
    tracing::callsite::rebuild_interest_cache();
    reentrant.desired(1).emit();
    let events = events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let failures: Vec<_> = events
        .iter()
        .filter(|e| {
            e.0.get("event")
                .is_some_and(|v| v == "\"reconcile.failed\"")
        })
        .collect();
    let kinds: Vec<_> = failures
        .iter()
        .map(|e| e.0.get("error_kind").map(String::as_str))
        .collect();
    assert_eq!(
        kinds,
        [
            Some("Some(Preparation)"),
            Some("Some(Validation)"),
            Some("Some(Preparation)"),
            Some("Some(Busy)")
        ]
    );
    assert_eq!(failures[2].0.get("revision").map(String::as_str), Some("1"));
    assert_eq!(
        failures[2].0.get("desired_revision").map(String::as_str),
        Some("2")
    );
    let nested: Vec<_> = events
        .iter()
        .filter(|e| e.0.get("runtime_id").is_some_and(|id| id == "78"))
        .collect();
    let sequences: Vec<u64> = nested
        .iter()
        .map(|e| {
            e.0.get("event_sequence")
                .ok_or("missing sequence")?
                .parse()
                .map_err(Into::into)
        })
        .collect::<Result<_, Box<dyn std::error::Error + Send + Sync>>>()?;
    assert_eq!(sequences, [2, 1]);
    assert_eq!(
        sequences.iter().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([1, 2])
    );
    assert_eq!(reentrant.status().desired_revision, Some(2));
    assert!(
        events
            .iter()
            .all(|e| !format!("{:?}", e.0).contains("private"))
    );
    drop(events);
    Ok(())
}
