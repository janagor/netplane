//! Real rollout audit records retain stage-local decisions across later transitions.
mod rollout_support;
use netplane_control::rollout::{RolloutDriver, RolloutState};
use rollout_support::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    instrument::WithSubscriber,
};
use tracing_subscriber::{Layer, layer::SubscriberExt};

#[derive(Default)]
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}
struct Capture(Arc<Mutex<Vec<Fields>>>);
impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(fields);
    }
}

#[tokio::test(start_paused = true)]
async fn audit_event_consistency() -> TestResult {
    let (updates, reconciliation, _) = fixture().await?;
    let metrics = Metrics::new(0);
    let (driver, handle) = RolloutDriver::start(
        updates.clone(),
        metrics.clone(),
        spec(&[5, 100]),
        Duration::from_secs(1),
    )?;
    let events = Arc::new(Mutex::new(Vec::new()));
    let task = tokio::spawn(
        driver
            .run()
            .with_subscriber(tracing_subscriber::registry().with(Capture(events.clone()))),
    );
    state(&handle, RolloutState::Blocked).await;
    metrics.eligibility.store(1, Ordering::SeqCst);
    ticks(2).await;
    metrics.count.store(100, Ordering::SeqCst);
    state(&handle, RolloutState::Completed).await;
    task.await?;
    {
        let events = events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let blocked = events
            .iter()
            .find(|event| {
                event
                    .0
                    .get("blocked_reason")
                    .is_some_and(|value| value == "Some(EligibilityChanged)")
            })
            .ok_or("missing eligibility transition")?;
        assert_eq!(
            blocked.0.get("event").map(String::as_str),
            Some("\"rollout.stage.blocked\"")
        );
        assert_eq!(blocked.0.get("state").map(String::as_str), Some("Blocked"));
        assert_eq!(
            blocked.0.get("evaluation").map(String::as_str),
            Some("Some(Insufficient(EligibilityChanged))")
        );
        let passed: Vec<_> = events
            .iter()
            .filter(|event| {
                event
                    .0
                    .get("event")
                    .is_some_and(|value| value == "\"rollout.stage.passed\"")
            })
            .collect();
        assert_eq!(passed.len(), 2);
        for (index, event) in passed.iter().enumerate() {
            assert_eq!(event.0.get("stage"), Some(&index.to_string()));
            assert_eq!(
                event.0.get("evaluation").map(String::as_str),
                Some("Some(Passed)")
            );
            assert_eq!(
                event.0.get("blocked_reason").map(String::as_str),
                Some("None")
            );
            assert!(event.0.contains_key("publication_id"));
        }
        drop(events);
    }
    updates.shutdown();
    reconciliation.await?;
    Ok(())
}
