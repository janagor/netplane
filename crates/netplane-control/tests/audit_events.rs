//! Snapshot consistency, cancellation, and structured tracing contracts.
use netplane::{
    ControllerOptions, GraphDraft, GraphVersion, Netplane, ReconfigurationController, RuntimeLimits,
};
use netplane_control::{
    GraphPreparer, Reconciler, ReconciliationDriver, ReconciliationPhase as Phase, UpdatePolicy,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::SubscriberExt};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
fn graph(value: u32) -> Result<GraphVersion<(), u32>, netplane::ConfigError> {
    GraphDraft::new(tower::service_fn(move |()| async move {
        Ok::<_, Infallible>(value)
    }))
    .validate()
}
struct Preparer(Option<Arc<Semaphore>>);
impl GraphPreparer<u32, (), u32> for Preparer {
    type Error = &'static str;
    fn validate(&self, desired: &u32) -> Result<(), Self::Error> {
        if *desired == 0 {
            Err("private application data")
        } else {
            Ok(())
        }
    }
    async fn prepare(&self, desired: u32) -> Result<GraphVersion<(), u32>, Self::Error> {
        if desired == 9 {
            return Err("private transient error");
        }
        if let Some(gate) = &self.0 {
            gate.acquire().await.map_err(|_| "closed")?.forget();
        }
        graph(desired).map_err(|_| "graph rejected")
    }
}
fn controller()
-> Result<Arc<ReconfigurationController<(), u32>>, Box<dyn std::error::Error + Send + Sync>> {
    Ok(Arc::new(ReconfigurationController::new(
        Netplane::new(graph(1)?, RuntimeLimits { max_outstanding: 4 })?,
        ControllerOptions::default(),
    )))
}
fn policy() -> UpdatePolicy {
    UpdatePolicy {
        debounce: Duration::ZERO,
        ..UpdatePolicy::default()
    }
}
#[derive(Default)]
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}
struct Capture {
    events: Arc<Mutex<Vec<Fields>>>,
    inspect: Box<dyn Fn() + Send + Sync>,
    on_publish: Box<dyn Fn() + Send + Sync>,
}
impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields
            .0
            .get("event")
            .is_some_and(|v| v == "\"reconfiguration.published\"")
        {
            (self.on_publish)();
        }
        if event.metadata().target() == "netplane_control" {
            (self.inspect)();
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(fields);
        }
    }
}

#[tokio::test]
async fn audit_events_allow_reentrant_status_and_preserve_commit_race() -> TestResult {
    let control = controller()?;
    let runtime_id = control.runtime_id();
    let gate = Arc::new(Semaphore::new(2));
    let (driver, updates) = ReconciliationDriver::new(
        Reconciler::new(control, Preparer(Some(Arc::clone(&gate)))),
        policy(),
    )?;
    let inspect = updates.clone();
    let submit = updates.clone();
    let once = AtomicBool::new(false);
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Capture {
        events: Arc::clone(&events),
        inspect: Box::new(move || {
            if inspect.status().phase == Phase::Stopped {
                assert_eq!(
                    inspect.submit(42),
                    Err(netplane_control::UpdateError::Stopped)
                );
            }
        }),
        on_publish: Box::new(move || {
            if !once.swap(true, Ordering::SeqCst) {
                assert_eq!(submit.submit(3), Ok(2));
            }
        }),
    });
    tracing::subscriber::set_global_default(subscriber)?;
    let mut status = updates.subscribe_status();
    updates.submit(2)?;
    let worker = tokio::spawn(driver.run(|_| true));
    loop {
        if status.borrow_and_update().is_converged() && updates.status().applied_revision == Some(2)
        {
            break;
        }
        status.changed().await?;
    }
    updates.submit(3)?;
    loop {
        if status.borrow_and_update().applied_revision == Some(3) {
            break;
        }
        status.changed().await?;
    }
    updates.submit(0)?;
    loop {
        if status.borrow_and_update().phase == Phase::Blocked {
            break;
        }
        status.changed().await?;
    }
    updates.submit(9)?;
    wait_phase(&mut status, Phase::RetryScheduled).await?;
    updates.submit(6)?;
    wait_phase(&mut status, Phase::Preparing).await?;
    updates.submit(7)?;
    gate.add_permits(2);
    wait_phase(&mut status, Phase::Idle).await?;
    assert_eq!(updates.status().applied_revision, Some(7));
    updates.shutdown();
    worker.await?;
    assert_eq!(updates.status().phase, Phase::Stopped);
    let events = events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    verify_events(&events, runtime_id)?;
    drop(events);
    Ok(())
}

async fn wait_phase(
    status: &mut tokio::sync::watch::Receiver<netplane_control::ReconciliationStatus>,
    phase: Phase,
) -> TestResult {
    loop {
        if status.borrow_and_update().phase == phase {
            return Ok(());
        }
        status.changed().await?;
    }
}

fn verify_events(events: &[Fields], runtime_id: u64) -> TestResult {
    let mut sequences = BTreeSet::new();
    for event in events {
        let sequence: u64 = event
            .0
            .get("event_sequence")
            .ok_or("missing sequence")?
            .parse()?;
        let reconciler = event.0.get("reconciler_id").ok_or("missing reconciler")?;
        assert!(sequence > 0);
        assert!(
            sequences.insert((reconciler, sequence)),
            "duplicate logical event"
        );
        assert_eq!(event.0.get("runtime_id"), Some(&runtime_id.to_string()));
        assert!(!format!("{:?}", event.0).contains("private application data"));
    }
    for name in [
        "desired_updated",
        "started",
        "applied",
        "unchanged",
        "failed",
        "retry_scheduled",
        "superseded",
        "stopped",
    ] {
        assert!(
            events
                .iter()
                .any(|e| e.0.get("event") == Some(&format!("\"reconcile.{name}\""))),
            "missing {name}: {:?}",
            events.iter().map(|e| &e.0).collect::<Vec<_>>()
        );
    }
    assert!(events.iter().any(
        |e| e.0.get("event") == Some(&"\"reconcile.applied\"".into())
            && e.0.get("revision") == Some(&"1".into())
            && e.0.get("desired_revision") == Some(&"2".into())
            && e.0.get("applied_revision") == Some(&"1".into())
    ));
    Ok(())
}
