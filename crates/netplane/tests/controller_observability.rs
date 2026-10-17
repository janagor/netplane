//! Control event fields and subscriber reentrancy.

use std::{
    convert::Infallible,
    error::Error,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use netplane::{ControllerOptions, GraphDraft, Netplane, ReconfigurationController, RuntimeLimits};
use tower::service_fn;
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::SubscriberExt};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;
type Controller = ReconfigurationController<usize, usize>;

fn graph() -> Result<netplane::GraphVersion<usize, usize>, netplane::ConfigError> {
    GraphDraft::new(service_fn(|value: usize| async move {
        Ok::<_, Infallible>(value)
    }))
    .validate()
}

#[derive(Default)]
struct Fields(Vec<(String, String)>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.push((field.name().into(), format!("{value:?}")));
    }
}

impl Fields {
    fn has(&self, name: &str, value: &str) -> bool {
        self.0
            .iter()
            .any(|(field, recorded)| field == name && recorded == value)
    }
}

struct Capture {
    events: Arc<Mutex<Vec<Fields>>>,
    controller: Option<Weak<Controller>>,
    callbacks: Arc<AtomicUsize>,
}

impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "netplane" {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let publication = fields.has("event", "\"version.published\"")
            || fields.has("event", "\"reconfiguration.published\"");
        let shutdown = fields.has("event", "\"shutdown.started\"");
        if (publication || shutdown)
            && let Some(controller) = self.controller.as_ref().and_then(Weak::upgrade)
        {
            let snapshot = controller.inspect();
            if (publication && snapshot.active.is_some() && snapshot.token.is_some())
                || (shutdown && snapshot.token.is_none())
            {
                self.callbacks.fetch_add(1, Ordering::SeqCst);
            }
        }
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(fields);
    }
}

fn has(events: &[Fields], event: &str, field: &str, value: &str) -> bool {
    events
        .iter()
        .any(|fields| fields.has("event", &format!("{event:?}")) && fields.has(field, value))
}

#[tokio::test]
async fn controller_events_include_publication_rollback_and_rejection() -> TestResult {
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Capture {
        events: Arc::clone(&events),
        controller: None,
        callbacks: Arc::new(AtomicUsize::new(0)),
    });
    let _scope = tracing::subscriber::set_default(subscriber);
    let runtime = Netplane::new(graph()?, RuntimeLimits { max_outstanding: 2 })?;
    let control = ReconfigurationController::new(
        runtime,
        ControllerOptions {
            max_retained_versions: 1,
        },
    );
    let snapshot = control.inspect();
    let original = snapshot.active.ok_or("missing active")?.id;
    let token = snapshot.token.ok_or("missing token")?;
    let replacement = control.publish_if_current(token, graph()?)?;
    assert!(control.publish_if_current(token, graph()?).is_err());
    control.rollback_if_current(control.inspect().token.ok_or("missing token")?, original)?;
    assert!(
        control
            .rollback_if_current(control.inspect().token.ok_or("missing token")?, original)
            .is_err()
    );
    control.shutdown(Duration::from_secs(1)).await?;
    assert!(control.publish_if_current(token, graph()?).is_err());
    let events = events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(has(
        &events,
        "reconfiguration.published",
        "new_version",
        &replacement.published.get().to_string()
    ));
    assert!(has(
        &events,
        "reconfiguration.rollback",
        "source",
        &original.get().to_string()
    ));
    assert!(has(
        &events,
        "reconfiguration.rejected",
        "reason",
        "\"stale\""
    ));
    assert!(has(
        &events,
        "reconfiguration.rejected",
        "source",
        &original.get().to_string()
    ));
    let stale = events
        .iter()
        .find(|fields| {
            fields.has("event", "\"reconfiguration.rejected\"") && fields.has("reason", "\"stale\"")
        })
        .ok_or("missing stale event")?;
    assert!(!stale.0.iter().any(|(name, _)| name == "source"));
    let stopped = events
        .iter()
        .find(|fields| {
            fields.has("event", "\"reconfiguration.rejected\"")
                && fields.has("reason", "\"shutting_down\"")
        })
        .ok_or("missing shutdown rejection")?;
    assert!(!stopped.0.iter().any(|(name, _)| name == "actual"));
    assert!(has(
        &events,
        "reconfiguration.history_evicted",
        "evicted_version",
        &original.get().to_string()
    ));
    assert!(
        events
            .iter()
            .any(|fields| fields.has("event", "\"reconfiguration.history_cleared\""))
    );
    drop(events);
    Ok(())
}

#[tokio::test]
async fn publication_and_shutdown_subscribers_can_inspect_controller() -> TestResult {
    let runtime = Netplane::new(graph()?, RuntimeLimits { max_outstanding: 2 })?;
    let control = Arc::new(ReconfigurationController::new(
        runtime,
        ControllerOptions::default(),
    ));
    let token = control.inspect().token.ok_or("missing token")?;
    let candidate = graph()?;
    let callbacks = Arc::new(AtomicUsize::new(0));
    let subscriber = tracing_subscriber::registry().with(Capture {
        events: Arc::new(Mutex::new(Vec::new())),
        controller: Some(Arc::downgrade(&control)),
        callbacks: Arc::clone(&callbacks),
    });
    let (send, receive) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let _scope = tracing::subscriber::set_default(subscriber);
        let result = control.publish_if_current(token, candidate);
        let _drain = control.shutdown(Duration::from_secs(1));
        let _ = send.send(result);
    });
    receive.recv_timeout(Duration::from_secs(2))??;
    thread.join().map_err(|_| "publication thread panicked")?;
    assert_eq!(callbacks.load(Ordering::SeqCst), 3);
    Ok(())
}
