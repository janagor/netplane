//! Structured runtime event and publication identity coverage.

use std::{
    convert::Infallible,
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

use netplane::{
    Capacity, GraphBuilder, GraphDraft, Netplane, PublishError, RuntimeLimits, VersionId,
};
use tower::service_fn;
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::SubscriberExt};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[derive(Default)]
struct Fields(Vec<(String, String)>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.push((field.name().into(), format!("{value:?}")));
    }
}

struct Events(Arc<Mutex<Vec<Fields>>>);

impl<S: Subscriber> Layer<S> for Events {
    fn on_event(&self, event: &Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == "netplane" {
            let mut fields = Fields::default();
            event.record(&mut fields);
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(fields);
        }
    }
}

fn graph() -> Result<netplane::GraphVersion<usize, usize>, netplane::ConfigError> {
    GraphDraft::new(service_fn(|value: usize| async move {
        Ok::<_, Infallible>(value)
    }))
    .validate()
}

fn contains(events: &[Fields], event: &str, version: Option<VersionId>) -> bool {
    events.iter().any(|fields| {
        let named = fields
            .0
            .iter()
            .any(|(name, value)| name == "event" && value == &format!("{event:?}"));
        let version_matches = version.is_none_or(|id| {
            fields
                .0
                .iter()
                .any(|(name, value)| name == "version_id" && value == &id.get().to_string())
        });
        named && version_matches
    })
}

fn count(events: &[Fields], event: &str, version: VersionId) -> usize {
    events
        .iter()
        .filter(|fields| -> bool { contains(std::slice::from_ref(fields), event, Some(version)) })
        .count()
}

fn field_is(events: &[Fields], event: &str, field: &str, value: &str) -> bool {
    events.iter().any(|fields| -> bool {
        fields.0.iter().any(|(name, recorded)| -> bool {
            name == "event" && recorded == &format!("{event:?}")
        }) && fields
            .0
            .iter()
            .any(|(name, recorded)| -> bool { name == field && recorded == value })
    })
}

fn event_position(events: &[Fields], event: &str) -> Option<usize> {
    events
        .iter()
        .position(|fields| -> bool { contains(std::slice::from_ref(fields), event, None) })
}

#[tokio::test]
async fn selected_version_and_terminal_events_follow_future_lifetime() -> TestResult {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Events(Arc::clone(&captured)));
    let _scope = tracing::subscriber::set_default(subscriber);
    let mut runtime = Netplane::new(graph()?, RuntimeLimits { max_outstanding: 2 })?;
    let first = runtime
        .active_version_id()
        .ok_or("missing initial version")?;
    assert_eq!(first.get(), 1);
    let handle = runtime.handle();
    let old = handle.try_execute(1)?;
    runtime.publish(graph()?)?;
    let second = runtime.active_version_id().ok_or("missing replacement")?;
    assert_eq!(second.get(), 2);
    assert_eq!(handle.try_execute(2)?.await?, 2);
    {
        let events = captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!contains(&events, "version.released", Some(first)));
    }
    drop(old);
    runtime.shutdown(Duration::from_secs(1)).await?;
    let events = captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(contains(&events, "request.cancelled", Some(first)));
    assert!(contains(&events, "request.completed", Some(second)));
    assert_eq!(count(&events, "request.cancelled", first), 1);
    assert_eq!(count(&events, "request.completed", second), 1);
    assert!(contains(&events, "request.admitted", Some(first)));
    assert!(contains(&events, "version.released", Some(first)));
    assert!(contains(&events, "shutdown.completed", None));
    assert_eq!(runtime.active_version_id(), None);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rejection_and_failure_have_structured_categories() -> TestResult {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Events(Arc::clone(&captured)));
    let _scope = tracing::subscriber::set_default(subscriber);
    let failed = GraphDraft::new(service_fn(|_: usize| async move {
        Err::<usize, _>(std::io::Error::other("private service message"))
    }))
    .validate()?;
    let mut runtime = Netplane::new(failed, RuntimeLimits { max_outstanding: 1 })?;
    let handle = runtime.handle();
    let pending = handle.try_execute(1)?;
    assert!(handle.try_execute(2).is_err());
    drop(pending);
    assert!(handle.try_execute(3)?.await.is_err());
    let drain = runtime.shutdown(Duration::from_secs(1));
    assert!(handle.try_execute(4).is_err());
    drain.await?;
    let events = captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(contains(&events, "request.failed", None));
    assert!(field_is(
        &events,
        "request.failed",
        "error_kind",
        "\"service_error\""
    ));
    assert!(contains(&events, "request.rejected", None));
    assert!(field_is(
        &events,
        "request.rejected",
        "reason",
        "\"capacity_exceeded\""
    ));
    assert!(field_is(
        &events,
        "request.rejected",
        "reason",
        "\"admission_closed\""
    ));
    assert!(
        !format!(
            "{:?}",
            events.iter().map(|fields| &fields.0).collect::<Vec<_>>()
        )
        .contains("private service message")
    );
    Ok(())
}

#[tokio::test]
async fn rejected_publication_keeps_generation_and_validation_has_safe_reason() -> TestResult {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Events(Arc::clone(&captured)));
    let _scope = tracing::subscriber::set_default(subscriber);
    let invalid = GraphDraft::new(service_fn(|value: usize| async move {
        Ok::<_, Infallible>(value)
    }))
    .capacity(Capacity {
        max_in_flight: 0,
        buffer_capacity: 1,
    })
    .validate();
    assert!(invalid.is_err());
    let mut runtime = Netplane::new(graph()?, RuntimeLimits { max_outstanding: 1 })?;
    let initial = runtime.active_version_id();
    let drain = runtime.shutdown(Duration::from_secs(1));
    assert!(matches!(
        runtime.publish(graph()?),
        Err(PublishError::ShuttingDown)
    ));
    assert_eq!(runtime.active_version_id(), initial);
    drain.await?;
    let events = captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(field_is(
        &events,
        "graph.validation_failed",
        "reason",
        "\"zero_concurrency\""
    ));
    assert!(contains(&events, "version.publish_rejected", None));
    Ok(())
}

#[tokio::test]
async fn shutdown_timeout_and_retry_report_distinct_attempts() -> TestResult {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Events(Arc::clone(&captured)));
    let _scope = tracing::subscriber::set_default(subscriber);
    let mut runtime = Netplane::new(graph()?, RuntimeLimits { max_outstanding: 1 })?;
    let pending = runtime.handle().try_execute(1)?;
    assert!(runtime.shutdown(Duration::ZERO).await.is_err());
    drop(pending);
    runtime.shutdown(Duration::from_secs(1)).await?;
    let events = captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(contains(&events, "shutdown.timed_out", None));
    assert!(contains(&events, "shutdown.completed", None));
    assert!(field_is(
        &events,
        "shutdown.started",
        "shutdown_attempt_id",
        "1"
    ));
    assert!(field_is(
        &events,
        "shutdown.timed_out",
        "shutdown_attempt_id",
        "1"
    ));
    assert!(field_is(
        &events,
        "shutdown.started",
        "shutdown_attempt_id",
        "2"
    ));
    assert!(field_is(
        &events,
        "shutdown.completed",
        "shutdown_attempt_id",
        "2"
    ));
    Ok(())
}

#[tokio::test]
async fn buffer_worker_panic_is_reported_without_panic_payload() -> TestResult {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Events(Arc::clone(&captured)));
    let _scope = tracing::subscriber::set_default(subscriber);
    let failed = GraphDraft::new(service_fn(
        |_: usize| -> std::future::Ready<Result<usize, Infallible>> {
            std::panic::resume_unwind(Box::new("private worker panic payload"))
        },
    ))
    .validate()?;
    let runtime = Netplane::new(failed, RuntimeLimits { max_outstanding: 1 })?;
    assert!(runtime.handle().execute(1).await.is_err());
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if contains(
                &captured
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                "worker.terminated",
                None,
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let events = captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(contains(&events, "worker.terminated", None));
    assert!(field_is(
        &events,
        "request.failed",
        "error_kind",
        "\"buffer_closed\""
    ));
    assert!(field_is(
        &events,
        "worker.terminated",
        "reason",
        "\"panicked\""
    ));
    let worker_endpoint_id = events
        .iter()
        .find(|fields| -> bool {
            contains(std::slice::from_ref(fields), "worker.terminated", None)
        })
        .and_then(|fields| -> Option<&str> {
            fields
                .0
                .iter()
                .find(|(name, _)| name == "endpoint_id")
                .map(|(_, value)| value.as_str())
        })
        .ok_or("missing worker endpoint ID")?;
    assert!(field_is(
        &events,
        "version.endpoint",
        "endpoint_id",
        worker_endpoint_id
    ));
    assert!(
        !format!(
            "{:?}",
            events.iter().map(|fields| &fields.0).collect::<Vec<_>>()
        )
        .contains("private worker panic payload")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_and_release_events_follow_accepted_futures_across_threads() -> TestResult {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Events(Arc::clone(&captured)));
    let _scope = tracing::subscriber::set_default(subscriber);
    let mut runtime = Netplane::new(graph()?, RuntimeLimits { max_outstanding: 2 })?;
    let first = runtime
        .active_version_id()
        .ok_or("missing initial version")?;
    let handle = runtime.handle();
    let completed = handle.try_execute(1)?;
    let cancelled = handle.try_execute(2)?;
    let task = tokio::spawn(completed);
    assert_eq!(task.await??, 1);
    runtime.publish(graph()?)?;
    std::thread::spawn(move || drop(cancelled))
        .join()
        .map_err(|_| "drop thread panicked")?;
    let events = captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(count(&events, "request.completed", first), 1);
    assert_eq!(count(&events, "request.cancelled", first), 1);
    assert_eq!(count(&events, "version.released", first), 1);
    Ok(())
}

#[tokio::test]
async fn replacement_events_precede_unretained_version_release() -> TestResult {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Events(Arc::clone(&captured)));
    let _scope = tracing::subscriber::set_default(subscriber);
    let mut runtime = Netplane::new(graph()?, RuntimeLimits { max_outstanding: 1 })?;
    runtime.publish(graph()?)?;
    let events = captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let published = events
        .iter()
        .position(|fields| -> bool {
            field_is(
                std::slice::from_ref(fields),
                "version.published",
                "new_version",
                "2",
            )
        })
        .ok_or("missing replacement publication")?;
    let replaced = event_position(&events, "version.replaced").ok_or("missing replacement")?;
    let released = event_position(&events, "version.released").ok_or("missing release")?;
    assert!(contains(&events, "version.endpoint", None));
    drop(events);
    assert!(published < replaced && replaced < released);
    Ok(())
}

#[tokio::test]
async fn graph_builder_validation_reports_structural_reason() -> TestResult {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Events(Arc::clone(&captured)));
    let _scope = tracing::subscriber::set_default(subscriber);
    assert!(GraphBuilder::<usize, usize>::new().validate().is_err());
    let events = captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(field_is(
        &events,
        "graph.validation_failed",
        "reason",
        "\"missing_entry\""
    ));
    Ok(())
}

#[tokio::test]
async fn execution_and_shutdown_work_without_a_subscriber() -> TestResult {
    let mut runtime = Netplane::new(graph()?, RuntimeLimits { max_outstanding: 1 })?;
    assert_eq!(runtime.handle().execute(1).await?, 1);
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn worker_and_publication_keep_their_respective_subscribers() -> TestResult {
    let worker_events = Arc::new(Mutex::new(Vec::new()));
    let publication_events = Arc::new(Mutex::new(Vec::new()));
    let version = {
        let subscriber = tracing_subscriber::registry().with(Events(Arc::clone(&worker_events)));
        let _scope = tracing::subscriber::set_default(subscriber);
        graph()?
    };
    {
        let subscriber =
            tracing_subscriber::registry().with(Events(Arc::clone(&publication_events)));
        let _scope = tracing::subscriber::set_default(subscriber);
        let runtime = Netplane::new(version, RuntimeLimits { max_outstanding: 1 })?;
        let published = publication_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(contains(&published, "version.endpoint", None));
        drop(published);
        drop(runtime);
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let terminated = {
                let observed = worker_events
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                contains(&observed, "worker.terminated", None)
            };
            if terminated {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let worker = worker_events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let publication = publication_events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(contains(&worker, "worker.terminated", None));
    assert!(!contains(&worker, "version.endpoint", None));
    assert!(!contains(&publication, "worker.terminated", None));
    Ok(())
}
