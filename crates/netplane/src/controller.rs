//! Serialized control of validated graph publications.

use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use thiserror::Error;

use crate::observability::trace_publication;
use crate::{
    DrainError, GraphMetadata, GraphVersion, Handle, Netplane, PublishError, PublishedVersion,
    VersionId,
};

/// Maximum number of displaced publications kept for rollback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControllerOptions {
    /// Retained publication limit. Zero disables rollback.
    pub max_retained_versions: usize,
}

impl Default for ControllerOptions {
    fn default() -> Self {
        Self {
            max_retained_versions: 2,
        }
    }
}

/// Runtime-scoped optimistic concurrency token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionToken {
    runtime_id: u64,
    version: VersionId,
}

/// Metadata for one publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionSnapshot {
    /// Publication generation.
    pub id: VersionId,
    /// Validated immutable graph declaration.
    pub metadata: GraphMetadata,
}

/// A coherent controller inspection result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControllerSnapshot {
    /// Current publication, absent after a completed shutdown.
    pub active: Option<VersionSnapshot>,
    /// Token for this exact inspection, absent once shutdown begins.
    pub token: Option<VersionToken>,
    /// Retained publications in oldest-to-newest displacement order.
    pub retained: Vec<VersionSnapshot>,
    /// Last successful controller publication.
    pub last_outcome: Option<ReconfigurationOutcome>,
}

/// Kind of successful publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationKind {
    /// New validated graph.
    Replacement,
    /// Republished executable graph from a historical publication.
    Rollback {
        /// Historical publication providing the executable graph.
        source: VersionId,
    },
}

/// Result of a successful controller publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReconfigurationOutcome {
    /// Displaced publication.
    pub previous: VersionId,
    /// Fresh publication generation.
    pub published: VersionId,
    /// Publication kind.
    pub kind: PublicationKind,
}

/// A controller operation failed without changing the active publication.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControllerError {
    /// Shutdown has begun.
    #[error("runtime is shutting down")]
    ShuttingDown,
    /// The token came from another runtime.
    #[error("reconfiguration token belongs to another runtime")]
    ForeignRuntime,
    /// A newer publication exists.
    #[error("stale version: expected {expected:?}, actual {actual:?}")]
    Stale {
        /// Generation supplied by the caller.
        expected: VersionId,
        /// Current active generation.
        actual: VersionId,
    },
    /// Target is absent from retained history.
    #[error("retained version {0:?} is unavailable")]
    UnknownVersion(VersionId),
    /// Publication counter cannot advance.
    #[error("publication generation exhausted")]
    GenerationExhausted,
}

struct ControllerState<Request: Send, Response> {
    runtime: Netplane<Request, Response>,
    history: VecDeque<Arc<PublishedVersion<Request, Response>>>,
    limit: usize,
    last_outcome: Option<ReconfigurationOutcome>,
}

/// Concurrency-safe owner of one Netplane publisher.
///
/// Cloneable [`Handle`]s execute independently of this controller's mutex.
pub struct ReconfigurationController<Request: Send, Response> {
    state: Mutex<ControllerState<Request, Response>>,
    handle: Handle<Request, Response>,
}

impl<Request, Response> ReconfigurationController<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    /// Take exclusive ownership of a runtime and configure bounded history.
    #[must_use]
    pub fn new(runtime: Netplane<Request, Response>, options: ControllerOptions) -> Self {
        let handle = runtime.handle();
        Self {
            state: Mutex::new(ControllerState {
                runtime,
                history: VecDeque::new(),
                limit: options.max_retained_versions,
                last_outcome: None,
            }),
            handle,
        }
    }

    /// Obtain a cloneable execution handle.
    #[must_use]
    pub fn handle(&self) -> Handle<Request, Response> {
        self.handle.clone()
    }

    /// Return the current runtime-scoped token while admission is open.
    ///
    /// Use [`Self::inspect`] when a candidate depends on inspected metadata,
    /// so the token and metadata refer to the same publication.
    #[must_use]
    pub fn current_token(&self) -> Option<VersionToken> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.runtime.admission.can_publish() {
            return None;
        }
        state
            .runtime
            .active_version_id()
            .map(|version| VersionToken {
                runtime_id: state.runtime.runtime_id,
                version,
            })
    }

    /// Inspect active and retained publications under one control lock.
    #[must_use]
    pub fn inspect(&self) -> ControllerSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let active = state.runtime.active.load_full();
        ControllerSnapshot {
            active: active.as_deref().map(snapshot),
            token: if state.runtime.admission.can_publish() {
                active.as_ref().map(|version| VersionToken {
                    runtime_id: state.runtime.runtime_id,
                    version: version.id,
                })
            } else {
                None
            },
            retained: state
                .history
                .iter()
                .map(|version| snapshot(version))
                .collect(),
            last_outcome: state.last_outcome,
        }
    }

    /// Publish a validated candidate only if the token still names the active publication.
    ///
    /// A failed operation leaves the active graph unchanged. A rejected candidate is dropped.
    ///
    /// # Errors
    /// Returns a specific shutdown, foreign token, stale token, or exhaustion error.
    pub fn publish_if_current(
        &self,
        expected: VersionToken,
        candidate: GraphVersion<Request, Response>,
    ) -> Result<ReconfigurationOutcome, ControllerError> {
        let graph = Arc::new(candidate);
        self.publish_graph(expected, &graph, PublicationKind::Replacement)
    }

    /// Republish a retained executable graph under a fresh generation.
    ///
    /// The Tower service state is shared with the historical graph; rollback does not reset it.
    ///
    /// # Errors
    /// Returns a specific shutdown, foreign token, stale token, unavailable target, or exhaustion error.
    pub fn rollback_if_current(
        &self,
        expected: VersionToken,
        target: VersionId,
    ) -> Result<ReconfigurationOutcome, ControllerError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = (|| {
            check(&state, expected)?;
            let graph = state
                .history
                .iter()
                .find(|version| version.id == target)
                .map(|version| Arc::clone(&version.graph))
                .ok_or(ControllerError::UnknownVersion(target))?;
            publish_locked(
                &mut state,
                &graph,
                PublicationKind::Rollback { source: target },
            )
        })();
        let runtime_id = state.runtime.runtime_id;
        let actual = state.runtime.active_version_id();
        drop(state);
        finish_publication(result, runtime_id, expected, actual)
    }

    fn publish_graph(
        &self,
        expected: VersionToken,
        graph: &Arc<GraphVersion<Request, Response>>,
        kind: PublicationKind,
    ) -> Result<ReconfigurationOutcome, ControllerError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = check(&state, expected).and_then(|()| publish_locked(&mut state, graph, kind));
        let runtime_id = state.runtime.runtime_id;
        let actual = state.runtime.active_version_id();
        drop(state);
        finish_publication(result, runtime_id, expected, actual)
    }

    /// Close admission synchronously, release controller history, and return a retryable drain.
    ///
    /// Admission closes before the returned future is polled. A timeout leaves shutdown active;
    /// call this method again to retry draining. Historical graphs are never restored.
    ///
    /// # Errors
    /// The future returns [`DrainError::TimedOut`] if accepted requests remain at its deadline.
    pub fn shutdown(
        &self,
        grace_period: Duration,
    ) -> impl Future<Output = Result<(), DrainError>> + Send + 'static + use<Request, Response>
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (drain, shutdown_event) = state.runtime.shutdown_preparing(grace_period);
        let history = std::mem::take(&mut state.history);
        let runtime_id = state.runtime.runtime_id;
        let retained_count = history.len();
        drop(state);
        shutdown_event.emit();
        tracing::debug!(target: "netplane", event = "reconfiguration.history_cleared", runtime_id, retained_count);
        drop(history);
        drain
    }
}

fn snapshot<Request: Send, Response>(
    version: &PublishedVersion<Request, Response>,
) -> VersionSnapshot {
    VersionSnapshot {
        id: version.id,
        metadata: version.graph.metadata().clone(),
    }
}

fn check<Request, Response>(
    state: &ControllerState<Request, Response>,
    expected: VersionToken,
) -> Result<(), ControllerError>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    if !state.runtime.admission.can_publish() {
        Err(ControllerError::ShuttingDown)
    } else if expected.runtime_id != state.runtime.runtime_id {
        Err(ControllerError::ForeignRuntime)
    } else if let Some(actual) = state.runtime.active_version_id() {
        if actual == expected.version {
            Ok(())
        } else {
            Err(ControllerError::Stale {
                expected: expected.version,
                actual,
            })
        }
    } else {
        Err(ControllerError::ShuttingDown)
    }
}

struct PublicationEvents {
    runtime_id: u64,
    outcome: ReconfigurationOutcome,
    endpoint_ids: Vec<u64>,
    evicted_ids: Vec<VersionId>,
}

impl PublicationEvents {
    fn emit(&self) {
        let runtime_id = self.runtime_id;
        let outcome = self.outcome;
        trace_publication(
            runtime_id,
            outcome.previous,
            outcome.published,
            &self.endpoint_ids,
        );
        for id in &self.evicted_ids {
            tracing::debug!(target: "netplane", event = "reconfiguration.history_evicted", runtime_id, evicted_version = id.get());
        }
        tracing::info!(target: "netplane", event = "reconfiguration.published", runtime_id, old_version = outcome.previous.get(), new_version = outcome.published.get());
        if let PublicationKind::Rollback { source } = outcome.kind {
            tracing::info!(target: "netplane", event = "reconfiguration.rollback", runtime_id, source = source.get(), new_version = outcome.published.get());
        }
    }
}

fn finish_publication<Request: Send, Response>(
    result: Result<
        (
            PublicationEvents,
            Vec<Arc<PublishedVersion<Request, Response>>>,
        ),
        ControllerError,
    >,
    runtime_id: u64,
    expected: VersionToken,
    actual: Option<VersionId>,
) -> Result<ReconfigurationOutcome, ControllerError> {
    match result {
        Ok((events, evicted)) => {
            events.emit();
            drop(evicted);
            Ok(events.outcome)
        }
        Err(error) => {
            let reason = match error {
                ControllerError::ShuttingDown => "shutting_down",
                ControllerError::ForeignRuntime => "foreign_runtime",
                ControllerError::Stale { .. } => "stale",
                ControllerError::UnknownVersion(_) => "unknown_version",
                ControllerError::GenerationExhausted => "generation_exhausted",
            };
            let source = match error {
                ControllerError::UnknownVersion(id) => Some(id),
                _ => None,
            };
            trace_rejection(runtime_id, expected.version, actual, source, reason);
            Err(error)
        }
    }
}

fn trace_rejection(
    runtime_id: u64,
    expected: VersionId,
    actual: Option<VersionId>,
    source: Option<VersionId>,
    reason: &'static str,
) {
    let expected = expected.get();
    match (actual, source) {
        (Some(actual), Some(source)) => {
            tracing::debug!(target: "netplane", event = "reconfiguration.rejected", runtime_id, expected, actual = actual.get(), source = source.get(), reason);
        }
        (Some(actual), None) => {
            tracing::debug!(target: "netplane", event = "reconfiguration.rejected", runtime_id, expected, actual = actual.get(), reason);
        }
        (None, Some(source)) => {
            tracing::debug!(target: "netplane", event = "reconfiguration.rejected", runtime_id, expected, source = source.get(), reason);
        }
        (None, None) => {
            tracing::debug!(target: "netplane", event = "reconfiguration.rejected", runtime_id, expected, reason);
        }
    }
}

fn publish_locked<Request, Response>(
    state: &mut ControllerState<Request, Response>,
    graph: &Arc<GraphVersion<Request, Response>>,
    kind: PublicationKind,
) -> Result<
    (
        PublicationEvents,
        Vec<Arc<PublishedVersion<Request, Response>>>,
    ),
    ControllerError,
>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    let endpoint_ids = graph.endpoint_ids.clone();
    let previous = state
        .runtime
        .publish_retaining(graph)
        .map_err(|error| match error {
            PublishError::ShuttingDown => ControllerError::ShuttingDown,
            PublishError::GenerationExhausted => ControllerError::GenerationExhausted,
        })?;
    let published = VersionId(state.runtime.generation);
    let outcome = ReconfigurationOutcome {
        previous: previous.id,
        published,
        kind,
    };
    state.history.push_back(previous);
    state.last_outcome = Some(outcome);
    let mut evicted = Vec::new();
    while state.history.len() > state.limit {
        if let Some(version) = state.history.pop_front() {
            evicted.push(version);
        }
    }
    let events = PublicationEvents {
        runtime_id: state.runtime.runtime_id,
        outcome,
        endpoint_ids,
        evicted_ids: evicted.iter().map(|version| version.id).collect(),
    };
    Ok((events, evicted))
}
