use super::{
    ControllerError, PublicationKind, PublishedVersion, ReconfigurationOutcome, VersionId,
    VersionToken,
};
use crate::observability::trace_publication;
use std::sync::Arc;

pub(super) struct PublicationEvents {
    pub(super) runtime_id: u64,
    pub(super) outcome: ReconfigurationOutcome,
    pub(super) endpoint_ids: Vec<u64>,
    pub(super) evicted_ids: Vec<VersionId>,
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

pub(super) fn finish_publication<Request: Send, Response>(
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
