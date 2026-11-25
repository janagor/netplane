use super::{
    ControllerError, PublicationKind, ReconfigurationController, ReconfigurationOutcome,
    VersionToken, check,
};
use crate::{GraphVersion, VersionId};
use std::sync::Arc;

impl<Request: Send + 'static, Response: Send + 'static>
    ReconfigurationController<Request, Response>
{
    /// Accept a bounded control-plane decision only while this publication is current.
    ///
    /// Advanced synchronization primitive for control-plane integrations. Ordinary
    /// callers should use `inspect` and conditional publication instead.
    /// Runs `accept` under the publication mutex, including exclusion of shutdown
    /// and rollback. It must not block, panic, emit tracing, call application code or
    /// reenter this controller. Even calling `inspect` recursively can deadlock.
    /// Acquire external guards before calling this method;
    /// release them before tracing or application-owned resource cleanup.
    /// This adds no locks to request admission or execution paths.
    ///
    /// # Errors
    /// Returns `Stale` or `ShuttingDown` without running `accept`.
    pub fn with_current_publication<T>(
        &self,
        expected: VersionId,
        accept: impl FnOnce() -> T,
    ) -> Result<T, ControllerError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        check(
            &state,
            VersionToken {
                runtime_id: state.runtime.runtime_id,
                version: expected,
            },
        )?;
        let accepted = accept();
        drop(state);
        Ok(accepted)
    }

    /// Publish with an infallible, bounded commit acknowledgement under the control lock.
    ///
    /// Advanced synchronization primitive for control-plane integrations. Prefer
    /// `publish_if_current` or `publish_if_current_guarded` for ordinary callers.
    /// `committed` runs only after a successful swap, before releasing `guard` or
    /// invoking tracing callbacks. It must only update internal synchronization
    /// state: never block, panic, emit tracing, reenter the controller or run
    /// application callbacks. Use this to distinguish pending and committed intent.
    /// Recursive controller access can deadlock. A callback panic happens after
    /// the swap and does not undo publication; callbacks must be infallible.
    /// Lock ordering and guard cleanup follow `publish_if_current_guarded`.
    ///
    /// # Errors
    /// Returns the conditional-publication errors without calling `committed`.
    pub fn publish_if_current_guarded_with<G>(
        &self,
        expected: VersionToken,
        candidate: GraphVersion<Request, Response>,
        guard: G,
        committed: impl FnOnce(ReconfigurationOutcome),
    ) -> Result<ReconfigurationOutcome, ControllerError> {
        self.publish_graph(
            expected,
            &Arc::new(candidate),
            PublicationKind::Replacement,
            guard,
            committed,
        )
    }
}
