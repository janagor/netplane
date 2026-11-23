use std::sync::{Arc, Mutex};

use netplane::{ControllerError, ReconfigurationController};
use tokio::sync::Semaphore;

use crate::{ApplyError, ApplyOutcome, GraphPreparer, LastApplied};

/// Single-flight, explicit reconciler sharing an existing runtime controller.
///
/// Separate reconcilers coordinate through conditional publication, not shared
/// preparation permits. Each keeps an independent last-applied record. External
/// publishers may supersede any successful result. Prefer one authoritative
/// reconciler per managed runtime, with configuration producers feeding it. This
/// integration recommendation is not enforced by the API.
pub struct Reconciler<D, P, Request: Send, Response> {
    controller: Arc<ReconfigurationController<Request, Response>>,
    preparer: P,
    preparation: Semaphore,
    last_applied: Mutex<Option<Arc<LastApplied<D>>>>,
}

impl<D, P, Request, Response> Reconciler<D, P, Request, Response>
where
    D: Clone + Eq + Send + Sync,
    Request: Send + 'static,
    Response: Send + 'static,
    P: GraphPreparer<D, Request, Response> + Sync,
{
    /// Start with no configuration record, even if the controller already runs a graph.
    #[must_use]
    pub const fn new(
        controller: Arc<ReconfigurationController<Request, Response>>,
        preparer: P,
    ) -> Self {
        Self {
            controller,
            preparer,
            preparation: Semaphore::const_new(1),
            last_applied: Mutex::new(None),
        }
    }

    /// Return the last successful application, which may have been superseded externally.
    #[must_use]
    pub fn last_applied(&self) -> Option<Arc<LastApplied<D>>> {
        self.last_applied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Validate, compare, prepare, and conditionally publish one desired configuration.
    ///
    /// Validation precedes deduplication. Equal configurations skip construction
    /// only when the recorded generation is still active at inspection. There is
    /// no retry after a conflict: inspect and explicitly resubmit if appropriate.
    /// Dropping this future before publication releases preparation resources and
    /// the permit. Publication and recording success contain no intervening await.
    /// As with all inspections, external publication can occur after a no-op check.
    ///
    /// # Errors
    /// Returns [`ApplyError::Busy`] immediately when another polled call is active,
    /// or a validation, preparation, or conditional publication error. Shutdown is
    /// rejected before preparation when already visible, or at publication otherwise.
    pub async fn apply(&self, desired: D) -> Result<ApplyOutcome, ApplyError<P::Error>> {
        self.apply_checked(desired, || true).await
    }

    pub(crate) async fn apply_checked(
        &self,
        desired: D,
        current: impl Fn() -> bool + Send,
    ) -> Result<ApplyOutcome, ApplyError<P::Error>> {
        let _permit = self
            .preparation
            .try_acquire()
            .map_err(|_| ApplyError::Busy)?;
        self.preparer
            .validate(&desired)
            .map_err(ApplyError::Validation)?;
        let snapshot = self.controller.inspect();
        let token = snapshot
            .token
            .ok_or(ApplyError::Publication(ControllerError::ShuttingDown))?;
        if let Some(last) = self.last_applied() {
            if last.desired == desired
                && snapshot
                    .active
                    .as_ref()
                    .is_some_and(|active| active.id == last.published)
            {
                return Ok(ApplyOutcome::Unchanged(last.published));
            }
        }
        let recorded = desired.clone();
        let candidate = self
            .preparer
            .prepare(desired)
            .await
            .map_err(ApplyError::Preparation)?;
        // This check and conditional publication are separate operations. Driver
        // shutdown or a newer update can race after the check and before commit.
        if !current() {
            return Err(ApplyError::Superseded);
        }
        let outcome = self
            .controller
            .publish_if_current(token, candidate)
            .map_err(ApplyError::Publication)?;
        let record = Arc::new(LastApplied {
            desired: recorded,
            published: outcome.published,
        });
        let previous = self
            .last_applied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(record);
        drop(previous);
        Ok(ApplyOutcome::Applied(outcome))
    }
}
