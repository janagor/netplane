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
    pub(crate) observation: Arc<crate::events::Observation>,
}

impl<D, P, Request, Response> Reconciler<D, P, Request, Response>
where
    D: Clone + Eq + Send + Sync,
    Request: Send + 'static,
    Response: Send + 'static,
    P: GraphPreparer<D, Request, Response> + Sync,
{
    /// Start with no configuration record, even if the controller already runs a graph.
    ///
    /// This constructor inspects the controller to initialize publication status.
    /// It is no longer `const`; consumers must construct reconcilers at runtime.
    #[must_use]
    pub fn new(controller: Arc<ReconfigurationController<Request, Response>>, preparer: P) -> Self {
        let snapshot = controller.inspect();
        let mut observation = crate::events::Observation::new(
            controller.runtime_id(),
            snapshot.active.map(|active| active.id),
        );
        let inspector = Arc::downgrade(&controller);
        observation.inspect_publication = Some(Box::new(move || {
            inspector
                .upgrade()
                .and_then(|controller| controller.inspect().active.map(|active| active.id))
        }));
        let controller_guard = Arc::downgrade(&controller);
        observation.accept_publication = Some(Box::new(move |publication, accept| {
            controller_guard.upgrade().is_some_and(|controller| {
                controller
                    .with_current_publication(publication, accept)
                    .is_ok()
            })
        }));
        let observation = Arc::new(observation);
        Self {
            observation,
            controller,
            preparer,
            preparation: Semaphore::const_new(1),
            last_applied: Mutex::new(None),
        }
    }

    /// Read a consistent reconciliation snapshot. External publications are not monitored.
    #[must_use]
    pub fn status(&self) -> crate::ReconciliationStatus {
        self.observation.status()
    }

    /// Subscribe to bounded latest-value status; intermediate transitions may coalesce.
    #[must_use]
    pub fn subscribe_status(&self) -> tokio::sync::watch::Receiver<crate::ReconciliationStatus> {
        self.observation.subscribe()
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
    /// Exhausted local revision numbers reject new work before validation.
    pub async fn apply(&self, desired: D) -> Result<ApplyOutcome, ApplyError<P::Error>> {
        if self.status().phase == crate::ReconciliationPhase::Stopped {
            return Err(ApplyError::Publication(ControllerError::ShuttingDown));
        }
        let _permit = self
            .preparation
            .try_acquire()
            .map_err(|_| ApplyError::Busy)?;
        let revision = self
            .status()
            .desired_revision
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(ApplyError::RevisionExhausted)?;
        self.observation.desired(revision).emit();
        self.observation.started(revision);
        let mut guard = AttemptGuard {
            observation: Arc::clone(&self.observation),
            revision,
            armed: true,
        };
        let result = self.apply_candidate(desired, || Some(()), |_| {}).await;
        guard.armed = false;
        self.observation.finish(revision, &result, false);
        if matches!(
            result,
            Err(ApplyError::Publication(ControllerError::ShuttingDown))
        ) {
            self.observation.stop();
        }
        result
    }

    pub(crate) fn active_publication(&self) -> Option<netplane::VersionId> {
        self.controller.inspect().active.map(|active| active.id)
    }

    pub(crate) async fn apply_checked<G>(
        &self,
        desired: D,
        current: impl Fn() -> Option<G> + Send,
        committed: impl Fn(netplane::VersionId) + Send,
    ) -> Result<ApplyOutcome, ApplyError<P::Error>> {
        let _permit = self
            .preparation
            .try_acquire()
            .map_err(|_| ApplyError::Busy)?;
        self.apply_candidate(desired, current, committed).await
    }

    async fn apply_candidate<G>(
        &self,
        desired: D,
        current: impl Fn() -> Option<G> + Send,
        committed: impl Fn(netplane::VersionId) + Send,
    ) -> Result<ApplyOutcome, ApplyError<P::Error>> {
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
                let _guard = current().ok_or(ApplyError::Superseded)?;
                self.controller
                    .with_current_publication(last.published, || committed(last.published))
                    .map_err(ApplyError::Publication)?;
                return Ok(ApplyOutcome::Unchanged(last.published));
            }
        }
        let recorded = desired.clone();
        let candidate = self
            .preparer
            .prepare(desired)
            .await
            .map_err(ApplyError::Preparation)?;
        // The caller's guard serializes producer invalidation with the atomic swap.
        let guard = current().ok_or(ApplyError::Superseded)?;
        let outcome = self
            .controller
            .publish_if_current_guarded_with(token, candidate, guard, |outcome| {
                committed(outcome.published);
            })
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

struct AttemptGuard {
    observation: Arc<crate::events::Observation>,
    revision: u64,
    armed: bool,
}
impl Drop for AttemptGuard {
    fn drop(&mut self) {
        if self.armed {
            self.observation
                .change(crate::events::Kind::Failed, Some(self.revision), |s| {
                    s.phase = crate::ReconciliationPhase::Blocked;
                    s.last_failure = Some(crate::ReconciliationFailure::Cancelled);
                })
                .with_failure(crate::ReconciliationFailure::Cancelled)
                .emit();
        }
    }
}
