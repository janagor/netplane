use crate::{ReconciliationFailure, ReconciliationPhase as Phase, ReconciliationStatus};
use netplane::VersionId;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::watch;

static NEXT_RECONCILER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
pub enum Kind {
    DesiredUpdated,
    Invalidated,
    Started,
    Applied,
    Unchanged,
    Superseded,
    RetryScheduled,
    Failed,
    Stopped,
}

pub struct Event {
    kind: Kind,
    runtime_id: u64,
    reconciler_id: u64,
    revision: Option<u64>,
    status: ReconciliationStatus,
    error_kind: Option<ReconciliationFailure>,
}
impl Event {
    pub(super) const fn with_failure(mut self, failure: ReconciliationFailure) -> Self {
        self.error_kind = Some(failure);
        self
    }
    pub(super) fn emit(self) {
        let s = self.status;
        let event = match self.kind {
            Kind::DesiredUpdated => "reconcile.desired_updated",
            Kind::Invalidated => "reconcile.invalidated",
            Kind::Started => "reconcile.started",
            Kind::Applied => "reconcile.applied",
            Kind::Unchanged => "reconcile.unchanged",
            Kind::Superseded => "reconcile.superseded",
            Kind::RetryScheduled => "reconcile.retry_scheduled",
            Kind::Failed => "reconcile.failed",
            Kind::Stopped => "reconcile.stopped",
        };
        macro_rules! record {
            ($level:ident) => {
                tracing::$level!(target: "netplane_control", event,
                    runtime_id = self.runtime_id, reconciler_id = self.reconciler_id, event_sequence = s.event_sequence,
                    revision = self.revision, desired_revision = s.desired_revision,
                    applied_revision = s.applied_revision,
                    publication_id = s.active_publication.map(VersionId::get),
                    retry_attempt = s.retry_attempt, error_kind = ?self.error_kind)
            };
        }
        match self.kind {
            Kind::DesiredUpdated | Kind::Applied | Kind::Stopped => record!(info),
            Kind::RetryScheduled | Kind::Failed | Kind::Invalidated => record!(warn),
            Kind::Started | Kind::Unchanged | Kind::Superseded => record!(debug),
        }
    }
}

pub struct Observation {
    runtime_id: u64,
    reconciler_id: u64,
    status: watch::Sender<ReconciliationStatus>,
}
impl Observation {
    pub(super) fn new(runtime_id: u64, publication: Option<VersionId>) -> Self {
        let (status, _) = watch::channel(ReconciliationStatus {
            desired_revision: None,
            applied_revision: None,
            active_publication: publication,
            phase: Phase::Idle,
            retry_attempt: 0,
            last_failure: None,
            event_sequence: 0,
        });
        Self {
            runtime_id,
            reconciler_id: NEXT_RECONCILER.fetch_add(1, Ordering::Relaxed),
            status,
        }
    }
    pub(super) fn status(&self) -> ReconciliationStatus {
        self.status.borrow().clone()
    }
    pub(super) fn subscribe(&self) -> watch::Receiver<ReconciliationStatus> {
        self.status.subscribe()
    }
    // Only bounded, internal mutations execute under the watch lock. Emit afterwards.
    pub(super) fn change(
        &self,
        kind: Kind,
        revision: Option<u64>,
        change: impl FnOnce(&mut ReconciliationStatus),
    ) -> Event {
        let mut snapshot = None;
        self.status.send_modify(|status| {
            change(status);
            status.event_sequence = status.event_sequence.saturating_add(1);
            snapshot = Some(status.clone());
        });
        Event {
            kind,
            runtime_id: self.runtime_id,
            reconciler_id: self.reconciler_id,
            revision,
            status: snapshot.unwrap_or_else(|| self.status()),
            error_kind: None,
        }
    }
    pub(super) fn desired(&self, revision: u64) -> Event {
        self.change(Kind::DesiredUpdated, Some(revision), |s| {
            s.desired_revision = Some(revision);
            s.phase = Phase::Pending;
            s.retry_attempt = 0;
            s.last_failure = None;
        })
    }
    pub(super) fn invalidated(&self, revision: u64) -> Event {
        self.change(Kind::Invalidated, Some(revision), |s| {
            s.desired_revision = Some(revision);
            s.phase = Phase::Blocked;
            s.retry_attempt = 0;
            s.last_failure = Some(ReconciliationFailure::Invalidated);
        })
    }
    pub(super) fn started(&self, revision: u64) {
        self.change(Kind::Started, Some(revision), |s| {
            if s.desired_revision == Some(revision) {
                s.phase = Phase::Preparing;
            }
        })
        .emit();
    }
    pub(super) fn failed<E>(&self, revision: u64, error: &crate::ApplyError<E>, retry: bool) {
        use crate::{ApplyError, ReconciliationFailure as Failure};
        let failure = match error {
            ApplyError::Validation(_) => Failure::Validation,
            ApplyError::Preparation(_) => Failure::Preparation,
            ApplyError::Busy => Failure::Busy,
            ApplyError::RevisionExhausted => Failure::RevisionExhausted,
            ApplyError::Publication(netplane::ControllerError::Stale { .. }) => Failure::Conflict,
            ApplyError::Publication(_) => Failure::Publication,
            ApplyError::Superseded => {
                self.change(Kind::Superseded, Some(revision), |_| {}).emit();
                return;
            }
        };
        self.change(
            if retry {
                Kind::RetryScheduled
            } else {
                Kind::Failed
            },
            Some(revision),
            |s| {
                if s.desired_revision == Some(revision) && s.phase != Phase::Stopped {
                    if let ApplyError::Publication(netplane::ControllerError::Stale {
                        actual,
                        ..
                    }) = error
                    {
                        s.active_publication = Some(*actual);
                    }
                    s.last_failure = Some(failure);
                    s.phase = if retry {
                        Phase::RetryScheduled
                    } else {
                        Phase::Blocked
                    };
                    if retry {
                        s.retry_attempt = s.retry_attempt.saturating_add(1);
                    }
                }
            },
        )
        .with_failure(failure)
        .emit();
    }
    pub(super) fn finish<E>(
        &self,
        revision: u64,
        result: &Result<crate::ApplyOutcome, crate::ApplyError<E>>,
        retry: bool,
    ) {
        use crate::ApplyOutcome;
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(error) => {
                self.failed(revision, error, retry);
                return;
            }
        };
        let (kind, publication) = match outcome {
            ApplyOutcome::Applied(outcome) => (Kind::Applied, outcome.published),
            ApplyOutcome::Unchanged(id) => (Kind::Unchanged, *id),
        };
        self.change(kind, Some(revision), |s| {
            s.applied_revision = Some(revision);
            s.active_publication = Some(publication);
            if s.desired_revision == Some(revision) && s.phase != Phase::Stopped {
                s.phase = Phase::Idle;
                s.last_failure = None;
                s.retry_attempt = 0;
            }
        })
        .emit();
    }
    pub(super) fn stop(&self) {
        self.change(Kind::Stopped, None, |s| s.phase = Phase::Stopped)
            .emit();
    }
}

pub struct StopOnDrop<D, E> {
    pub(super) observation: std::sync::Arc<Observation>,
    pub(super) outcomes: watch::Sender<std::sync::Arc<crate::DriverStatus<E>>>,
    pub(super) updates: std::sync::Weak<watch::Sender<crate::updates::Mailbox<D>>>,
}
impl<D, E> Drop for StopOnDrop<D, E> {
    fn drop(&mut self) {
        // Close admission before announcing stop, including an unused driver drop.
        if let Some(updates) = self.updates.upgrade() {
            updates.send_modify(|mailbox| mailbox.stopped = true);
        }
        self.outcomes
            .send_replace(std::sync::Arc::new(crate::DriverStatus::Stopped));
        self.observation.stop();
    }
}
