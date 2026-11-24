use std::{sync::Arc, time::Duration};

use netplane::ControllerError;
use tokio::{
    sync::watch,
    time::{Instant, sleep},
};

use crate::{
    ApplyError, ApplyOutcome, GraphPreparer, InvalidUpdatePolicy, Reconciler, UpdatePolicy,
    UpdateSender, updates::Mailbox,
};

/// Most recent worker transition; intermediate transitions may be overwritten.
#[derive(Debug)]
pub enum DriverStatus<E> {
    /// Waiting for the first desired state.
    Idle,
    /// Pending work was withdrawn; waiting for a new desired state.
    Invalidated(u64),
    /// Validating or preparing this local sequence.
    Preparing(u64),
    /// This sequence completed successfully at the reported publication.
    Applied(u64, ApplyOutcome),
    /// A failed attempt; `retry_in` is absent for permanent or exhausted failures.
    Failed {
        /// Failed local sequence.
        sequence: u64,
        /// Original typed reconciliation failure.
        error: ApplyError<E>,
        /// Delay until another attempt, interruptible by updates and shutdown.
        retry_in: Option<Duration>,
    },
    /// The worker exited or was dropped and released its owned resources.
    Stopped,
}

/// Caller-owned single reconciliation worker with bounded latest-value input.
///
/// Construction spawns no tasks. Poll [`Self::run`] directly or spawn it on an
/// application-owned executor and retain the join handle. Graceful shutdown waits
/// for active preparation to finish and can wait indefinitely for an uncooperative
/// preparer. For forced cancellation, drop the `run` future, or abort and join its
/// task; this drops preparation following [`GraphPreparer`]'s cleanup contract.
/// Shutdown and invalidation serialize with the atomic publication step. A swap
/// that completed first remains active; after either operation returns, an earlier
/// candidate cannot commit. Await worker completion to ensure cleanup is finished.
/// No child task or timer survives that drop. Independently spawned application work remains its responsibility.
pub struct ReconciliationDriver<D, P, Request, Response>
where
    P: GraphPreparer<D, Request, Response>,
    Request: Send + 'static,
    Response: Send + 'static,
{
    reconciler: Reconciler<D, P, Request, Response>,
    _stop: crate::events::StopOnDrop<D, P::Error>,
    policy: UpdatePolicy,
    updates: watch::Receiver<Mailbox<D>>,
    status: watch::Sender<Arc<DriverStatus<P::Error>>>,
}

impl<D, P, Request, Response> ReconciliationDriver<D, P, Request, Response>
where
    D: Clone + Eq + Send + Sync,
    Request: Send + 'static,
    Response: Send + 'static,
    P: GraphPreparer<D, Request, Response> + Sync,
    P::Error: Send + Sync,
{
    /// Create a worker and cloneable input handle without requiring a runtime.
    /// Preparation failures retry only when `is_transient` passed to `run` says so.
    /// A previously stopped reconciler produces a stopped driver: update admission
    /// is closed, both status channels report `Stopped`, and `run` exits immediately.
    /// # Errors
    /// Rejects zero or inconsistent scheduling limits and overflowing durations.
    pub fn new(
        reconciler: Reconciler<D, P, Request, Response>,
        policy: UpdatePolicy,
    ) -> Result<(Self, UpdateSender<D>), InvalidUpdatePolicy> {
        policy.validate()?;
        let stopped = reconciler.status().phase == crate::ReconciliationPhase::Stopped;
        let (sender, updates) = watch::channel(Mailbox {
            sequence: 0,
            update: None,
            stopped,
        });
        let sender = Arc::new(sender);
        let initial = if stopped {
            DriverStatus::Stopped
        } else {
            DriverStatus::Idle
        };
        let (status, _) = watch::channel(Arc::new(initial));
        let observation = Arc::clone(&reconciler.observation);
        Ok((
            Self {
                reconciler,
                _stop: crate::events::StopOnDrop {
                    observation: Arc::clone(&observation),
                    outcomes: status.clone(),
                    updates: Arc::downgrade(&sender),
                },
                policy,
                updates,
                status,
            },
            UpdateSender(sender, observation),
        ))
    }

    /// Subscribe to the latest transition, including typed failure and exhaustion.
    ///
    /// Graceful completion, forced cancellation, and unused driver drop all leave
    /// this channel at `Stopped`, matching the reconciliation lifecycle snapshot.
    /// The two channels are separate observations, not an atomic combined sample.
    #[must_use]
    pub fn status(&self) -> watch::Receiver<Arc<DriverStatus<P::Error>>> {
        self.status.subscribe()
    }

    fn stopped(&self) -> bool {
        self.updates.borrow().stopped || self.updates.has_changed().is_err()
    }

    fn is_current(&self, sequence: u64) -> bool {
        self.updates
            .borrow()
            .update
            .as_ref()
            .is_some_and(|latest| latest.sequence == sequence)
    }

    fn current_guard(&self, sequence: u64) -> Option<watch::Ref<'_, Mailbox<D>>> {
        let mailbox = self.updates.borrow();
        (!mailbox.stopped && mailbox.sequence == sequence && mailbox.update.is_some())
            .then_some(mailbox)
    }

    fn latest(&mut self, completed: &mut u64) -> Option<Arc<crate::DesiredUpdate<D>>> {
        let (update, sequence) = {
            let mailbox = self.updates.borrow_and_update();
            (mailbox.update.clone(), mailbox.sequence)
        };
        if update.is_none() && sequence != *completed {
            self.status
                .send_replace(Arc::new(DriverStatus::Invalidated(sequence)));
            *completed = sequence;
        }
        update
    }

    async fn coalesce(&mut self) -> bool {
        let deadline = Instant::now() + self.policy.max_coalesce_window;
        loop {
            let quiet = (Instant::now() + self.policy.debounce).min(deadline);
            tokio::select! {
                () = tokio::time::sleep_until(quiet) => return !self.stopped(),
                changed = self.updates.changed() => {
                    if changed.is_err() || self.stopped() { return false; }
                }
            }
            if Instant::now() >= deadline {
                return true;
            }
        }
    }

    /// Run until explicit shutdown, all producers disappear, or runtime shutdown.
    ///
    /// Validation errors are permanent. The classifier applies only to preparation
    /// errors. Stale publications always re-inspect and rebuild after bounded
    /// backoff; other publication errors are permanent except shutdown, which exits.
    /// A finite retry budget leaves the desired state retained in the input slot;
    /// resubmit it to retrigger. External publications after success require another
    /// submission; this worker does not poll the runtime for drift.
    pub async fn run(mut self, is_transient: impl Fn(&P::Error) -> bool + Send) {
        let observation = Arc::clone(&self.reconciler.observation);
        let mut completed = 0;
        'worker: loop {
            if self.stopped() {
                break;
            }
            let update = self.latest(&mut completed);
            let Some(_) = update.filter(|update| update.sequence != completed) else {
                if self.updates.changed().await.is_err() {
                    break;
                }
                continue;
            };
            if !self.coalesce().await {
                break;
            }
            let Some(update) = self.updates.borrow_and_update().update.clone() else {
                continue;
            };
            let sequence = update.sequence;
            let mut retries = 0_usize;
            loop {
                if self.stopped() {
                    break 'worker;
                }
                if !self.is_current(sequence) {
                    continue 'worker;
                }
                observation.started(sequence);
                self.status
                    .send_replace(Arc::new(DriverStatus::Preparing(sequence)));
                let result = self
                    .reconciler
                    .apply_checked(update.desired.clone(), || self.current_guard(sequence))
                    .await;
                // A producer can update after the atomic swap but before event delivery.
                // Record that commit without making the newer desired revision idle.
                if result.is_ok() {
                    observation.finish(sequence, &result, false);
                } else if !self.is_current(sequence) || self.stopped() {
                    observation.failed(sequence, &ApplyError::<P::Error>::Superseded, false);
                }
                if self.stopped() {
                    break 'worker;
                }
                if !self.is_current(sequence) {
                    continue 'worker;
                }
                let error = match result {
                    Ok(outcome) => {
                        self.status
                            .send_replace(Arc::new(DriverStatus::Applied(sequence, outcome)));
                        completed = sequence;
                        break;
                    }
                    Err(error @ ApplyError::Publication(ControllerError::ShuttingDown)) => {
                        observation.failed(sequence, &error, false);
                        break 'worker;
                    }
                    Err(error) => error,
                };
                let transient = match &error {
                    ApplyError::Preparation(error) => is_transient(error),
                    ApplyError::Publication(ControllerError::Stale { .. }) | ApplyError::Busy => {
                        true
                    }
                    _ => false,
                };
                let retry_in = (transient
                    && self
                        .policy
                        .max_retry_attempts
                        .is_none_or(|max| retries < max))
                .then(|| self.policy.delay(retries, sequence));
                observation.failed(sequence, &error, retry_in.is_some());
                self.status.send_replace(Arc::new(DriverStatus::Failed {
                    sequence,
                    error,
                    retry_in,
                }));
                let Some(delay) = retry_in else {
                    completed = sequence;
                    break;
                };
                retries = retries.saturating_add(1);
                tokio::select! {
                    () = sleep(delay) => {},
                    changed = self.updates.changed() => {
                        if changed.is_err() { break 'worker; }
                        continue 'worker;
                    }
                }
            }
        }
        // Drop preparation ownership before reporting completed cleanup.
        drop(self.reconciler);
    }
}
