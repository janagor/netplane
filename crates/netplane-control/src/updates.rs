use std::sync::Arc;
use tokio::sync::watch;

/// An accepted desired state, independent of application revision strings.
#[derive(Debug)]
pub struct DesiredUpdate<D> {
    /// Locally increasing sequence; resubmitting equal state also increments it.
    pub sequence: u64,
    /// Application-owned configuration.
    pub desired: D,
}

pub struct Mailbox<D> {
    pub sequence: u64,
    pub guarded_publication: Option<netplane::VersionId>,
    pub intent: Option<Arc<OwnedIntent>>,
    pub update: Option<Arc<DesiredUpdate<D>>>,
    pub stopped: bool,
}

#[derive(Default)]
pub struct OwnedIntent {
    pub committed: std::sync::OnceLock<netplane::VersionId>,
}

/// A bounded latest-value producer. Clones share sequencing and one pending slot.
#[derive(Clone)]
pub struct UpdateSender<D>(
    pub(crate) Arc<watch::Sender<Mailbox<D>>>,
    pub(crate) Arc<crate::events::Observation>,
);

/// An update could not be accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum UpdateError {
    /// The worker stopped or its future was dropped.
    #[error("reconciliation worker is stopped")]
    Stopped,
    /// No further local sequence numbers are available.
    #[error("update sequence exhausted")]
    SequenceExhausted,
    /// Another producer superseded the expected desired revision.
    #[error("desired revision changed")]
    Conflict,
}

impl<D> UpdateSender<D> {
    /// Replace pending desired state and return its local sequence.
    ///
    /// Resubmission explicitly retriggers invalid or exhausted updates.
    /// # Errors
    /// Returns an error after shutdown, worker drop, or sequence exhaustion.
    /// A previously stopped reconciler cannot accept updates through a new driver.
    pub fn submit(&self, desired: D) -> Result<u64, UpdateError> {
        self.replace(Some(desired), None, None).map(Receipt::emit)
    }

    /// Supersede all pending/preparing candidates without stopping the driver.
    ///
    /// Advances the shared sequence and clears desired work. Publication holds a
    /// read guard on this mailbox through the atomic swap: after this call returns,
    /// no earlier candidate can commit. A swap already completed before invalidation
    /// remains active. Recovery requires a new `submit`, even for equal desired state.
    /// Preparation may finish before its candidate is discarded; no extra task is
    /// spawned. The active graph and accepted requests are unaffected.
    ///
    /// # Errors
    /// Returns an error after shutdown/driver drop or on sequence exhaustion.
    pub fn invalidate_pending(&self) -> Result<u64, UpdateError> {
        self.replace(None, None, None).map(Receipt::emit)
    }

    pub(crate) fn submit_guarded(
        &self,
        desired: D,
        expected: u64,
        publication: netplane::VersionId,
    ) -> Result<Receipt<D>, UpdateError> {
        self.replace(Some(desired), Some(expected), Some(publication))
    }

    pub(crate) fn withdraw(&self, expected: u64) -> Option<Receipt<D>> {
        self.replace(None, Some(expected), None).ok()
    }

    fn replace(
        &self,
        mut desired: Option<D>,
        expected: Option<u64>,
        publication: Option<netplane::VersionId>,
    ) -> Result<Receipt<D>, UpdateError> {
        if self.0.is_closed() {
            return Err(UpdateError::Stopped);
        }
        let mut result = Err(UpdateError::Stopped);
        let mut event = None;
        let mut retired = None;
        self.0.send_if_modified(|mailbox| {
            let status = self.1.status();
            if mailbox.stopped || status.phase == crate::ReconciliationPhase::Stopped {
                return false;
            }
            if expected.is_some_and(|expected| expected != mailbox.sequence)
                || (expected.is_some()
                    && desired.is_none()
                    && mailbox
                        .intent
                        .as_ref()
                        .is_none_or(|intent| intent.committed.get().is_some()))
            {
                result = Err(UpdateError::Conflict);
                return false;
            }
            let previous = mailbox.sequence.max(status.desired_revision.unwrap_or(0));
            let Some(sequence) = previous.checked_add(1) else {
                result = Err(UpdateError::SequenceExhausted);
                return false;
            };
            let replaced = {
                let mut replace = || {
                    mailbox.sequence = sequence;
                    mailbox.guarded_publication = publication;
                    mailbox.intent = publication.map(|_| Arc::new(OwnedIntent::default()));
                    let invalidated = desired.is_none();
                    retired = std::mem::replace(
                        &mut mailbox.update,
                        desired
                            .take()
                            .map(|desired| Arc::new(DesiredUpdate { sequence, desired })),
                    );
                    event = Some(if invalidated {
                        self.1.invalidated(sequence)
                    } else {
                        self.1.desired(sequence)
                    });
                    result = Ok(sequence);
                };
                if let Some(publication) = publication {
                    self.1
                        .accept_publication
                        .as_ref()
                        .is_some_and(|guard| guard(publication, &mut replace))
                } else {
                    replace();
                    true
                }
            };
            if !replaced {
                result = Err(UpdateError::Conflict);
            }
            replaced
        });
        result.map(|sequence| Receipt {
            sequence,
            event,
            retired,
        })
    }

    /// Read a consistent snapshot, including the latest accepted desired revision.
    #[must_use]
    pub fn status(&self) -> crate::ReconciliationStatus {
        self.1.status()
    }

    /// Subscribe to latest-value observations without retaining an event queue.
    #[must_use]
    pub fn subscribe_status(&self) -> watch::Receiver<crate::ReconciliationStatus> {
        self.1.subscribe()
    }

    /// Close update admission immediately and request graceful worker shutdown.
    ///
    /// Active preparation is allowed to finish before its candidate is discarded;
    /// an uncooperative preparer can delay shutdown indefinitely. Publication that
    /// completed its atomic swap remains active; no earlier candidate can commit
    /// after this call returns.
    /// Await the worker future or its join handle to confirm completed cleanup.
    /// For forced cancellation, drop the worker future, or abort and join its task;
    /// preparation cleanup then follows [`crate::GraphPreparer`]'s contract.
    /// This does not shut down the runtime or its accepted requests.
    pub fn shutdown(&self) {
        self.0.send_modify(|mailbox| mailbox.stopped = true);
    }
}

// Defer tracing callbacks and application-owned destructors until every producer
// control lock has been released, not just the mailbox lock.
pub struct Receipt<D> {
    pub sequence: u64,
    event: Option<crate::events::Event>,
    retired: Option<Arc<DesiredUpdate<D>>>,
}
impl<D> Receipt<D> {
    pub fn emit(self) -> u64 {
        if let Some(event) = self.event {
            event.emit();
        }
        drop(self.retired);
        self.sequence
    }
}

#[cfg(test)]
mod tests {
    use super::{DesiredUpdate, Mailbox, UpdateError, UpdateSender};
    use std::sync::Arc;
    use tokio::sync::watch;

    #[test]
    fn stopped_observation_rejects_submission_even_if_mailbox_is_open() {
        let (sender, receiver) = watch::channel(Mailbox {
            sequence: 0,
            guarded_publication: None,
            intent: None,
            update: None,
            stopped: false,
        });
        let observation = Arc::new(crate::events::Observation::new(0, None));
        observation.stop();
        let updates = UpdateSender(Arc::new(sender), observation);
        let stopped = updates.status();
        assert_eq!(updates.submit(1), Err(UpdateError::Stopped));
        assert_eq!(updates.status(), stopped);
        assert!(receiver.borrow().update.is_none());
    }

    #[test]
    fn overwritten_inputs_are_released_and_sequences_are_checked() -> Result<(), UpdateError> {
        let (sender, receiver) = watch::channel(Mailbox {
            sequence: 0,
            guarded_publication: None,
            intent: None,
            update: None,
            stopped: false,
        });
        let updates = UpdateSender(
            Arc::new(sender),
            Arc::new(crate::events::Observation::new(0, None)),
        );
        for sequence in 1..=1000 {
            let desired = Arc::new(sequence);
            let weak = Arc::downgrade(&desired);
            assert_eq!(updates.submit(desired)?, sequence * 2 - 1);
            updates.submit(Arc::new(0))?;
            assert!(weak.upgrade().is_none());
        }
        updates.0.send_modify(|mailbox| {
            mailbox.sequence = u64::MAX;
            mailbox.update = Some(Arc::new(DesiredUpdate {
                sequence: u64::MAX,
                desired: Arc::new(0),
            }));
        });
        assert_eq!(
            updates.submit(Arc::new(1)),
            Err(UpdateError::SequenceExhausted)
        );
        assert_eq!(
            updates.invalidate_pending(),
            Err(UpdateError::SequenceExhausted)
        );
        assert_eq!(
            receiver.borrow().update.as_ref().map(|u| u.sequence),
            Some(u64::MAX)
        );
        Ok(())
    }
}

#[cfg(test)]
mod concurrent_tests {
    use super::{Mailbox, UpdateSender};
    use std::sync::Arc;
    use tokio::{
        sync::{Barrier, watch},
        task::JoinSet,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_producers_share_sequences_and_latest_value()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (sender, receiver) = watch::channel(Mailbox {
            sequence: 0,
            guarded_publication: None,
            intent: None,
            update: None,
            stopped: false,
        });
        let updates = UpdateSender(
            Arc::new(sender),
            Arc::new(crate::events::Observation::new(0, None)),
        );
        let barrier = Arc::new(Barrier::new(8));
        let mut producers = JoinSet::new();
        for producer in 0..8 {
            let updates = updates.clone();
            let barrier = Arc::clone(&barrier);
            producers.spawn(async move {
                barrier.wait().await;
                (0..100)
                    .map(|item| {
                        let desired = producer * 100 + item;
                        updates.submit(desired).map(|sequence| (sequence, desired))
                    })
                    .collect::<Result<Vec<_>, _>>()
            });
        }
        let mut accepted = Vec::new();
        while let Some(result) = producers.join_next().await {
            accepted.extend(result??);
        }
        accepted.sort_unstable_by_key(|(sequence, _)| *sequence);
        assert_eq!(accepted.len(), 800);
        for (expected, (actual, _)) in (1_u64..).zip(&accepted) {
            assert_eq!(expected, *actual);
        }
        let latest = receiver
            .borrow()
            .update
            .clone()
            .ok_or("missing latest update")?;
        assert_eq!(accepted.last(), Some(&(latest.sequence, latest.desired)));
        assert_eq!(updates.status().desired_revision, Some(latest.sequence));
        Ok(())
    }
}
