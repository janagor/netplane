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
    pub update: Option<Arc<DesiredUpdate<D>>>,
    pub stopped: bool,
}

/// A bounded latest-value producer. Clones share sequencing and one pending slot.
#[derive(Clone)]
pub struct UpdateSender<D>(pub(crate) watch::Sender<Mailbox<D>>);

/// An update could not be accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum UpdateError {
    /// The worker stopped or its future was dropped.
    #[error("reconciliation worker is stopped")]
    Stopped,
    /// No further local sequence numbers are available.
    #[error("update sequence exhausted")]
    SequenceExhausted,
}

impl<D> UpdateSender<D> {
    /// Replace pending desired state and return its local sequence.
    ///
    /// Resubmission explicitly retriggers invalid or exhausted updates.
    /// # Errors
    /// Returns an error after shutdown, worker drop, or sequence exhaustion.
    pub fn submit(&self, desired: D) -> Result<u64, UpdateError> {
        if self.0.is_closed() {
            return Err(UpdateError::Stopped);
        }
        let mut result = Err(UpdateError::Stopped);
        self.0.send_if_modified(|mailbox| {
            if mailbox.stopped {
                return false;
            }
            let previous = mailbox.update.as_ref().map_or(0, |update| update.sequence);
            let Some(sequence) = previous.checked_add(1) else {
                result = Err(UpdateError::SequenceExhausted);
                return false;
            };
            mailbox.update = Some(Arc::new(DesiredUpdate { sequence, desired }));
            result = Ok(sequence);
            true
        });
        result
    }

    /// Close update admission immediately and request graceful worker shutdown.
    ///
    /// Active preparation is allowed to finish before its candidate is discarded;
    /// an uncooperative preparer can delay shutdown indefinitely. Publication that
    /// already passed the final freshness check may still commit after this call.
    /// Await the worker future or its join handle to confirm completed cleanup.
    /// For forced cancellation, drop the worker future, or abort and join its task;
    /// preparation cleanup then follows [`crate::GraphPreparer`]'s contract.
    /// This does not shut down the runtime or its accepted requests.
    pub fn shutdown(&self) {
        self.0.send_modify(|mailbox| mailbox.stopped = true);
    }
}

#[cfg(test)]
mod tests {
    use super::{DesiredUpdate, Mailbox, UpdateError, UpdateSender};
    use std::sync::Arc;
    use tokio::sync::watch;

    #[test]
    fn overwritten_inputs_are_released_and_sequences_are_checked() -> Result<(), UpdateError> {
        let (sender, receiver) = watch::channel(Mailbox {
            update: None,
            stopped: false,
        });
        let updates = UpdateSender(sender);
        for sequence in 1..=1000 {
            let desired = Arc::new(sequence);
            let weak = Arc::downgrade(&desired);
            assert_eq!(updates.submit(desired)?, sequence * 2 - 1);
            updates.submit(Arc::new(0))?;
            assert!(weak.upgrade().is_none());
        }
        updates.0.send_modify(|mailbox| {
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
            update: None,
            stopped: false,
        });
        let updates = UpdateSender(sender);
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
        Ok(())
    }
}
