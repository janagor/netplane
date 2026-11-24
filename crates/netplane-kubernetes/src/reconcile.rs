use crate::{
    DesiredState, DiscoveryHandle, DiscoveryObservation, ServiceBinding, TranslationError,
    WatchStatus, translate,
};
use netplane_control::{UpdateError, UpdateSender};
use std::sync::Arc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Latest translation observation. Reconciliation outcomes remain on the driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IntegrationStatus {
    /// No fully initialized, healthy discovery view is currently available.
    Waiting,
    /// A complete snapshot was submitted to the bounded control mailbox.
    Submitted {
        /// Discovery revision, independent of reconciliation and publication IDs.
        revision: u64,
        /// Local sequence returned by the existing reconciliation driver.
        sequence: u64,
        /// Eligible direct destinations, zero for an unavailable version.
        backends: usize,
    },
    /// Invalid external topology; the active execution graph remains usable.
    Rejected {
        /// Rejected discovery revision.
        revision: u64,
        /// Typed validation failure.
        error: TranslationError,
    },
    /// Runner completed or its future was dropped; update admission is closed.
    Stopped,
}

/// Terminal integration failures. Retrying API errors belong to discovery.
#[derive(Debug, thiserror::Error)]
pub enum IntegrationError {
    /// Discovery stopped unexpectedly; its last active graph is retained.
    #[error("discovery stopped")]
    DiscoveryStopped,
    /// The control mailbox could not accept an update.
    #[error(transparent)]
    Update(#[from] UpdateError),
}

/// Caller-owned bridge from discovery to the existing latest-value reconciler.
///
/// No tasks are spawned. Poll `run` alongside discovery and `ReconciliationDriver`.
/// Only snapshots observed with both watchers healthy are submitted. Recovery and
/// incomplete relists invalidate pending candidates and retain the active graph.
/// Topology and health come from one coherent local discovery observation; Kubernetes
/// collections remain eventually consistent. Observations coalesce: an interruption
/// followed by recovery before this runner polls may be unseen. With an unchanged
/// topology revision, that unseen interruption does not invalidate or resubmit work.
/// Invalidation starts when this runner processes a degraded or rejected observation,
/// not when discovery produces it. The driver owns publication and retry.
/// Cancel this runner when shutting down the runtime. Dropping it closes update
/// admission; await the driver to establish that publication has finished.
pub struct KubernetesIntegration {
    binding: ServiceBinding,
    observations: watch::Receiver<Arc<DiscoveryObservation>>,
    updates: UpdateSender<DesiredState>,
    status: watch::Sender<IntegrationStatus>,
}
impl KubernetesIntegration {
    /// Bind exactly one Service and transfer snapshots to an existing control driver.
    ///
    /// Construction performs no I/O. Observe translation failures with `status`.
    #[must_use]
    pub fn new(
        discovery: &DiscoveryHandle,
        binding: ServiceBinding,
        updates: UpdateSender<DesiredState>,
    ) -> Self {
        let (status, _) = watch::channel(IntegrationStatus::Waiting);
        Self {
            binding,
            observations: discovery.subscribe_observations(),
            updates,
            status,
        }
    }
    /// Subscribe to bounded latest-value translation observations.
    #[must_use]
    pub fn status(&self) -> watch::Receiver<IntegrationStatus> {
        self.status.subscribe()
    }

    /// Forward complete snapshots until cancellation or terminal discovery failure.
    ///
    /// Translation failures are reported and skipped; later valid snapshots recover.
    /// Cancellation/drop closes the shared update sender and releases all guards.
    /// No active request depends on this runner or Kubernetes API availability.
    ///
    /// # Errors
    /// Returns an error if discovery terminates or update admission fails.
    pub async fn run(mut self, cancellation: CancellationToken) -> Result<(), IntegrationError> {
        let mut processed = None;
        let mut control_status = self.updates.subscribe_status();
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if control_status.borrow_and_update().phase
                == netplane_control::ReconciliationPhase::Stopped
            {
                return Ok(());
            }
            let observation = self.observations.borrow_and_update().clone();
            let health = &observation.status;
            let snapshot = observation.snapshot.clone();
            if [health.services, health.endpoint_slices]
                .into_iter()
                .any(|state| matches!(state, WatchStatus::Stopped | WatchStatus::Failed))
            {
                return Err(IntegrationError::DiscoveryStopped);
            }
            if health.services == WatchStatus::Watching
                && health.endpoint_slices == WatchStatus::Watching
            {
                if let Some(snapshot) =
                    snapshot.filter(|snapshot| processed != Some(snapshot.revision))
                {
                    processed = Some(snapshot.revision);
                    match translate(&snapshot, &self.binding) {
                        Ok(desired) => {
                            let backends = desired.backends().len();
                            let sequence = self.updates.submit(desired)?;
                            self.status.send_replace(IntegrationStatus::Submitted {
                                revision: snapshot.revision,
                                sequence,
                                backends,
                            });
                            tracing::info!(
                                revision = snapshot.revision,
                                sequence,
                                backends,
                                "kubernetes_desired_submitted"
                            );
                        }
                        Err(error) => {
                            self.updates.invalidate_pending()?;
                            tracing::warn!(revision = snapshot.revision, %error, "kubernetes_translation_rejected");
                            self.status.send_replace(IntegrationStatus::Rejected {
                                revision: snapshot.revision,
                                error,
                            });
                        }
                    }
                }
            } else {
                // Reconsider even an unchanged snapshot when watcher health recovers.
                // The reconciler's semantic equality still prevents a new publication.
                if processed.take().is_some() {
                    self.updates.invalidate_pending()?;
                }
                self.status.send_replace(IntegrationStatus::Waiting);
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(()),
                changed = self.observations.changed() => { changed.map_err(|_| IntegrationError::DiscoveryStopped)?; }
                changed = control_status.changed() => { if changed.is_err() { return Ok(()); } }
            }
        }
    }
}
impl Drop for KubernetesIntegration {
    fn drop(&mut self) {
        self.updates.shutdown();
        self.status.send_replace(IntegrationStatus::Stopped);
    }
}
