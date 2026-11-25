use super::{HealthSchedulerHandle, Shared};
use std::sync::Weak;

/// One coherent sample of scheduler registration and probe budgets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchedulerStatus {
    /// Distinct registrations retained by configuration or graph owners.
    pub registered_backends: usize,
    /// Maximum distinct registrations across all live graph versions and candidates.
    pub max_registered_backends: usize,
    /// Scheduled or executing probe futures, including cancellations awaiting drop.
    pub running_checks: usize,
    /// Independent upper bound on simultaneous probe futures.
    pub max_concurrent_checks: usize,
}

pub(super) struct CheckSlot(pub(super) Weak<Shared>);
impl Drop for CheckSlot {
    fn drop(&mut self) {
        if let Some(shared) = self.0.upgrade() {
            shared
                .registrations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .running_checks -= 1;
        }
    }
}
impl HealthSchedulerHandle {
    /// Sample both resource budgets without waiting for running probes.
    ///
    /// Registration capacity covers the union of active, historical, in-flight,
    /// and candidate graph identities. A replacement can remain blocked until
    /// an old graph releases its final registration, even if the new topology
    /// alone fits the budget. Cancelled checks retain a running slot until their
    /// futures are dropped; registration slots are released independently.
    #[must_use]
    pub fn status(&self) -> SchedulerStatus {
        let registrations = self
            .shared
            .registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        SchedulerStatus {
            registered_backends: registrations.entries.len(),
            max_registered_backends: self.shared.capacity,
            running_checks: registrations.running_checks,
            max_concurrent_checks: self.shared.concurrency,
        }
    }
}
