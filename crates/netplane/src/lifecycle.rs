//! Admission accounting and bounded shutdown draining.

use std::sync::{Arc, Mutex};

use thiserror::Error;
use tokio::sync::watch;

/// Why a request was not admitted for execution.
#[derive(Debug, Error)]
pub enum AdmissionError {
    /// Shutdown has closed admission for this runtime.
    #[error("netplane admission is closed")]
    Closed,
    /// The runtime has reached its configured outstanding request limit.
    #[error("outstanding request limit {limit} reached")]
    AtCapacity {
        /// Configured maximum number of accepted unfinished requests.
        limit: usize,
    },
}

/// A graceful drain did not complete within its deadline.
#[derive(Debug, Error)]
pub enum DrainError {
    /// Accepted request futures are still outstanding after the deadline.
    #[error("shutdown timed out with {remaining} outstanding requests")]
    TimedOut {
        /// Number of accepted request futures still outstanding.
        remaining: usize,
    },
}

struct State {
    phase: LifecycleState,
    outstanding: usize,
}

/// Admission and shutdown phase of a Netplane runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleState {
    /// New requests may be admitted.
    Running,
    /// Admission is closed while accepted request futures drain.
    Draining,
    /// A successful drain has completed and the active publication was cleared.
    Stopped,
}

/// A read-only sample of runtime-global admission and lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeStatus {
    /// Accepted, unfinished request futures across all handles and versions,
    /// including futures that have not been polled.
    pub outstanding: usize,
    /// Configured runtime-global admission limit.
    pub max_outstanding: usize,
    /// Lifecycle phase sampled with `outstanding` under one lock.
    pub lifecycle: LifecycleState,
}

pub struct Admission {
    state: Mutex<State>,
    max_outstanding: usize,
    changed: watch::Sender<usize>,
}

impl Admission {
    pub(crate) fn new(max_outstanding: usize) -> Arc<Self> {
        let (changed, _) = watch::channel(0);
        Arc::new(Self {
            state: Mutex::new(State {
                phase: LifecycleState::Running,
                outstanding: 0,
            }),
            max_outstanding,
            changed,
        })
    }

    pub(crate) fn try_admit(self: &Arc<Self>) -> Result<AdmissionGuard, AdmissionError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != LifecycleState::Running {
            return Err(AdmissionError::Closed);
        }
        if state.outstanding >= self.max_outstanding {
            return Err(AdmissionError::AtCapacity {
                limit: self.max_outstanding,
            });
        }
        state.outstanding += 1;
        self.changed.send_replace(state.outstanding);
        drop(state);
        Ok(AdmissionGuard(Arc::clone(self)))
    }

    pub(crate) fn close(&self) -> watch::Receiver<usize> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase == LifecycleState::Running {
            state.phase = LifecycleState::Draining;
        }
        drop(state);
        self.changed.subscribe()
    }

    pub(crate) fn can_publish(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase
            == LifecycleState::Running
    }

    pub(crate) fn mark_stopped(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase = LifecycleState::Stopped;
    }

    pub(crate) fn status(&self) -> RuntimeStatus {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        RuntimeStatus {
            outstanding: state.outstanding,
            max_outstanding: self.max_outstanding,
            lifecycle: state.phase,
        }
    }

    pub(crate) fn outstanding(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .outstanding
    }
}

pub struct AdmissionGuard(Arc<Admission>);

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.outstanding -= 1;
        self.0.changed.send_replace(state.outstanding);
    }
}
