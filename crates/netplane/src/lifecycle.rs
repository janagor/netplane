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
    /// The outstanding request counter cannot represent another request.
    #[error("too many outstanding requests")]
    TooManyOutstanding,
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
    phase: Phase,
    outstanding: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Running,
    Draining,
    Stopped,
}

pub struct Admission {
    state: Mutex<State>,
    changed: watch::Sender<usize>,
}

impl Admission {
    pub(crate) fn new() -> Arc<Self> {
        let (changed, _) = watch::channel(0);
        Arc::new(Self {
            state: Mutex::new(State {
                phase: Phase::Running,
                outstanding: 0,
            }),
            changed,
        })
    }

    pub(crate) fn try_admit(self: &Arc<Self>) -> Result<AdmissionGuard, AdmissionError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != Phase::Running {
            return Err(AdmissionError::Closed);
        }
        let Some(next) = state.outstanding.checked_add(1) else {
            return Err(AdmissionError::TooManyOutstanding);
        };
        state.outstanding = next;
        self.changed.send_replace(next);
        drop(state);
        Ok(AdmissionGuard(Arc::clone(self)))
    }

    pub(crate) fn close(&self) -> watch::Receiver<usize> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase == Phase::Running {
            state.phase = Phase::Draining;
        }
        drop(state);
        self.changed.subscribe()
    }

    pub(crate) fn can_publish(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase
            == Phase::Running
    }

    pub(crate) fn mark_stopped(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase = Phase::Stopped;
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
