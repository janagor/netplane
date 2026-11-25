use super::{RolloutState, RolloutStatus, ValidationError};
use crate::UpdateSender;
use tokio::sync::watch;

/// Rollout construction or lifecycle rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RolloutError {
    /// A stage or logical group is invalid.
    #[error(transparent)]
    Invalid(#[from] ValidationError),
    /// Another rollout owns this reconciler, including an unpolled driver.
    #[error("a rollout already owns this reconciler")]
    Busy,
    /// Start or resume requires a converged and owned desired configuration.
    #[error("desired traffic policy is unavailable or conflicts with rollout ownership")]
    Conflict,
    /// Polling interval is zero or overflows the monotonic clock.
    #[error("observation polling interval must be positive and finite")]
    Interval,
    /// The reconciler exhausted its local rollout identifier stream.
    #[error("rollout identifier exhausted")]
    IdentifierExhausted,
    /// Driver exited or reached a terminal state.
    #[error("rollout is terminal or its driver was dropped")]
    Terminal,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Running,
    Paused,
    Cancelled,
}
pub(super) struct Control {
    pub mode: Mode,
    pub generation: u64,
    pub pending: Option<u64>,
}

/// Cloneable bounded lifecycle control. Methods take effect synchronously;
/// watch delivery may coalesce pause/resume, but old observations are invalidated.
///
/// Neither cancellation nor failure restores weights. A committed swap remains active.
#[derive(Clone)]
pub struct RolloutHandle<D> {
    pub(super) updates: UpdateSender<D>,
    pub(super) control: watch::Sender<Control>,
    pub(super) status: watch::Receiver<RolloutStatus>,
}
impl<D> RolloutHandle<D> {
    /// Latest read-only snapshot; intermediate audit transitions may coalesce.
    #[must_use]
    pub fn status(&self) -> RolloutStatus {
        self.status.borrow().clone()
    }

    /// Subscribe without retaining an unbounded event history.
    #[must_use]
    pub fn subscribe_status(&self) -> watch::Receiver<RolloutStatus> {
        self.status.clone()
    }

    /// Suspend progression; an already requested stage may still publish.
    /// # Errors
    /// Returns `Terminal` after driver drop or a terminal transition.
    pub fn pause(&self) -> Result<(), RolloutError> {
        self.change(Mode::Paused)
    }

    /// Request fresh validation and a new observation epoch, even after timeout.
    /// # Errors
    /// Returns `Terminal` after driver drop or a terminal transition. Configuration
    /// conflicts remain blocked in status until the administrator restores ownership.
    pub fn resume(&self) -> Result<(), RolloutError> {
        self.change(Mode::Running)
    }

    /// Prevent subsequent publication of an owned uncommitted intent and stop work.
    /// Await the caller-owned driver to confirm all observation cleanup completed.
    /// # Errors
    /// Returns `Terminal` after completion, failure or driver drop. Repeated cancel
    /// is successful while the driver is still processing the first cancellation.
    pub fn cancel(&self) -> Result<(), RolloutError> {
        self.change(Mode::Cancelled)
    }

    fn change(&self, mode: Mode) -> Result<(), RolloutError> {
        if self.control.is_closed() || self.status().state.is_terminal() {
            return Err(RolloutError::Terminal);
        }
        let mut accepted = false;
        let mut withdrawn = None;
        self.control.send_modify(|control| {
            if self.status.borrow().state.is_terminal() {
                return;
            }
            if control.mode == Mode::Cancelled {
                accepted = mode == Mode::Cancelled;
                return;
            }
            if control.mode != Mode::Cancelled {
                control.mode = mode;
                control.generation = control.generation.saturating_add(1);
                if mode == Mode::Cancelled {
                    if let Some(sequence) = control.pending.take() {
                        withdrawn = self.updates.withdraw(sequence);
                    }
                }
                accepted = true;
            }
        });
        if let Some(receipt) = withdrawn {
            receipt.emit();
        }
        if accepted {
            Ok(())
        } else {
            Err(RolloutError::Terminal)
        }
    }
}

pub(super) struct Stop<D> {
    pub updates: UpdateSender<D>,
    pub control: watch::Sender<Control>,
    pub status: watch::Sender<RolloutStatus>,
}
impl<D> Drop for Stop<D> {
    fn drop(&mut self) {
        let mut withdrawn = None;
        self.control.send_modify(|control| {
            control.mode = Mode::Cancelled;
            if let Some(sequence) = control.pending.take() {
                withdrawn = self.updates.withdraw(sequence);
            }
        });
        if let Some(receipt) = withdrawn {
            receipt.emit();
        }
        self.status.send_modify(|status| {
            if !status.state.is_terminal() {
                status.state = RolloutState::Cancelled;
            }
        });
    }
}
