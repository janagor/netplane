//! Shared active probing with bounded registrations and running checks.

mod status;
#[cfg(test)]
mod tests;
use status::CheckSlot;
pub use status::SchedulerStatus;

use super::{BackendIdentity, HealthHandle, HealthMode, HealthOutcome, ObservationToken};
use std::{
    collections::{BTreeMap, HashMap},
    future::{Future, poll_fn},
    num::NonZeroUsize,
    pin::{Pin, pin},
    sync::{Arc, Mutex, Weak},
    task::Poll,
    time::Duration,
};
use tokio::{
    sync::Notify,
    task::{AbortHandle, JoinSet},
    time::Instant,
};

/// An independently budgeted check; dropping its future must cancel local work.
pub trait HealthProbe: Send + Sync + 'static {
    /// One check, without borrowing the registration or consuming request admission.
    type Future: Future<Output = HealthOutcome> + Send + 'static;
    /// Describe all behavior-affecting configuration. Equal keys promise equivalent
    /// checks, including transport settings; changing a backend incarnation is
    /// not a substitute for changing this configuration identity.
    fn identity(&self) -> ProbeIdentity;
    /// Begin one check. The scheduler enforces timeout and prevents overlap.
    fn check(&self) -> Self::Future;
}

/// Compatibility key for probe behavior, independent of physical incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeIdentity {
    /// Nonempty caller-defined identity of all behavior-affecting settings.
    pub configuration_key: String,
}

/// Resource limits shared by all registrations in one scheduler.
#[derive(Clone, Copy, Debug)]
pub struct ActiveHealthConfig {
    /// Delay after a completed check; missed intervals never accumulate.
    pub interval: Duration,
    /// Maximum duration of a check, including response collection.
    pub timeout: Duration,
    /// Maximum simultaneous checks, independent of request capacity.
    pub max_concurrent_checks: NonZeroUsize,
    /// Maximum registrations, bounding pending work and metadata.
    pub max_registered_backends: NonZeroUsize,
}

/// Active probing configuration or registration failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProbeError {
    /// Probe configuration identities must be nonempty.
    #[error("probe configuration key must not be empty")]
    InvalidProbeIdentity,
    /// A live backend registration uses different probing behavior.
    #[error("backend already registered with incompatible probe configuration")]
    IncompatibleProbeConfiguration,
    /// Durations must be positive and representable by monotonic deadlines.
    #[error("probe interval and timeout must be positive representable durations")]
    InvalidDuration,
    /// The scheduler's registration budget has been exhausted.
    #[error("active health registration limit reached")]
    AtCapacity,
    /// Shutdown has permanently closed registration.
    #[error("active health scheduler is closed")]
    Closed,
    /// The same identity was supplied with a different health record.
    #[error("backend identity belongs to a different health record")]
    ConflictingBackend,
    /// Active recovery must be configured before the backend becomes routable.
    #[error("active probes require a backend registered in active mode")]
    PassiveBackend,
    /// All registration identifiers have been used.
    #[error("active health registration identifiers exhausted")]
    IdentifierExhausted,
}

type ProbeFuture = Pin<Box<dyn Future<Output = HealthOutcome> + Send>>;
type Probe = Arc<dyn Fn() -> ProbeFuture + Send + Sync>;
struct Entry {
    health: HealthHandle,
    probe: Probe,
    identity: ProbeIdentity,
    due: Instant,
    running: Option<AbortHandle>,
    lease: Weak<RegistrationLease>,
}
#[derive(Default)]
struct Registrations {
    entries: BTreeMap<u64, Entry>,
    running_checks: usize,
    next: u64,
    closed: bool,
}
struct Shared {
    registrations: Mutex<Registrations>,
    changed: Notify,
    capacity: usize,
    concurrency: usize,
}

impl Shared {
    fn close(&self) {
        let entries = {
            let mut registrations = self
                .registrations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            registrations.closed = true;
            std::mem::take(&mut registrations.entries)
        };
        for entry in entries.into_values() {
            if let Some(task) = entry.running {
                task.abort();
            }
        }
        self.changed.notify_one();
    }
}

/// Registration lifetime. Dropping it removes pending work and cancels its check.
///
/// Retained graphs can keep their own registration guards. Preparing and dropping
/// an unrelated candidate therefore cannot remove the retained graph's checks.
#[derive(Clone)]
pub struct ProbeRegistration {
    _lease: Arc<RegistrationLease>,
}
struct RegistrationLease {
    shared: Weak<Shared>,
    id: u64,
}
impl Drop for RegistrationLease {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            let entry = shared
                .registrations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .remove(&self.id);
            if let Some(task) = entry.and_then(|entry| entry.running) {
                task.abort();
            }
            shared.changed.notify_one();
        }
    }
}

/// Cloneable registration and shutdown access; this owns no background task.
#[derive(Clone)]
pub struct HealthSchedulerHandle {
    shared: Arc<Shared>,
}
impl HealthSchedulerHandle {
    /// Register a backend for immediate probing followed by fixed-delay checks.
    ///
    /// # Errors
    /// Returns an error after shutdown, at the registration limit, for duplicate
    /// passive-mode handles, or exhausted identifiers. Repeated registrations
    /// share ownership only when both health record and probe identity match.
    /// Incompatible settings are rejected until all previous guards are released.
    pub fn register<P: HealthProbe>(
        &self,
        health: HealthHandle,
        probe: P,
    ) -> Result<ProbeRegistration, ProbeError> {
        if health.mode() != HealthMode::Active {
            return Err(ProbeError::PassiveBackend);
        }
        let identity = probe.identity();
        if identity.configuration_key.is_empty() {
            return Err(ProbeError::InvalidProbeIdentity);
        }
        let probe: Probe = Arc::new(move || Box::pin(probe.check()));
        let lease = {
            let mut registrations = self
                .shared
                .registrations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if registrations.closed {
                return Err(ProbeError::Closed);
            }
            if let Some(entry) = registrations
                .entries
                .values()
                .find(|entry| entry.health.identity() == health.identity())
            {
                if !entry.health.same_record(&health) {
                    return Err(ProbeError::ConflictingBackend);
                }
                if entry.identity != identity {
                    return Err(ProbeError::IncompatibleProbeConfiguration);
                }
                if let Some(lease) = entry.lease.upgrade() {
                    return Ok(ProbeRegistration { _lease: lease });
                }
            }
            if registrations.entries.len() == self.shared.capacity {
                return Err(ProbeError::AtCapacity);
            }
            let id = registrations.next;
            registrations.next = id.checked_add(1).ok_or(ProbeError::IdentifierExhausted)?;
            let lease = Arc::new(RegistrationLease {
                shared: Arc::downgrade(&self.shared),
                id,
            });
            registrations.entries.insert(
                id,
                Entry {
                    health,
                    probe,
                    identity,
                    due: Instant::now(),
                    running: None,
                    lease: Arc::downgrade(&lease),
                },
            );
            lease
        };
        self.shared.changed.notify_one();
        Ok(ProbeRegistration { _lease: lease })
    }

    /// Close registration and cancel running checks. Await `run` for full cleanup.
    pub fn shutdown(&self) {
        self.shared.close();
    }
}

/// One caller-owned scheduler; it spawns at most the configured number of checks.
///
/// Poll [`Self::run`] on a Tokio runtime. Dropping that future closes registration
/// and aborts its managed checks. Shutdown is independent of runtime admission.
pub struct HealthScheduler {
    shared: Arc<Shared>,
    config: ActiveHealthConfig,
}
struct Completed {
    id: u64,
    token: ObservationToken,
    outcome: HealthOutcome,
}
struct ProbeActivity(HealthHandle);
impl Drop for ProbeActivity {
    fn drop(&mut self) {
        self.0.probe_finished();
    }
}
impl HealthScheduler {
    /// Validate bounded scheduling configuration without spawning any work.
    ///
    /// # Errors
    /// Returns [`ProbeError::InvalidDuration`] for zero or overflowing durations.
    pub fn new(config: ActiveHealthConfig) -> Result<Self, ProbeError> {
        let now = Instant::now();
        if config.interval.is_zero()
            || config.timeout.is_zero()
            || now.checked_add(config.interval).is_none()
            || now.checked_add(config.timeout).is_none()
        {
            return Err(ProbeError::InvalidDuration);
        }
        Ok(Self {
            shared: Arc::new(Shared {
                registrations: Mutex::default(),
                changed: Notify::new(),
                capacity: config.max_registered_backends.get(),
                concurrency: config.max_concurrent_checks.get(),
            }),
            config,
        })
    }

    /// Obtain synchronous registration and shutdown access.
    #[must_use]
    pub fn handle(&self) -> HealthSchedulerHandle {
        HealthSchedulerHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Probe due backends fairly by oldest deadline until shutdown.
    ///
    /// A slow backend occupies one slot and never overlaps itself. Each completed
    /// check sets a fresh interval, so neither missed ticks nor tasks accumulate.
    /// Shutdown aborts and joins all checks; cancellation is never a failure.
    pub async fn run(self) {
        let mut tasks = JoinSet::new();
        let mut running = HashMap::new();
        loop {
            let changed = self.shared.changed.notified();
            let mut changed = pin!(changed);
            changed.as_mut().enable();
            let std::ops::ControlFlow::Continue(deadline) =
                self.start_due_checks(&mut tasks, &mut running)
            else {
                break;
            };
            let sleep = tokio::time::sleep_until(
                deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(86400)),
            );
            let mut sleep = pin!(sleep);
            let completed = poll_fn(|cx| {
                if let Poll::Ready(Some(completed)) = tasks.poll_join_next_with_id(cx) {
                    return Poll::Ready(Some(completed));
                }
                if changed.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(None);
                }
                if deadline.is_some() && sleep.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(None);
                }
                Poll::Pending
            })
            .await;
            if let Some(completed) = completed {
                let task_id = match &completed {
                    Ok((task_id, _)) => *task_id,
                    Err(error) => error.id(),
                };
                running.remove(&task_id);
                self.observe_completed(completed.map(|(_, completed)| completed));
            }
        }
        tasks.shutdown().await;
    }
    fn start_due_checks(
        &self,
        tasks: &mut JoinSet<Completed>,
        running: &mut HashMap<tokio::task::Id, BackendIdentity>,
    ) -> std::ops::ControlFlow<(), Option<Instant>> {
        let mut registrations = self
            .shared
            .registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registrations.closed {
            return std::ops::ControlFlow::Break(());
        }
        while tasks.len() < self.config.max_concurrent_checks.get() {
            let Some((&id, entry)) = registrations
                .entries
                .iter_mut()
                .filter(|(_, entry)| {
                    entry.running.is_none()
                        && entry.due <= Instant::now()
                        && !running
                            .values()
                            .any(|backend| backend == entry.health.identity())
                })
                .min_by_key(|(id, entry)| (entry.due, **id))
            else {
                break;
            };
            let health = entry.health.clone();
            let probe = Arc::clone(&entry.probe);
            let timeout = self.config.timeout;
            let backend = entry.health.identity().clone();
            let slot = CheckSlot(Arc::downgrade(&self.shared));
            let task = tasks.spawn(async move {
                let _slot = slot;
                health.probe_started();
                let _activity = ProbeActivity(health.clone());
                let token = health.begin_probe();
                let outcome = tokio::time::timeout(timeout, async move { probe().await })
                    .await
                    .unwrap_or_else(|_| {
                        tracing::debug!(target: "netplane", event = "health_probe_timed_out");
                        HealthOutcome::Failure
                    });
                if outcome == HealthOutcome::Failure {
                    tracing::debug!(target: "netplane", event = "health_probe_failed");
                }
                Completed { id, token, outcome }
            });
            running.insert(task.id(), backend);
            entry.running = Some(task);
            registrations.running_checks += 1;
        }
        let deadline = (tasks.len() < self.config.max_concurrent_checks.get())
            .then(|| {
                registrations
                    .entries
                    .values()
                    .filter(|entry| {
                        entry.running.is_none()
                            && !running
                                .values()
                                .any(|backend| backend == entry.health.identity())
                    })
                    .map(|entry| entry.due)
                    .min()
            })
            .flatten();
        drop(registrations);
        std::ops::ControlFlow::Continue(deadline)
    }
    fn observe_completed(&self, completed: Result<Completed, tokio::task::JoinError>) {
        if let Some(events) = self.accept_completed(completed) {
            events.emit();
        }
    }

    fn accept_completed(
        &self,
        completed: Result<Completed, tokio::task::JoinError>,
    ) -> Option<super::registry::ObservationEvents> {
        let mut registrations = self
            .shared
            .registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let observation = match completed {
            Ok(completed) => {
                let Some(entry) = registrations.entries.get_mut(&completed.id) else {
                    // Releasing the token can destroy its record and emit tracing.
                    drop(registrations);
                    drop(completed);
                    return None;
                };
                entry.running = None;
                entry.due = Instant::now() + self.config.interval;
                Some(
                    entry
                        .health
                        .report_deferred(completed.token, completed.outcome),
                )
            }
            Err(error) => {
                if let Some(entry) = registrations.entries.values_mut().find(|entry| {
                    entry
                        .running
                        .as_ref()
                        .is_some_and(|task| task.id() == error.id())
                }) {
                    entry.running = None;
                    entry.due = Instant::now() + self.config.interval;
                }
                // A panic payload may have a user-defined destructor.
                drop(registrations);
                drop(error);
                return None;
            }
        };
        drop(registrations);
        observation
    }
}
impl Drop for HealthScheduler {
    fn drop(&mut self) {
        self.shared.close();
    }
}
