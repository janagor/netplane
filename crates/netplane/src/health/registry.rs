use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::time::Instant;

use super::{
    BackendHealthSnapshot, BackendIdentity, HealthError, HealthMode, HealthOutcome, HealthPolicy,
    HealthSnapshot, HealthState, HealthSummary, ObservationSource,
};

#[derive(Default)]
struct Counters {
    transitions: AtomicU64,
    active_probes: AtomicUsize,
}

/// Registry of weak health records; handles and observation tokens own their lifetime.
///
/// Removing a backend from a graph does not invalidate retained graph handles.
/// Releasing the last handle or observation reclaims its registry entry.
#[derive(Clone, Default)]
pub struct HealthRegistry {
    records: Arc<Mutex<BTreeMap<BackendIdentity, Weak<Record>>>>,
    counters: Arc<Counters>,
}

impl HealthRegistry {
    /// Register a passive backend, reusing compatible live state.
    ///
    /// # Errors
    /// Rejects empty identity components, unrepresentable cooldowns, or an
    /// incompatible policy for the same live identity.
    pub fn register(
        &self,
        backend: BackendIdentity,
        policy: HealthPolicy,
    ) -> Result<HealthHandle, HealthError> {
        self.register_with_mode(backend, policy, HealthMode::Passive)
    }

    /// Register a backend with explicit recovery mode.
    ///
    /// # Errors
    /// Returns [`HealthError`] for invalid identity or delay, or incompatible
    /// live registration. Change incarnation when replacing a physical instance.
    pub fn register_with_mode(
        &self,
        backend: BackendIdentity,
        policy: HealthPolicy,
        mode: HealthMode,
    ) -> Result<HealthHandle, HealthError> {
        if backend.service.is_empty()
            || backend.instance.is_empty()
            || backend.incarnation.is_empty()
        {
            return Err(HealthError::InvalidIdentity);
        }
        let now = Instant::now();
        if now.checked_add(policy.recovery_delay).is_none() {
            return Err(HealthError::InvalidRecoveryDelay);
        }
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = records.get(&backend).and_then(Weak::upgrade) {
            if existing.policy != policy || existing.mode != mode {
                drop(records);
                return Err(HealthError::IncompatibleRegistration);
            }
            return Ok(HealthHandle { record: existing });
        }
        let state = match mode {
            HealthMode::Passive => HealthState::Healthy,
            HealthMode::Active => HealthState::Recovering,
        };
        let record = Arc::new(Record {
            identity: backend.clone(),
            policy,
            mode,
            registry: Arc::downgrade(&self.records),
            counters: Arc::clone(&self.counters),
            eligibility: AtomicU8::new(if mode == HealthMode::Passive { 0 } else { 2 }),
            state: Mutex::new(Observations {
                state,
                failures: 0,
                successes: 0,
                generation: Arc::new(()),
                last_transition: now,
                last_observation: None,
            }),
        });
        records.insert(backend, Arc::downgrade(&record));
        drop(records);
        tracing::debug!(target: "netplane", event = "backend_registered", service = %record.identity.service, instance = %record.identity.instance, incarnation = %record.identity.incarnation);
        Ok(HealthHandle { record })
    }

    /// Sample live records without retaining them after inspection.
    #[must_use]
    pub fn snapshot(&self) -> HealthSnapshot {
        let live = {
            let records = self
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            records
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        let backends: Vec<_> = live
            .into_iter()
            .map(|record| {
                let handle = HealthHandle { record };
                (handle.identity().clone(), handle.snapshot())
            })
            .collect();
        let mut summary = HealthSummary {
            tracked_backends: backends.len(),
            active_probes: self.counters.active_probes.load(Ordering::Relaxed),
            health_transitions: self.counters.transitions.load(Ordering::Relaxed),
            ..HealthSummary::default()
        };
        for (_, backend) in &backends {
            match backend.state {
                HealthState::Healthy => summary.healthy_backends += 1,
                HealthState::Unhealthy => summary.unhealthy_backends += 1,
                HealthState::Recovering => summary.recovering_backends += 1,
            }
        }
        HealthSnapshot { backends, summary }
    }

    /// Sample one live identity without retaining it beyond this call.
    #[must_use]
    pub fn backend_snapshot(&self, backend: &BackendIdentity) -> Option<BackendHealthSnapshot> {
        let record = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(backend)
            .and_then(Weak::upgrade)?;
        Some(HealthHandle { record }.snapshot())
    }
}

struct Record {
    eligibility: AtomicU8,
    registry: Weak<Mutex<BTreeMap<BackendIdentity, Weak<Self>>>>,
    identity: BackendIdentity,
    policy: HealthPolicy,
    mode: HealthMode,
    state: Mutex<Observations>,
    counters: Arc<Counters>,
}
impl Drop for Record {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut records = registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if records
                .get(&self.identity)
                .is_some_and(|record| std::ptr::eq(record.as_ptr(), self))
            {
                records.remove(&self.identity);
            }
        }
        tracing::debug!(target: "netplane", event = "backend_removed", service = %self.identity.service, instance = %self.identity.instance, incarnation = %self.identity.incarnation);
    }
}
struct Observations {
    state: HealthState,
    failures: u32,
    successes: u32,
    generation: Arc<()>,
    last_transition: Instant,
    last_observation: Option<Instant>,
}
impl Observations {
    fn transition(
        &mut self,
        next: HealthState,
        now: Instant,
        record: &Record,
    ) -> Option<HealthState> {
        if next == self.state {
            return None;
        }
        self.state = next;
        self.generation = Arc::new(());
        self.last_transition = now;
        record.counters.transitions.fetch_add(1, Ordering::Relaxed);
        record.eligibility.store(
            match next {
                HealthState::Healthy => 0,
                HealthState::Unhealthy => 1,
                HealthState::Recovering if record.mode == HealthMode::Passive => 0,
                HealthState::Recovering => 2,
            },
            Ordering::Release,
        );
        Some(next)
    }
    fn refresh(&mut self, record: &Record, now: Instant) -> Option<HealthState> {
        if self.state == HealthState::Unhealthy
            && now.duration_since(self.last_transition) >= record.policy.recovery_delay
        {
            self.failures = 0;
            self.successes = 0;
            return self.transition(HealthState::Recovering, now, record);
        }
        None
    }
}
fn emit_transition(transition: Option<HealthState>, identity: &BackendIdentity) {
    match transition {
        Some(HealthState::Unhealthy) => {
            tracing::warn!(target: "netplane", event = "backend_marked_unhealthy", service = %identity.service, instance = %identity.instance, incarnation = %identity.incarnation);
        }
        Some(HealthState::Recovering) => {
            tracing::info!(target: "netplane", event = "backend_recovery_started", service = %identity.service, instance = %identity.instance, incarnation = %identity.incarnation);
        }
        Some(HealthState::Healthy) => {
            tracing::info!(target: "netplane", event = "backend_recovered", service = %identity.service, instance = %identity.instance, incarnation = %identity.incarnation);
        }
        None => {}
    }
}

/// Cloneable per-instance reference used by selectors, middleware and probes.
#[derive(Clone)]
pub struct HealthHandle {
    record: Arc<Record>,
}

/// One execution's generation; consuming it prevents duplicate reporting.
///
/// The token retains its original record. A token from another handle or an old
/// generation cannot alter this handle's state. Its source controls recovery
/// evidence in active mode. Dropping it reports nothing.
pub struct ObservationToken {
    record: Arc<Record>,
    generation: Arc<()>,
    source: ObservationSource,
}

impl std::fmt::Debug for HealthHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HealthHandle")
            .field("identity", &self.record.identity)
            .finish_non_exhaustive()
    }
}

/// State updates are complete before these callbacks run. Keep the record alive
/// so emitting or discarding events never releases it under a scheduler lock.
pub(super) struct ObservationEvents {
    handle: HealthHandle,
    cooldown: Option<HealthState>,
    transition: Option<HealthState>,
    stale: bool,
}
impl ObservationEvents {
    pub(super) fn emit(self) {
        let identity = self.handle.identity();
        emit_transition(self.cooldown, identity);
        emit_transition(self.transition, identity);
        if self.stale {
            tracing::trace!(target: "netplane", event = "stale_health_observation_ignored", service = %identity.service, instance = %identity.instance, incarnation = %identity.incarnation);
        }
    }
}

impl HealthHandle {
    /// Backend identity including the incarnation retained by this handle.
    #[must_use]
    pub fn identity(&self) -> &BackendIdentity {
        &self.record.identity
    }

    /// Recovery mode fixed at registration.
    #[must_use]
    pub fn mode(&self) -> HealthMode {
        self.record.mode
    }

    /// Capture passive evidence when actual backend execution starts.
    ///
    /// Passive successes cannot restore eligibility in active recovery mode.
    #[must_use]
    pub fn begin_observation(&self) -> ObservationToken {
        self.begin(ObservationSource::Passive)
    }

    /// Capture active-probe evidence; only this source can recover active backends.
    #[must_use]
    pub fn begin_probe(&self) -> ObservationToken {
        self.begin(ObservationSource::ActiveProbe)
    }

    fn begin(&self, source: ObservationSource) -> ObservationToken {
        let mut state = self
            .record
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transition = state.refresh(&self.record, Instant::now());
        let generation = Arc::clone(&state.generation);
        drop(state);
        emit_transition(transition, &self.record.identity);
        ObservationToken {
            record: Arc::clone(&self.record),
            generation,
            source,
        }
    }

    /// Record one completed execution; stale and ignored results have no effect.
    /// Passive successes do not count toward recovery in active mode.
    pub fn report(&self, token: ObservationToken, outcome: HealthOutcome) {
        self.report_deferred(token, outcome).emit();
    }

    // Scheduler acceptance and registration removal share a mutex. Only health
    // state changes run there; tracing callbacks must be emitted after unlocking.
    pub(super) fn report_deferred(
        &self,
        token: ObservationToken,
        outcome: HealthOutcome,
    ) -> ObservationEvents {
        let mut events = ObservationEvents {
            handle: self.clone(),
            cooldown: None,
            transition: None,
            stale: false,
        };
        let ObservationToken {
            record,
            generation,
            source,
        } = token;
        let same_record = Arc::ptr_eq(&self.record, &record);
        drop(record);
        if outcome == HealthOutcome::Ignored || !same_record {
            return events;
        }
        let mut state = self
            .record
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        let cooldown_transition = state.refresh(&self.record, now);
        if !Arc::ptr_eq(&state.generation, &generation) {
            drop(state);
            events.cooldown = cooldown_transition;
            events.stale = true;
            return events;
        }
        state.last_observation = Some(now);
        let next = match (state.state, outcome) {
            (HealthState::Healthy, HealthOutcome::Success) => {
                state.failures = 0;
                None
            }
            (HealthState::Healthy, HealthOutcome::Failure) => {
                state.failures = state.failures.saturating_add(1);
                (state.failures >= self.record.policy.failure_threshold.get())
                    .then_some(HealthState::Unhealthy)
            }
            (HealthState::Recovering, HealthOutcome::Success)
                if self.record.mode == HealthMode::Passive
                    || source == ObservationSource::ActiveProbe =>
            {
                state.successes = state.successes.saturating_add(1);
                (state.successes >= self.record.policy.recovery_threshold.get())
                    .then_some(HealthState::Healthy)
            }
            (HealthState::Recovering, HealthOutcome::Failure) => {
                state.failures = 1;
                state.successes = 0;
                Some(HealthState::Unhealthy)
            }
            _ => None,
        };
        let transition = next.and_then(|next| state.transition(next, now, &self.record));
        drop(state);
        events.cooldown = cooldown_transition;
        events.transition = transition;
        events
    }

    /// Read current health eligibility, advancing an elapsed cooldown.
    #[must_use]
    pub fn is_eligible(&self) -> bool {
        match self.record.eligibility.load(Ordering::Acquire) {
            0 => true,
            2 => false,
            _ => self.snapshot().eligible,
        }
    }

    /// Sample coherent state and counters, advancing an elapsed cooldown.
    #[must_use]
    pub fn snapshot(&self) -> BackendHealthSnapshot {
        let mut state = self
            .record
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transition = state.refresh(&self.record, Instant::now());
        let snapshot = BackendHealthSnapshot {
            state: state.state,
            consecutive_failures: state.failures,
            consecutive_successes: state.successes,
            last_transition: state.last_transition,
            last_observation: state.last_observation,
            eligible: state.state == HealthState::Healthy
                || (state.state == HealthState::Recovering
                    && self.record.mode == HealthMode::Passive),
        };
        drop(state);
        emit_transition(transition, &self.record.identity);
        snapshot
    }

    pub(crate) fn same_record(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.record, &other.record)
    }

    pub(crate) fn probe_started(&self) {
        self.record
            .counters
            .active_probes
            .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn probe_finished(&self) {
        self.record
            .counters
            .active_probes
            .fetch_sub(1, Ordering::Relaxed);
    }
}
