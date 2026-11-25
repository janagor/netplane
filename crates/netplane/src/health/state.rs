use std::{num::NonZeroU32, time::Duration};
use tokio::time::Instant;

/// Discovery identity plus an explicit physical-instance incarnation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BackendIdentity {
    /// Logical service identity, including namespace where applicable.
    pub service: String,
    /// Endpoint identity within the service.
    pub instance: String,
    /// Replacement discriminator, for example a Pod UID.
    pub incarnation: String,
}

/// Operational state; eligibility also depends on recovery mode and topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthState {
    /// Available for ordinary traffic.
    Healthy,
    /// Excluded until the cooldown expires.
    Unhealthy,
    /// Gathering evidence of recovery.
    Recovering,
}

/// A completed execution's operational meaning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthOutcome {
    /// Evidence that the backend works.
    Success,
    /// Evidence of backend failure.
    Failure,
    /// No evidence; counters and observation time remain unchanged.
    Ignored,
}

/// Origin of one execution's health evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservationSource {
    /// Application execution; active-mode recovery ignores successful results.
    Passive,
    /// An explicit active check, eligible to supply recovery evidence.
    ActiveProbe,
}

/// Whether recovery uses application traffic or separate active checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthMode {
    /// Starts healthy; recovering instances receive weighted application traffic.
    Passive,
    /// Starts recovering and stays ineligible until enough active-probe successes arrive.
    Active,
}

/// Consecutive-observation thresholds and monotonic cooldown.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthPolicy {
    /// Consecutive failures required to exclude a healthy backend.
    pub failure_threshold: NonZeroU32,
    /// Consecutive successes required to complete recovery.
    pub recovery_threshold: NonZeroU32,
    /// Delay after failure before recovery evidence is accepted; zero is valid.
    pub recovery_delay: Duration,
}

/// Invalid registration, rejected before creating health state.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HealthError {
    /// All identity components must be nonempty.
    #[error("health identity components must be nonempty")]
    InvalidIdentity,
    /// The monotonic clock cannot represent the recovery deadline.
    #[error("health recovery delay exceeds the monotonic clock range")]
    InvalidRecoveryDelay,
    /// A live identity already has a different policy or recovery mode.
    #[error("backend identity already registered with a different health policy or mode")]
    IncompatibleRegistration,
}

/// An owned observation; changing it cannot mutate the registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendHealthSnapshot {
    /// Current state after advancing an elapsed cooldown.
    pub state: HealthState,
    /// Failures since the latest successful observation or recovery start.
    pub consecutive_failures: u32,
    /// Successes collected during the current recovery attempt.
    pub consecutive_successes: u32,
    /// Most recent state transition on the monotonic clock.
    pub last_transition: Instant,
    /// Most recent accepted, non-ignored observation.
    pub last_observation: Option<Instant>,
    /// Health eligibility, independent of discovery and configured weight.
    pub eligible: bool,
}

/// Aggregate health diagnostics sampled without holding a lock across I/O.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HealthSummary {
    /// Live records retained by handles or outstanding observations.
    pub tracked_backends: usize,
    /// Records currently healthy.
    pub healthy_backends: usize,
    /// Records currently unhealthy.
    pub unhealthy_backends: usize,
    /// Records currently recovering.
    pub recovering_backends: usize,
    /// Active checks currently executing in this registry.
    pub active_probes: usize,
    /// State transitions since registry creation, including reclaimed records.
    pub health_transitions: u64,
}

/// Read-only owned sample of live backend records and aggregate counters.
#[derive(Clone, Debug, Default)]
pub struct HealthSnapshot {
    /// Live records in identity order; final-owner drop reclaims registry entries.
    pub backends: Vec<(BackendIdentity, BackendHealthSnapshot)>,
    /// Aggregate state counts and cumulative transition counter.
    pub summary: HealthSummary,
}
