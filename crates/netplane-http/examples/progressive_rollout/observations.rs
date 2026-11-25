use bytes::Bytes;
use http::{Request, Response};
use netplane::{
    ReconfigurationController,
    health::{HealthHandle, HealthRegistry},
};
use netplane_control::rollout::{
    GroupObservation, ObservationProvider, ObservationRequest, ObservationUnavailable,
    RolloutObservation,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::time::Instant;

pub struct Sample {
    pub graph: String,
    pub group: String,
    pub accepted: Instant,
    pub completed: Instant,
    pub failed: bool,
}
#[derive(Default)]
pub struct Samples {
    outcomes: VecDeque<Sample>,
    evicted: Option<Instant>,
}
impl Samples {
    pub fn record(&mut self, sample: Sample) {
        if self.outcomes.len() == 4096 {
            if let Some(old) = self.outcomes.pop_front() {
                self.evicted = Some(old.completed);
            }
        }
        self.outcomes.push_back(sample);
    }
}
pub type SharedSamples = Arc<Mutex<Samples>>;
pub struct Observations {
    pub samples: SharedSamples,
    pub controller: Arc<ReconfigurationController<Request<Bytes>, Response<Bytes>>>,
    pub registry: HealthRegistry,
    pub health: [HealthHandle; 2],
}
impl ObservationProvider for Observations {
    async fn observe(
        &self,
        request: ObservationRequest,
    ) -> Result<RolloutObservation, ObservationUnavailable> {
        let active = self
            .controller
            .inspect()
            .active
            .ok_or(ObservationUnavailable)?;
        if active.id != request.publication {
            return Err(ObservationUnavailable);
        }
        let snapshots = self.health.each_ref().map(HealthHandle::snapshot);
        let revision = self.registry.snapshot().summary.health_transitions;
        let samples = self
            .samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let complete = samples
            .evicted
            .is_none_or(|evicted| evicted < request.since);
        let groups = request
            .groups
            .iter()
            .zip(&snapshots)
            .map(|(group, health)| {
                let matching: Vec<_> = samples
                    .outcomes
                    .iter()
                    .filter(|sample| {
                        sample.graph == active.metadata.entry
                            && sample.group == *group
                            && sample.accepted >= request.since
                            && sample.completed <= request.until
                    })
                    .collect();
                let mut latencies: Vec<_> = matching
                    .iter()
                    .map(|sample| sample.completed.saturating_duration_since(sample.accepted))
                    .collect();
                latencies.sort_unstable();
                let p95_latency = latencies
                    .get((latencies.len() * 95).div_ceil(100).saturating_sub(1))
                    .copied();
                GroupObservation {
                    group: group.clone(),
                    completed_requests: matching.len() as u64,
                    failed_requests: matching.iter().filter(|sample| sample.failed).count() as u64,
                    p95_latency,
                    eligible_backends: usize::from(health.eligible),
                    eligibility_revision: revision,
                }
            })
            .collect();
        drop(samples);
        let eligibility_changed = snapshots
            .iter()
            .any(|health| health.last_transition > request.since);
        Ok(RolloutObservation {
            request,
            complete,
            eligibility_changed,
            groups,
        })
    }
}
