use netplane::{
    ControllerOptions, GraphDraft, GraphVersion, Netplane, ReconfigurationController, RuntimeLimits,
};
use netplane_control::rollout::*;
use netplane_control::{
    GraphPreparer, Reconciler, ReconciliationDriver, UpdatePolicy, UpdateSender,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

pub type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Config {
    pub weight: u8,
    pub unrelated: u64,
    pub membership: u64,
}
impl TrafficConfiguration for Config {
    fn group_policy(&self, group: &str) -> Option<GroupPolicy> {
        let weight = match group {
            "stable" => 100 - self.weight,
            "canary" => self.weight,
            _ => return None,
        };
        Some(GroupPolicy {
            weight,
            membership_revision: self.membership,
        })
    }
    fn set_weights(&mut self, _: &str, _: &str, weight: u8) {
        self.weight = weight;
    }
}
pub struct Preparer;
impl GraphPreparer<Config, (), Config> for Preparer {
    type Error = netplane::ConfigError;
    fn validate(&self, _: &Config) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn prepare(&self, desired: Config) -> Result<GraphVersion<(), Config>, Self::Error> {
        GraphDraft::new(tower::service_fn(move |()| {
            let desired = desired.clone();
            async { Ok::<_, std::convert::Infallible>(desired) }
        }))
        .validate()
    }
}
#[derive(Clone)]
pub struct Metrics {
    pub count: Arc<AtomicU64>,
    pub failed: Arc<AtomicU64>,
    pub eligibility: Arc<AtomicU64>,
}
impl Metrics {
    pub fn new(count: u64) -> Self {
        Self {
            count: Arc::new(AtomicU64::new(count)),
            failed: Arc::new(AtomicU64::new(0)),
            eligibility: Arc::new(AtomicU64::new(0)),
        }
    }
}
impl ObservationProvider for Metrics {
    async fn observe(
        &self,
        request: ObservationRequest,
    ) -> Result<RolloutObservation, ObservationUnavailable> {
        let groups = request
            .groups
            .iter()
            .map(|group| GroupObservation {
                group: group.clone(),
                completed_requests: self.count.load(Ordering::SeqCst),
                failed_requests: self.failed.load(Ordering::SeqCst),
                p95_latency: Some(Duration::from_millis(10)),
                eligible_backends: 1,
                eligibility_revision: self.eligibility.load(Ordering::SeqCst),
            })
            .collect();
        Ok(RolloutObservation {
            request,
            complete: true,
            eligibility_changed: false,
            groups,
        })
    }
}
pub fn spec(weights: &[u8]) -> RolloutSpec {
    RolloutSpec {
        stable: "stable".into(),
        canary: "canary".into(),
        stages: weights
            .iter()
            .map(|weight| Stage {
                canary_weight: *weight,
                min_observation: Duration::from_secs(2),
                timeout: Duration::from_secs(20),
                conditions: if *weight == 0 {
                    vec![Condition::Eligible {
                        group: "canary".into(),
                        minimum: 1,
                    }]
                } else {
                    vec![Condition::ErrorRate {
                        group: "canary".into(),
                        maximum_basis_points: 100,
                        minimum_requests: 100,
                    }]
                },
            })
            .collect(),
    }
}
pub async fn fixture() -> Result<
    (
        UpdateSender<Config>,
        tokio::task::JoinHandle<()>,
        Arc<ReconfigurationController<(), Config>>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let config = Config {
        weight: 0,
        unrelated: 7,
        membership: 1,
    };
    let runtime = Netplane::new(
        Preparer.prepare(config.clone()).await?,
        RuntimeLimits { max_outstanding: 8 },
    )?;
    let controller = Arc::new(ReconfigurationController::new(
        runtime,
        ControllerOptions::default(),
    ));
    let reconciler = Reconciler::new(controller.clone(), Preparer);
    let (driver, updates) = ReconciliationDriver::new(
        reconciler,
        UpdatePolicy {
            debounce: Duration::ZERO,
            ..UpdatePolicy::default()
        },
    )?;
    let task = tokio::spawn(driver.run(|_| false));
    updates.submit(config)?;
    converged(&updates).await;
    Ok((updates, task, controller))
}
pub async fn converged(updates: &UpdateSender<Config>) {
    let mut receiver = updates.subscribe_status();
    while !receiver.borrow_and_update().is_converged() {
        if receiver.changed().await.is_err() {
            return;
        }
    }
}
pub async fn state(handle: &RolloutHandle<Config>, wanted: RolloutState) {
    let mut receiver = handle.subscribe_status();
    loop {
        if receiver.borrow_and_update().state == wanted {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}
pub async fn ticks(seconds: u64) {
    for _ in 0..seconds {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
}
