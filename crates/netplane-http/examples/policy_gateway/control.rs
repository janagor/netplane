use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

use netplane::{
    ControllerError, ControllerSnapshot, LifecycleState, PublicationKind,
    ReconfigurationController, ReconfigurationOutcome,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, oneshot};

use super::{
    HttpRequest, HttpResponse,
    config::{GatewayConfig, PolicyConfig},
    graph::build_graph,
};

pub type Drain = Pin<Box<dyn Future<Output = Result<(), netplane::DrainError>> + Send>>;

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyInput {
    pub canary_percent: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconfigureRequest {
    pub expected_version: u64,
    pub policy: PolicyInput,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackRequest {
    pub expected_version: u64,
    pub target_version: u64,
}

#[derive(Clone, Serialize)]
pub struct PolicyView {
    pub canary_percent: u8,
}

#[derive(Serialize)]
pub struct VersionView {
    pub version: u64,
    pub policy: PolicyView,
}

#[derive(Serialize)]
pub struct OutcomeView {
    pub previous_version: u64,
    pub published_version: u64,
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_version: Option<u64>,
}

impl From<ReconfigurationOutcome> for OutcomeView {
    fn from(value: ReconfigurationOutcome) -> Self {
        let (kind, source_version) = match value.kind {
            PublicationKind::Replacement => ("replacement", None),
            PublicationKind::Rollback { source } => ("rollback", Some(source.get())),
        };
        Self {
            previous_version: value.previous.get(),
            published_version: value.published.get(),
            kind,
            source_version,
        }
    }
}

#[derive(Serialize)]
pub struct StatusView {
    pub active: Option<VersionView>,
    pub retained: Vec<VersionView>,
    pub retained_count: usize,
    pub last_publication: Option<OutcomeView>,
    pub runtime: RuntimeView,
}

#[derive(Serialize)]
pub struct RuntimeView {
    pub lifecycle: &'static str,
    pub outstanding: usize,
    pub max_outstanding: usize,
}

#[derive(Debug)]
pub enum ControlError {
    InvalidPolicy,
    Conflict,
    ShuttingDown,
    NotFound,
    Internal,
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ControlError {}

impl From<ControllerError> for ControlError {
    fn from(value: ControllerError) -> Self {
        match value {
            ControllerError::Stale { .. } => Self::Conflict,
            ControllerError::ShuttingDown => Self::ShuttingDown,
            ControllerError::UnknownVersion(_) => Self::NotFound,
            other => {
                tracing::error!(error = %other, "control operation failed");
                Self::Internal
            }
        }
    }
}

struct ControlState {
    policies: BTreeMap<u64, PolicyConfig>,
    shutting_down: bool,
    notification_failed: bool,
    drain_tx: Option<oneshot::Sender<Drain>>,
}

pub struct GatewayControl {
    controller: Arc<ReconfigurationController<HttpRequest, HttpResponse>>,
    config: GatewayConfig,
    state: Mutex<ControlState>,
}

impl GatewayControl {
    pub fn new(
        controller: Arc<ReconfigurationController<HttpRequest, HttpResponse>>,
        config: GatewayConfig,
        drain_tx: oneshot::Sender<Drain>,
    ) -> Result<Self, ControlError> {
        let id = controller
            .inspect()
            .active
            .ok_or(ControlError::Internal)?
            .id
            .get();
        Ok(Self {
            controller,
            config: config.clone(),
            state: Mutex::new(ControlState {
                policies: BTreeMap::from([(id, config.policy)]),
                shutting_down: false,
                notification_failed: false,
                drain_tx: Some(drain_tx),
            }),
        })
    }

    pub async fn status(&self) -> Result<StatusView, ControlError> {
        let state = self.state.lock().await;
        let snapshot = self.controller.inspect();
        let runtime = self.controller.handle().status();
        let view = |id: u64| -> Result<VersionView, ControlError> {
            let policy = state.policies.get(&id).ok_or(ControlError::Internal)?;
            Ok(VersionView {
                version: id,
                policy: PolicyView {
                    canary_percent: policy.canary_percent,
                },
            })
        };
        let active = snapshot
            .active
            .as_ref()
            .map(|v| view(v.id.get()))
            .transpose()?;
        let retained = snapshot
            .retained
            .iter()
            .map(|v| view(v.id.get()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(StatusView {
            active,
            retained_count: retained.len(),
            retained,
            last_publication: snapshot.last_outcome.map(Into::into),
            runtime: RuntimeView {
                lifecycle: match runtime.lifecycle {
                    LifecycleState::Running => "running",
                    LifecycleState::Draining => "draining",
                    LifecycleState::Stopped => "stopped",
                },
                outstanding: runtime.outstanding,
                max_outstanding: runtime.max_outstanding,
            },
        })
    }

    fn current(
        snapshot: &ControllerSnapshot,
        expected: u64,
    ) -> Result<netplane::VersionToken, ControlError> {
        let active = snapshot.active.as_ref().ok_or(ControlError::ShuttingDown)?;
        let token = snapshot.token.ok_or(ControlError::ShuttingDown)?;
        if active.id.get() != expected {
            return Err(ControlError::Conflict);
        }
        Ok(token)
    }

    fn prune(&self, state: &mut ControlState) {
        let snapshot = self.controller.inspect();
        state.policies.retain(|id, _| {
            snapshot.active.as_ref().is_some_and(|v| v.id.get() == *id)
                || snapshot.retained.iter().any(|v| v.id.get() == *id)
        });
    }

    pub async fn reconfigure(
        &self,
        request: ReconfigureRequest,
    ) -> Result<OutcomeView, ControlError> {
        if request.policy.canary_percent > 100 {
            return Err(ControlError::InvalidPolicy);
        }
        let mut state = self.state.lock().await;
        if state.shutting_down {
            return Err(ControlError::ShuttingDown);
        }
        let snapshot = self.controller.inspect();
        let token = Self::current(&snapshot, request.expected_version)?;
        let mut policy = state
            .policies
            .get(&request.expected_version)
            .ok_or(ControlError::Internal)?
            .clone();
        policy.revision = policy
            .revision
            .checked_add(1)
            .ok_or(ControlError::Internal)?;
        policy.canary_percent =
            u8::try_from(request.policy.canary_percent).map_err(|_| ControlError::InvalidPolicy)?;
        let candidate = build_graph(
            &policy,
            self.config.outer_capacity,
            self.config.backend_capacity,
        )
        .map_err(|error| {
            tracing::error!(%error, "candidate construction failed");
            ControlError::Internal
        })?;
        let outcome = self.controller.publish_if_current(token, candidate)?;
        state.policies.insert(outcome.published.get(), policy);
        self.prune(&mut state);
        drop(state);
        Ok(outcome.into())
    }

    pub async fn rollback(&self, request: RollbackRequest) -> Result<OutcomeView, ControlError> {
        let mut state = self.state.lock().await;
        if state.shutting_down {
            return Err(ControlError::ShuttingDown);
        }
        let snapshot = self.controller.inspect();
        let token = Self::current(&snapshot, request.expected_version)?;
        let target = snapshot
            .retained
            .iter()
            .find(|v| v.id.get() == request.target_version)
            .ok_or(ControlError::NotFound)?;
        let policy = state
            .policies
            .get(&request.target_version)
            .ok_or(ControlError::Internal)?
            .clone();
        let outcome = self.controller.rollback_if_current(token, target.id)?;
        state.policies.insert(outcome.published.get(), policy);
        self.prune(&mut state);
        drop(state);
        Ok(outcome.into())
    }

    pub async fn shutdown(&self) -> Result<(), ControlError> {
        let mut state = self.state.lock().await;
        if state.shutting_down {
            return if state.notification_failed {
                Err(ControlError::Internal)
            } else {
                Ok(())
            };
        }
        let drain = self.controller.shutdown(self.config.shutdown_grace_period);
        state.shutting_down = true;
        let delivered = state
            .drain_tx
            .take()
            .is_some_and(|sender| sender.send(Box::pin(drain)).is_ok());
        if !delivered {
            tracing::error!("shutdown drain notification could not reach supervisor");
            state.notification_failed = true;
            drop(state);
            return Err(ControlError::Internal);
        }
        drop(state);
        Ok(())
    }
}
