//! Real loopback HTTP traffic with successful, failed and insufficient rollout scenarios.
mod graph;
mod observations;
use axum::{Router, routing::get};
use graph::{Config, Preparer};
use http::StatusCode;
use netplane::{
    ControllerOptions, Netplane, ReconfigurationController, RuntimeLimits,
    health::{BackendIdentity, HealthPolicy, HealthRegistry},
};
use netplane_control::{
    GraphPreparer, Reconciler, ReconciliationDriver, UpdatePolicy,
    rollout::{Condition, EvidenceReason, RolloutDriver, RolloutSpec, RolloutState, Stage},
};
use netplane_http::{HttpAdapter, HttpLimits};
use observations::{Observations, Samples};
use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex, atomic::AtomicU64},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::watch,
    task::JoinSet,
    time::{Instant, sleep},
};
use tower::BoxError;

async fn server(
    app: Router,
    mut stopped: watch::Receiver<bool>,
    tasks: &mut JoinSet<Result<(), std::io::Error>>,
) -> Result<String, BoxError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    tasks.spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = stopped.changed().await;
            })
            .await
    });
    Ok(origin)
}

type Controller =
    Arc<ReconfigurationController<http::Request<bytes::Bytes>, http::Response<bytes::Bytes>>>;
struct Fixture {
    stop: watch::Sender<bool>,
    servers: JoinSet<Result<(), std::io::Error>>,
    controller: Controller,
    worker: tokio::task::JoinHandle<()>,
    updates: netplane_control::UpdateSender<Config>,
    gateway: String,
    source: Observations,
}
fn health_handles(
    registry: &HealthRegistry,
) -> Result<[netplane::health::HealthHandle; 2], BoxError> {
    let policy = HealthPolicy {
        failure_threshold: NonZeroU32::new(3).ok_or("threshold")?,
        recovery_threshold: NonZeroU32::new(1).ok_or("threshold")?,
        recovery_delay: Duration::from_secs(1),
    };
    let health = ["stable", "canary"]
        .map(|group| {
            registry.register(
                BackendIdentity {
                    service: group.into(),
                    instance: "loopback".into(),
                    incarnation: "demo".into(),
                },
                policy.clone(),
            )
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    health.try_into().map_err(|_| "health groups".into())
}

async fn fixture(mode: &str) -> Result<Fixture, BoxError> {
    let (stop, stopped) = watch::channel(false);
    let mut servers = JoinSet::new();
    let stable = server(
        Router::new().fallback(get(|| async { "stable-v1" })),
        stopped.clone(),
        &mut servers,
    )
    .await?;
    let fail = mode == "failure";
    let slow = mode == "latency-failure";
    let canary = server(
        Router::new().fallback(get(move || async move {
            if slow {
                sleep(Duration::from_millis(40)).await;
            }
            (
                if fail {
                    StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    StatusCode::OK
                },
                "canary-v2",
            )
        })),
        stopped.clone(),
        &mut servers,
    )
    .await?;
    let registry = HealthRegistry::default();
    let health = health_handles(&registry)?;
    let samples = Arc::new(Mutex::new(Samples::default()));
    let preparer = Preparer {
        origins: [stable, canary],
        health: health.clone(),
        samples: samples.clone(),
        next_graph: AtomicU64::new(1),
    };
    let desired = Config { canary_weight: 0 };
    let runtime = Netplane::new(
        preparer.prepare(desired.clone()).await?,
        RuntimeLimits {
            max_outstanding: 32,
        },
    )?;
    let controller = Arc::new(ReconfigurationController::new(
        runtime,
        ControllerOptions::default(),
    ));
    let reconciler = Reconciler::new(controller.clone(), preparer);
    let (reconciliation, updates) = ReconciliationDriver::new(
        reconciler,
        UpdatePolicy {
            debounce: Duration::ZERO,
            ..UpdatePolicy::default()
        },
    )?;
    let worker = tokio::spawn(reconciliation.run(|_| false));
    updates.submit(desired)?;
    let mut status = updates.subscribe_status();
    while !status.borrow_and_update().is_converged() {
        status.changed().await?;
    }
    let adapter = HttpAdapter::new(
        controller.handle(),
        HttpLimits::default(),
        |mut request| {
            request.extensions_mut().insert(Instant::now());
            Ok(request)
        },
        Ok,
    )?;
    let gateway = server(adapter.router(), stopped.clone(), &mut servers).await?;
    let source = Observations {
        samples,
        controller: controller.clone(),
        registry,
        health,
    };
    Ok(Fixture {
        stop,
        servers,
        controller,
        worker,
        updates,
        gateway,
        source,
    })
}

async fn scenario(mode: &str) -> Result<RolloutState, BoxError> {
    let Fixture {
        stop,
        mut servers,
        controller,
        worker,
        updates,
        gateway,
        source,
    } = fixture(mode).await?;
    let spec = RolloutSpec {
        stable: "stable".into(),
        canary: "canary".into(),
        stages: [5, 25, 50, 100]
            .map(|canary_weight| Stage {
                canary_weight,
                min_observation: Duration::from_millis(300),
                timeout: Duration::from_secs(5),
                conditions: vec![
                    Condition::ErrorRate {
                        group: "canary".into(),
                        maximum_basis_points: 100,
                        minimum_requests: 5,
                    },
                    Condition::P95Latency {
                        group: "canary".into(),
                        maximum: Duration::from_millis(if mode == "latency-failure" {
                            10
                        } else {
                            500
                        }),
                        minimum_requests: 5,
                    },
                    Condition::Eligible {
                        group: "canary".into(),
                        minimum: 1,
                    },
                ],
            })
            .to_vec(),
    };
    let (rollout, handle) =
        RolloutDriver::start(updates.clone(), source, spec, Duration::from_millis(100))?;
    let rollout_task = tokio::spawn(rollout.run());
    let traffic_delay = if mode == "insufficient" {
        Duration::from_millis(250)
    } else {
        Duration::from_millis(2)
    };
    let (traffic_stop, mut traffic_stopped) = watch::channel(false);
    let traffic = tokio::spawn(async move {
        let client = netplane_http::upstream::client(Duration::from_secs(1))?;
        let mut key = 0_u64;
        loop {
            tokio::select! {
                _ = traffic_stopped.changed() => break,
                () = sleep(traffic_delay) => {
                    let response = client.get(format!("{gateway}/{key}")).send().await?;
                    let _ = response.bytes().await?;
                    key += 1;
                }
            }
        }
        Ok::<_, BoxError>(())
    });
    let mut progress = handle.subscribe_status();
    let result = loop {
        let snapshot = progress.borrow_and_update().clone();
        println!(
            "{mode}: stage {} weight {:?}: {:?} {:?}",
            snapshot.current_stage + 1,
            snapshot.applied_canary_weight,
            snapshot.state,
            snapshot.blocked_reason
        );
        if snapshot.state.is_terminal() {
            break snapshot.state;
        }
        if mode == "insufficient" && snapshot.blocked_reason == Some(EvidenceReason::Timeout) {
            // Retain the 5% stage after its sample deadline expires.
            handle.cancel()?;
            break RolloutState::Blocked;
        }
        progress.changed().await?;
    };
    rollout_task.await?;
    updates.shutdown();
    worker.await?;
    traffic_stop.send_replace(true);
    traffic.await??;
    stop.send_replace(true);
    controller.shutdown(Duration::from_secs(2)).await?;
    while let Some(server) = servers.join_next().await {
        server??;
    }
    Ok(result)
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "success".into());
    if ![
        "success",
        "failure",
        "insufficient",
        "latency-success",
        "latency-failure",
    ]
    .contains(&mode.as_str())
    {
        return Err(
            "use success, failure, insufficient, latency-success or latency-failure".into(),
        );
    }
    println!("result: {:?}", scenario(&mode).await?);
    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn real_http_latency_success() -> Result<(), tower::BoxError> {
        assert_eq!(
            super::scenario("latency-success").await?,
            netplane_control::rollout::RolloutState::Completed
        );
        Ok(())
    }
    #[tokio::test]
    async fn real_http_latency_violation() -> Result<(), tower::BoxError> {
        assert_eq!(
            super::scenario("latency-failure").await?,
            netplane_control::rollout::RolloutState::Failed
        );
        Ok(())
    }
    #[tokio::test]
    async fn real_http_scenarios() -> Result<(), tower::BoxError> {
        use netplane_control::rollout::RolloutState;
        for (mode, expected) in [
            ("success", RolloutState::Completed),
            ("failure", RolloutState::Failed),
            ("insufficient", RolloutState::Blocked),
        ] {
            assert_eq!(super::scenario(mode).await?, expected);
        }
        Ok(())
    }
}
