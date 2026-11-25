use super::observations::{Sample, SharedSamples};
use bytes::Bytes;
use http::{Request, Response};
use netplane::{
    Capacity, GraphBuilder, GraphVersion,
    health::HealthHandle,
    routing::{HealthAwareRouting, WeightedRouting},
};
use netplane_control::{
    GraphPreparer,
    rollout::{GroupPolicy, TrafficConfiguration},
};
use netplane_http::upstream::{self, HttpUpstream};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;
use tower::{BoxError, service_fn};

#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    pub canary_weight: u8,
}
impl TrafficConfiguration for Config {
    fn group_policy(&self, group: &str) -> Option<GroupPolicy> {
        Some(GroupPolicy {
            weight: match group {
                "stable" => 100 - self.canary_weight,
                "canary" => self.canary_weight,
                _ => return None,
            },
            membership_revision: 1,
        })
    }
    fn set_weights(&mut self, _: &str, _: &str, weight: u8) {
        self.canary_weight = weight;
    }
}
pub struct Preparer {
    pub origins: [String; 2],
    pub health: [HealthHandle; 2],
    pub samples: SharedSamples,
    pub next_graph: AtomicU64,
}
impl GraphPreparer<Config, Request<Bytes>, Response<Bytes>> for Preparer {
    type Error = BoxError;
    fn validate(&self, desired: &Config) -> Result<(), BoxError> {
        WeightedRouting::try_new([
            ("stable", 100 - desired.canary_weight),
            ("canary", desired.canary_weight),
        ])?;
        Ok(())
    }
    async fn prepare(
        &self,
        desired: Config,
    ) -> Result<GraphVersion<Request<Bytes>, Response<Bytes>>, BoxError> {
        let name = format!(
            "rollout-{}",
            self.next_graph.fetch_add(1, Ordering::Relaxed)
        );
        let policy = HealthAwareRouting::try_new(
            WeightedRouting::try_new([
                ("stable", 100 - desired.canary_weight),
                ("canary", desired.canary_weight),
            ])?,
            ["stable", "canary"]
                .into_iter()
                .zip(self.health.clone())
                .map(|(group, health)| (group.into(), health)),
        )?;
        let mut builder = GraphBuilder::new()
            .try_router(name.clone(), move |request: &Request<Bytes>| {
                policy
                    .select(request.uri().path().as_bytes())
                    .map(str::to_owned)
            })
            .entry(name.clone())
            .fallback(name.clone(), "stable")
            .without_outer_capacity()
            .backend_capacity(Capacity {
                max_in_flight: 8,
                buffer_capacity: 16,
            });
        for (group, origin) in ["stable", "canary"].into_iter().zip(&self.origins) {
            let upstream =
                HttpUpstream::new(upstream::client(Duration::from_secs(1))?, origin, 1024)?;
            let samples = Arc::clone(&self.samples);
            let graph = name.clone();
            builder = builder
                .service(
                    group,
                    service_fn(move |request: Request<Bytes>| {
                        let upstream = upstream.clone();
                        let samples = samples.clone();
                        let graph = graph.clone();
                        async move {
                            let accepted = request
                                .extensions()
                                .get::<Instant>()
                                .copied()
                                .unwrap_or_else(Instant::now);
                            let response = upstream.execute(request).await;
                            let failed = response
                                .as_ref()
                                .map_or(true, |response| response.status().is_server_error());
                            samples
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .record(Sample {
                                    graph,
                                    group: group.into(),
                                    accepted,
                                    completed: Instant::now(),
                                    failed,
                                });
                            response
                        }
                    }),
                )
                .route(name.clone(), group, group);
        }
        Ok(builder.validate()?)
    }
}
