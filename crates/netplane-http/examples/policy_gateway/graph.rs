use netplane::{Capacity, GraphBuilder, GraphVersion};
use tower::BoxError;

use super::{HttpRequest, HttpResponse, backends, config::PolicyConfig, routing::select_backend};

pub fn build_graph(
    policy: &PolicyConfig,
    outer_capacity: Capacity,
    backend_capacity: Capacity,
) -> Result<GraphVersion<HttpRequest, HttpResponse>, BoxError> {
    policy.validate()?;
    let snapshot = policy.clone();
    Ok(GraphBuilder::new()
        .service("stable", backends::stable_backend(policy))
        .service("canary", backends::canary_backend(policy))
        .service("fallback", backends::fallback_backend(policy))
        .router("policy", move |request: &HttpRequest| {
            select_backend(request, &snapshot)
        })
        .route("policy", "stable", "stable")
        .route("policy", "canary", "canary")
        .fallback("policy", "fallback")
        .entry("policy")
        .capacity(outer_capacity)
        .backend_capacity(backend_capacity)
        .validate()?)
}
