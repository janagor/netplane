use netplane::{Capacity, GraphBuilder, GraphVersion, WeightedRouting};
use tower::BoxError;

use super::{HttpRequest, HttpResponse, backends, config::PolicyConfig, routing::select_backend};

pub fn build_graph(
    policy: &PolicyConfig,
    outer_capacity: Capacity,
    backend_capacity: Capacity,
) -> Result<GraphVersion<HttpRequest, HttpResponse>, BoxError> {
    policy.validate()?;
    let weighted = WeightedRouting::try_new([
        ("canary", policy.canary_percent),
        ("stable", 100 - policy.canary_percent),
    ])?;
    let snapshot = weighted.clone();
    let mut builder = GraphBuilder::new()
        .service("stable", backends::stable_backend(policy))
        .service("canary", backends::canary_backend(policy))
        .service("fallback", backends::fallback_backend(policy))
        .router("policy", move |request: &HttpRequest| {
            select_backend(request, &snapshot).to_owned()
        });
    for target in weighted.targets() {
        builder = builder.route("policy", target, target);
    }
    Ok(builder
        .fallback("policy", "fallback")
        .entry("policy")
        .capacity(outer_capacity)
        .backend_capacity(backend_capacity)
        .validate()?)
}
