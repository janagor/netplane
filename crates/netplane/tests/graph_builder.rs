//! Public graph-builder validation and execution tests.

use std::{
    convert::Infallible,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use netplane::{
    Capacity, ConfigError, GraphBuildError, GraphBuilder, GraphDraft, Netplane, NodeKind,
    RuntimeLimits,
};
use tower::{
    BoxError, ServiceBuilder, ServiceExt, filter::FilterLayer, service_fn, util::BoxService,
};

type Request = u32;
type Response = String;

fn backend(name: &'static str) -> BoxService<Request, Response, BoxError> {
    BoxService::new(
        service_fn(move |request| async move { Ok::<_, Infallible>(format!("{name}:{request}")) })
            .map_err(Into::into),
    )
}

fn builder() -> GraphBuilder<Request, Response> {
    GraphBuilder::new()
        .service("a", backend("a"))
        .service("b", backend("b"))
        .router("dispatch", |request: &Request| {
            if request.is_multiple_of(2) {
                "even"
            } else {
                "unknown"
            }
        })
        .route("dispatch", "even", "a")
        .fallback("dispatch", "b")
        .entry("dispatch")
}

#[tokio::test]
async fn routes_and_metadata_are_deterministic() -> Result<(), BoxError> {
    let graph = builder().validate()?;
    let metadata = graph.metadata();
    assert_eq!(metadata.entry, "dispatch");
    assert_eq!(
        metadata
            .nodes
            .iter()
            .map(|node| (node.name.as_str(), node.kind))
            .collect::<Vec<_>>(),
        vec![
            ("a", NodeKind::Service),
            ("b", NodeKind::Service),
            ("dispatch", NodeKind::Router)
        ]
    );
    assert_eq!(metadata.routes[0].label, "even");
    assert_eq!(metadata.fallback.as_deref(), Some("b"));
    let runtime = Netplane::new(graph, RuntimeLimits { max_outstanding: 2 })?;
    assert_eq!(runtime.handle().execute(2).await?, "a:2");
    assert_eq!(runtime.handle().execute(3).await?, "b:3");
    Ok(())
}

#[tokio::test]
async fn rejects_invalid_declarations_before_publication() -> Result<(), BoxError> {
    assert!(matches!(
        GraphBuilder::<Request, Response>::new().validate(),
        Err(GraphBuildError::MissingEntry)
    ));
    assert!(matches!(
        builder().service("a", backend("again")).validate(),
        Err(GraphBuildError::DuplicateNode(_))
    ));
    assert!(matches!(
        builder().entry("missing").validate(),
        Err(GraphBuildError::UnknownEntry(_))
    ));
    assert!(matches!(
        GraphBuilder::<Request, Response>::new()
            .service("a", backend("a"))
            .router("dispatch", |_| "a")
            .route("dispatch", "a", "a")
            .entry("dispatch")
            .validate(),
        Err(GraphBuildError::MissingFallback)
    ));
    assert!(matches!(
        builder().route("dispatch", "other", "missing").validate(),
        Err(GraphBuildError::UnknownTarget(_))
    ));
    assert!(matches!(
        GraphBuilder::<Request, Response>::new()
            .service("a", backend("a"))
            .router("dispatch", |_| "a")
            .route("dispatch", "a", "a")
            .fallback("dispatch", "missing")
            .entry("dispatch")
            .validate(),
        Err(GraphBuildError::UnknownTarget(_))
    ));
    assert!(matches!(
        builder().route("dispatch", "even", "b").validate(),
        Err(GraphBuildError::DuplicateRoute(_))
    ));
    assert!(matches!(
        builder().service("unused", backend("unused")).validate(),
        Err(GraphBuildError::UnsupportedTopology(_))
    ));
    assert!(matches!(
        builder().router("nested", |_| "x").validate(),
        Err(GraphBuildError::UnsupportedTopology(_))
    ));
    assert!(matches!(
        builder().router("dispatch", |_| "x").validate(),
        Err(GraphBuildError::DuplicateNode(name)) if name == "dispatch"
    ));
    assert!(matches!(
        builder().fallback("dispatch", "a").validate(),
        Err(GraphBuildError::DuplicateFallback(name)) if name == "dispatch"
    ));
    assert!(matches!(
        builder()
            .capacity(Capacity {
                max_in_flight: 0,
                buffer_capacity: 1
            })
            .validate(),
        Err(GraphBuildError::Capacity(ConfigError::ZeroConcurrency))
    ));
    assert!(matches!(
        builder()
            .backend_capacity(Capacity {
                max_in_flight: 1,
                buffer_capacity: 0
            })
            .validate(),
        Err(GraphBuildError::Capacity(ConfigError::ZeroBuffer))
    ));
    let old = GraphDraft::new(backend("old")).validate()?;
    assert_eq!(old.metadata().entry, "root");
    let mut runtime = Netplane::new(old, RuntimeLimits { max_outstanding: 2 })?;
    assert!(
        builder()
            .route("dispatch", "bad", "missing")
            .validate()
            .is_err()
    );
    assert_eq!(runtime.handle().execute(7).await?, "old:7");
    runtime.publish(builder().validate()?)?;
    assert_eq!(runtime.handle().execute(7).await?, "b:7");
    Ok(())
}

#[tokio::test]
async fn middleware_error_propagates() -> Result<(), BoxError> {
    let guarded = ServiceBuilder::new()
        .layer(FilterLayer::new(|request: Request| {
            (request != 2)
                .then_some(request)
                .ok_or_else(|| io::Error::other("rejected"))
        }))
        .service(backend("a"));
    let graph = GraphBuilder::new()
        .service("a", guarded)
        .router("dispatch", |_| "a")
        .route("dispatch", "a", "a")
        .fallback("dispatch", "a")
        .entry("dispatch")
        .validate()?;
    let runtime = Netplane::new(graph, RuntimeLimits { max_outstanding: 1 })?;
    assert_eq!(runtime.handle().execute(1).await?, "a:1");
    assert!(
        matches!(runtime.handle().execute(2).await, Err(error) if error.to_string() == "rejected")
    );
    Ok(())
}

#[tokio::test]
async fn owned_dynamic_selector_labels_are_supported() -> Result<(), BoxError> {
    let graph = GraphBuilder::new()
        .service("a", backend("a"))
        .service("b", backend("b"))
        .router("dispatch", |request: &Request| format!("key-{request}"))
        .route("dispatch", "key-7", "a")
        .fallback("dispatch", "b")
        .entry("dispatch")
        .validate()?;
    let runtime = Netplane::new(graph, RuntimeLimits { max_outstanding: 2 })?;
    assert_eq!(runtime.handle().execute(7).await?, "a:7");
    assert_eq!(runtime.handle().execute(8).await?, "b:8");
    Ok(())
}

#[tokio::test]
async fn backend_failure_does_not_reroute_and_releases_admission() -> Result<(), BoxError> {
    let b_calls = Arc::new(AtomicUsize::new(0));
    let observed = b_calls.clone();
    let graph = GraphBuilder::new()
        .service(
            "a",
            service_fn(|_: Request| async {
                Err::<Response, _>(io::Error::other("backend A failed"))
            }),
        )
        .service(
            "b",
            service_fn(move |request: Request| {
                observed.fetch_add(1, Ordering::SeqCst);
                async move { Ok::<_, Infallible>(format!("b:{request}")) }
            }),
        )
        .router("dispatch", |request: &Request| {
            if request.is_multiple_of(2) { "a" } else { "b" }
        })
        .route("dispatch", "a", "a")
        .route("dispatch", "b", "b")
        .fallback("dispatch", "b")
        .entry("dispatch")
        .validate()?;
    let runtime = Netplane::new(graph, RuntimeLimits { max_outstanding: 1 })?;
    assert!(matches!(runtime.handle().execute(2).await,
        Err(error) if error.downcast_ref::<io::Error>().is_some_and(|error| error.to_string() == "backend A failed")));
    assert_eq!(b_calls.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.handle().execute(3).await?, "b:3");
    assert_eq!(b_calls.load(Ordering::SeqCst), 1);
    Ok(())
}
