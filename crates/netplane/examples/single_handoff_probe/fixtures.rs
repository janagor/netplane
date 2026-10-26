//! Five compared fixtures. Only `SingleHandoff` is experimental.

use crate::config::{Config, Implementation, Workload};
use netplane::{Capacity, Endpoint, GraphBuilder, GraphDraft, Handle, Netplane, RuntimeLimits};
use std::{
    borrow::Cow, convert::Infallible, error::Error, future::Future, pin::Pin, sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
use tower::{Service, ServiceBuilder, ServiceExt, service_fn};

pub type ProbeError = Box<dyn Error + Send + Sync>;
pub type CallFuture = Pin<Box<dyn Future<Output = Result<u64, ProbeError>> + Send>>;
pub type Call = Arc<dyn Fn(u64) -> CallFuture + Send + Sync>;

const fn capacity(config: &Config) -> Capacity {
    Capacity {
        max_in_flight: config.max_in_flight,
        buffer_capacity: config.buffer_capacity,
    }
}

fn service(
    workload: Workload,
    work_us: u64,
) -> impl Service<
    u64,
    Response = u64,
    Error = Infallible,
    Future = impl Future<Output = Result<u64, Infallible>> + Send,
> + Send
+ 'static {
    service_fn(move |value: u64| async move {
        if workload == Workload::Delayed {
            tokio::time::sleep(Duration::from_micros(work_us)).await;
        }
        Ok::<_, Infallible>(value.wrapping_add(1))
    })
}

fn admit(permits: &Arc<Semaphore>, request: u64, inner: &Call) -> CallFuture {
    let Ok(permit) = Arc::clone(permits).try_acquire_owned() else {
        return Box::pin(async { Err("experimental admission rejected at capacity".into()) });
    };
    let future = inner(request);
    Box::pin(async move {
        let response = future.await?;
        drop(permit);
        Ok(response)
    })
}

fn tower_bounded(config: &Config) -> Call {
    let service = ServiceBuilder::new()
        .buffer(config.buffer_capacity)
        .concurrency_limit(config.max_in_flight)
        .service(service(config.workload, config.work_us));
    let permits = Arc::new(Semaphore::new(config.max_outstanding));
    let inner: Call = Arc::new(move |request| {
        let mut service = service.clone();
        Box::pin(async move {
            let response = service.ready().await?.call(request).await?;
            Ok(response)
        })
    });
    Arc::new(move |request| admit(&permits, request, &inner))
}

fn tower_double_buffered(config: &Config) -> Call {
    let routes = config.routes;
    let workload = config.workload;
    let work_us = config.work_us;
    let buffer_capacity = config.buffer_capacity;
    let max_in_flight = config.max_in_flight;
    let backends: Arc<Vec<_>> = Arc::new(
        (0..routes)
            .map(|_| {
                ServiceBuilder::new()
                    .buffer(buffer_capacity)
                    .concurrency_limit(max_in_flight)
                    .service(service(workload, work_us))
            })
            .collect(),
    );
    let selector = service_fn(move |request: u64| {
        let mut backend = backends[usize::try_from(request).unwrap_or(0) % routes].clone();
        async move { backend.ready().await?.call(request).await }
    });
    let outer = ServiceBuilder::new()
        .buffer(buffer_capacity)
        .concurrency_limit(max_in_flight)
        .service(selector);
    let permits = Arc::new(Semaphore::new(config.max_outstanding));
    let inner: Call = Arc::new(move |request| {
        let mut outer = outer.clone();
        Box::pin(async move {
            let response = outer.ready().await?.call(request).await?;
            Ok(response)
        })
    });
    Arc::new(move |request| admit(&permits, request, &inner))
}

fn graph_builder(config: &Config) -> Result<Call, ProbeError> {
    let routes = config.routes;
    let workload = config.workload;
    let work_us = config.work_us;
    let mut builder = GraphBuilder::new()
        .router("router", move |value: &u64| {
            Cow::Owned(format!("route-{}", value % routes as u64))
        })
        .entry("router")
        .capacity(capacity(config))
        .backend_capacity(capacity(config));
    for index in 0..routes {
        let name = format!("backend-{index}");
        builder = builder
            .service(name.clone(), service(workload, work_us))
            .route("router", format!("route-{index}"), name);
    }
    let runtime = Netplane::new(
        builder.fallback("router", "backend-0").validate()?,
        RuntimeLimits {
            max_outstanding: config.max_outstanding,
        },
    )?;
    Ok(handle_call(runtime.handle()))
}

fn graph_draft(config: &Config) -> Result<Call, ProbeError> {
    // GraphDraft has no router; compare the one-handoff Netplane path itself.
    let runtime = Netplane::new(
        GraphDraft::new(service(config.workload, config.work_us))
            .capacity(capacity(config))
            .validate()?,
        RuntimeLimits {
            max_outstanding: config.max_outstanding,
        },
    )?;
    Ok(handle_call(runtime.handle()))
}

fn handle_call(handle: Handle<u64, u64>) -> Call {
    Arc::new(move |request| {
        let handle = handle.clone();
        Box::pin(async move {
            handle
                .execute(request)
                .await
                .map_err(|error| -> ProbeError { error })
        })
    })
}

/// Experimental single-handoff router over backend [`Endpoint`]s.
///
/// # Capacity model (intentionally incomplete)
/// - Backend `max_in_flight` and `buffer_capacity` are enforced by each endpoint.
/// - Global `max_outstanding` uses a non-blocking semaphore, matching the Tower
///   fixtures in this probe.
/// - Outer [`GraphBuilder`] `max_in_flight` / `buffer_capacity` are **not** enforced.
///   A production replacement must decide how to preserve those semantics without
///   reintroducing a second buffer worker. See `docs/single-handoff-prototype.md`.
fn single_handoff(config: &Config) -> Result<Call, ProbeError> {
    let routes = config.routes;
    let workload = config.workload;
    let work_us = config.work_us;
    let backend_capacity = capacity(config);
    let backends: Arc<Vec<_>> = Arc::new(
        (0..routes)
            .map(|_| Endpoint::new(service(workload, work_us), backend_capacity))
            .collect::<Result<Vec<_>, _>>()?,
    );
    let permits = Arc::new(Semaphore::new(config.max_outstanding));
    let inner: Call = Arc::new(move |request| {
        let endpoint = backends[usize::try_from(request).unwrap_or(0) % routes].clone();
        Box::pin(async move {
            endpoint
                .execute(request)
                .await
                .map_err(|error| -> ProbeError { error })
        })
    });
    Ok(Arc::new(move |request| admit(&permits, request, &inner)))
}

pub fn build(config: &Config) -> Result<Call, ProbeError> {
    match config.implementation {
        Implementation::TowerBounded => Ok(tower_bounded(config)),
        Implementation::TowerDoubleBuffered => Ok(tower_double_buffered(config)),
        Implementation::GraphBuilder => graph_builder(config),
        Implementation::SingleHandoff => single_handoff(config),
        Implementation::GraphDraft => graph_draft(config),
    }
}
