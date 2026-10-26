#![expect(
    clippy::unwrap_used,
    reason = "Invalid benchmark fixtures must fail immediately"
)]

use std::{
    borrow::Cow,
    collections::BTreeMap,
    convert::Infallible,
    future::{Ready, ready},
    sync::Arc,
};

use netplane::{
    Capacity, Endpoint, GraphBuilder, GraphDraft, GraphVersion, Netplane, RuntimeLimits,
};
use tower::{Service, ServiceBuilder, ServiceExt, service_fn};

/// Default endpoint capacity for the zero-delay fixtures.
pub const CAPACITY: Capacity = Capacity {
    max_in_flight: 64,
    buffer_capacity: 128,
};

/// Static route labels shared by routing fixtures and lookup benches.
pub const ROUTE_LABELS: [&str; 64] = [
    "route-0", "route-1", "route-2", "route-3", "route-4", "route-5", "route-6", "route-7",
    "route-8", "route-9", "route-10", "route-11", "route-12", "route-13", "route-14", "route-15",
    "route-16", "route-17", "route-18", "route-19", "route-20", "route-21", "route-22", "route-23",
    "route-24", "route-25", "route-26", "route-27", "route-28", "route-29", "route-30", "route-31",
    "route-32", "route-33", "route-34", "route-35", "route-36", "route-37", "route-38", "route-39",
    "route-40", "route-41", "route-42", "route-43", "route-44", "route-45", "route-46", "route-47",
    "route-48", "route-49", "route-50", "route-51", "route-52", "route-53", "route-54", "route-55",
    "route-56", "route-57", "route-58", "route-59", "route-60", "route-61", "route-62", "route-63",
];

/// Construct the four-worker runtime outside timed iterations.
///
/// # Panics
/// Panics when Tokio cannot start the runtime.
#[must_use]
pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

/// Construct the immediate integer Tower service.
#[must_use]
pub fn service()
-> impl tower::Service<u64, Response = u64, Error = Infallible, Future = Ready<Result<u64, Infallible>>>
+ Send
+ 'static {
    service_fn(|value: u64| ready(Ok::<_, Infallible>(value.wrapping_add(1))))
}

/// Construct a service that yields once before returning.
#[must_use]
pub fn yielding_service() -> impl tower::Service<
    u64,
    Response = u64,
    Error = Infallible,
    Future = impl std::future::Future<Output = Result<u64, Infallible>> + Send,
> + Send
+ 'static {
    service_fn(|value: u64| async move {
        tokio::task::yield_now().await;
        Ok::<_, Infallible>(value.wrapping_add(1))
    })
}

/// Validate a single-root graph fixture.
///
/// # Panics
/// Panics if fixture capacity validation fails.
#[must_use]
pub fn draft() -> GraphVersion<u64, u64> {
    GraphDraft::new(service())
        .capacity(CAPACITY)
        .validate()
        .unwrap()
}

/// Construct a runtime with the requested positive admission budget.
///
/// # Panics
/// Panics if the budget is zero or fixture validation fails.
#[must_use]
pub fn netplane(max_outstanding: usize) -> Netplane<u64, u64> {
    Netplane::new(draft(), RuntimeLimits { max_outstanding }).unwrap()
}

/// Select a borrowed route label for the declared-route fixtures.
#[must_use]
pub fn borrowed_route_label(value: u64, routes: usize) -> Cow<'static, str> {
    if value == u64::MAX {
        return Cow::Borrowed("fallback");
    }
    Cow::Borrowed(ROUTE_LABELS[usize::try_from(value).unwrap_or(0) % routes])
}

/// Validate one router with between one and 64 declared routes.
///
/// # Panics
/// Panics if route count or fixture validation is invalid.
#[must_use]
pub fn routed(routes: usize, dynamic: bool) -> GraphVersion<u64, u64> {
    let mut builder = GraphBuilder::new()
        .router("router", move |value: &u64| {
            if dynamic {
                if *value == u64::MAX {
                    return Cow::Borrowed("fallback");
                }
                Cow::Owned(format!("route-{}", value % routes as u64))
            } else {
                borrowed_route_label(*value, routes)
            }
        })
        .entry("router")
        .capacity(CAPACITY)
        .backend_capacity(CAPACITY);
    for index in 0..routes {
        let name = format!("backend-{index}");
        builder = builder.service(name.clone(), service()).route(
            "router",
            format!("route-{index}"),
            name,
        );
    }
    builder.fallback("router", "backend-0").validate().unwrap()
}

/// Build a manually composed Tower router with the same two buffer handoffs.
///
/// Each backend is independently buffered and concurrency-limited, then an outer
/// buffer wraps a selector that clones the chosen backend and dispatches through
/// it. This mirrors [`GraphBuilder`] worker topology without Netplane admission.
///
/// # Panics
/// Panics if `routes` is zero.
#[must_use]
pub fn tower_double_buffered_router(
    routes: usize,
) -> impl Service<u64, Response = u64, Error = tower::BoxError> + Clone + Send + 'static {
    assert!(routes > 0);
    let backends: Arc<Vec<_>> = Arc::new(
        (0..routes)
            .map(|_| {
                ServiceBuilder::new()
                    .buffer(CAPACITY.buffer_capacity)
                    .concurrency_limit(CAPACITY.max_in_flight)
                    .service(service())
            })
            .collect(),
    );
    let selector = service_fn(move |request: u64| {
        let mut backend = backends[usize::try_from(request).unwrap_or(0) % routes].clone();
        async move { backend.ready().await?.call(request).await }
    });
    ServiceBuilder::new()
        .buffer(CAPACITY.buffer_capacity)
        .concurrency_limit(CAPACITY.max_in_flight)
        .service(selector)
}

/// Build a Tower router with one buffer handoff per request.
///
/// Backends keep independent buffer and concurrency limits. The selector runs
/// in the caller task and submits directly to the chosen backend, so there is
/// no outer buffer worker. Outer `max_in_flight` / `buffer_capacity` semantics
/// from [`GraphBuilder`] are intentionally not reproduced here.
///
/// # Panics
/// Panics if `routes` is zero.
#[must_use]
pub fn tower_single_buffered_router(
    routes: usize,
) -> impl Service<u64, Response = u64, Error = tower::BoxError> + Clone + Send + 'static {
    assert!(routes > 0);
    let backends: Arc<Vec<_>> = Arc::new(
        (0..routes)
            .map(|_| {
                ServiceBuilder::new()
                    .buffer(CAPACITY.buffer_capacity)
                    .concurrency_limit(CAPACITY.max_in_flight)
                    .service(service())
            })
            .collect(),
    );
    service_fn(move |request: u64| {
        let mut backend = backends[usize::try_from(request).unwrap_or(0) % routes].clone();
        async move { backend.ready().await?.call(request).await }
    })
}

/// Experimental Netplane-endpoint single-handoff router.
///
/// Reuses backend [`Endpoint`]s and routes directly into
/// [`Endpoint::execute`](netplane::Endpoint::execute). There is no outer
/// `Endpoint::new` and therefore no second Tower buffer worker.
///
/// Outer capacity is **not** equivalent to production [`GraphBuilder`]: this
/// fixture only preserves backend bounds. Callers that need a stand-in for the
/// outer `max_in_flight` limit should apply their own semaphore outside the
/// timed path, and must not treat that as Tower buffer queue semantics.
///
/// # Panics
/// Panics if `routes` is zero or endpoint construction fails.
#[must_use]
pub fn single_handoff_endpoints(routes: usize) -> SingleHandoffRouter {
    assert!(routes > 0);
    let backends: Vec<_> = (0..routes)
        .map(|_| Endpoint::new(service(), CAPACITY).unwrap())
        .collect();
    SingleHandoffRouter {
        backends: Arc::new(backends),
    }
}

/// Cloneable router over independently bounded backend endpoints.
#[derive(Clone)]
pub struct SingleHandoffRouter {
    backends: Arc<Vec<Endpoint<u64, u64>>>,
}

impl SingleHandoffRouter {
    /// Select a backend and submit through its single buffer worker.
    ///
    /// # Errors
    /// Returns a service or buffer worker error from the selected endpoint.
    pub fn execute(
        &self,
        request: u64,
    ) -> impl std::future::Future<Output = Result<u64, tower::BoxError>> + Send + 'static {
        let routes = self.backends.len();
        let endpoint = self.backends[usize::try_from(request).unwrap_or(0) % routes].clone();
        endpoint.execute(request)
    }
}

/// Build a label-to-index map matching the `GraphBuilder` route table shape.
#[must_use]
pub fn route_table(routes: usize) -> BTreeMap<String, usize> {
    (0..routes)
        .map(|index| (format!("route-{index}"), index))
        .collect()
}

/// Warm one execution handle with validated responses.
///
/// # Panics
/// Panics if a fixture request fails or returns the wrong response.
pub async fn warm(handle: &netplane::Handle<u64, u64>) {
    for value in 0..100 {
        assert_eq!(handle.execute(value).await.unwrap(), value + 1);
    }
}
