use crate::config::{Config, route};
use netplane::{
    Capacity, ControllerOptions, GraphBuilder, GraphVersion, Netplane, ReconfigurationController,
    RuntimeLimits,
};
use std::{
    borrow::Cow, collections::BTreeMap, convert::Infallible, error::Error, future::Future,
    sync::Arc, time::Duration,
};
use tower::{Service, ServiceBuilder, ServiceExt, service_fn};

pub type ProbeError = Box<dyn Error + Send + Sync>;
pub type Response = (usize, u64);

#[derive(Clone, Copy)]
pub struct Request {
    pub backend: usize,
}

fn service(
    backend: usize,
    slow_ms: u64,
    generation: u64,
) -> impl Service<
    Request,
    Response = Response,
    Error = Infallible,
    Future = impl Future<Output = Result<Response, Infallible>> + Send,
> + Send
+ 'static {
    service_fn(move |_: Request| async move {
        if backend == 1 && slow_ms > 0 {
            tokio::time::sleep(Duration::from_millis(slow_ms)).await;
        }
        Ok::<_, Infallible>((backend, generation))
    })
}

pub fn request(sequence: u64, config: &Config) -> Request {
    Request {
        backend: route(sequence, config.seed, config.scenario),
    }
}

pub fn graph(
    config: &Config,
    generation: u64,
) -> Result<GraphVersion<Request, Response>, ProbeError> {
    let outer = Capacity {
        max_in_flight: config.outer_in_flight,
        buffer_capacity: config.outer_buffer,
    };
    let backend = Capacity {
        max_in_flight: config.backend_in_flight,
        buffer_capacity: config.backend_buffer,
    };
    let mut builder = GraphBuilder::new()
        .router("router", |request: &Request| {
            Cow::Owned(format!("backend-{}", request.backend))
        })
        .entry("router")
        .capacity(outer)
        .backend_capacity(backend);
    for index in 0..config.scenario.backends() {
        let name = format!("backend-{index}");
        let capacity = if index == 1 {
            Capacity {
                max_in_flight: config.slow_in_flight,
                buffer_capacity: config.backend_buffer,
            }
        } else {
            backend
        };
        builder = builder
            .service_with_capacity(
                name.clone(),
                service(index, config.slow_ms, generation),
                capacity,
            )
            .route("router", name.clone(), name);
    }
    Ok(builder.fallback("router", "backend-0").validate()?)
}

pub fn netplane(
    config: &Config,
) -> Result<ReconfigurationController<Request, Response>, ProbeError> {
    let runtime = Netplane::new(
        graph(config, 0)?,
        RuntimeLimits {
            max_outstanding: config.max_outstanding,
        },
    )?;
    Ok(ReconfigurationController::new(
        runtime,
        ControllerOptions {
            max_retained_versions: 0,
        },
    ))
}

// Keep the concrete Tower type local; callers receive a boxed request function.
pub fn tower(
    config: &Config,
) -> impl Fn(
    Request,
)
    -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, ProbeError>> + Send>>
+ Clone
+ Send
+ Sync
+ 'static {
    let backends: Arc<Vec<_>> = Arc::new(
        (0..config.scenario.backends())
            .map(|index| {
                let capacity = if index == 1 {
                    config.slow_in_flight
                } else {
                    config.backend_in_flight
                };
                ServiceBuilder::new()
                    .buffer(config.backend_buffer)
                    .concurrency_limit(capacity)
                    .service(service(index, config.slow_ms, 0))
            })
            .collect(),
    );
    let route_table = Arc::new(
        (0..config.scenario.backends())
            .map(|index| (format!("backend-{index}"), index))
            .collect::<BTreeMap<_, _>>(),
    );
    let router = service_fn(move |request: Request| {
        let backends = Arc::clone(&backends);
        let route_table = Arc::clone(&route_table);
        async move {
            let label = format!("backend-{}", request.backend);
            let selected = route_table.get(&label).copied().unwrap_or(0);
            let mut backend = backends[selected].clone();
            backend.ready().await?.call(request).await
        }
    });
    let outer = ServiceBuilder::new()
        .buffer(config.outer_buffer)
        .concurrency_limit(config.outer_in_flight)
        .service(router);
    move |request| {
        let mut outer = outer.clone();
        Box::pin(async move {
            let response = outer.ready().await?.call(request).await?;
            Ok(response)
        })
    }
}
