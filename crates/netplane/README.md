# netplane

Netplane currently provides a single-version, bounded Tower service endpoint.
Graph publication and version retention are planned follow-up work.

Build a Tower composition with `ServiceBuilder` and `Layer`, then pass it to
`Endpoint::new` inside a Tokio runtime. Each endpoint has its own concurrency
limit and bounded buffer. `execute` returns a future that waits for capacity
and for the service result. The buffer worker polls readiness on the same
service instance it calls.

```rust,no_run
use netplane::{Capacity, Endpoint};
use tower::{ServiceBuilder, service_fn};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let service = ServiceBuilder::new().service(service_fn(|value: usize| async move {
    Ok::<_, std::convert::Infallible>(value + 1)
}));
let endpoint = Endpoint::new(service, Capacity {
    max_in_flight: 32,
    buffer_capacity: 64,
})?;
let result = endpoint.execute(1).await?;
assert_eq!(result, 2);
# Ok(())
# }
```

The endpoint must be constructed while a Tokio runtime is active. Capacity
limits apply to this endpoint alone. Dropping an execution future cancels its
wait; cancellation of work already dispatched depends on the underlying
service. Shutdown admission control is not implemented yet.
